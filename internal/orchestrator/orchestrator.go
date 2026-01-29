package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alekspetrov/pilot/internal/adapters/github"
	"github.com/alekspetrov/pilot/internal/adapters/jira"
	"github.com/alekspetrov/pilot/internal/adapters/linear"
	"github.com/alekspetrov/pilot/internal/adapters/slack"
	"github.com/alekspetrov/pilot/internal/executor"
	"github.com/alekspetrov/pilot/internal/logging"
)

// Config holds orchestrator configuration
type Config struct {
	Model         string
	MaxConcurrent int
}

// Orchestrator coordinates ticket processing and task execution
type Orchestrator struct {
	config   *Config
	bridge   *Bridge
	runner   *executor.Runner
	monitor  *executor.Monitor
	notifier *slack.Notifier

	taskQueue        chan *Task
	running          map[string]bool
	progressCallback func(taskID, phase string, progress int, message string)
	mu               sync.Mutex
	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
}

// Task represents a task to be processed
type Task struct {
	ID          string
	Ticket      *linear.Issue
	Document    *TaskDocument
	ProjectPath string
	Branch      string
	Priority    float64
	IssueKey    string // Original issue key (e.g., PROJ-123)
	CallbackURL string // URL to notify when task completes
}

// NewOrchestrator creates a new orchestrator
func NewOrchestrator(config *Config, notifier *slack.Notifier) (*Orchestrator, error) {
	bridge, err := NewBridge()
	if err != nil {
		return nil, fmt.Errorf("failed to create bridge: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	o := &Orchestrator{
		config:    config,
		bridge:    bridge,
		runner:    executor.NewRunner(),
		monitor:   executor.NewMonitor(),
		notifier:  notifier,
		taskQueue: make(chan *Task, 100),
		running:   make(map[string]bool),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Set up progress callback
	o.runner.OnProgress(o.handleProgress)

	return o, nil
}

// Start starts the orchestrator workers
func (o *Orchestrator) Start() {
	maxWorkers := o.config.MaxConcurrent
	if maxWorkers <= 0 {
		maxWorkers = 2
	}

	for i := 0; i < maxWorkers; i++ {
		o.wg.Add(1)
		go o.worker(i)
	}

	logging.WithComponent("orchestrator").Info("Orchestrator started", slog.Int("workers", maxWorkers))
}

// Stop stops the orchestrator
func (o *Orchestrator) Stop() {
	o.cancel()
	close(o.taskQueue)
	o.wg.Wait()
	logging.WithComponent("orchestrator").Info("Orchestrator stopped")
}

// ProcessTicket processes a new ticket from Linear
func (o *Orchestrator) ProcessTicket(ctx context.Context, issue *linear.Issue, projectPath string) error {
	// Convert ticket to task document
	ticket := &TicketData{
		ID:          issue.ID,
		Identifier:  issue.Identifier,
		Title:       issue.Title,
		Description: issue.Description,
		Priority:    issue.Priority,
		Labels:      extractLabelNames(issue.Labels),
	}

	doc, err := o.bridge.PlanTicket(ctx, ticket)
	if err != nil {
		return fmt.Errorf("failed to plan ticket: %w", err)
	}

	// Save task document
	if err := o.saveTaskDocument(projectPath, doc); err != nil {
		logging.WithComponent("orchestrator").Warn("Failed to save task document", slog.Any("error", err))
	}

	// Create task
	task := &Task{
		ID:          doc.ID,
		Ticket:      issue,
		Document:    doc,
		ProjectPath: projectPath,
		Branch:      fmt.Sprintf("pilot/%s", issue.Identifier),
	}

	// Queue task
	o.QueueTask(task)

	return nil
}

// QueueTask adds a task to the processing queue
func (o *Orchestrator) QueueTask(task *Task) {
	o.monitor.Register(task.ID, task.Document.Title)

	select {
	case o.taskQueue <- task:
		logging.WithTask(task.ID).Info("Task queued")
	default:
		logging.WithTask(task.ID).Warn("Task queue full, dropping task")
	}
}

// worker processes tasks from the queue
func (o *Orchestrator) worker(id int) {
	defer o.wg.Done()

	for task := range o.taskQueue {
		select {
		case <-o.ctx.Done():
			return
		default:
			o.processTask(task)
		}
	}
}

// processTask processes a single task
func (o *Orchestrator) processTask(task *Task) {
	o.mu.Lock()
	if o.running[task.ID] {
		o.mu.Unlock()
		return
	}
	o.running[task.ID] = true
	o.mu.Unlock()

	defer func() {
		o.mu.Lock()
		delete(o.running, task.ID)
		o.mu.Unlock()
	}()

	logging.WithTask(task.ID).Info("Processing task", slog.String("title", task.Document.Title))
	o.monitor.Start(task.ID)

	// Notify Slack
	if o.notifier != nil {
		_ = o.notifier.TaskStarted(o.ctx, task.ID, task.Document.Title)
	}

	// Execute task
	priority := task.Priority
	if priority == 0 && task.Ticket != nil {
		priority = float64(task.Ticket.Priority)
	}
	execTask := &executor.Task{
		ID:          task.ID,
		Title:       task.Document.Title,
		Description: task.Document.Markdown,
		Priority:    int(priority),
		ProjectPath: task.ProjectPath,
		Branch:      task.Branch,
		CreatePR:    true,
	}

	result, err := o.runner.Execute(o.ctx, execTask)
	if err != nil {
		logging.WithTask(task.ID).Error("Task execution error", slog.Any("error", err))
		o.monitor.Fail(task.ID, err.Error())
		if o.notifier != nil {
			_ = o.notifier.TaskFailed(o.ctx, task.ID, task.Document.Title, err.Error())
		}
		// Send callback for failure
		o.sendCallback(task, "failed", "", err.Error())
		return
	}

	if !result.Success {
		logging.WithTask(task.ID).Error("Task failed", slog.String("error", result.Error))
		o.monitor.Fail(task.ID, result.Error)
		if o.notifier != nil {
			_ = o.notifier.TaskFailed(o.ctx, task.ID, task.Document.Title, result.Error)
		}
		// Send callback for failure
		o.sendCallback(task, "failed", "", result.Error)
		return
	}

	logging.WithTask(task.ID).Info("Task completed", slog.Duration("duration", result.Duration))
	o.monitor.Complete(task.ID, result.PRUrl)

	// Notify Slack
	if o.notifier != nil {
		_ = o.notifier.TaskCompleted(o.ctx, task.ID, task.Document.Title, result.PRUrl)
	}

	// Send callback for success
	o.sendCallback(task, "completed", result.PRUrl, "")
}

// handleProgress handles progress updates from the executor
func (o *Orchestrator) handleProgress(taskID, phase string, progress int, message string) {
	o.monitor.UpdateProgress(taskID, phase, progress, message)

	// Optionally notify Slack on significant progress
	if progress > 0 && progress%25 == 0 && o.notifier != nil {
		_ = o.notifier.TaskProgress(o.ctx, taskID, phase, progress)
	}

	// Forward to external callback if registered
	o.mu.Lock()
	cb := o.progressCallback
	o.mu.Unlock()
	if cb != nil {
		cb(taskID, phase, progress, message)
	}
}

// OnProgress registers an external callback for task progress updates
func (o *Orchestrator) OnProgress(callback func(taskID, phase string, progress int, message string)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.progressCallback = callback
}

// saveTaskDocument saves a task document to the project
func (o *Orchestrator) saveTaskDocument(projectPath string, doc *TaskDocument) error {
	taskDir := filepath.Join(projectPath, ".agent", "tasks")
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		return err
	}

	filename := filepath.Join(taskDir, fmt.Sprintf("%s.md", doc.ID))
	return os.WriteFile(filename, []byte(doc.Markdown), 0644)
}

// GetTaskStates returns current task states
func (o *Orchestrator) GetTaskStates() []*executor.TaskState {
	return o.monitor.GetAll()
}

// GetRunningTasks returns currently running tasks
func (o *Orchestrator) GetRunningTasks() []*executor.TaskState {
	return o.monitor.GetRunning()
}

// SetAlertProcessor sets the alert processor on the underlying runner for task lifecycle events.
// The processor interface is satisfied by alerts.Engine.
func (o *Orchestrator) SetAlertProcessor(processor executor.AlertEventProcessor) {
	o.runner.SetAlertProcessor(processor)
}

// extractLabelNames extracts label names from Linear labels
func extractLabelNames(labels []linear.Label) []string {
	names := make([]string, len(labels))
	for i, label := range labels {
		names[i] = label.Name
	}
	return names
}

// ProcessGithubTicket processes a new ticket from GitHub Issues
func (o *Orchestrator) ProcessGithubTicket(ctx context.Context, task *github.TaskInfo, projectPath string) error {
	// Convert GitHub task to task document via bridge
	ticket := &TicketData{
		ID:          task.ID,
		Identifier:  task.ID, // GH-42 format
		Title:       task.Title,
		Description: task.Description,
		Priority:    int(task.Priority),
		Labels:      task.Labels,
	}

	doc, err := o.bridge.PlanTicket(ctx, ticket)
	if err != nil {
		return fmt.Errorf("failed to plan ticket: %w", err)
	}

	// Save task document
	if err := o.saveTaskDocument(projectPath, doc); err != nil {
		logging.WithComponent("orchestrator").Warn("Failed to save task document", slog.Any("error", err))
	}

	// Create internal task
	internalTask := &Task{
		ID:          doc.ID,
		Document:    doc,
		ProjectPath: projectPath,
		Branch:      fmt.Sprintf("pilot/%s", task.ID),
		Priority:    float64(task.Priority),
	}

	// Queue task
	o.QueueTask(internalTask)

	return nil
}

// ProcessJiraTicket processes a new ticket from Jira
func (o *Orchestrator) ProcessJiraTicket(ctx context.Context, task *jira.TaskInfo, projectPath, callbackURL string) error {
	// Convert Jira task to task document via bridge
	ticket := &TicketData{
		ID:          task.ID,
		Identifier:  task.IssueKey, // PROJ-123 format
		Title:       task.Title,
		Description: task.Description,
		Priority:    int(task.Priority),
		Labels:      task.Labels,
	}

	doc, err := o.bridge.PlanTicket(ctx, ticket)
	if err != nil {
		return fmt.Errorf("failed to plan ticket: %w", err)
	}

	// Save task document
	if err := o.saveTaskDocument(projectPath, doc); err != nil {
		logging.WithComponent("orchestrator").Warn("Failed to save task document", slog.Any("error", err))
	}

	// Create internal task
	internalTask := &Task{
		ID:          doc.ID,
		Document:    doc,
		ProjectPath: projectPath,
		Branch:      fmt.Sprintf("pilot/%s", task.IssueKey),
		Priority:    float64(task.Priority),
		IssueKey:    task.IssueKey,
		CallbackURL: callbackURL,
	}

	// Queue task
	o.QueueTask(internalTask)

	return nil
}

// sendCallback sends a completion callback to the configured URL
func (o *Orchestrator) sendCallback(task *Task, status, prURL, errMsg string) {
	if task.CallbackURL == "" {
		return
	}

	payload := map[string]interface{}{
		"task_id":   task.ID,
		"issue_key": task.IssueKey,
		"status":    status,
	}
	if prURL != "" {
		payload["pr_url"] = prURL
	}
	if errMsg != "" {
		payload["error"] = errMsg
	}

	body, err := json.Marshal(payload)
	if err != nil {
		logging.WithTask(task.ID).Warn("Failed to marshal callback payload", slog.Any("error", err))
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(o.ctx, http.MethodPost, task.CallbackURL, bytes.NewReader(body))
	if err != nil {
		logging.WithTask(task.ID).Warn("Failed to create callback request", slog.Any("error", err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		logging.WithTask(task.ID).Warn("Failed to send callback", slog.Any("error", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		logging.WithTask(task.ID).Warn("Callback returned error", slog.Int("status", resp.StatusCode))
	} else {
		logging.WithTask(task.ID).Info("Callback sent successfully", slog.String("url", task.CallbackURL))
	}
}
