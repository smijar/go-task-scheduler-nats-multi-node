package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// TaskStatus represents the current state of a task
type TaskStatus string

const (
	StatusQueued    TaskStatus = "queued"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
	StatusCancelled TaskStatus = "cancelled"
)

// Task represents a unit of work to be processed
type Task struct {
	ID         string      `json:"id"`
	Type       string      `json:"type"`
	Parameters interface{} `json:"parameters"`
	Status     TaskStatus  `json:"status"`
	CreatedAt  time.Time   `json:"created_at"`
	StartedAt  *time.Time  `json:"started_at,omitempty"`
	FinishedAt *time.Time  `json:"finished_at,omitempty"`
	Error      string      `json:"error,omitempty"`
}

// TaskResult represents the completion status of a task
type TaskResult struct {
	TaskID     string     `json:"taskId"`
	Status     TaskStatus `json:"status"`
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt time.Time  `json:"finishedAt,omitempty"`
}

// Worker represents a task processing worker
type Worker struct {
	id             int
	natsConn       *nats.Conn
	taskSubject    string
	respSubject    string
	cancelSubject  string
	queueGroup     string
	cancelTasks    sync.Map
	stopChan       chan struct{}
	isRunning      bool
	activeTaskChan chan struct{}
	maxConcurrent  int
}

// NewWorker creates a new task processing worker
func NewWorker(
	id int,
	natsConn *nats.Conn,
	taskSubject string,
	respSubject string,
	queueGroup string,
	maxConcurrent int,
) *Worker {
	return &Worker{
		id:             id,
		natsConn:       natsConn,
		taskSubject:    taskSubject,
		respSubject:    respSubject,
		cancelSubject:  taskSubject + ".cancel",
		queueGroup:     queueGroup,
		cancelTasks:    sync.Map{},
		stopChan:       make(chan struct{}),
		isRunning:      false,
		activeTaskChan: make(chan struct{}, maxConcurrent),
		maxConcurrent:  maxConcurrent,
	}
}

// Start begins task processing
func (w *Worker) Start() error {
	if w.isRunning {
		return fmt.Errorf("worker #%d already running", w.id)
	}

	log.Printf("Worker #%d starting with max concurrency %d...", w.id, w.maxConcurrent)

	// Fill the channel with tokens representing available task slots
	for i := 0; i < w.maxConcurrent; i++ {
		w.activeTaskChan <- struct{}{}
	}

	// Subscribe to task queue
	taskSub, err := w.natsConn.QueueSubscribe(
		w.taskSubject,
		w.queueGroup,
		func(msg *nats.Msg) {
			// Try to acquire a concurrency token
			select {
			case <-w.activeTaskChan:
				// Got token, process the task
				go func() {
					w.processTask(msg)
					// Return the token when done
					w.activeTaskChan <- struct{}{}
				}()
			default:
				// No token available, at max concurrency
				log.Printf("Worker #%d: At max concurrency, ignoring task", w.id)
				return
			}
		},
	)
	if err != nil {
		return fmt.Errorf("worker #%d failed to subscribe to task subject: %w", w.id, err)
	}

	// Subscribe to cancellation requests
	cancelSub, err := w.natsConn.Subscribe(
		w.cancelSubject,
		func(msg *nats.Msg) {
			w.handleCancellation(msg)
		},
	)
	if err != nil {
		taskSub.Unsubscribe()
		return fmt.Errorf("worker #%d failed to subscribe to cancel subject: %w", w.id, err)
	}

	log.Printf("Worker #%d subscribed to task subject '%s' with queue group '%s'",
		w.id, w.taskSubject, w.queueGroup)
	log.Printf("Worker #%d subscribed to cancel subject '%s'", w.id, w.cancelSubject)

	w.isRunning = true

	// Wait for stop signal
	<-w.stopChan

	// Clean up subscriptions
	taskSub.Unsubscribe()
	cancelSub.Unsubscribe()

	log.Printf("Worker #%d stopped", w.id)
	w.isRunning = false
	return nil
}

// Stop signals the worker to stop processing
func (w *Worker) Stop() {
	if w.isRunning {
		close(w.stopChan)
	}
}

// processTask handles task execution
func (w *Worker) processTask(msg *nats.Msg) {
	// Decode message
	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		log.Printf("Worker #%d: Failed to unmarshal task: %v", w.id, err)
		return
	}

	log.Printf("Worker #%d: Processing task %s (Type: %s)", w.id, task.ID, task.Type)

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	w.cancelTasks.Store(task.ID, cancel)
	defer w.cancelTasks.Delete(task.ID)

	// Create result with started status
	startTime := time.Now().UTC()
	result := TaskResult{
		TaskID:    task.ID,
		StartedAt: startTime,
		Status:    StatusRunning,
	}

	// Send started notification
	w.sendResult(result)

	// Execute task
	taskErr := w.executeTaskLogic(ctx, task.Type, task.Parameters)

	// Update result with completion info
	finishTime := time.Now().UTC()
	result.FinishedAt = finishTime

	if taskErr != nil {
		if ctx.Err() != nil {
			result.Status = StatusCancelled
			result.Error = "Task was cancelled"
		} else {
			result.Status = StatusFailed
			result.Error = taskErr.Error()
		}
		log.Printf("Worker #%d: Task %s failed: %v", w.id, task.ID, taskErr)
	} else {
		result.Status = StatusCompleted
		log.Printf("Worker #%d: Task %s completed successfully", w.id, task.ID)
	}

	// Send completion notification
	w.sendResult(result)
}

// handleCancellation processes task cancellation requests
func (w *Worker) handleCancellation(msg *nats.Msg) {
	var cancelReq struct {
		ID     string `json:"id"`
		Action string `json:"action"`
	}

	if err := json.Unmarshal(msg.Data, &cancelReq); err != nil {
		log.Printf("Worker #%d: Failed to unmarshal cancel request: %v", w.id, err)
		return
	}

	if cancelReq.Action != "cancel" || cancelReq.ID == "" {
		return
	}

	// Check if we're handling this task
	if cancelFunc, exists := w.cancelTasks.Load(cancelReq.ID); exists {
		log.Printf("Worker #%d: Cancelling task %s", w.id, cancelReq.ID)
		cancel := cancelFunc.(context.CancelFunc)
		cancel()
	}
}

// sendResult publishes task results to the response subject
func (w *Worker) sendResult(result TaskResult) {
	payload, err := json.Marshal(result)
	if err != nil {
		log.Printf("Worker #%d: Failed to marshal result for task %s: %v",
			w.id, result.TaskID, err)
		return
	}

	if err := w.natsConn.Publish(w.respSubject, payload); err != nil {
		log.Printf("Worker #%d: Failed to publish result for task %s: %v",
			w.id, result.TaskID, err)
	}
}

// executeTaskLogic simulates performing the actual work
func (w *Worker) executeTaskLogic(ctx context.Context, taskType string, params interface{}) error {
	log.Printf("Worker #%d: Executing task of type '%s'", w.id, taskType)

	// Example task implementations - replace with real business logic
	switch taskType {
	case "long_computation":
		return w.doLongComputation(ctx)

	case "quick_job":
		return w.doQuickJob(ctx)

	case "failing_job":
		return w.doFailingJob(ctx)

	default:
		return fmt.Errorf("unknown task type: %s", taskType)
	}
}

// doLongComputation simulates a long-running task
func (w *Worker) doLongComputation(ctx context.Context) error {
	select {
	case <-time.After(15 * time.Second):
		return nil // Success
	case <-ctx.Done():
		return ctx.Err() // Cancelled
	}
}

// doQuickJob simulates a quick task
func (w *Worker) doQuickJob(ctx context.Context) error {
	select {
	case <-time.After(1 * time.Second):
		return nil // Success
	case <-ctx.Done():
		return ctx.Err() // Cancelled
	}
}

// doFailingJob simulates a failing task
func (w *Worker) doFailingJob(ctx context.Context) error {
	select {
	case <-time.After(2 * time.Second):
		return fmt.Errorf("simulated error for failing_job") // Failure
	case <-ctx.Done():
		return ctx.Err() // Cancelled
	}
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// Main entry point for the worker process
func main() {
	log.Println("Starting Task Worker...")

	// --- Configuration ---
	natsURL := getEnv("NATS_URL", "nats://localhost:4222")
	natsTaskSubject := getEnv("NATS_TASK_SUBJECT", "tasks.process")
	natsRespSubject := getEnv("NATS_RESP_SUBJECT", "tasks.result")
	natsQueueGroup := getEnv("NATS_QUEUE_GROUP", "task-workers")

	workerIDStr := getEnv("WORKER_ID", "1")
	workerID, err := strconv.Atoi(workerIDStr)
	if err != nil {
		workerID = 1
	}

	maxConcurrentStr := getEnv("MAX_CONCURRENT_TASKS", "5")
	maxConcurrent, err := strconv.Atoi(maxConcurrentStr)
	if err != nil || maxConcurrent < 1 {
		maxConcurrent = 5
	}

	// --- NATS Connection ---
	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(5),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Println("NATS connection closed.")
		}),
	)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	// --- Create Worker ---
	worker := NewWorker(
		workerID,
		nc,
		natsTaskSubject,
		natsRespSubject,
		natsQueueGroup,
		maxConcurrent,
	)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start worker in a goroutine
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- worker.Start()
	}()

	// Wait for signal or worker exit
	select {
	case sig := <-sigChan:
		log.Printf("Received signal: %s. Shutting down worker...", sig.String())
		worker.Stop()
		<-workerDone
	case err := <-workerDone:
		if err != nil {
			log.Printf("Worker stopped with error: %v", err)
		}
	}

	log.Println("Worker shutdown complete.")
}
