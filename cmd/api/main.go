package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/time/rate"
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
	ID         string      `json:"id" bson:"_id"`
	Type       string      `json:"type" bson:"type"`
	Parameters interface{} `json:"parameters" bson:"parameters"`
	Status     TaskStatus  `json:"status" bson:"status"`
	CreatedAt  time.Time   `json:"created_at" bson:"createdAt"`
	StartedAt  *time.Time  `json:"started_at,omitempty" bson:"startedAt,omitempty"`
	FinishedAt *time.Time  `json:"finished_at,omitempty" bson:"finishedAt,omitempty"`
	Error      string      `json:"error,omitempty" bson:"error,omitempty"`
}

// TaskResult represents the completion status of a task sent by workers
type TaskResult struct {
	TaskID     string     `json:"taskId"`
	Status     TaskStatus `json:"status"`
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt time.Time  `json:"finishedAt,omitempty"`
}

// Scheduler coordinates task distribution and status tracking
type Scheduler struct {
	natsConn        *nats.Conn
	natsTaskSubject string
	natsRespSubject string
	natsQueueGrp    string
	mongoClient     *mongo.Client
	mongoTasksColl  *mongo.Collection

	cancelTasks sync.Map // For task cancellation

	mu            sync.RWMutex
	isPaused      bool
	submitLimiter *rate.Limiter
}

// NewScheduler creates a new task scheduler instance
func NewScheduler(
	natsURL, natsTaskSubject, natsRespSubject, queueGroup string,
	mongoURI, mongoDBName, mongoCollName string,
	submitRateLimit rate.Limit,
) (*Scheduler, error) {
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
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	log.Println("Successfully connected to NATS.")

	// --- MongoDB Connection ---
	ctxMongo, cancelMongo := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMongo()
	clientOpts := options.Client().ApplyURI(mongoURI)
	mongoClient, err := mongo.Connect(ctxMongo, clientOpts)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	err = mongoClient.Ping(ctxMongo, nil)
	if err != nil {
		mongoClient.Disconnect(context.Background())
		nc.Close()
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}
	log.Println("Successfully connected to MongoDB.")
	db := mongoClient.Database(mongoDBName)
	tasksColl := db.Collection(mongoCollName)

	// Create indexes if needed
	ctxIndex, cancelIndex := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelIndex()

	// Index on status
	_, err = tasksColl.Indexes().CreateOne(ctxIndex, mongo.IndexModel{
		Keys: bson.D{{"status", 1}},
	})
	if err != nil {
		log.Printf("Warning: Failed to create status index: %v", err)
	}

	// Index on createdAt
	_, err = tasksColl.Indexes().CreateOne(ctxIndex, mongo.IndexModel{
		Keys: bson.D{{"createdAt", -1}},
	})
	if err != nil {
		log.Printf("Warning: Failed to create createdAt index: %v", err)
	}

	// --- Initialize Scheduler ---
	s := &Scheduler{
		natsConn:        nc,
		natsTaskSubject: natsTaskSubject,
		natsRespSubject: natsRespSubject,
		natsQueueGrp:    queueGroup,
		mongoClient:     mongoClient,
		mongoTasksColl:  tasksColl,
		cancelTasks:     sync.Map{},
		isPaused:        false,
		submitLimiter:   rate.NewLimiter(submitRateLimit, 1),
	}

	return s, nil
}

// Close releases all scheduler resources
func (s *Scheduler) Close() {
	log.Println("Closing scheduler resources...")
	if s.natsConn != nil && !s.natsConn.IsClosed() {
		log.Println("Draining NATS connection...")
		if err := s.natsConn.Drain(); err != nil {
			log.Printf("Error draining NATS connection: %v", err)
		} else {
			log.Println("NATS connection drained and closed.")
		}
	}
	if s.mongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.mongoClient.Disconnect(ctx); err != nil {
			log.Printf("Error disconnecting from MongoDB: %v", err)
		} else {
			log.Println("MongoDB connection closed.")
		}
	}
}

// Start initializes the scheduler's components
func (s *Scheduler) Start() error {
	log.Println("Starting scheduler...")

	// Set up response listener for worker feedback
	if err := s.setupResponseListener(); err != nil {
		return fmt.Errorf("failed to set up response listener: %w", err)
	}

	log.Println("Scheduler ready to process tasks.")
	return nil
}

// setupResponseListener sets up NATS subscription for task results
func (s *Scheduler) setupResponseListener() error {
	_, err := s.natsConn.Subscribe(s.natsRespSubject, func(msg *nats.Msg) {
		var result TaskResult
		if err := json.Unmarshal(msg.Data, &result); err != nil {
			log.Printf("Failed to unmarshal task result: %v", err)
			return
		}

		log.Printf("Received result for task %s: Status=%s",
			result.TaskID, result.Status)

		// Update task in MongoDB
		switch result.Status {
		case StatusRunning:
			s.updateTaskState(result.TaskID, StatusRunning, nil, &result.StartedAt, nil)
		case StatusCompleted:
			s.updateTaskState(result.TaskID, StatusCompleted, nil, nil, &result.FinishedAt)
		case StatusFailed:
			var taskErr error
			if result.Error != "" {
				taskErr = fmt.Errorf(result.Error)
			}
			s.updateTaskState(result.TaskID, StatusFailed, taskErr, nil, &result.FinishedAt)
		}
	})

	if err != nil {
		return fmt.Errorf("failed to subscribe to response subject: %w", err)
	}

	log.Printf("Subscribed to response subject '%s'", s.natsRespSubject)
	return nil
}

// SubmitTask creates and queues a new task
func (s *Scheduler) SubmitTask(taskType string, params interface{}) (string, error) {

	// Apply rate limiting
	if s.submitLimiter != nil {
		ctxWait, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelWait()
		if err := s.submitLimiter.Wait(ctxWait); err != nil {
			return "", fmt.Errorf("task submission rate limit exceeded: %w", err)
		}
	}

	// Create task
	taskID := uuid.NewString()
	task := &Task{
		ID:         taskID,
		Type:       taskType,
		Parameters: params,
		Status:     StatusQueued,
		CreatedAt:  time.Now().UTC(),
	}

	// Store in MongoDB
	ctxMongo, cancelMongo := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMongo()
	_, err := s.mongoTasksColl.InsertOne(ctxMongo, task)
	if err != nil {
		return "", fmt.Errorf("failed to insert task into MongoDB: %w", err)
	}

	log.Printf("DEBUG: Task %s stored in MongoDB", taskID)

	// Check if scheduler is paused
	s.mu.RLock()
	paused := s.isPaused
	s.mu.RUnlock()

	// If paused, don't publish to NATS
	if paused {
		log.Printf("Scheduler is paused. Task %s stored as queued.", task.ID)
		return task.ID, nil
	}

	// Prepare task message (full task data for workers)
	msgData, err := json.Marshal(task)
	if err != nil {
		log.Printf("Failed to marshal task for NATS: %v. Task is in DB!", err)
		s.updateTaskState(task.ID, StatusFailed, fmt.Errorf("failed to marshal for NATS: %w", err), nil, nil)
		return task.ID, fmt.Errorf("failed to marshal task for NATS: %w", err)
	}

	log.Printf("DEBUG: Attempting to submit task type: %s", taskType)

	// Publish to task subject
	err = s.natsConn.Publish(s.natsTaskSubject, msgData)

	if err != nil {
		log.Printf("Failed to publish task %s to NATS: %v", task.ID, err)
		s.updateTaskState(task.ID, StatusFailed, fmt.Errorf("failed to publish to NATS: %w", err), nil, nil)
		return task.ID, fmt.Errorf("failed to publish task to NATS: %w", err)
	}

	log.Printf("DEBUG: Publishing task to NATS subject: %s", s.natsTaskSubject)

	log.Printf("Submitted task %s to subject: %s (Type: %s) to NATS", task.ID, s.natsTaskSubject, task.Type)
	return task.ID, nil
}

// updateTaskState updates task status in MongoDB
func (s *Scheduler) updateTaskState(taskID string, status TaskStatus, taskErr error, started *time.Time, finished *time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"_id": taskID}
	updateFields := bson.M{"status": status}

	if taskErr != nil {
		updateFields["error"] = taskErr.Error()
	}
	if started != nil {
		updateFields["startedAt"] = *started
	}
	if finished != nil {
		updateFields["finishedAt"] = *finished
	}

	update := bson.M{}
	if len(updateFields) > 0 {
		update["$set"] = updateFields
	}

	// Clear error field if not needed
	if taskErr == nil && (status == StatusCompleted || status == StatusRunning) {
		if update["$unset"] == nil {
			update["$unset"] = bson.M{}
		}
		update["$unset"].(bson.M)["error"] = ""
	}

	if len(update) == 0 {
		return
	}

	result, err := s.mongoTasksColl.UpdateOne(ctx, filter, update)
	if err != nil {
		log.Printf("Failed to update task state in MongoDB for %s: %v", taskID, err)
		return
	}

	if result.MatchedCount == 0 {
		log.Printf("Warning: No document matched for task ID %s update", taskID)
	}
}

// GetTaskStatus retrieves a task by ID
func (s *Scheduler) GetTaskStatus(taskID string) (*Task, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var task Task
	filter := bson.M{"_id": taskID}
	err := s.mongoTasksColl.FindOne(ctx, filter).Decode(&task)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, false, nil
		}
		log.Printf("Error fetching task %s: %v", taskID, err)
		return nil, false, fmt.Errorf("database error: %w", err)
	}

	return &task, true, nil
}

// GetAllTaskStatuses retrieves tasks with optional filtering
func (s *Scheduler) GetAllTaskStatuses(ctx context.Context, filter bson.M) ([]*Task, error) {
	reqCtx := ctx
	if reqCtx == nil {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
	}

	var tasks []*Task
	findFilter := filter
	if findFilter == nil {
		findFilter = bson.M{}
	}

	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"createdAt", -1}})

	cursor, err := s.mongoTasksColl.Find(reqCtx, findFilter, findOptions)
	if err != nil {
		log.Printf("Error finding tasks %v: %v", findFilter, err)
		return nil, fmt.Errorf("database error: %w", err)
	}
	defer cursor.Close(reqCtx)

	if err = cursor.All(reqCtx, &tasks); err != nil {
		log.Printf("Error decoding tasks: %v", err)
		return nil, fmt.Errorf("database error: %w", err)
	}

	return tasks, nil
}

// CancelTask attempts to cancel a task
func (s *Scheduler) CancelTask(taskID string) error {
	task, found, err := s.GetTaskStatus(taskID)
	if err != nil {
		return fmt.Errorf("cancel check failed: %w", err)
	}
	if !found {
		return fmt.Errorf("task %s not found", taskID)
	}

	switch task.Status {
	case StatusRunning:
		// Publish cancellation request
		cancelMsg := map[string]string{"id": taskID, "action": "cancel"}
		msgData, err := json.Marshal(cancelMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal cancel request: %w", err)
		}

		// Use a special cancel subject
		cancelSubject := s.natsTaskSubject + ".cancel"
		if err := s.natsConn.Publish(cancelSubject, msgData); err != nil {
			return fmt.Errorf("failed to publish cancel request: %w", err)
		}

		log.Printf("Published cancellation request for task %s", taskID)
		return nil

	case StatusQueued:
		// Direct cancellation for queued tasks
		now := time.Now().UTC()
		cancelErr := fmt.Errorf("cancelled by API before execution")
		s.updateTaskState(taskID, StatusCancelled, cancelErr, nil, &now)
		return nil

	default:
		return fmt.Errorf("task %s in final state (%s), cannot cancel", taskID, task.Status)
	}
}

// PauseWorkers pauses task processing
func (s *Scheduler) PauseWorkers() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isPaused {
		s.isPaused = true
		log.Println("Scheduler PAUSED.")
	} else {
		log.Println("Scheduler already paused.")
	}
}

// ResumeWorkers resumes task processing
func (s *Scheduler) ResumeWorkers() {
	s.mu.Lock()
	wasPaused := s.isPaused
	if s.isPaused {
		s.isPaused = false
	}
	s.mu.Unlock()

	if wasPaused {
		log.Println("Scheduler RESUMED.")
		// Requeue paused tasks
		s.requeuePausedTasks()
	} else {
		log.Println("Scheduler already running.")
	}
}

// requeuePausedTasks publishes queued tasks to NATS
func (s *Scheduler) requeuePausedTasks() {
	log.Println("Requeuing paused tasks...")

	// Find all queued tasks
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{"status": StatusQueued}
	tasks, err := s.GetAllTaskStatuses(ctx, filter)
	if err != nil {
		log.Printf("Error fetching queued tasks: %v", err)
		return
	}

	log.Printf("Found %d queued tasks to resubmit", len(tasks))

	// Resubmit each task
	for _, task := range tasks {
		taskData, err := json.Marshal(task)
		if err != nil {
			log.Printf("Failed to marshal task %s for requeue: %v", task.ID, err)
			continue
		}

		err = s.natsConn.Publish(s.natsTaskSubject, taskData)
		if err != nil {
			log.Printf("Failed to requeue task %s: %v", task.ID, err)
		} else {
			log.Printf("Requeued task %s (Type: %s) to NATS", task.ID, task.Type)
		}
	}
}

// API Handler for HTTP API
type APIHandler struct {
	scheduler *Scheduler
}

func NewAPIHandler(s *Scheduler) *APIHandler {
	return &APIHandler{scheduler: s}
}

type SubmitTaskRequest struct {
	Type       string      `json:"type"`
	Parameters interface{} `json:"parameters"`
}

type SubmitTaskResponse struct {
	TaskID string `json:"taskId"`
}

type ErrorResponse struct {
	Message string `json:"message"`
}

type SchedulerStatusResponse struct {
	IsPaused bool `json:"isPaused"`
}

// submitTaskHandler handles task submission API requests
func (h *APIHandler) submitTaskHandler(c echo.Context) error {
	var req SubmitTaskRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, ErrorResponse{Message: err.Error()})
	}

	if req.Type == "" {
		return c.JSON(http.StatusBadRequest, ErrorResponse{Message: "task type required"})
	}

	taskID, err := h.scheduler.SubmitTask(req.Type, req.Parameters)
	if err != nil {
		if strings.Contains(err.Error(), "rate limit") {
			return c.JSON(http.StatusTooManyRequests, ErrorResponse{Message: "Rate limit exceeded"})
		}
		log.Printf("API Submit Error: %v", err)
		return c.JSON(http.StatusInternalServerError, ErrorResponse{Message: "Failed to submit task"})
	}

	return c.JSON(http.StatusCreated, SubmitTaskResponse{TaskID: taskID})
}

// getTaskStatusHandler handles task status API requests
func (h *APIHandler) getTaskStatusHandler(c echo.Context) error {
	taskID := c.Param("id")
	if taskID == "" {
		return c.JSON(http.StatusBadRequest, ErrorResponse{Message: "Task ID required"})
	}

	task, found, err := h.scheduler.GetTaskStatus(taskID)
	if err != nil {
		log.Printf("API GetStatus Error: %v", err)
		return c.JSON(http.StatusInternalServerError, ErrorResponse{Message: "Failed to get task status"})
	}

	if !found {
		return c.JSON(http.StatusNotFound, ErrorResponse{Message: "Task not found"})
	}

	return c.JSON(http.StatusOK, task)
}

// getAllTasksHandler handles retrieving all tasks API requests
func (h *APIHandler) getAllTasksHandler(c echo.Context) error {
	filter := bson.M{}

	// Apply status filter if provided
	statusParam := c.QueryParam("status")
	if statusParam != "" {
		statusValue := TaskStatus(strings.ToLower(statusParam))
		switch statusValue {
		case StatusQueued, StatusRunning, StatusCompleted, StatusFailed, StatusCancelled:
			filter["status"] = statusValue
		default:
			return c.JSON(http.StatusBadRequest, ErrorResponse{Message: "Invalid status value"})
		}
	}

	tasks, err := h.scheduler.GetAllTaskStatuses(c.Request().Context(), filter)
	if err != nil {
		log.Printf("API GetAll Error: %v", err)
		return c.JSON(http.StatusInternalServerError, ErrorResponse{Message: "Failed to retrieve tasks"})
	}

	if tasks == nil {
		tasks = []*Task{}
	}

	return c.JSON(http.StatusOK, tasks)
}

// cancelTaskHandler handles task cancellation API requests
func (h *APIHandler) cancelTaskHandler(c echo.Context) error {
	taskID := c.Param("id")
	if taskID == "" {
		return c.JSON(http.StatusBadRequest, ErrorResponse{Message: "Task ID required"})
	}

	err := h.scheduler.CancelTask(taskID)
	if err != nil {
		errMsg := err.Error()

		if strings.Contains(errMsg, "not found") {
			return c.JSON(http.StatusNotFound, ErrorResponse{Message: errMsg})
		}

		if strings.Contains(errMsg, "final state") {
			return c.JSON(http.StatusConflict, ErrorResponse{Message: errMsg})
		}

		log.Printf("API Cancel Error: %v", err)
		return c.JSON(http.StatusInternalServerError, ErrorResponse{Message: "Failed to cancel task"})
	}

	return c.JSON(http.StatusAccepted, echo.Map{"message": "Cancel request accepted"})
}

// pauseWorkersHandler pauses the scheduler
func (h *APIHandler) pauseWorkersHandler(c echo.Context) error {
	h.scheduler.PauseWorkers()
	return c.JSON(http.StatusOK, echo.Map{"message": "Scheduler paused"})
}

// resumeWorkersHandler resumes the scheduler
func (h *APIHandler) resumeWorkersHandler(c echo.Context) error {
	h.scheduler.ResumeWorkers()
	return c.JSON(http.StatusOK, echo.Map{"message": "Scheduler resumed"})
}

// getSchedulerStatusHandler returns scheduler status
func (h *APIHandler) getSchedulerStatusHandler(c echo.Context) error {
	h.scheduler.mu.RLock()
	status := SchedulerStatusResponse{
		IsPaused: h.scheduler.isPaused,
	}
	h.scheduler.mu.RUnlock()

	return c.JSON(http.StatusOK, status)
}

// Main Function
func main() {
	log.Println("Starting Task Scheduler Service...")

	// --- Configuration ---
	natsURL := getEnv("NATS_URL", "nats://localhost:4222")
	mongoURI := getEnv("MONGO_URI", "mongodb://localhost:27017")
	mongoDBName := getEnv("MONGO_DB_NAME", "task_scheduler_db")
	mongoCollName := getEnv("MONGO_COLL_NAME", "tasks")
	apiPort := getEnv("API_PORT", "8080")

	natsTaskSubject := getEnv("NATS_TASK_SUBJECT", "tasks.process")
	natsRespSubject := getEnv("NATS_RESP_SUBJECT", "tasks.result")
	natsQueueGroup := getEnv("NATS_QUEUE_GROUP", "task-workers")

	submitRateLimitStr := getEnv("SUBMIT_RATE_LIMIT_PER_SEC", "5.0")
	submitRateLimitPerSec := 5.0
	if val, err := strconv.ParseFloat(submitRateLimitStr, 64); err == nil && val > 0 {
		submitRateLimitPerSec = val
	}

	// --- Scheduler Initialization ---
	scheduler, err := NewScheduler(
		natsURL, natsTaskSubject, natsRespSubject, natsQueueGroup,
		mongoURI, mongoDBName, mongoCollName,
		rate.Limit(submitRateLimitPerSec),
	)
	if err != nil {
		log.Fatalf("FATAL: Failed to create scheduler: %v", err)
	}
	defer scheduler.Close()

	if err := scheduler.Start(); err != nil {
		log.Fatalf("FATAL: Failed to start scheduler: %v", err)
	}

	// --- API Handler and Echo Setup ---
	apiHandler := NewAPIHandler(scheduler)
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete},
	}))

	// Tasks API endpoints
	tasksGroup := e.Group("/tasks")
	tasksGroup.POST("", apiHandler.submitTaskHandler)
	tasksGroup.GET("", apiHandler.getAllTasksHandler)
	tasksGroup.GET("/:id", apiHandler.getTaskStatusHandler)
	tasksGroup.DELETE("/:id", apiHandler.cancelTaskHandler)

	// Scheduler API endpoints
	schedulerGroup := e.Group("/scheduler")
	schedulerGroup.GET("/status", apiHandler.getSchedulerStatusHandler)
	schedulerGroup.POST("/pause", apiHandler.pauseWorkersHandler)
	schedulerGroup.POST("/resume", apiHandler.resumeWorkersHandler)

	// Health check endpoint
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, echo.Map{"status": "ok"})
	})

	// --- Start API Server & Graceful Shutdown ---
	go func() {
		log.Printf("API server listening on port %s", apiPort)
		if err := e.Start(":" + apiPort); err != nil && err != http.ErrServerClosed {
			log.Fatalf("FATAL: API server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	sig := <-quit
	log.Printf("Received signal: %s. Starting graceful shutdown...", sig.String())

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	log.Println("Shutting down API server...")
	if err := e.Shutdown(shutdownCtx); err != nil {
		log.Printf("API server shutdown error: %v", err)
	} else {
		log.Println("API server shutdown complete.")
	}

	log.Println("Service shutdown complete.")
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
