Health Check

Bash

# Check if the API server is responding
curl http://localhost:8080/health
# Expected: {"status":"ok"}
2. Submit Tasks

Bash

# Submit a "quick_job" task
# Note the response, especially the "taskId"
curl -X POST -H "Content-Type: application/json" \
  -d '{"type":"quick_job", "parameters":{"value": 123, "comment":"First task"}}' \
  http://localhost:8080/tasks

# Submit a "long_computation" task
curl -X POST -H "Content-Type: application/json" \
  -d '{"type":"long_computation", "parameters":{"input_file": "/path/to/data.csv"}}' \
  http://localhost:8080/tasks

# Submit a "failing_job" task
curl -X POST -H "Content-Type: application/json" \
  -d '{"type":"failing_job", "parameters":null}' \
  http://localhost:8080/tasks

# Example: Capture the Task ID (Bash/Zsh) - Replace with actual ID from response if needed
TASK_ID_1=$(curl -s -X POST -H "Content-Type: application/json" \
  -d '{"type":"quick_job", "parameters":{"source": "curl_test_1"}}' \
  http://localhost:8080/tasks | jq -r '.taskId')

echo "Submitted task with ID: $TASK_ID_1"
3. Get Task Status (Specific Task)

Bash

# Replace <task_id> with an actual ID received from submitting a task
TASK_ID="PASTE_AN_ID_HERE" # e.g., $TASK_ID_1 from above

curl http://localhost:8080/tasks/$TASK_ID | jq
4. Get Task Status (Filtered List)

Bash

# Get all tasks (most recent first based on default sort)
curl "http://localhost:8080/tasks" | jq

# Get only tasks currently running
curl "http://localhost:8080/tasks?status=running" | jq

# Get only tasks that are queued (waiting to be processed)
curl "http://localhost:8080/tasks?status=queued" | jq

# Get only completed tasks
curl "http://localhost:8080/tasks?status=completed" | jq

# Get only failed tasks
curl "http://localhost:8080/tasks?status=failed" | jq

# Get only cancelled tasks
curl "http://localhost:8080/tasks?status=cancelled" | jq
(Note: Quotes around the URL are helpful if your shell interprets ? or &)

5. Cancel a Task

Bash

# Replace <task_id> with the ID of a task you want to cancel
# Try cancelling a 'long_computation' task while it's likely running
# Or try cancelling a 'queued' task
TASK_ID_TO_CANCEL="PASTE_AN_ID_HERE"

curl -X DELETE http://localhost:8080/tasks/$TASK_ID_TO_CANCEL

# Check its status afterwards
sleep 2 # Give time for status update potentially
curl http://localhost:8080/tasks/$TASK_ID_TO_CANCEL | jq
6. Scheduler Control

Bash

# Get the current scheduler status (workers, paused state)
curl http://localhost:8080/scheduler/status | jq

# Pause the scheduler (workers will stop picking up new tasks)
curl -X POST http://localhost:8080/scheduler/pause

# Check status again (should show "isPaused": true)
curl http://localhost:8080/scheduler/status | jq

# Submit a task while paused (it should get queued)
curl -X POST -H "Content-Type: application/json" \
  -d '{"type":"quick_job", "parameters":{"when": "paused"}}' \
  http://localhost:8080/tasks
# Check queued tasks: curl "http://localhost:8080/tasks?status=queued" | jq

# Resume the scheduler (workers will start processing queued tasks)
curl -X POST http://localhost:8080/scheduler/resume

# Check status again (should show "isPaused": false)
curl http://localhost:8080/scheduler/status | jq

# Set the target number of workers (e.g., to 3)
# Note: Scale-down isn't fully implemented to stop specific workers in the example code
curl -X PUT -H "Content-Type: application/json" \
  -d '{"count": 3}' \
  http://localhost:8080/scheduler/workers

# Check status again (should show "targetWorkers": 3)
curl http://localhost:8080/scheduler/status | jq
Run these commands in your terminal while the Go application is running to interact with your task scheduler API. Remember to replace placeholders like <task_id> with actual IDs.
