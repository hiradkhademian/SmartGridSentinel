#!/bin/bash
# Capture docker compose logs to a text file

OUTPUT_DIR="/Users/hiradkhademian/Desktop/smartgrid-sentinel"
LOG_FILE="$OUTPUT_DIR/system_logs.txt"

# Clear previous log file
> "$LOG_FILE"

# Capture logs with timestamps
docker compose logs --timestamps -f >> "$LOG_FILE" 2>&1 &
LOG_PID=$!

# Let it run for the duration of compose services
# In practice, this runs in background while docker compose up is active
echo "Logging started. PID: $LOG_PID"
echo "Logs will be saved to: $LOG_FILE"

# Keep the log capture process running
wait $LOG_PID
