#!/bin/bash
# Orchestrator script: Run docker compose with automatic logging and Excel export

set -e

ROOT_DIR="/Users/hiradkhademian/Desktop/smartgrid-sentinel"
LOG_FILE="$ROOT_DIR/system_logs.txt"
EXCEL_FILE="$ROOT_DIR/grid_sentinel_logs.xlsx"

echo "=========================================="
echo "SmartGrid Sentinel - Docker Compose + Logging"
echo "=========================================="

# Step 1: Clear old logs
echo "🧹 Clearing previous logs..."
> "$LOG_FILE"

# Step 2: Start docker compose with log capture
echo "🚀 Starting Docker Compose..."
cd "$ROOT_DIR"

# Run docker compose in background and capture logs
docker compose up -d > /dev/null 2>&1

# Start capturing logs in background
echo "📝 Capturing logs to: $LOG_FILE"
docker compose logs --timestamps -f > "$LOG_FILE" 2>&1 &
LOG_CAPTURE_PID=$!

# Let it capture for a bit, then we'll generate the Excel
echo "⏳ Capturing logs (will continue in background)..."

# Give services time to start and generate logs
echo "⌛ Waiting 10 seconds for services to initialize..."
sleep 10

# Step 3: Parse logs and create Excel (async)
echo "📊 Generating Excel report..."
python3 "$ROOT_DIR/parse_logs_to_excel.py"

echo ""
echo "=========================================="
echo "✓ System running in background"
echo "📁 Logs: $LOG_FILE"
echo "📊 Excel: $EXCEL_FILE"
echo "=========================================="
echo ""
echo "View logs with:"
echo "  docker compose logs -f"
echo ""
echo "Stop system with:"
echo "  docker compose down"
echo ""
echo "Re-generate Excel report with:"
echo "  python3 parse_logs_to_excel.py"
echo ""
