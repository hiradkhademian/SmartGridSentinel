# SmartGrid Sentinel - Comprehensive Testing Guide

## Overview
Complete testing strategy for the distributed microservices grid monitoring system with 11 services, Kafka streaming, PostgreSQL persistence, and real-time anomaly detection.

---

## 1. INFRASTRUCTURE & DEPLOYMENT TESTS

### 1.1 Service Startup Verification
```bash
# Start all services
docker compose up -d

# Verify all 11 services are running
docker compose ps

# Expected Output: All services should show "Up"
```

**Pass Criteria:**
- ✓ All 11 containers in "Up" state
- ✓ No "Restarting" or "Exit" states
- ✓ Ports correctly mapped

### 1.2 Service Health Checks
```bash
# Check individual service logs for startup
docker compose logs zookeeper | grep "Started"
docker compose logs kafka | grep "started"
docker compose logs postgres | grep "ready to accept"
docker compose logs redis | grep "Ready to accept"

# Check microservice startup
docker compose logs ingestion-service | grep "listening"
docker compose logs mock-engine | grep "listening"
docker compose logs real-time-analysis | grep "subscribed"
docker compose logs action-gateway | grep "started"
```

**Pass Criteria:**
- ✓ Zookeeper started successfully
- ✓ Kafka broker started and registered
- ✓ PostgreSQL accepting connections
- ✓ Redis accepting connections
- ✓ All gRPC services listening on correct ports
- ✓ Kafka consumers subscribed to topics

### 1.3 Port Connectivity Verification
```bash
# Check all ports are accessible
netstat -an | grep LISTEN | grep -E "5432|6379|2181|29092|8000|50051|50052|9092"

# Or using lsof
lsof -i :5432   # PostgreSQL
lsof -i :6379   # Redis
lsof -i :2181   # Zookeeper
lsof -i :29092  # Kafka
lsof -i :8000   # Management API
lsof -i :50051  # Ingestion Service gRPC
lsof -i :50052  # Mock Engine gRPC
```

**Pass Criteria:**
- ✓ All ports listening
- ✓ Correct service on each port

---

## 2. gRPC COMMUNICATION TESTS

### 2.1 Ingestion Service gRPC Server
```bash
# Check Mock Engine can connect
docker compose logs mock-engine | grep "Connected\|Connection\|gRPC"

# Check Ingestion Service received data
docker compose logs ingestion-service | grep "Received\|telemetry\|METER"
```

**Pass Criteria:**
- ✓ Mock Engine successfully connects to Ingestion Service on :50051
- ✓ Telemetry data being received
- ✓ Multiple meters generating data (METER-01A, METER-02H, METER-03Z, METER-04W)
- ✓ No connection errors or timeouts

### 2.2 Mock Engine Simulation
```bash
# Verify mock data generation
docker compose logs mock-engine | tail -50

# Look for patterns
docker compose logs mock-engine | grep "METER\|voltage\|current\|consumption"
```

**Pass Criteria:**
- ✓ Mock Engine generating telemetry for 4 meters
- ✓ Voltage, current, and consumption values reasonable
- ✓ Continuous data generation every 2-5 seconds

---

## 3. KAFKA MESSAGE FLOW TESTS

### 3.1 Topic Verification
```bash
# List all Kafka topics
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:29092

# Expected topics:
# - telemetry-stream
# - emergency-alerts
# - trend-region-events
# - action-commands
# - telemetry-dlq
# - emergency-alerts-dlq
# - trend-region-dlq
# - action-gateway-dlq
```

**Pass Criteria:**
- ✓ All 8 topics exist (4 main + 4 DLQ)
- ✓ Correct replication factor
- ✓ No error messages

### 3.2 Message Production
```bash
# Monitor main topic with consumer
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic telemetry-stream \
  --from-beginning \
  --max-messages 5

# Should see protobuf messages flowing through
```

**Pass Criteria:**
- ✓ Messages appearing on telemetry-stream topic
- ✓ Real-time data updates
- ✓ No errors or timeouts

### 3.3 Topic Consumer Lag
```bash
# Check consumer groups
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --list

# Check consumer group details
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group real-time-analysis-group \
  --describe
```

**Pass Criteria:**
- ✓ All consumer groups created
- ✓ Lag is 0 or near 0 (being consumed in real-time)
- ✓ No inactive members

---

## 4. DATABASE PERSISTENCE TESTS

### 4.1 PostgreSQL Connection
```bash
# Connect to PostgreSQL
docker compose exec postgres psql -U smartgrid -d smartgrid_db -c "\dt"

# Expected tables:
# - command_audit_logs
# - dead_letter_logs
```

**Pass Criteria:**
- ✓ Connected successfully
- ✓ Both tables exist

### 4.2 Audit Log Persistence
```bash
# Check audit logs being recorded
docker compose exec postgres psql -U smartgrid -d smartgrid_db -c \
  "SELECT COUNT(*), command_type FROM command_audit_logs GROUP BY command_type;"

# Should show records with RESTART_METER, CUT_POWER, THROTTLE_CONSUMPTION
```

**Pass Criteria:**
- ✓ Audit logs table has records
- ✓ Multiple command types present
- ✓ Timestamps are recent

### 4.3 Dead Letter Queue Logging
```bash
# Check DLQ records
docker compose exec postgres psql -U smartgrid -d smartgrid_db -c \
  "SELECT COUNT(*), reason FROM dead_letter_logs GROUP BY reason;"

# Should show corruption reasons
```

**Pass Criteria:**
- ✓ DLQ table has records
- ✓ Contains failed messages with reasons
- ✓ Timestamps recorded correctly

### 4.4 Data Consistency
```bash
# Verify row count consistency over time
docker compose exec postgres psql -U smartgrid -d smartgrid_db -c \
  "SELECT 'audit_logs' as table_name, COUNT(*) FROM command_audit_logs 
   UNION ALL 
   SELECT 'dlq_logs', COUNT(*) FROM dead_letter_logs;"

# Run this twice with 30-second interval - counts should increase
```

**Pass Criteria:**
- ✓ Row counts increase over time
- ✓ Data being actively written
- ✓ No data loss or duplicates

---

## 5. REDIS CACHE TESTS

### 5.1 Cache Connectivity
```bash
# Connect to Redis
docker compose exec redis redis-cli PING

# Should return: PONG
```

**Pass Criteria:**
- ✓ Redis responds to PING
- ✓ Connection established

### 5.2 Cache Population
```bash
# Check cache keys
docker compose exec redis redis-cli KEYS "*"

# Should see patterns like:
# - region:ZONE-*:consumption_window
# - region:ZONE-*:anomaly_threshold
# - meter:METER-*:*
```

**Pass Criteria:**
- ✓ Cache keys being created
- ✓ Region-based keys for zones
- ✓ Meter-specific cache entries

### 5.3 Cache Expiration
```bash
# Check TTL on cache entries
docker compose exec redis redis-cli TTL "region:ZONE-ALPHA:consumption_window"

# Should return positive number (seconds remaining)
```

**Pass Criteria:**
- ✓ TTL values set correctly
- ✓ Sliding window maintained
- ✓ Keys expire appropriately

---

## 6. ANOMALY DETECTION TESTS

### 6.1 Blackout Detection
```bash
# Monitor Real-Time Analysis service
docker compose logs real-time-analysis -f &

# Wait 30 seconds for blackout anomaly to be detected
# Should see: "Blackout Detected" in logs

# Stop monitoring (Ctrl+C)
```

**Pass Criteria:**
- ✓ Blackout detected within 30 seconds
- ✓ Anomaly logged with meter ID
- ✓ Alert sent to Kafka

### 6.2 Voltage Spike Detection
```bash
# Look for voltage spike anomalies
docker compose logs real-time-analysis | grep -i "voltage"

# Should see patterns like:
# "Voltage Spike Detected on METER-02H"
```

**Pass Criteria:**
- ✓ Voltage anomalies detected
- ✓ Threshold violations identified
- ✓ Correct meter identification

### 6.3 Current Spike Detection
```bash
# Look for current spike anomalies
docker compose logs real-time-analysis | grep -i "current"

# Should see patterns like:
# "Current Spike Detected on METER-03Z"
```

**Pass Criteria:**
- ✓ Current anomalies detected
- ✓ Threshold violations identified
- ✓ Appropriate alert level

### 6.4 Suspicious Consumption Detection
```bash
# Look for consumption anomalies
docker compose logs real-time-analysis | grep -i "suspicious\|consumption"

# Should see patterns like:
# "Suspicious Consumption Pattern on ZONE-BETA"
```

**Pass Criteria:**
- ✓ Consumption anomalies detected
- ✓ Zone-level and meter-level tracking
- ✓ Pattern matching working

---

## 7. COMMAND EXECUTION TESTS

### 7.1 Command Generation
```bash
# Monitor Action Gateway for incoming commands
docker compose logs action-gateway -f &

# Wait for anomalies (30-60 seconds)
# Should see commands like:
# - "RESTART_METER Command"
# - "CUT_POWER Command"
# - "THROTTLE_CONSUMPTION Command"
```

**Pass Criteria:**
- ✓ Commands generated for anomalies
- ✓ Correct command type for anomaly
- ✓ Commands executed within 2-5 seconds of detection

### 7.2 Command Acknowledgment
```bash
# Look for ACK responses
docker compose logs action-gateway | grep -i "ack\|acknowledged"

# Should see: "Command Acknowledged (ACK)"
```

**Pass Criteria:**
- ✓ ACK received for each command
- ✓ ACK logged with timestamp
- ✓ No unacknowledged commands hanging

### 7.3 Audit Trail
```bash
# Query audit logs for command execution
docker compose exec postgres psql -U smartgrid -d smartgrid_db -c \
  "SELECT timestamp, command_type, target_meter, status FROM command_audit_logs ORDER BY timestamp DESC LIMIT 10;"

# Should show recent commands with status
```

**Pass Criteria:**
- ✓ All executed commands logged
- ✓ Status shows SUCCESS or ACKNOWLEDGED
- ✓ Timestamps match log timestamps

---

## 8. DLQ (DEAD LETTER QUEUE) TESTS

### 8.1 Simulated Message Corruption
```bash
# Check DLQ topic for messages
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic telemetry-dlq \
  --from-beginning \
  --max-messages 3

# Should see corrupted/invalid protobuf data
```

**Pass Criteria:**
- ✓ Messages appear in DLQ
- ✓ Corrupted messages are isolated
- ✓ Valid messages continue to flow

### 8.2 DLQ Monitoring Service
```bash
# Check DLQ Monitor logs
docker compose logs dlq-monitor | grep -i "dlq\|dead\|letter\|corrupt"

# Should see DLQ events being monitored
```

**Pass Criteria:**
- ✓ DLQ Monitor service running
- ✓ Reading from all DLQ topics
- ✓ Logging DLQ events

### 8.3 DLQ Database Records
```bash
# Query dead_letter_logs table
docker compose exec postgres psql -U smartgrid -d smartgrid_db -c \
  "SELECT timestamp, topic, reason FROM dead_letter_logs ORDER BY timestamp DESC LIMIT 10;"
```

**Pass Criteria:**
- ✓ Records in dead_letter_logs table
- ✓ Topic and reason captured
- ✓ Timestamps accurate

---

## 9. API ENDPOINT TESTS

### 9.1 Health Check Endpoint
```bash
curl -X GET http://localhost:8000/healthz

# Expected response:
# {"status": "healthy", "services": 11, "timestamp": "..."}
```

**Pass Criteria:**
- ✓ Returns 200 OK
- ✓ Status is "healthy"
- ✓ All services accounted for

### 9.2 Trend Query Endpoint
```bash
curl -X GET "http://localhost:8000/trends?zone=ZONE-ALPHA&days=1"

# Should return trend data for the zone
```

**Pass Criteria:**
- ✓ Returns 200 OK
- ✓ Contains trend data
- ✓ Correct zone in response

### 9.3 Incident History Endpoint
```bash
curl -X GET "http://localhost:8000/incidents?limit=10"

# Should return recent incidents
```

**Pass Criteria:**
- ✓ Returns 200 OK
- ✓ Contains incident records
- ✓ Ordered by timestamp (recent first)

### 9.4 DLQ Status Endpoint
```bash
curl -X GET http://localhost:8000/dlq-status

# Should return DLQ queue statistics
```

**Pass Criteria:**
- ✓ Returns 200 OK
- ✓ Shows DLQ message count
- ✓ Broken down by topic

---

## 10. LOGGING & REPORTING TESTS

### 10.1 Log Capture
```bash
# Verify logs are being captured
docker compose logs --timestamps > /tmp/test_logs.txt
wc -l /tmp/test_logs.txt

# Should show thousands of log lines
```

**Pass Criteria:**
- ✓ Logs captured successfully
- ✓ All services represented
- ✓ Timestamps present on each line

### 10.2 Excel Report Generation
```bash
# Generate report from current logs
cd /Users/hiradkhademian/Desktop/smartgrid-sentinel
python3 parse_logs_to_excel.py

# Check file created
ls -lh grid_sentinel_logs.xlsx
```

**Pass Criteria:**
- ✓ Script runs without errors
- ✓ Excel file created
- ✓ File size > 5KB (contains data)

### 10.3 Excel Data Validation
```bash
# Verify Excel structure
python3 << 'EOF'
import pandas as pd
df = pd.read_excel("grid_sentinel_logs.xlsx", sheet_name='Grid Sentinel Logs', skiprows=2)

print(f"Total Rows: {len(df)}")
print(f"Columns: {list(df.columns)}")
print(f"\nServices found:")
print(df['Kaynak Servis'].value_counts())
print(f"\nDLQ Events: {df['DLQ Durumu'].notna().sum()}")
EOF
```

**Pass Criteria:**
- ✓ 7 columns present and named correctly
- ✓ Only 4 core services (no DLQ Monitor/Mock Engine)
- ✓ Data rows populated
- ✓ DLQ column tracked

### 10.4 Excel Formatting Validation
```bash
# Verify professional formatting
python3 << 'EOF'
from openpyxl import load_workbook
wb = load_workbook("grid_sentinel_logs.xlsx")
ws = wb.active

# Check header formatting
header_fill = ws['A3'].fill
print(f"Header color: {header_fill.start_color.rgb if header_fill else 'None'}")
print(f"Header font bold: {ws['A3'].font.bold}")

# Check data formatting
print(f"Borders applied: {ws['A4'].border}")
print(f"Frozen panes: {ws.freeze_panes}")
EOF
```

**Pass Criteria:**
- ✓ Header row has blue background
- ✓ Bold text on headers
- ✓ Borders on all cells
- ✓ Frozen panes set
- ✓ Auto-filter enabled

---

## 11. INTEGRATION END-TO-END TESTS

### 11.1 Complete Anomaly-to-Action Flow
```bash
# 1. Start system
docker compose down
docker compose up -d

# 2. Wait for services to initialize
sleep 10

# 3. Monitor all services for 60 seconds
docker compose logs -f &
LOG_PID=$!

# 4. Wait and observe
sleep 60

# Kill log monitoring
kill $LOG_PID

# 5. Check each component
echo "=== Checking flow ==="

# Mock Engine generating?
docker compose logs mock-engine | grep "METER" | wc -l

# Real-Time Analysis detecting?
docker compose logs real-time-analysis | grep -i "detected" | wc -l

# Action Gateway executing?
docker compose logs action-gateway | grep "Command" | wc -l

# Database recording?
docker compose exec postgres psql -U smartgrid -d smartgrid_db -c \
  "SELECT COUNT(*) FROM command_audit_logs;"
```

**Pass Criteria:**
- ✓ Mock Engine: >10 telemetry events
- ✓ Real-Time Analysis: ≥1 anomaly detected
- ✓ Action Gateway: ≥1 command executed and ACKed
- ✓ PostgreSQL: ≥1 audit log recorded

### 11.2 Multi-Meter Coordination
```bash
# Verify all 4 meters are being processed
docker compose logs | grep -o "METER-[0-9A-Z]*" | sort | uniq -c

# Should show all 4 meters represented
```

**Pass Criteria:**
- ✓ METER-01A: Active
- ✓ METER-02H: Active
- ✓ METER-03Z: Active
- ✓ METER-04W: Active

### 11.3 Zone-Level Analysis
```bash
# Verify regional analysis working
docker compose logs trend-regional-analysis | grep -i "zone\|region" | tail -20
```

**Pass Criteria:**
- ✓ ZONE-ALPHA: Processed
- ✓ ZONE-BETA: Processed
- ✓ ZONE-GAMMA: Processed
- ✓ Zone correlations detected

---

## 12. PERFORMANCE & STRESS TESTS

### 12.1 Throughput Measurement
```bash
# Measure telemetry ingestion rate
docker compose logs ingestion-service -f &

# Let it run for 30 seconds
sleep 30

# Kill logs, count events
docker compose logs ingestion-service | grep -c "telemetry\|Received"
# Divide by 30 = events/second
```

**Pass Criteria:**
- ✓ Throughput: 2-8 events/second
- ✓ No message loss
- ✓ Consistent rate

### 12.2 Latency Measurement
```bash
# Check time from detection to action
docker compose logs real-time-analysis | grep "Detected" > /tmp/detection.log
docker compose logs action-gateway | grep "Command" > /tmp/command.log

# Compare timestamps (should be < 5 seconds apart)
```

**Pass Criteria:**
- ✓ Detection to command: < 5 seconds
- ✓ Command to ACK: < 2 seconds
- ✓ Total latency: < 7 seconds

### 12.3 Resource Usage
```bash
# Check Docker container resource usage
docker stats --no-stream

# Individual services shouldn't use > 200MB memory
```

**Pass Criteria:**
- ✓ Each service < 200MB RAM
- ✓ CPU usage reasonable (< 50% per container)
- ✓ No memory leaks over time

---

## 13. ERROR HANDLING & RECOVERY TESTS

### 13.1 Service Restart Recovery
```bash
# Stop a service
docker compose stop real-time-analysis

# Wait 5 seconds
sleep 5

# Restart it
docker compose up -d real-time-analysis

# Check it reconnects to Kafka
docker compose logs real-time-analysis | tail -20 | grep -i "subscribed\|connected"
```

**Pass Criteria:**
- ✓ Service restarts successfully
- ✓ Reconnects to Kafka
- ✓ Resumes consuming messages
- ✓ No data loss

### 13.2 Database Connection Failure Recovery
```bash
# Check behavior with DB unavailable
# (This tests the resilience of audit logging)

docker compose logs action-gateway | grep -i "database\|error" | tail -5
```

**Pass Criteria:**
- ✓ Services don't crash
- ✓ Error handling graceful
- ✓ Recovery attempted

### 13.3 Kafka Broker Unavailability
```bash
# Monitor services during temporary broker issues
# (Already have redundancy/retry logic)

docker compose logs | grep -i "broker\|reconnect" | tail -10
```

**Pass Criteria:**
- ✓ Services attempt reconnection
- ✓ No permanent failures
- ✓ Recovery successful

---

## 14. TEST EXECUTION SCRIPT

```bash
#!/bin/bash
# comprehensive_test.sh

echo "🧪 SMARTGRID SENTINEL - COMPREHENSIVE TEST SUITE"
echo "=================================================="

# Test 1: Startup
echo -e "\n[1/14] Testing Service Startup..."
docker compose down > /dev/null 2>&1
sleep 2
docker compose up -d > /dev/null 2>&1
sleep 10

UP_COUNT=$(docker compose ps | grep "Up" | wc -l)
if [ "$UP_COUNT" -eq 11 ]; then
    echo "✅ PASS: All 11 services running"
else
    echo "❌ FAIL: Only $UP_COUNT services running (expected 11)"
fi

# Test 2: gRPC Communication
echo -e "\n[2/14] Testing gRPC Communication..."
gRPC_LOGS=$(docker compose logs mock-engine | grep -c "Connected\|telemetry")
if [ "$gRPC_LOGS" -gt 0 ]; then
    echo "✅ PASS: Mock Engine communicating via gRPC"
else
    echo "❌ FAIL: No gRPC communication detected"
fi

# Test 3: Kafka Topics
echo -e "\n[3/14] Testing Kafka Topics..."
TOPIC_COUNT=$(docker compose exec kafka kafka-topics --list --bootstrap-server localhost:29092 2>/dev/null | wc -l)
if [ "$TOPIC_COUNT" -ge 8 ]; then
    echo "✅ PASS: All Kafka topics created"
else
    echo "❌ FAIL: Only $TOPIC_COUNT topics found (expected ≥8)"
fi

# Test 4: PostgreSQL
echo -e "\n[4/14] Testing PostgreSQL..."
PG_TABLES=$(docker compose exec postgres psql -U smartgrid -d smartgrid_db -c "\dt" 2>/dev/null | grep -c "command_audit_logs\|dead_letter_logs")
if [ "$PG_TABLES" -eq 2 ]; then
    echo "✅ PASS: Database tables created"
else
    echo "❌ FAIL: Expected 2 tables, found $PG_TABLES"
fi

# Test 5: Redis
echo -e "\n[5/14] Testing Redis Cache..."
REDIS_PING=$(docker compose exec redis redis-cli PING 2>/dev/null)
if [ "$REDIS_PING" = "PONG" ]; then
    echo "✅ PASS: Redis cache responsive"
else
    echo "❌ FAIL: Redis not responding"
fi

# Test 6: Anomaly Detection (wait 60 seconds)
echo -e "\n[6/14] Testing Anomaly Detection (wait 60s)..."
sleep 60
ANOMALY_COUNT=$(docker compose logs real-time-analysis | grep -c -i "detected")
if [ "$ANOMALY_COUNT" -gt 0 ]; then
    echo "✅ PASS: $ANOMALY_COUNT anomalies detected"
else
    echo "❌ FAIL: No anomalies detected"
fi

# Test 7: Command Execution
echo -e "\n[7/14] Testing Command Execution..."
COMMAND_COUNT=$(docker compose logs action-gateway | grep -c "Command")
if [ "$COMMAND_COUNT" -gt 0 ]; then
    echo "✅ PASS: $COMMAND_COUNT commands executed"
else
    echo "❌ FAIL: No commands executed"
fi

# Test 8: DLQ Handling
echo -e "\n[8/14] Testing DLQ Handling..."
DLQ_COUNT=$(docker compose logs dlq-monitor | grep -c -i "dlq\|dead")
if [ "$DLQ_COUNT" -gt 0 ]; then
    echo "✅ PASS: DLQ events being monitored"
else
    echo "❌ FAIL: DLQ monitoring not working"
fi

# Test 9: API Health
echo -e "\n[9/14] Testing API Health Endpoint..."
HEALTH=$(curl -s http://localhost:8000/healthz | grep -c "healthy")
if [ "$HEALTH" -gt 0 ]; then
    echo "✅ PASS: Management API responding"
else
    echo "❌ FAIL: Management API not responding"
fi

# Test 10: Audit Logging
echo -e "\n[10/14] Testing Audit Logging..."
AUDIT_COUNT=$(docker compose exec postgres psql -U smartgrid -d smartgrid_db -c "SELECT COUNT(*) FROM command_audit_logs;" 2>/dev/null | grep -oE '[0-9]+' | tail -1)
if [ "$AUDIT_COUNT" -gt 0 ]; then
    echo "✅ PASS: $AUDIT_COUNT audit logs recorded"
else
    echo "❌ FAIL: No audit logs recorded"
fi

# Test 11: Multi-Meter Processing
echo -e "\n[11/14] Testing Multi-Meter Processing..."
METER_COUNT=$(docker compose logs | grep -o "METER-[0-9A-Z]*" | sort | uniq | wc -l)
if [ "$METER_COUNT" -eq 4 ]; then
    echo "✅ PASS: All 4 meters being processed"
else
    echo "❌ FAIL: Only $METER_COUNT meters found (expected 4)"
fi

# Test 12: Regional Analysis
echo -e "\n[12/14] Testing Regional Analysis..."
ZONE_COUNT=$(docker compose logs trend-regional-analysis | grep -o "ZONE-[A-Z]*" | sort | uniq | wc -l)
if [ "$ZONE_COUNT" -eq 3 ]; then
    echo "✅ PASS: All 3 zones being analyzed"
else
    echo "❌ FAIL: Only $ZONE_COUNT zones found (expected 3)"
fi

# Test 13: Log Capture
echo -e "\n[13/14] Testing Log Capture..."
LOG_LINES=$(wc -l < /tmp/smartgrid_test.log 2>/dev/null || echo "0")
docker compose logs > /tmp/smartgrid_test.log
NEW_LINES=$(wc -l < /tmp/smartgrid_test.log)
if [ "$NEW_LINES" -gt 1000 ]; then
    echo "✅ PASS: $NEW_LINES log lines captured"
else
    echo "❌ FAIL: Only $NEW_LINES log lines (expected >1000)"
fi

# Test 14: Excel Report
echo -e "\n[14/14] Testing Excel Report Generation..."
python3 parse_logs_to_excel.py > /dev/null 2>&1
if [ -f "grid_sentinel_logs.xlsx" ] && [ -s "grid_sentinel_logs.xlsx" ]; then
    SIZE=$(du -h grid_sentinel_logs.xlsx | cut -f1)
    echo "✅ PASS: Excel report generated ($SIZE)"
else
    echo "❌ FAIL: Excel report not generated"
fi

echo -e "\n=================================================="
echo "🏁 TEST SUITE COMPLETE"
echo "=================================================="
```

Save as `comprehensive_test.sh` and run:
```bash
chmod +x comprehensive_test.sh
./comprehensive_test.sh
```

---

## 15. TEST REPORTING

After running tests, create summary:

```markdown
# Test Results - [Date]

## Summary
- Total Tests: 14
- Passed: X/14
- Failed: 0/14
- Success Rate: 100%

## Infrastructure
- ✓ All 11 services running
- ✓ Port mappings verified
- ✓ Connectivity confirmed

## Data Pipeline
- ✓ gRPC communication functional
- ✓ Kafka topics created and active
- ✓ Consumer groups healthy
- ✓ PostgreSQL persisting data
- ✓ Redis cache operational

## Business Logic
- ✓ Anomaly detection working
- ✓ Commands executing
- ✓ Acknowledgments received
- ✓ Audit trail recorded

## Quality
- ✓ DLQ isolation functional
- ✓ Error handling graceful
- ✓ Logs captured cleanly
- ✓ Excel reports professional

## Performance
- Throughput: X events/second
- Latency: Y seconds (detection to action)
- Resource Usage: Z% CPU, W MB RAM

## Recommendations
- [Any issues found]
- [Performance improvements]
- [Next steps]
```

---

## Test Execution Timeline

| Phase | Duration | Purpose |
|-------|----------|---------|
| Setup | 30s | Start 11 services, wait for initialization |
| Warm-up | 30s | Allow services to fully connect |
| Collection | 60s | Observe normal operation, collect data |
| Analysis | 60s | Verify all components working |
| Stress | 30s | Optional: sustained load testing |
| Reporting | 30s | Generate report and summary |
| **Total** | **~4 minutes** | Full test cycle |

---

## Success Criteria Summary

✅ **Infrastructure**: All 11 services UP
✅ **Communication**: gRPC, Kafka, PostgreSQL, Redis all working
✅ **Detection**: ≥1 anomaly in 60 seconds
✅ **Response**: ≥1 command executed and ACKed
✅ **Persistence**: ≥1 audit log recorded
✅ **Isolation**: DLQ monitoring operational
✅ **Reporting**: Excel with clean data format
✅ **Performance**: Latency < 7 seconds, throughput > 2 events/sec

If all criteria met: **✅ SYSTEM PRODUCTION-READY**
