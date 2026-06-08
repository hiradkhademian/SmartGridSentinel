# SmartGrid Sentinel - Logging & Monitoring Manual

**System Version:** 1.0  
**Last Updated:** June 2026  
**Author:** Engineering Team  

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Detailed Procedures](#detailed-procedures)
5. [Monitoring & Analysis](#monitoring--analysis)
6. [Troubleshooting](#troubleshooting)
7. [API Reference](#api-reference)

---

## System Overview

### What is SmartGrid Sentinel?

SmartGrid Sentinel is a distributed microservices architecture for monitoring smart grid operations. It captures:

- **Telemetry Data** - Voltage, current, power consumption from meters
- **Anomalies** - Real-time detection of grid faults and abnormalities  
- **Commands** - Actions executed on meters (power cut, throttle, restart)
- **Audit Logs** - Complete history of all grid operations

### Architecture Components

| Service | Purpose | Port |
|---------|---------|------|
| **Ingestion Service** | Receives gRPC telemetry from mock engine | :50051 |
| **Mock Engine** | Simulates 4 smart meters with anomalies | :50052 |
| **Real-Time Analysis** | Detects anomalies as they occur | Kafka |
| **Trend & Regional Analysis** | Computes statistical trends per zone | Kafka |
| **Action Gateway** | Executes commands on meters | Kafka |
| **DLQ Monitor** | Handles corrupted/failed messages | Kafka |
| **Management API** | REST/Admin interface | :8000 |

### Infrastructure

| Component | Purpose | Port |
|-----------|---------|------|
| **Kafka** | Event streaming backbone | :29092 |
| **PostgreSQL** | Persistent audit logs & DLQ storage | :5432 |
| **Redis** | Live telemetry caching | :6379 |
| **Zookeeper** | Kafka coordination | :2181 |

---

## Prerequisites

### System Requirements

- **OS:** macOS or Linux
- **Docker:** 20.10+ (`docker --version`)
- **Docker Compose:** 1.29+ (`docker compose version`)
- **Python:** 3.12 or higher (for log parsing)
- **Internet:** Required for pulling container images

### Verify Installation

```bash
# Check Docker
docker --version

# Check Docker Compose
docker compose version

# Check Python
python3 --version
```

### Python Dependencies (Already Installed)

```bash
# openpyxl - Excel file generation
# pandas - Data manipulation
# Both installed during initial setup
```

---

## Quick Start

### Step 1: Navigate to Project Directory

```bash
cd /Users/hiradkhademian/Desktop/smartgrid-sentinel
```

### Step 2: Stop Any Running Services (Cleanup)

```bash
docker compose down --remove-orphans
```

### Step 3: Start System with Automatic Logging

```bash
./run_with_logging.sh
```

**What happens:**
- ✓ All 11 Docker services start
- ✓ Logs automatically captured to `system_logs.txt`
- ✓ Excel report automatically generated

### Step 4: Monitor in Real-Time (Optional)

In a **new terminal tab**, run:

```bash
docker compose logs -f
```

Press `Ctrl+C` to stop monitoring.

### Step 5: Regenerate Excel Report Anytime

```bash
python3 parse_logs_to_excel.py
```

### Step 6: View Results

Files generated in project root:
- `system_logs.txt` - Raw Docker logs (500 KB typical)
- `grid_sentinel_logs.xlsx` - Formatted Excel report

---

## Detailed Procedures

### Procedure A: Full System Startup with Logging

#### Manual Method (For troubleshooting)

**Terminal 1 - Start Infrastructure:**
```bash
cd /Users/hiradkhademian/Desktop/smartgrid-sentinel

# Clear old logs
> system_logs.txt

# Start base infrastructure (Kafka, Redis, PostgreSQL, Zookeeper)
docker compose up -d zookeeper kafka redis postgres
```

Wait 10 seconds for Kafka to be ready.

**Terminal 1 - Start Microservices:**
```bash
# Start all microservices
docker compose up -d ingestion-service mock-engine real-time-analysis \
  trend-regional-analysis action-gateway dlq-monitor management_api
```

**Terminal 2 - Start Log Capture:**
```bash
cd /Users/hiradkhademian/Desktop/smartgrid-sentinel
docker compose logs --timestamps -f > system_logs.txt &
echo "Log capture PID: $!"
```

Let it run for **20-30 seconds** to capture anomalies.

**Terminal 1 - Generate Excel:**
```bash
python3 parse_logs_to_excel.py
```

---

### Procedure B: Monitoring Individual Services

#### Real-Time Analysis (Anomaly Detection)

```bash
docker compose logs -f real-time-analysis
```

**What to look for:**
```
ANOMALY DETECTED: Voltage Spike (245.71V) on METER-03H
ANOMALY DETECTED: CurrentSpikeDetected on METER-02H
```

#### Ingestion Service (Data Intake)

```bash
docker compose logs -f ingestion-service
```

**What to look for:**
```
Ingested & Published event [UUID] for meter METER-04H
```

#### Mock Engine (Hardware Simulation)

```bash
docker compose logs -f mock-engine
```

**What to look for:**
```
ANOMALY GENERATED: VoltageSpikeDetected (245.71V) on METER-02H
COMMAND EXECUTED: CUT_POWER on METER-02H
```

#### Action Gateway (Command Execution)

```bash
docker compose logs -f action-gateway
```

**What to look for:**
```
Command: CUT_POWER | Target: METER-02H | Status: ACK Received
```

---

### Procedure C: Understanding Log Events

#### Event Type: Anomaly Detection

**Log Entry:**
```
real-time-analysis | 14:42:26,123 - INFO - ANOMALY DETECTED: VoltageSpikeDetected (245.71V) on METER-03H
```

**Excel Row:**
| Zaman Damgası | Kaynak Servis | Hedef | Tetikleyici Olay | Alınan Aksiyon | Sonuç |
|---|---|---|---|---|---|
| 14:42:26 | Real-Time Analysis | METER-03H | Voltage Spike (245.71V) | — | — |

---

#### Event Type: Command Execution

**Log Entry:**
```
action-gateway | 14:42:28,456 - INFO - Command: CUT_POWER | Target: METER-02H | Status: ACK Received
```

**Excel Row:**
| Zaman Damgası | Kaynak Servis | Hedef | Tetikleyici Olay | Alınan Aksiyon | Sonuç |
|---|---|---|---|---|---|
| 14:42:28 | Action Gateway | METER-02H | — | CUT_POWER Command | Komut başarıyla iletildi (ACK) |

---

#### Event Type: Power Restoration

**Log Entry:**
```
mock-engine | 14:42:58,789 - INFO - Power restored to METER-02H (Auto-recovery after 30s)
```

**Excel Row:**
| Zaman Damgası | Kaynak Servis | Hedef | Tetikleyici Olay | Alınan Aksiyon | Sonuç |
|---|---|---|---|---|---|
| 14:42:58 | Mock Engine (Hardware) | METER-02H | Auto-recovery Timer | Power Restored | Şebeke gücü otonom olarak geri getirildi. |

---

## Monitoring & Analysis

### Step 1: Collect Sufficient Log Data

Let the system run for **2-5 minutes** to capture:
- Normal operations (97%)
- Anomalies (2%)
- Corrupted packets (1%)

### Step 2: Generate Excel Report

```bash
python3 parse_logs_to_excel.py
```

**Output:**
```
============================================================
SmartGrid Sentinel - Log Parser & Excel Generator
============================================================
✓ Parsed 47 events from logs
✓ Excel file created: /path/to/grid_sentinel_logs.xlsx
📊 Total events logged: 47
✓ Export complete
```

### Step 3: Open Excel Report

```bash
# On macOS
open grid_sentinel_logs.xlsx

# On Linux
libreoffice grid_sentinel_logs.xlsx
```

### Step 4: Analyze Columns

#### Zaman Damgası (Timestamp)
- 24-hour format: `HH:MM:SS`
- Use for chronological analysis
- Sort to find event sequences

#### Kaynak Servis (Source Service)
- **Ingestion Service** - Data intake
- **Real-Time Analysis** - Anomaly detection
- **Trend & Regional Analysis** - Statistical analysis
- **Action Gateway** - Command execution
- **Mock Engine** - Hardware simulation

#### Hedef (Target/Device)
- `METER-01H` to `METER-04H` - Individual meters
- `ZONE-ALPHA` - Regional aggregate
- `Şebeke Geneli` - Network-wide

#### Tetikleyici Olay (Triggering Event)
- Anomalies detected: Voltage/Current spikes, blackouts
- Chaos tests: Corrupted packets
- Auto-recovery: Power restoration timers

#### Alınan Aksiyon (Action Taken)
- `CUT_POWER` - Emergency disconnection
- `RESTART_METER` - Reset device
- `THROTTLE_CONSUMPTION` - Load reduction
- `DLQ Isolation` - Corrupted message handling

#### Sonuç (Result/Status)
- Turkish descriptions of outcomes
- Green text = Success
- Red text = Error
- Blue text = Status update

---

### Step 5: Generate Insights

**Example Query 1: Find all voltage spikes**

Filter column D (Tetikleyici Olay) for: `Voltage Spike`

**Example Query 2: Find meter METER-03H events**

Filter column C (Hedef) for: `METER-03H`

**Example Query 3: Find failed commands**

Filter column F (Sonuç) containing: `hata` or `error`

---

## Troubleshooting

### Issue 1: Excel Shows "0 events parsed"

**Symptom:**
```
✓ Parsed 0 events from logs
```

**Cause:** System just started, no anomalies generated yet

**Solution:**
```bash
# Wait 30 seconds for anomalies to occur
sleep 30

# Regenerate Excel
python3 parse_logs_to_excel.py
```

---

### Issue 2: Services fail to start (Kafka connection error)

**Symptom:**
```
Unable to bootstrap from [('kafka', 29092, <AddressFamily.AF_UNSPEC: 0>)]
```

**Cause:** Kafka not ready yet (race condition)

**Solution:**
```bash
# Stop everything
docker compose down

# Start infrastructure first
docker compose up -d zookeeper kafka redis postgres

# Wait 15 seconds
sleep 15

# Start microservices
docker compose up -d ingestion-service mock-engine real-time-analysis \
  trend-regional-analysis action-gateway dlq-monitor
```

---

### Issue 3: Docker container out of disk space

**Symptom:**
```
Error response from daemon: mkdir /var/lib/docker/...
```

**Solution:**
```bash
# Clean up old containers and images
docker compose down --remove-orphans
docker system prune -a --volumes
```

---

### Issue 4: Logs not being captured

**Symptom:** `system_logs.txt` remains empty or very small

**Solution:**
```bash
# Manually start log capture
docker compose logs --timestamps -f > system_logs.txt &

# Let it run for 30 seconds
sleep 30

# Check file size
ls -lh system_logs.txt

# Should be >100KB for normal operation
```

---

### Issue 5: Excel file won't open

**Symptom:** "File is corrupted" or "Cannot open file"

**Cause:** Parser crashed during Excel generation

**Solution:**
```bash
# Remove corrupted file
rm grid_sentinel_logs.xlsx

# Regenerate
python3 parse_logs_to_excel.py

# If still fails, check Python installation
python3 -c "import openpyxl; import pandas; print('OK')"
```

---

## API Reference

The Management API provides programmatic access to grid data.

### Base URL

```
http://localhost:8000
```

### Endpoint 1: System Health Check

**Request:**
```bash
curl http://localhost:8000/healthz
```

**Response (200 OK):**
```json
{
  "api_gateway": "healthy",
  "database": "healthy",
  "redis_cache": "healthy"
}
```

**Response (503 Service Unavailable):**
```json
{
  "detail": {
    "message": "Grid operational failure inside dependencies.",
    "report": {
      "database": "unhealthy",
      "redis_cache": "healthy"
    }
  }
}
```

---

### Endpoint 2: Regional Telemetry

**Request:**
```bash
curl http://localhost:8000/api/v1/grid/trends/ZONE-ALPHA
```

**Response:**
```json
{
  "region_id": "ZONE-ALPHA",
  "data_points_analyzed": 42,
  "raw_sliding_window_kw": [1.85, 1.92, 1.88, 1.95, ...],
  "rolling_consumption_avg_kw": 1.89,
  "status": "OPERATIONAL_STABLE"
}
```

---

### Endpoint 3: Historical Incidents

**Request:**
```bash
curl "http://localhost:8000/api/v1/grid/incidents?target_id=METER-03H&limit=10"
```

**Response:**
```json
[
  {
    "event_id": "evt-12345",
    "target_id": "METER-03H",
    "command_type": "RESTART_METER",
    "details": "Anomaly detected, device restarted",
    "executed_at": "2026-06-08T14:42:26"
  },
  ...
]
```

---

### Endpoint 4: Dead Letter Queue (DLQ) - List Pending

**Request:**
```bash
curl http://localhost:8000/api/v1/admin/dlq/pending?limit=5
```

**Response:**
```json
[
  {
    "log_id": 1,
    "origin_topic": "telemetry-dlq",
    "raw_payload_preview": "b'{\"meter\": \"METER-01H\", ...}'",
    "isolated_at": "2026-06-08T14:42:00"
  }
]
```

---

### Endpoint 5: Dead Letter Queue (DLQ) - Replay Event

**Request:**
```bash
curl -X POST http://localhost:8000/api/v1/admin/dlq/replay/1 \
  -H "Content-Type: application/json" \
  -d '{"corrected_payload": null}'
```

**Response:**
```json
{
  "status": "SUCCESSFUL_RECOVERY_REPLAY",
  "log_id": 1,
  "replayed_to_topic": "telemetry-stream",
  "message": "Isolated payload successfully re-injected into the live event mesh."
}
```

---

## Complete Command Reference

### Start System
```bash
./run_with_logging.sh                    # Automated (recommended)
docker compose up -d                     # Manual start all
docker compose up -d <service>           # Start single service
```

### Monitor System
```bash
docker compose logs -f                   # All services
docker compose logs -f <service>         # Single service
tail -f system_logs.txt                  # Raw logs file
```

### Generate Reports
```bash
python3 parse_logs_to_excel.py           # Create/update Excel
```

### Stop System
```bash
docker compose down                      # Stop all services
docker compose down --remove-orphans     # Stop + cleanup
```

### Check Status
```bash
docker compose ps                        # Running services
curl http://localhost:8000/healthz       # API health
```

### Cleanup
```bash
docker system prune -a --volumes         # Remove all stopped containers
rm system_logs.txt grid_sentinel_logs.xlsx  # Remove old files
```

---

## Engineering Notes

### Log Capture Mechanism

The system uses two methods:

**Method 1: Docker Compose Logs (Real-time)**
```bash
docker compose logs --timestamps -f > system_logs.txt &
```
- Captures stdout/stderr from all containers
- Includes Docker-generated timestamps
- Runs in background

**Method 2: Log Parser (Batch Processing)**
```bash
python3 parse_logs_to_excel.py
```
- Parses raw text logs using regex
- Extracts structured fields (timestamp, service, target, etc.)
- Generates formatted Excel report

### Excel Generation Process

1. **Read** `system_logs.txt`
2. **Pattern Match** for:
   - Timestamps: `HH:MM:SS`
   - Service names: ingestion, real-time-analysis, etc.
   - Device IDs: METER-01H, ZONE-ALPHA, etc.
   - Event keywords: ANOMALY, COMMAND, ACK, etc.
3. **Extract** structured fields
4. **Format** with:
   - Blue header row (#1F4E78)
   - Alternating row colors (light blue)
   - Borders and alignment
   - Conditional text colors
   - Frozen header row
5. **Write** to `grid_sentinel_logs.xlsx`

### Performance Notes

- System generates **~2 telemetry events per second** per meter (8 total)
- **~2% anomaly rate** = 1 anomaly every 25 seconds
- **~1% corruption rate** = 1 corrupted packet every 100 seconds
- Excel generation: **<1 second** for typical 50-event log

---

## Support & Contact

For issues or questions:
1. Check **Troubleshooting** section above
2. Review logs: `docker compose logs <service>`
3. Check Docker running: `docker ps`
4. Verify network: `docker network ls`

---

**End of Manual**
