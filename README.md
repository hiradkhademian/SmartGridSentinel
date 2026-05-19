# SmartGrid Sentinel

A **distributed, event-driven microservices architecture** for real-time smart grid monitoring, anomaly detection, and automated control. Built with gRPC, Apache Kafka, Redis, PostgreSQL, and Python asyncio for high-throughput telemetry processing and grid-wide situational awareness.

---

## 📋 Table of Contents

1. [System Overview](#system-overview)
2. [Architecture & Design Patterns](#architecture--design-patterns)
3. [Components](#components)
4. [Data Model](#data-model)
5. [Communication Flows](#communication-flows)
6. [Setup & Deployment](#setup--deployment)
7. [Running the System](#running-the-system)
8. [Tradeoffs & Design Decisions](#tradeoffs--design-decisions)
9. [Troubleshooting](#troubleshooting)

---

## System Overview

**SmartGrid Sentinel** is a **real-time grid intelligence platform** that:

- **Ingests** high-frequency telemetry from thousands of distributed smart meters via gRPC
- **Analyzes** voltage spikes, consumption anomalies, and regional overload patterns in-flight
- **Persists** audit logs and command histories to PostgreSQL
- **Caches** sliding-window statistics in Redis for low-latency regional trend computation
- **Routes** faults and malformed payloads to dead letter queues (DLQ) for human recovery
- **Commands** grid actuators (mock engines) to execute corrective actions (power cuts, restarts, throttling)

The system is designed for **zero-downtime operations**, with emphasis on:
- **Resilience**: Retry loops, exponential backoff, DLQ isolation
- **Traceability**: Event IDs, audit logs, command acknowledgment tracking
- **Scalability**: Horizontal scaling via Kafka consumer groups, stateless service instances
- **Observability**: Comprehensive logging at every stage

---

## Architecture & Design Patterns

### 1. **Event-Driven Microservices**

Each service is an independent process that:
- Listens to Kafka topics for domain events
- Applies business logic (analysis, transformation, persistence)
- Publishes derived events to downstream topics
- Never blocks; uses async/await throughout

**Benefits:**
- Services can be deployed, scaled, and updated independently
- Decoupled coupling reduces blast radius of failures
- Natural horizontal scaling via Kafka consumer groups

**Tradeoff:**
- Eventual consistency (no distributed transactions)
- Requires idempotency checks to handle duplicate message delivery
- Operational complexity (more components, monitoring, debugging)

### 2. **Anti-Corruption Layer (Validation)**

The **Ingestion Service** acts as a boundary guard:
- Validates incoming telemetry packets against a schema (voltage > 0, meter_id present)
- Rejects malformed data early before it propagates downstream
- Publishes only canonicalized `TelemetryDomainEvent` messages

**Benefits:**
- Prevents garbage data from polluting the event stream
- Downstream services can assume data integrity
- Failed validations are isolated (sent to DLQ for recovery)

### 3. **Strategy Pattern for Pluggable Analysis**

The **Real-Time Analysis Service** uses pluggable threshold strategies:

```python
class AnalysisStrategy:
    def check(self, event: TelemetryDomainEvent) -> bool:
        raise NotImplementedError

class VoltageSpikeStrategy(AnalysisStrategy):
    def check(self, event: TelemetryDomainEvent) -> bool:
        return event.voltage > 240.0
```

**Benefits:**
- New analysis types can be added without modifying existing code
- Easy to test and compose multiple strategies
- Configuration-driven rule changes

### 4. **Observer Pattern for Control Delivery**

The **Action Gateway** uses Observers to manage bidirectional gRPC channels:

```python
class GridControlObserver:
    async def update(self, command: GridControlCommand) -> bool:
        # Retry logic, backoff, ACK handling
```

**Benefits:**
- Loose coupling between command generation and delivery
- Supports multiple hardware endpoints in the future
- Centralized retry and error handling

### 5. **Idempotency & Audit Logging**

Every command and event carries a **unique `event_id`** (UUID):
- Commands are logged to PostgreSQL before delivery
- Duplicate command IDs are detected and rejected
- ACK timestamps are recorded for post-mortem analysis

**Benefits:**
- Safe to retry network calls without fear of duplicate execution
- Complete audit trail for grid operations and compliance
- Easy incident reconstruction

### 6. **Dead Letter Queue (DLQ) Isolation**

Malformed or unprocessable messages are routed to dedicated DLQ topics:
- `telemetry-dlq`: Failed ingestion/parsing
- `trend-region-dlq`: Failed regional analysis
- `emergency-alerts-dlq`: Failed action gateway processing

**Benefits:**
- Prevents poison pills from crashing services
- Humans can inspect and manually recover messages
- System remains online even if a subset of messages fail

### 7. **Sliding-Window Aggregation with Redis**

The **Trend & Regional Analysis Service** maintains a Redis-backed sliding window:
- Pushes each consumption rate into a per-region list
- Trims to keep only the last 15 observations
- Computes rolling average for trend detection

**Benefits:**
- O(1) per-event update and query
- Ephemeral (no database persistence overhead)
- Can detect sustained anomalies vs. transient spikes

---

## Components

### **1. Ingestion Service** (`services/ingestion/main.py`)

**Role:** Entry point for all telemetry data

**Inputs:**
- gRPC streaming telemetry from mock engines and real meters

**Outputs:**
- `telemetry-stream` (Kafka): Validated `TelemetryDomainEvent` messages
- `telemetry-dlq` (Kafka): Malformed or rejected packets

**Key Functions:**
- `StreamTelemetry()`: gRPC handler that validates incoming packets and publishes to Kafka
  - Validates: `meter_id` is present, `voltage > 0`
  - Generates unique `event_id` (UUID) for idempotency
  - Routes debug/malformed test packets to DLQ
  - Returns `IngestionResponse` to client

**Configuration:**
- `KAFKA_BROKER`: Broker address (env: `KAFKA_BROKER`, default: `localhost:9092`)
- `GRPC_PORT`: Server port (`[::]:50051`)
- `KAFKA_TOPIC`: `telemetry-stream`
- `DLQ_TOPIC`: `telemetry-dlq`

**Deployment:**
- Python 3.12 venv
- Dependencies: `grpcio==1.62.1`, `aiokafka==0.10.0`, `protobuf==4.25.3`

---

### **2. Mock Engine** (`services/mock_engine/simulator.py`)

**Role:** Simulates distributed smart meters for testing and development

**Inputs:**
- gRPC control commands from Action Gateway

**Outputs:**
- gRPC streaming telemetry to Ingestion Service
- Control Server listening for incoming commands

**Key Functions:**
- `run_mock_engine(meter_id, stub)`: Continuously streams telemetry for a single meter
  - Generates realistic voltage (220–235V, baseline), current (5–15A)
  - Injects 4% overvoltage anomalies (242–255V) for testing
  - Sends `TelemetryPacket` to Ingestion Service every 2 seconds
  - Logs errors if ingestion rejects the packet

- `start_control_server()`: gRPC server listening on `[::]:50052`
  - Receives `GridControlCommand` messages (CUT_POWER, RESTART_METER, THROTTLE_CONSUMPTION)
  - Logs command execution (simulated with 100ms delay)
  - Returns `CommandAcknowledgement` to confirm execution

**Configuration:**
- `INGESTION_SERVER_ADDR`: Ingestion service gRPC endpoint (env: `INGESTION_SERVER_ADDR`, default: `localhost:50051`)
- `CONTROL_PORT`: Server listen address (`[::]:50052`)
- `MOCK_METER_IDS`: List of simulated meter identifiers

**Deployment:**
- Python 3.12 venv
- Dependencies: `grpcio==1.62.1`, `grpcio-tools==1.62.1`, `protobuf==4.25.3`
- Command: `python simulator.py`

---

### **3. Real-Time Analysis Service** (`services/real_time_analysis/main.py`)

**Role:** Detects immediate grid anomalies (voltage spikes, emergency conditions)

**Inputs:**
- `telemetry-stream` (Kafka): Raw telemetry events

**Outputs:**
- `emergency-alerts` (Kafka): Alert events for threshold breaches
- `telemetry-dlq` (Kafka): Parsing failures

**Key Functions:**
- `GridAnalyzer`: Strategy-based anomaly detector
  - `VoltageSpikeStrategy.check()`: Returns `True` if voltage > 240.0V
  - Extensible for additional strategies (current overload, frequency drift, etc.)

- `process_message(msg, analyzer, producer)`: Main event handler
  - Deserializes `TelemetryDomainEvent` from Kafka message
  - Runs through all strategies
  - If anomaly detected, emits `EmergencyAlertEvent` to `emergency-alerts` topic
  - Implements retry logic (3 attempts with exponential backoff)
  - Routes parsing failures to DLQ

**Configuration:**
- `KAFKA_BROKER`: Broker address (env: `KAFKA_BROKER`, default: `localhost:9092`)
- `SRC_TOPIC`: `telemetry-stream`
- `ALERT_TOPIC`: `emergency-alerts`
- `DLQ_TOPIC`: `telemetry-dlq`

**Deployment:**
- Python 3.12 venv
- Dependencies: `aiokafka==0.10.0`, `grpcio==1.62.1`, `grpcio-tools==1.62.1`, `protobuf==4.25.3`

**Scaling:**
- Multiple instances form a consumer group; each processes a subset of partitions
- Kafka automatically load-balances as instances join/leave

---

### **4. Trend & Regional Analysis Service** (`services/trend_regional_analysis/main.py`)

**Role:** Detects sustained regional anomalies (overload, consumption patterns)

**Inputs:**
- `telemetry-stream` (Kafka): Raw telemetry events
- Redis: Sliding-window state per region

**Outputs:**
- `trend-region-events` (Kafka): Regional trend alerts
- `trend-region-dlq` (Kafka): Processing failures

**Key Functions:**
- `process_trends(event, redis_client, producer)`: Stateful aggregation
  - Groups meters into a region (currently hardcoded to `ZONE-ALPHA`)
  - Uses Redis pipeline to atomically:
    1. Push consumption rate to `region:{region_id}:consumption_window` list
    2. Trim list to last 15 entries (sliding window)
    3. Fetch all values and compute rolling average
  - Emits `TrendRegionEvent` if rolling average > 3.2 kW threshold
  - Logs rolling average and current meter contribution

- `manage_message_cycle(msg, redis_client, producer)`: Error handling wrapper
  - Retry loop with exponential backoff
  - Routes persistent failures to DLQ

**Configuration:**
- `KAFKA_BROKER`: Broker address (env: `KAFKA_BROKER`, default: `localhost:9092`)
- `SRC_TOPIC`: `telemetry-stream`
- `TREND_TOPIC`: `trend-region-events`
- `DLQ_TOPIC`: `trend-region-dlq`
- `REDIS_URL`: Redis connection (env: `REDIS_URL`, default: `redis://localhost:6379`)
- `SLIDING_WINDOW_SIZE`: 15 observations
- `REGIONAL_OVERLOAD_THRESHOLD`: 3.2 kW

**Deployment:**
- Python 3.12 venv
- Dependencies: `aiokafka==0.10.0`, `redis==5.0.4`, `protobuf==4.25.3`

**Tradeoffs:**
- Redis-backed state is ephemeral (not persisted); lost on restart
  - Benefit: Low latency, no database overhead
  - Cost: Loses historical context across restarts
  - Mitigation: Can replay Kafka offset to rebuild state if needed

---

### **5. Action Gateway** (`services/action_gateway/main.py`)

**Role:** Consumes alerts/trends, persists decisions, dispatches control commands

**Inputs:**
- `emergency-alerts` (Kafka): Voltage spike alerts
- `trend-region-events` (Kafka): Regional overload alerts

**Outputs:**
- PostgreSQL: Command audit logs
- gRPC calls to Mock Engine: Grid control commands

**Key Functions:**
- `CommandAuditLog` (SQLAlchemy model): Audit trail for every command
  - `event_id`: UUID linking to originating alert
  - `target_id`: Meter or region affected
  - `command_type`: CUT_POWER, RESTART_METER, THROTTLE_CONSUMPTION
  - `details`: Human-readable reason (e.g., "Voltage spike 245V > 240V threshold")
  - `executed_at`: Timestamp when command was sent
  - `ack_received`: Timestamp when hardware acknowledged

- `GridControlObserver`: Observer pattern for hardware channel management
  - `update(command)`: Sends command via gRPC to Mock Engine Control Service
  - Retry loop with exponential backoff (up to 5 seconds between retries)
  - Waits for `CommandAcknowledgement` before returning
  - Logs all delivery attempts and failures

- `GridControlSubject`: Subject managing registered observers
  - `notify_observers(command)`: Broadcasts to all registered hardware endpoints

- `is_event_duplicate(session_factory, event_id)`: Idempotency check
  - Queries PostgreSQL to detect duplicate command IDs
  - Prevents re-execution of already-processed alerts

**Configuration:**
- `KAFKA_BROKER`: Broker address (env: `KAFKA_BROKER`, default: `localhost:9092`)
- `ALERT_TOPIC`: `emergency-alerts`
- `TREND_TOPIC`: `trend-region-events`
- `MOCK_ENGINE_CONTROL_ADDR`: Target for control commands (env: `MOCK_ENGINE_CONTROL_ADDR`, default: `localhost:50052`)
- `DATABASE_URL`: PostgreSQL connection (env: `DATABASE_URL`)
  - Format: `postgresql+asyncpg://sentinel_admin:sentinel_password@localhost:5432/smartgrid_db`

**Deployment:**
- Python 3.12 venv
- Dependencies: `aiokafka==0.10.0`, `grpcio==1.62.1`, `grpcio-tools==1.62.1`, `sqlalchemy==2.0.29`, `asyncpg==0.29.0`, `protobuf==4.25.3`

**Scaling:**
- Postgres becomes the bottleneck at scale (audit logging)
  - Mitigation: Batch writes, async I/O
  - Alternative: Log to Kafka + async write to DB
- Single consumer group (no parallelism within a service instance)
  - Benefit: Idempotency + ordering guarantees
  - Cost: Throughput limited to single consumer

---

### **6. Infrastructure Components**

#### **Apache Kafka** (Message Backbone)
- **Topics:**
  - `telemetry-stream`: Ingestion → Real-Time Analysis, Trend Analysis
  - `emergency-alerts`: Real-Time Analysis → Action Gateway
  - `trend-region-events`: Trend Analysis → Action Gateway
  - `telemetry-dlq`, `trend-region-dlq`, `emergency-alerts-dlq`: Dead letter queues
- **Broker:** Listens on `localhost:9092` (outside Docker) or `kafka:29092` (inside Docker)
- **Consumer Groups:**
  - `real-time-analysis-group`: Real-Time Analysis Service
  - `trend-analysis-group`: Trend & Regional Analysis Service
  - Action Gateway reads directly from topics (no consumer group)

#### **Redis** (Ephemeral State Cache)
- Stores sliding-window consumption rates for regional trend detection
- Keys: `region:{region_id}:consumption_window` (list data structure)
- No persistence; state lost on restart
- Accessed by Trend & Regional Analysis Service for low-latency aggregations

#### **PostgreSQL** (Persistent Store)
- Database: `smartgrid_db`
- Credentials: `sentinel_admin` / `sentinel_password`
- Table: `command_audit_logs`
  - `event_id` (PK): Unique command identifier
  - `target_id`: Meter or region ID
  - `command_type`: Command enum (CUT_POWER, etc.)
  - `details`: Reason/description
  - `executed_at`: Timestamp
  - `ack_received`: ACK timestamp or NULL

#### **Zookeeper** (Kafka Coordination)
- Used by Kafka broker for consensus and leader election
- Accessible on `localhost:2181`
- Typically not accessed directly by services

---

## Data Model

### **Protocol Buffers (gRPC/Kafka Messages)**

See `proto/telemetry.proto` for the complete schema.

#### **Ingestion Path**

```protobuf
message TelemetryPacket {
  string meter_id = 1;
  double voltage = 2;
  double current = 3;
  double consumption_rate = 4;
  int64 timestamp = 5;
}

message IngestionResponse {
  bool success = 1;
  string message = 2;
}
```

#### **Canonical Domain Event**

```protobuf
message TelemetryDomainEvent {
  string event_id = 1;          // UUID for idempotency
  string meter_id = 2;
  double voltage = 3;
  double current = 4;
  double consumption_rate = 5;
  int64 timestamp = 6;
}
```

#### **Analysis Events**

```protobuf
message EmergencyAlertEvent {
  string event_id = 1;
  string meter_id = 2;
  string alert_type = 3;        // "VoltageSpikeDetected", "EmergencyDetected"
  double trigger_value = 4;     // e.g., voltage reading
  int64 timestamp = 5;
}

message TrendRegionEvent {
  string event_id = 1;
  string region_id = 2;
  string anomaly_type = 3;      // "RegionalOverloadDetected", etc.
  double moving_average = 4;
  int64 timestamp = 5;
}
```

#### **Control Commands**

```protobuf
enum CommandType {
  UNKNOWN = 0;
  CUT_POWER = 1;
  RESTART_METER = 2;
  THROTTLE_CONSUMPTION = 3;
}

message GridControlCommand {
  string command_id = 1;        // UUID linking to originating event
  string meter_id = 2;
  CommandType type = 3;
  string details = 4;
}

message CommandAcknowledgement {
  string command_id = 1;
  string meter_id = 2;
  bool success = 3;
  int64 ack_timestamp = 4;
}
```

---

## Communication Flows

### **1. Data Ingestion Flow**

```
Mock Engine (meter METER-01H)
    ↓ [gRPC] TelemetryPacket(voltage=245V, ...)
Ingestion Service
    ↓ Validates & enriches with event_id
    ↓ Serializes TelemetryDomainEvent
    ↓ [Kafka] publish to telemetry-stream
Kafka Broker
```

### **2. Anomaly Detection Flow (Real-Time)**

```
Kafka telemetry-stream
    ↓ [Kafka Consumer] TelemetryDomainEvent (voltage=245V)
Real-Time Analysis Service
    ↓ Applies VoltageSpikeStrategy (245V > 240V → True)
    ↓ Generates EmergencyAlertEvent
    ↓ [Kafka] publish to emergency-alerts
Kafka Broker
    ↓
Action Gateway
    ↓ Detects duplicate event_id? → Yes → Skip
    ↓ Log CommandAuditLog to PostgreSQL
    ↓ [gRPC] Send GridControlCommand to Mock Engine
Mock Engine
    ↓ Simulates hardware action
    ↓ [gRPC] Return CommandAcknowledgement
Action Gateway
    ↓ Update ack_received timestamp in PostgreSQL
```

### **3. Trend Detection Flow (Regional)**

```
Kafka telemetry-stream (multiple meters per second)
    ↓ [Kafka Consumer] TelemetryDomainEvent
Trend & Regional Analysis Service
    ↓ Push consumption_rate to Redis window for region
    ↓ Compute rolling average (15-entry window)
    ↓ Check if avg > 3.2 kW → True
    ↓ Generates TrendRegionEvent
    ↓ [Kafka] publish to trend-region-events
Kafka Broker
    ↓
Action Gateway
    ↓ Similar to above (duplicate check, audit, command dispatch)
```

### **4. Error & Fault Isolation**

```
If parsing/validation fails at any stage:
    ↓ Service logs error + reason
    ↓ Routes raw payload to service-specific DLQ topic
    ↓ telemetry-dlq, trend-region-dlq, emergency-alerts-dlq
    ↓ Message remains for manual inspection and recovery
    ↓ Service continues processing other messages
```

---

## Setup & Deployment

### **Local Development Setup**

#### **Prerequisites**

- Python 3.12+ (required for gRPC wheels; Python 3.13 not fully supported)
- Homebrew (macOS): `brew install python@3.12`
- Docker Desktop (for running Kafka, Redis, PostgreSQL)

#### **Step 1: Clone & Navigate**

```bash
cd /Users/hiradkhademian/Desktop/smartgrid-sentinel
```

#### **Step 2: Start Infrastructure**

```bash
docker compose build
docker compose up -d
```

This starts:
- Zookeeper on `2181`
- Kafka on `9092` (localhost) / `29092` (internal Docker)
- Redis on `6379`
- PostgreSQL on `5432` (internal to the Compose network)

Verify:

```bash
docker compose ps
docker compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
docker compose exec redis redis-cli PING
docker compose exec postgres psql -U sentinel_admin -d smartgrid_db
```

#### **Step 3: Set Up Each Service Venv**

Each service requires Python 3.12 and dependencies. Example for **Ingestion**:

```bash
cd services/ingestion
/opt/homebrew/bin/python3.12 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. ../../proto/telemetry.proto
```

**Repeat for all services:**
- `services/mock_engine`
- `services/real_time_analysis`
- `services/trend_regional_analysis`
- `services/action_gateway`

Or use the batch compile script:

```bash
cd /Users/hiradkhademian/Desktop/smartgrid-sentinel
for d in services/ingestion services/mock_engine services/real_time_analysis services/trend_regional_analysis services/action_gateway; do
  cd "$d"
  source venv/bin/activate
  pip install -r requirements.txt
  python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. ../../proto/telemetry.proto
  deactivate
  cd - >/dev/null
done
```

### **Docker Deployment**

Each service includes a `Dockerfile` (referenced in `docker-compose.yml`):

```yaml
services:
  ingestion-service:
    build: ./services/ingestion
    ports:
      - "50051:50051"
    environment:
      KAFKA_BROKER: "kafka:29092"
    depends_on:
      - kafka
```

To build and run:

```bash
docker compose build
docker compose up -d
```

---

## Running the System

### **Local Development (Manual Start)**

**Terminal 1: Ingestion Service**

```bash
cd services/ingestion
source venv/bin/activate
python main.py
# Output: "Ingestion gRPC Service starting on port [::]:50051..."
```

**Terminal 2: Mock Engine**

```bash
cd services/mock_engine
source venv/bin/activate
python simulator.py
# Output: "Mock Engine Control listening for incoming commands..."
# + "Starting telemetry outbound streaming for METER-01H", etc.
```

**Terminal 3: Real-Time Analysis**

```bash
cd services/real_time_analysis
source venv/bin/activate
python main.py
# Output: "Real-Time Analysis Service running. Listening to 'telemetry-stream'..."
```

**Terminal 4: Trend & Regional Analysis**

```bash
cd services/trend_regional_analysis
source venv/bin/activate
python main.py
# Output: "Trend & Regional Analysis Service running..."
```

**Terminal 5: Action Gateway**

```bash
cd services/action_gateway
source venv/bin/activate
python main.py
# Output: "Action Gateway listening to emergency-alerts and trend-region-events..."
```

Wait ~10 seconds for all services to start, then observe logs:

- Mock Engine will stream telemetry
- Real-Time Analysis will detect ~4% anomalies (voltage spikes)
- Trend Analysis will compute rolling averages
- Action Gateway will dispatch control commands

### **Monitoring Kafka Topics**

**List all topics:**

```bash
docker compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Consume telemetry-stream (raw protobuf, binary output):**

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic telemetry-stream \
  --from-beginning \
  --max-messages 5
```

**Consume emergency-alerts:**

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic emergency-alerts \
  --from-beginning \
  --max-messages 5
```

### **Checking PostgreSQL Audit Logs**

```bash
docker compose exec postgres psql -U sentinel_admin -d smartgrid_db

smartgrid_db=> SELECT * FROM command_audit_logs LIMIT 10;
 event_id | target_id | command_type | details | executed_at | ack_received
----------+-----------+--------------+---------+-------------+-------------
```

### **Checking Redis State**

```bash
docker compose exec redis redis-cli

127.0.0.1:6379> KEYS *
127.0.0.1:6379> LRANGE region:ZONE-ALPHA:consumption_window 0 -1
```

---

## Tradeoffs & Design Decisions

### **1. Python 3.12 Requirement**

**Why:** `grpcio==1.62.1` does not have prebuilt macOS wheels for Python 3.13. Building from source fails due to incompatible Cython code.

**Tradeoff:**
- ✅ Stable, tested gRPC version
- ❌ Limited to Python 3.12 (less future-proof)
- **Alternative:** Upgrade `grpcio` to latest (1.80+) which supports Python 3.13, but requires testing

**Decision:** Use Python 3.12 for all venvs (simple, immediate solution).

---

### **2. Kafka as Event Backbone**

**Why:** High throughput, durable event log, consumer group scalability.

**Tradeoff:**
- ✅ Horizontal scaling (add consumers for parallelism)
- ✅ Fault recovery (replay from offset)
- ✅ Decoupled services
- ❌ Eventual consistency (not immediate)
- ❌ Operational overhead (ZK, broker management)
- ❌ Does not guarantee ordering across topics

**Alternative:** Direct HTTP/gRPC calls between services (simpler, but less resilient).

**Decision:** Kafka for event durability and scalability requirements.

---

### **3. Redis for Sliding-Window State**

**Why:** O(1) operations, low latency, no database overhead.

**Tradeoff:**
- ✅ Instant aggregations (milliseconds)
- ✅ Ephemeral (no persistence overhead)
- ❌ Data lost on restart
- ❌ Single point of failure (no built-in clustering)

**Mitigation:** State can be replayed from Kafka if needed.

**Alternative:** Store in PostgreSQL (persistent but slower).

**Decision:** Redis for sub-second trend detection.

---

### **4. PostgreSQL for Audit Logs**

**Why:** ACID compliance, compliance requirements.

**Tradeoff:**
- ✅ Durability (crash-safe)
- ✅ Compliance (audit trail for regulatory)
- ❌ Slower writes (row-by-row INSERT)
- ❌ Database becomes bottleneck at scale

**Mitigation:** Batch inserts, async I/O.

**Alternative:** Write alerts to Kafka, async sink to DB.

**Decision:** PostgreSQL for legal/compliance audit trail.

---

### **5. gRPC for Synchronous Control**

**Why:** Strongly-typed APIs, efficient binary protocol, bidirectional streaming.

**Tradeoff:**
- ✅ Type safety (protobuf schemas)
- ✅ Efficient (binary, no JSON parsing)
- ✅ Supports streaming
- ❌ Requires protobuf compilation step
- ❌ Not as human-readable as REST/JSON

**Alternative:** HTTP REST (simpler, but less efficient).

**Decision:** gRPC for high-frequency telemetry and command acknowledgments.

---

### **6. Idempotency Keys (UUID event_id)**

**Why:** Ensure safe retry loops without duplicate execution.

**Tradeoff:**
- ✅ Command de-duplication
- ✅ Audit trail linking
- ❌ Storage overhead (UUID string per message)
- ❌ Requires database lookup per command

**Alternative:** At-most-once semantics (no retries, potential message loss).

**Decision:** At-least-once with idempotency for critical grid operations.

---

### **7. Dead Letter Queues**

**Why:** Prevent poison pills from crashing services.

**Tradeoff:**
- ✅ System remains online under partial failures
- ✅ Enables manual recovery
- ❌ Adds operational burden (monitoring DLQ depth, recovery procedures)
- ❌ Delayed visibility into failures

**Alternative:** Fail fast, halt service on bad data.

**Decision:** DLQ isolation for production resilience.

---

### **8. Single Consumer Instance per Service**

**Why:** Simplifies ordering guarantees and idempotency checks.

**Tradeoff:**
- ✅ No distributed state synchronization
- ✅ Guaranteed ordering within a topic partition
- ❌ Single point of failure (if consumer crashes, events queue up)
- ❌ Throughput limited to single consumer

**Mitigation:** Use Kubernetes for auto-restart.

**Alternative:** Multiple consumers per service, distributed state (complex).

**Decision:** Single consumer per service for simplicity.

---

### **9. Synchronous gRPC Commands (Mock Engine)**

**Why:** Immediate feedback on hardware execution.

**Tradeoff:**
- ✅ Confirmation of command success/failure
- ✅ Real-time user feedback
- ❌ Blocks Action Gateway if hardware is slow/offline
- ❌ Requires retry logic and timeout handling

**Mitigation:** Exponential backoff (0.5s → 5s), timeout on requests.

**Alternative:** Fire-and-forget Kafka (async, but no confirmation).

**Decision:** Synchronous with retry for critical grid safety.

---

### **10. Regional Aggregation (Zone-Based)**

**Why:** Simplified model for MVP; real deployments would use geospatial queries.

**Tradeoff:**
- ✅ Simple prototype
- ✅ Fast (single Redis key per zone)
- ❌ Hardcoded to `ZONE-ALPHA`
- ❌ No geographic isolation (all meters in one zone)

**Mitigation:** Can derive zone from meter_id prefix in production.

**Alternative:** Geohashing, grid quadtrees (complex).

**Decision:** Hardcoded zone for MVP.

---

## Troubleshooting

### **Problem: `ModuleNotFoundError: No module named 'grpc_tools'`**

**Cause:** `grpcio-tools` not installed or wrong Python version.

**Solution:**

```bash
pip install grpcio-tools==1.62.1
```

If using Python 3.13:

```bash
# Use Python 3.12 instead
/opt/homebrew/bin/python3.12 -m venv venv
source venv/bin/activate
pip install grpcio-tools==1.62.1
```

---

### **Problem: `KafkaConnectionError: Unable to bootstrap from [('localhost', 9092)]`**

**Cause:** Kafka broker not running or misconfigured.

**Solution:**

```bash
# Start Docker containers
docker compose up -d

# Verify Kafka is running
docker compose ps
docker compose exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

If inside a Docker container, use `kafka:29092` instead of `localhost:9092`.

---

### **Problem: `error: call to undeclared function '_PyInterpreterState_GetConfig'` (asyncpg build fails)**

**Cause:** `asyncpg` 0.29.0 not compatible with Python 3.13; C extension compilation errors.

**Solution:**

Use Python 3.12:

```bash
/opt/homebrew/bin/python3.12 -m venv venv
source venv/bin/activate
pip install asyncpg==0.29.0
```

---

### **Problem: `psycopg2.OperationalError: could not connect to server`**

**Cause:** PostgreSQL not running or incorrect credentials.

**Solution:**

```bash
# Start PostgreSQL
docker compose up -d postgres

# Verify connection
psql -U sentinel_admin -d smartgrid_db -h localhost -c "SELECT 1"
```

---

### **Problem: Mock Engine not receiving control commands**

**Cause:** Action Gateway can't reach Mock Engine gRPC endpoint.

**Solution:**

1. **Verify Mock Engine is running:**

```bash
ps aux | grep simulator.py
```

2. **Check gRPC port availability:**

```bash
lsof -i :50052
```

3. **If inside Docker, update `MOCK_ENGINE_CONTROL_ADDR` env var:**

```yaml
# In docker-compose.yml
action-gateway:
  environment:
    MOCK_ENGINE_CONTROL_ADDR: "mock-engine:50052"  # Use service name, not localhost
```

---

### **Problem: Slow event processing, high latency**

**Cause:** Kafka broker, Redis, or database bottleneck.

**Solution:**

1. **Monitor broker throughput:**

```bash
docker compose exec kafka kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9092 \
  --topic telemetry-stream \
  --messages 10000 \
  --threads 1
```

2. **Check Redis performance:**

```bash
docker compose exec redis redis-cli INFO stats
```

3. **Profile service CPU/memory:**

```bash
# In service terminal, press Ctrl+C and restart with profiling
python -m cProfile -s cumulative main.py | head -30
```

---

## Summary

**SmartGrid Sentinel** demonstrates a production-grade, event-driven microservices architecture for real-time grid intelligence:

- **Telemetry ingestion** at scale via gRPC + Kafka
- **Real-time anomaly detection** with pluggable strategies
- **Regional trend analysis** using Redis sliding windows
- **Automated control dispatch** with idempotency and audit logging
- **Fault isolation** via dead letter queues
- **Horizontal scaling** via Kafka consumer groups
- **Resilience** through retry loops, backoff, and DLQ recovery

The system prioritizes **reliability and observability** over raw throughput, making it suitable for critical infrastructure where data integrity and auditability are paramount.

---

**For questions or contributions, please refer to the individual service `requirements.txt` files and the `proto/telemetry.proto` schema.**
