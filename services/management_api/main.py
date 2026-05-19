import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Configuration Constants ---
DATABASE_URL = "postgresql+asyncpg://sentinel_admin:sentinel_password@postgres:5432/smartgrid_db"
REDIS_URL = "redis://redis:6379"
KAFKA_BROKER = "kafka:29092"
API_PORT = 8000

# Central lifecycle state cache
state_engines = {
    "db_engine": None,
    "session_factory": None,
    "redis_client": None,
    "kafka_producer": None
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages structural connection lifecycles asynchronously during application boot & shutdown."""
    logging.info("🚀 Booting SmartGrid Sentinel FastAPI Management & Recovery Gateway...")
    
    # Initialize infrastructure connection pools concurrently
    state_engines["db_engine"] = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
    state_engines["session_factory"] = async_sessionmaker(state_engines["db_engine"], expire_on_commit=False)
    state_engines["redis_client"] = aioredis.from_url(REDIS_URL)
    
    # Initialize the administrative event replay producer
    state_engines["kafka_producer"] = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await state_engines["kafka_producer"].start()
    logging.info("📟 Administrative System Recovery Kafka Producer initialized.")
    
    yield  # API Servicing Execution Boundary
    
    logging.info("🛑 Tearing down Management API infrastructure resources...")
    await state_engines["kafka_producer"].stop()
    await state_engines["redis_client"].close()
    await state_engines["db_engine"].dispose()


app = FastAPI(
    title="SmartGrid Sentinel - Operational Management & Recovery System API",
    version="1.1.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 1. CORE HEALTH INTERFACES (Liveness & Readiness Probes) ---
@app.get("/healthz", status_code=status.HTTP_200_OK, tags=["Infrastructure"])
async def check_system_integrity() -> Dict[str, str]:
    """Evaluates dependencies across backing data layers to provide deep cluster status telemetry."""
    status_report = {"api_gateway": "healthy", "database": "unhealthy", "redis_cache": "unhealthy"}
    
    try:
        await state_engines["redis_client"].ping()
        status_report["redis_cache"] = "healthy"
    except Exception as re:
        logging.error(f"Healthz Check Failure on Redis: {str(re)}")
        
    try:
        async with state_engines["db_engine"].connect() as conn:
            await conn.execute(text("SELECT 1"))
        status_report["database"] = "healthy"
    except Exception as dbe:
        logging.error(f"Healthz Check Failure on PostgreSQL: {str(dbe)}")

    if "unhealthy" in status_report.values():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Grid operational failure inside dependencies.", "report": status_report}
        )
        
    return status_report


# --- 2. LIVE STATE ANALYSIS METRICS (Reads from Redis Cache) ---
@app.get("/api/v1/grid/trends/{region_id}", tags=["Live Grid Telemetry"])
async def get_regional_sliding_metrics(region_id: str) -> Dict:
    """Queries live telemetry caches directly to pull active time-series sliding metrics."""
    redis_client: aioredis.Redis = state_engines["redis_client"]
    region_window_key = f"region:{region_id}:consumption_window"
    
    try:
        raw_values = await redis_client.lrange(region_window_key, 0, -1)
        if not raw_values:
            return {
                "region_id": region_id,
                "data_points_analyzed": 0,
                "rolling_consumption_avg_kw": 0.0,
                "status": "No Active Metrics Streamed"
            }
            
        window_values = [float(val.decode('utf-8')) for val in raw_values]
        rolling_avg = sum(window_values) / len(window_values)
        
        return {
            "region_id": region_id,
            "data_points_analyzed": len(window_values),
            "raw_sliding_window_kw": window_values,
            "rolling_consumption_avg_kw": round(rolling_avg, 3),
            "status": "CRITICAL ANOMALY OVERLOAD" if rolling_avg > 3.2 else "OPERATIONAL_STABLE"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query cache state: {str(e)}"
        )


# --- 3. AUDIT & LOGGING FORENSICS (Reads from PostgreSQL Engine) ---
@app.get("/api/v1/grid/incidents", tags=["Historical Forensics"])
async def fetch_historical_incidents(
    target_id: Optional[str] = Query(None, description="Filter for a specific target ID (Meter ID or Region ID)"),
    command_type: Optional[str] = Query(None, description="Filter for specific commands (e.g., CutPowerCommand)"),
    limit: int = Query(20, le=100)
) -> List[Dict]:
    """Queries persistent relational engines to pull immutable mitigation logs compiled by the Action Gateway."""
    session_factory = state_engines["session_factory"]
    
    async with session_factory() as session:
        try:
            query_str = "SELECT event_id, target_id, command_type, details, executed_at FROM command_audit_logs"
            conditions = []
            params = {"limit": limit}
            
            if target_id:
                conditions.append("target_id = :target_id")
                params["target_id"] = target_id
            if command_type:
                conditions.append("command_type = :command_type")
                params["command_type"] = command_type
                
            if conditions:
                query_str += " WHERE " + " AND ".join(conditions)
                
            query_str += " ORDER BY executed_at DESC LIMIT :limit"
            
            result = await session.execute(text(query_str), params)
            rows = result.fetchall()
            
            return [
                {
                    "event_id": row[0],
                    "target_id": row[1],
                    "command_type": row[2],
                    "details": row[3],
                    "executed_at": row[4].isoformat() if row[4] else None
                }
                for row in rows
            ]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database lookup error: {str(e)}")


# --- 4. SYSTEM RECOVERY INTERFACES (Event Replay Core) ---
@app.get("/api/v1/admin/dlq/pending", tags=["Administrative System Recovery"])
async def list_pending_dead_letters(limit: int = Query(20, le=100)) -> List[Dict]:
    """Queries persistent isolated fault stores to inspect payloads holding up the data pipeline[cite: 96, 97]."""
    session_factory = state_engines["session_factory"]
    async with session_factory() as session:
        try:
            query_str = "SELECT id, origin_topic, payload_bytes, isolated_at FROM dead_letter_logs WHERE resolved = FALSE LIMIT :limit"
            result = await session.execute(text(query_str), {"limit": limit})
            rows = result.fetchall()
            
            return [
                {
                    "log_id": row[0],
                    "origin_topic": row[1],
                    "raw_payload_preview": str(row[2]),  # Exposes string format of byte array
                    "isolated_at": row[3].isoformat() if row[3] else None
                }
                for row in rows
            ]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to fetch diagnostic records: {str(e)}")


@app.post("/api/v1/admin/dlq/replay/{log_id}", tags=["Administrative System Recovery"])
async def execute_event_replay(log_id: int, corrected_payload: Optional[str] = None) -> Dict:
    """Implements explicit system recovery by re-injecting isolated event states back into primary streams."""
    session_factory = state_engines["session_factory"]
    producer: AIOKafkaProducer = state_engines["kafka_producer"]
    
    async with session_factory() as session:
        try:
            # Fetch target fault event from PostgreSQL DLQ storage
            query_str = "SELECT origin_topic, payload_bytes FROM dead_letter_logs WHERE id = :log_id AND resolved = FALSE"
            result = await session.execute(text(query_str), {"log_id": log_id})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Target isolated payload not found or already resolved.")
                
            origin_topic, original_payload = row[0], row[1]
            
            # Map the isolated queue name back to its primary recovery event topic counterpart [cite: 98]
            target_replay_topic = "telemetry-stream"
            if origin_topic == "emergency-alerts-dlq":
                target_replay_topic = "emergency-alerts"
            elif origin_topic == "trend-region-dlq":
                target_replay_topic = "trend-region-events"
            elif origin_topic == "action-gateway-dlq":
                target_replay_topic = "emergency-alerts"
                
            # If the operator provides an altered string to correct formatting or parsing bugs, use it 
            final_payload = corrected_payload.encode('utf-8') if corrected_payload else original_payload
            
            # Re-inject directly back into Kafka's persistent stream logs 
            await producer.send_and_wait(target_replay_topic, final_payload)
            
            # Mark item resolved in the relational tracker database
            await session.execute(text("UPDATE dead_letter_logs SET resolved = TRUE WHERE id = :log_id"), {"log_id": log_id})
            await session.commit()
            
            return {
                "status": "SUCCESSFUL_RECOVERY_REPLAY",
                "log_id": log_id,
                "replayed_to_topic": target_replay_topic,
                "message": "Isolated payload successfully re-injected into the live event mesh."
            }
            
        except HTTPException:
            raise
        except Exception as e:
            await session.rollback()
            raise HTTPException(status_code=500, detail=f"Recovery engine execution failure: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=API_PORT, reload=True)