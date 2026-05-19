import asyncio
import logging
import json
import datetime
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import create_async_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KAFKA_BROKER = "kafka:29092"
DATABASE_URL = "postgresql+asyncpg://sentinel_admin:sentinel_password@postgres:5432/smartgrid_db"

# The 4 explicit isolation targets mapped straight from the project proposal design
DLQ_TOPICS = [
    "telemetry-dlq",
    "emergency-alerts-dlq",
    "trend-region-dlq",
    "action-gateway-dlq"
]

async def initialize_database(engine):
    """Guarantees the diagnostic schema exists for historical debugging analysis."""
    async with engine.begin() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dead_letter_logs (
                id SERIAL PRIMARY KEY,
                origin_topic VARCHAR(100) NOT NULL,
                payload_bytes BYTEA NOT NULL,
                isolated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved BOOLEAN DEFAULT FALSE
            );
        """)
    logging.info("🏛️ DLQ diagnostic database schema validated.")

async def process_dlq_message(msg, engine):
    """Intercepts isolated failures and persists them for manual administrative review."""
    topic = msg.topic
    raw_payload = msg.value
    
    logging.error(
        f"🚨 [DLQ ALERT] Intercepted corrupted or unprocessable event from topic: '{topic}'! "
        f"Payload Size: {len(raw_payload)} bytes. Isolating record..."
    )
    
    # Persist directly into PostgreSQL via raw text bindings to keep it lightweight and fast
    async with engine.begin() as conn:
        await conn.execute(
            "INSERT INTO dead_letter_logs (origin_topic, payload_bytes) VALUES ($1, $2)",
            (topic, raw_payload)
        )
    logging.info(f"💾 Successfully isolated '{topic}' failure payload to persistence layer.")

async def main():
    logging.info("🚀 Starting SmartGrid Sentinel DLQ Diagnostic Service...")
    engine = create_async_engine(DATABASE_URL, echo=False)
    
    await initialize_database(engine)
    
    # Initialize the composite consumer to monitor all DLQ vectors concurrently
    consumer = AIOKafkaConsumer(
        *DLQ_TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        group_id="dlq-monitoring-cluster"
    )
    
    await consumer.start()
    logging.info(f"📥 DLQ Core active. Subscribed to tracking targets: {DLQ_TOPICS}")
    
    try:
        async for msg in consumer:
            await process_dlq_message(msg, engine)
    except Exception as e:
        logging.critical(f"DLQ Consumer loop encountered an unrecoverable fault: {str(e)}")
    finally:
        await consumer.stop()
        await engine.dispose()
        logging.info("🛑 DLQ Monitoring Service stopped safely.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass