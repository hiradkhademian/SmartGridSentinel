import asyncio
import logging
import json
import datetime
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KAFKA_BROKER = "kafka:29092"
DATABASE_URL = "postgresql+asyncpg://sentinel_admin:sentinel_password@postgres:5432/smartgrid_db"

# The 4 explicit isolation targets mapped straight from the project proposal design

DLQ_HANDLERS = {
    "telemetry-dlq": "Ingestion Service",
    "emergency-alerts-dlq": "Real-Time Analysis",
    "trend-region-dlq": "Trend Analysis",
    "action-gateway-dlq": "Action Gateway"
}

DLQ_TOPICS = list(DLQ_HANDLERS.keys())

async def initialize_database(engine):
    """Guarantees the diagnostic schema exists for historical debugging analysis."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dead_letter_logs (
                id SERIAL PRIMARY KEY,
                origin_service VARCHAR(50) NOT NULL,  -- 🟢 YENİ: Servis adını tutacağımız sütun
                origin_topic VARCHAR(100) NOT NULL,
                payload_bytes BYTEA NOT NULL,
                isolated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved BOOLEAN DEFAULT FALSE
            );
        """))
    logging.info("🏛️ DLQ diagnostic database schema validated.")

async def process_dlq_message(msg, engine):
    """Intercepts isolated failures and persists them for manual administrative review."""
    topic = msg.topic
    raw_payload = msg.value
    
    # 🟢 SENİN MANTIĞIN: Topic'ten servis adını eşleştir (bulamazsa Unknown yaz)
    service_name = DLQ_HANDLERS.get(topic, "Unknown Service")
    
    logging.error(
        f"🚨 [DLQ ALERT | {service_name}] Intercepted corrupted event from '{topic}'! "
        f"Payload Size: {len(raw_payload)} bytes. Isolating record..."
    )
    
    async with engine.begin() as conn:
        # 🟢 YENİ: Veritabanına origin_service verisi de yazılıyor
        await conn.execute(
            text("""
                INSERT INTO dead_letter_logs (origin_service, origin_topic, payload_bytes) 
                VALUES (:service, :topic, :payload)
            """),
            {"service": service_name, "topic": topic, "payload": raw_payload}
        )
    logging.info(f"💾 Successfully isolated '{topic}' failure payload to persistence layer.")

async def start_kafka_client_with_retry(client, label: str):
    backoff = 1.0
    client_type = client.__class__.__name__ 
    while True:
        try:
            await client.start()
            logging.info(f"{label} {client_type} started successfully.")
            return
        except Exception as exc:
            logging.warning(f"Unable to start {client_type} for {label}: {exc}. Retrying in {backoff:.1f}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

async def main():
    logging.info("🚀 Starting SmartGrid Sentinel DLQ Diagnostic Service...")
    engine = create_async_engine(DATABASE_URL, echo=False)
    
    await initialize_database(engine)
    
    consumer = AIOKafkaConsumer(
        *DLQ_TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        group_id="dlq-monitoring-cluster"
    )
    
    await start_kafka_client_with_retry(consumer, "DLQ Monitoring Service")
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