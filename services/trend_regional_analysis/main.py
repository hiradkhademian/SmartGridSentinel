import asyncio
import logging
import os
import uuid
import time
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis

# Import our updated compiled protobuf definitions
import telemetry_pb2

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SRC_TOPIC = "telemetry-stream"
TREND_TOPIC = "trend-region-events"
DLQ_TOPIC = "trend-region-dlq"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

SLIDING_WINDOW_SIZE = 15  # Expanded window size for regional macro trends
REGIONAL_OVERLOAD_THRESHOLD = 3.2  # kW threshold for regional overload trigger

# --- Error Isolation & DLQ Routing ---
async def route_to_dlq(producer: AIOKafkaProducer, raw_bytes: bytes, reason: str):
    """Isolates failed regional calculations and timeouts into trend-region-dlq."""
    try:
        logging.error(f"❌ Route to Trend DLQ triggered: {reason}")
        await producer.send_and_wait(DLQ_TOPIC, raw_bytes)
    except Exception as e:
        logging.critical(f"Trend DLQ Pipeline Failed! Cannot dump payload: {str(e)}")


# --- Stateful Regional Analytics Processor ---
async def process_trends(
    event: telemetry_pb2.TelemetryDomainEvent, 
    redis_client: aioredis.Redis, 
    producer: AIOKafkaProducer
):
    # Dynamically correlate individual meters into a macro grid region
    # For this simulation, we group all incoming meters into a single zone
    region_id = "ZONE-ALPHA"
    region_window_key = f"region:{region_id}:consumption_window"

    # 1. Pipeline atomic window mutations to our Redis in-memory cache
    async with redis_client.pipeline(transaction=True) as pipe:
        # Append the current consumption rate metrics to the regional list
        pipe.lpush(region_window_key, event.consumption_rate)
        # Keep the list trimmed to act as a sliding time window
        pipe.ltrim(region_window_key, 0, SLIDING_WINDOW_SIZE - 1)
        # Fetch all values residing in the regional window
        pipe.lrange(region_window_key, 0, -1)
        
        _, _, raw_values = await pipe.execute()

    # 2. Extract and parse time-series analytical boundaries
    window_values = [float(val.decode('utf-8')) for val in raw_values]
    rolling_avg = sum(window_values) / len(window_values)

    logging.info(
        f"📈 [Trend Engine] Region: {region_id} | Updated By: {event.meter_id} | "
        f"Current Rate: {event.consumption_rate:.2f} kW | "
        f"Regional Rolling Avg ({len(window_values)} pts): {rolling_avg:.2f} kW"
    )

    # 3. Evaluate multi-meter aggregate patterns for load anomalies
    if rolling_avg > REGIONAL_OVERLOAD_THRESHOLD:
        logging.warning(
            f"⚠️ [Trend Engine] Grid anomaly detected in {region_id}! "
            f"Rolling consumption average ({rolling_avg:.2f} kW) crosses safety threshold."
        )

        # Formulate the explicit typed TrendRegionEvent from your proposal specification
        trend_anomaly_event = telemetry_pb2.TrendRegionEvent(
            event_id=str(uuid.uuid4()), # Event tracing key
            region_id=region_id,
            anomaly_type="RegionalOverloadDetected",
            moving_average=rolling_avg,
            timestamp=int(time.time() * 1000)
        )

        # 4. Asynchronously broadcast the decision event to Kafka
        await producer.send_and_wait(TREND_TOPIC, trend_anomaly_event.SerializeToString())
        logging.info(
            f"📤 Dispatched regional alert {trend_anomaly_event.anomaly_type} "
            f"(ID: {trend_anomaly_event.event_id}) to '{TREND_TOPIC}' topic."
        )


# --- Worker Handler with Fault Isolation Loops ---
async def manage_message_cycle(msg, redis_client: aioredis.Redis, producer: AIOKafkaProducer):
    raw_payload = msg.value
    retries = 3
    backoff = 0.5

    for attempt in range(retries):
        try:
            event = telemetry_pb2.TelemetryDomainEvent()
            event.ParseFromString(raw_payload)
            
            # Execute stateful aggregation
            await process_trends(event, redis_client, producer)
            return

        except Exception as e:
            logging.warning(f"Trend processing attempt {attempt + 1} failed: {str(e)}")
            if attempt < retries - 1:
                await asyncio.sleep(backoff)
                backoff *= 2
            else:
                # Isolate persistent timeout or parsing faults straight to DLQ
                await route_to_dlq(producer, raw_payload, f"Persistent Trend processing failure: {str(e)}")


async def start_kafka_client_with_retry(client, label: str, client_type: str):
    backoff = 1.0
    while True:
        try:
            if client_type == "consumer":
                await client.start()
            else:
                await client.start()
            logging.info(f"{label} Kafka {client_type.capitalize()} started successfully.")
            return
        except Exception as exc:
            logging.warning(f"Unable to start Kafka {client_type} for {label}: {exc}. Retrying in {backoff:.1f}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)


async def main():
    # Connect to local dependencies
    redis_client = aioredis.from_url(REDIS_URL)
    consumer = AIOKafkaConsumer(
        SRC_TOPIC, 
        bootstrap_servers=KAFKA_BROKER, 
        group_id="trend-analysis-group"
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    
    await start_kafka_client_with_retry(consumer, "Trend Regional Analysis Service", "consumer")
    await start_kafka_client_with_retry(producer, "Trend Regional Analysis Service", "producer")
    
    logging.info(f"Stateful Trend & Regional Analysis Service operational.")
    logging.info(f"Subscribed to topic '{SRC_TOPIC}' | Stream target: '{TREND_TOPIC}'...")

    try:
        async for msg in consumer:
            await manage_message_cycle(msg, redis_client, producer)
    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.close()
        logging.info("Trend Analysis Service safely stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass