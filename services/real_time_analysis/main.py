import asyncio
import logging
import os
import uuid
import time
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Import compiled protobuf schemas
import telemetry_pb2

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SRC_TOPIC = "telemetry-stream"
ALERT_TOPIC = "emergency-alerts"
DLQ_TOPIC = "telemetry-dlq"

# --- Strategy Design Pattern for Threshold Analysis ---
class AnalysisStrategy:
    def check(self, event: telemetry_pb2.TelemetryDomainEvent) -> bool:
        raise NotImplementedError

class VoltageSpikeStrategy(AnalysisStrategy):
    def check(self, event: telemetry_pb2.TelemetryDomainEvent) -> bool:
        # Evaluate critical threshold violation (e.g., Voltage > 240V)
        return event.voltage > 240.0

class GridAnalyzer:
    def __init__(self):
        self._strategies = [VoltageSpikeStrategy()]

    def evaluate(self, event: telemetry_pb2.TelemetryDomainEvent) -> bool:
        return any(strategy.check(event) for strategy in self._strategies)


# --- Error Isolation & DLQ Routing ---
async def route_to_dlq(producer: AIOKafkaProducer, raw_bytes: bytes, reason: str):
    """Routes an unprocessable raw payload straight into the telemetry-dlq topic."""
    try:
        logging.error(f"❌ Route to DLQ triggered: {reason}")
        await producer.send_and_wait(DLQ_TOPIC, raw_bytes)
    except Exception as e:
        logging.critical(f"DLQ Pipeline Failed! Cannot dump event payload: {str(e)}")


# --- Main Service Processing Worker ---
# Replace the process_message function inside services/real_time_analysis/main.py with this:

async def process_message(msg, analyzer: GridAnalyzer, producer: AIOKafkaProducer):
    raw_payload = msg.value
    retries = 3
    backoff = 0.5

    for attempt in range(retries):
        try:
            event = telemetry_pb2.TelemetryDomainEvent()
            event.ParseFromString(raw_payload)
            
            if analyzer.evaluate(event):
                logging.warning(f"🚨 ANOMALY DETECTED: Critical threshold breach on meter {event.meter_id} ({event.voltage:.2f}V)!")
                
                # Formulate the explicit typed EmergencyAlertEvent from your proposal specification
                alert_event = telemetry_pb2.EmergencyAlertEvent(
                    event_id=str(uuid.uuid4()),
                    meter_id=event.meter_id,
                    alert_type="VoltageSpikeDetected" if event.voltage > 240.0 else "EmergencyDetected",
                    trigger_value=event.voltage,
                    timestamp=int(time.time() * 1000)
                )
                
                # Serialise and dispatch safely to emergency-alerts topic
                await producer.send_and_wait(ALERT_TOPIC, alert_event.SerializeToString())
                logging.info(f"Dispatched typed alert {alert_event.alert_type} (ID: {alert_event.event_id}) to '{ALERT_TOPIC}' topic.")
            return

        except Exception as e:
            logging.warning(f"Processing attempt {attempt + 1} failed: {str(e)}")
            if attempt < retries - 1:
                await asyncio.sleep(backoff)
                backoff *= 2
            else:
                await route_to_dlq(producer, raw_payload, f"Persistent processing failure: {str(e)}")

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
    # Initialize Consumer and Producer clients
    consumer = AIOKafkaConsumer(SRC_TOPIC, bootstrap_servers=KAFKA_BROKER, group_id="real-time-analysis-group")
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    
    await start_kafka_client_with_retry(consumer, "Real-Time Analysis Service", "consumer")
    await start_kafka_client_with_retry(producer, "Real-Time Analysis Service", "producer")
    
    analyzer = GridAnalyzer()
    logging.info(f"Real-Time Analysis Service running. Listening to '{SRC_TOPIC}'...")

    try:
        async for msg in consumer:
            await process_message(msg, analyzer, producer)
    finally:
        await consumer.stop()
        await producer.stop()
        logging.info("Real-Time Analysis Service safely stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass