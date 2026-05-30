import asyncio
import logging
import uuid
import os
import grpc
import re    
import time
from aiokafka import AIOKafkaProducer

# Import our compiled protobuf classes
import telemetry_pb2
import telemetry_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Environment Aware Configuration ---
# Uses 'kafka:29092' inside Docker Compose network, falls back to 'localhost:9092' if run standalone
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "telemetry-stream"
DLQ_TOPIC = "telemetry-dlq"  # The designated fault isolation target from the project proposal
GRPC_PORT = "[::]:50051"

class TelemetryIngestionServicer(telemetry_pb2_grpc.TelemetryIngestionServiceServicer):
    def __init__(self, kafka_producer: AIOKafkaProducer):
        self.producer = kafka_producer

    async def StreamTelemetry(
        self, 
        request: telemetry_pb2.TelemetryPacket, 
        context: grpc.aio.ServicerContext
    ) -> telemetry_pb2.IngestionResponse:
        
        # --- 1. LOCAL REPLAY DEBUGGING TRAP ---
        # If the incoming meter profile matches our debug keyword, simulate a validation breakdown
        if request.meter_id == "MALFORMED_TEST" or request.meter_id == "CORRUPT_METER":
            logging.warning(f"🚨 [DEBUG DETECTED] Intentionally isolating malformed test packet from meter: {request.meter_id}")
            try:
                # Serialize the raw invalid request packet
                serialized_raw = request.SerializeToString()
                
                # Directly bypass core stream and route straight to the isolation dead letter queue
                await self.producer.send_and_wait(DLQ_TOPIC, serialized_raw)
                logging.info(f"📥 Successfully routed simulated fault payload to '{DLQ_TOPIC}' for administrator recovery.")
                
                return telemetry_pb2.IngestionResponse(
                    success=False, 
                    message="Simulated parsing breakdown. Payload safely isolated to system DLQ infrastructure."
                )
            except Exception as dlq_err:
                logging.error(f"Failed to route debug payload to DLQ: {str(dlq_err)}")
                context.set_code(grpc.StatusCode.INTERNAL)
                return telemetry_pb2.IngestionResponse(success=False, message="DLQ internal error.")

        # --- 2. STANDARD RUNTIME VALIDATION (Anti-Corruption Layer Pattern) ---
        # 1. Meter ID format validation (e.g., METER-01H, must start with METER- followed by alphanumerics)
        is_valid_meter_id = bool(request.meter_id and re.match(r"^METER-[0-9A-Z]+$", request.meter_id))
        
        # 2. Metrics validation (Voltage must be > 0. Current and consumption can be 0 but not negative)
        is_valid_metrics = request.voltage > 0 and request.current >= 0 and request.consumption_rate >= 0
        
        # 3. Timestamp validity window (Must be within the last 24 hours, with a max 60 sec tolerance for future time due to device clock drift)
        current_time = int(time.time())
        is_valid_timestamp = (current_time - 86400) <= (request.timestamp / 1000) <= (current_time + 60)

        # Reject the packet if any of the rules are violated
        if not (is_valid_meter_id and is_valid_metrics and is_valid_timestamp):
            logging.warning(f"Rejected invalid packet from meter: {request.meter_id} | V:{request.voltage} I:{request.current} C:{request.consumption_rate} T:{request.timestamp}")
            return telemetry_pb2.IngestionResponse(
                success=False, 
                message="Invalid telemetry data fields, formatting, or temporal anomalies."
            )

        try:
            # 3. Transform into standard Domain Event with a unique event_id
            domain_event = telemetry_pb2.TelemetryDomainEvent(
                event_id=str(uuid.uuid4()),
                meter_id=request.meter_id,
                voltage=request.voltage,
                current=request.current,
                consumption_rate=request.consumption_rate,
                timestamp=request.timestamp
            )

            # 4. Serialize to binary and dispatch to Apache Kafka
            serialized_event = domain_event.SerializeToString()
            await self.producer.send_and_wait(KAFKA_TOPIC, serialized_event)
            
            logging.info(f"Ingested & Published event {domain_event.event_id} for meter {domain_event.meter_id}")
            
            return telemetry_pb2.IngestionResponse(
                success=True, 
                message=f"Event {domain_event.event_id} successfully streamed."
            )

        except Exception as e:
            logging.error(f"Failed to process telemetry packet: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal ingestion loop failure.")
            return telemetry_pb2.IngestionResponse(success=False, message=str(e))

async def start_kafka_producer_with_retry(producer: AIOKafkaProducer, label: str):
    backoff = 1.0
    while True:
        try:
            await producer.start()
            logging.info(f"{label} Kafka Producer started successfully.")
            return
        except Exception as exc:
            logging.warning(f"Unable to start Kafka producer for {label}: {exc}. Retrying in {backoff:.1f}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)


async def serve():
    # Initialize and spin up the async Kafka Producer
    logging.info(f"Connecting Asynchronous Kafka Producer to broker at: {KAFKA_BROKER}...")
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await start_kafka_producer_with_retry(producer, "Ingestion Service")

    try:
        # Initialize the Async gRPC Server
        server = grpc.aio.server()
        telemetry_pb2_grpc.add_TelemetryIngestionServiceServicer_to_server(
            TelemetryIngestionServicer(producer), server
        )
        
        server.add_insecure_port(GRPC_PORT)
        logging.info(f"Ingestion gRPC Service starting on port {GRPC_PORT}...")
        await server.start()
        await server.wait_for_termination()
    finally:
        # Gracefully close connections on stop
        await producer.stop()
        logging.info("Kafka Producer terminated safely.")

if __name__ == "__main__":
    asyncio.run(serve())