import asyncio
import logging
from datetime import datetime
import os
import uuid
import grpc
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy import String, Float, DateTime

# Import our updated compiled protobuf definitions
import telemetry_pb2
import telemetry_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
ALERT_TOPIC = "emergency-alerts"
TREND_TOPIC = "trend-region-events"
MOCK_ENGINE_CONTROL_ADDR = os.getenv("MOCK_ENGINE_CONTROL_ADDR", "localhost:50052")
ACTION_GATEWAY_DLQ = "action-gateway-dlq"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://sentinel_admin:sentinel_password@localhost:5432/smartgrid_db")

Base = declarative_base()

# --- PostgreSQL Schema for Audit Logs & Command Histories ---
class CommandAuditLog(Base):
    __tablename__ = "command_audit_logs"

    event_id: Mapped[str] = mapped_column(String(50), primary_key=True)  # Idempotency verification
    target_id: Mapped[str] = mapped_column(String(50), nullable=False)   # Meter or Region ID
    command_type: Mapped[str] = mapped_column(String(50), nullable=False)
    details: Mapped[str] = mapped_column(String(255), nullable=False)
    executed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    ack_received: Mapped[datetime] = mapped_column(DateTime, nullable=True)


# --- Observer Design Pattern for Connection Management ---
class GridControlObserver:
    """Abstract interface for our connection path Observers."""
    async def update(self, command: telemetry_pb2.GridControlCommand) -> bool:
        raise NotImplementedError

class MockEngineChannelObserver(GridControlObserver):
    """Concrete Observer managing an active bidirectional gRPC channel connection."""
    def __init__(self, address: str):
        self.address = address

    async def update(self, command: telemetry_pb2.GridControlCommand) -> bool:
        try:
            # Establish a transient or sticky connection path to the target hardware layer
            async with grpc.aio.insecure_channel(self.address) as channel:
                stub = telemetry_pb2_grpc.MockEngineControlServiceStub(channel)
                
                # --- Retry Until ACK Delivery Pattern Loop ---
                backoff = 0.5
                while True:
                    try:
                        cmd_string_name = telemetry_pb2.CommandType.Name(command.type)
                        logging.info(f"🔄 [Retry Loop] Dispatching {cmd_string_name} (ID: {command.command_id}) to {command.meter_id}...")
                        ack: telemetry_pb2.CommandAcknowledgement = await stub.ExecuteCommand(command, timeout=2.0)
                        
                        if ack.success:
                            logging.info(f"✅ [ACK Received] Target {ack.meter_id} verified execution of command {ack.command_id}.")
                            return True
                    except grpc.RpcError as grpc_err:
                        logging.warning(f"⚠️ [Delivery Failure] Connection drop or timeout: {grpc_err.details()}. Retrying...")
                    
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 5.0)

        except Exception as e:
            logging.error(f"Critical channel error inside observer path: {str(e)}")
            return False

class GridControlSubject:
    """The Subject managing registered hardware communication paths."""
    def __init__(self):
        self._observers = []

    def register_observer(self, observer: GridControlObserver):
        self._observers.append(observer)

    async def notify_observers(self, command: telemetry_pb2.GridControlCommand) -> bool:
        for observer in self._observers:
            success = await observer.update(command)
            if success:
                return True
        return False

# --- DLQ Routing ---
async def route_to_dlq(
    producer: AIOKafkaProducer,
    raw_bytes: bytes,
    reason: str
):
    try:
        logging.error(
            f"❌ Action Gateway DLQ Triggered: {reason}"
        )

        await producer.send_and_wait(
            ACTION_GATEWAY_DLQ,
            raw_bytes
        )

    except Exception as e:
        logging.critical(
            f"Action Gateway DLQ write failed: {e}"
        )
# --- Event Processing Pipelines ---
async def handle_incoming_pipeline(consumer: AIOKafkaConsumer, subject: GridControlSubject, session_factory: async_sessionmaker, producer: AIOKafkaProducer):
    async for msg in consumer:
        try:
            # =========================================================
            # 1. REAL-TIME ANALYSIS HANDLER (Emergency Alerts)
            # =========================================================
            if msg.topic == ALERT_TOPIC:
                alert = telemetry_pb2.EmergencyAlertEvent()
                alert.ParseFromString(msg.value)
                
                logging.warning(f"🚨 [Action Gateway] Consumed Alert Event: {alert.alert_type} from meter {alert.meter_id}")
                
                # Check Idempotency Record Cache in DB
                async with session_factory() as session:
                    existing = await session.get(CommandAuditLog, alert.event_id)
                    if existing:
                        logging.warning(f"⏭️ [Idempotency Block] Event {alert.event_id} already processed. Skipping.")
                        continue

                # --- POLIMORFİK KARAR AĞACI ---
                # Yangın/Aşırı Yük tehlikelerinde gücü tamamen kes
                if alert.alert_type in ["VoltageSpikeDetected", "PowerSpikeDetected"]:
                    cmd_type = telemetry_pb2.CommandType.CUT_POWER
                    cmd_name = "CutPowerCommand"
                # Akım dalgalanması veya düşük voltajda cihazı yeniden başlatıp kurtarmayı dene
                elif alert.alert_type in ["CurrentSpikeDetected", "BlackoutDetected"]:
                    cmd_type = telemetry_pb2.CommandType.RESTART_METER
                    cmd_name = "RestartMeterCommand"
                else:
                    cmd_type = telemetry_pb2.CommandType.CUT_POWER
                    cmd_name = "EmergencyCutPowerCommand"

                command = telemetry_pb2.GridControlCommand(
                    command_id=alert.event_id,
                    meter_id=alert.meter_id,
                    type=cmd_type,
                    details=f"[{alert.alert_type}] Triggered by real-time safety breach value: {alert.trigger_value:.2f}"
                )

                # Orchestrate control plane broadcast through our active Observers
                ack_status = await subject.notify_observers(command)
                
                if ack_status:
                    async with session_factory() as session:
                        async with session.begin():
                            audit = CommandAuditLog(
                                event_id=command.command_id,
                                target_id=command.meter_id,
                                command_type=cmd_name,
                                details=command.details,
                                ack_received=datetime.utcnow()
                            )
                            await session.merge(audit)
                    logging.info(f"💾 {cmd_name} audit log recorded cleanly for transaction {command.command_id}.")

            # =========================================================
            # 2. TREND & REGIONAL ANALYSIS HANDLER (Macro Events)
            # =========================================================
            elif msg.topic == TREND_TOPIC:
                trend = telemetry_pb2.TrendRegionEvent()
                trend.ParseFromString(msg.value)
                
                logging.warning(f"📈 [Action Gateway] Consumed Regional Event: {trend.anomaly_type} for region {trend.region_id}")
                
                async with session_factory() as session:
                    if await session.get(CommandAuditLog, trend.event_id):
                        continue

                # --- TREND KARAR AĞACI ---
                if trend.anomaly_type == "RegionalOverloadDetected":
                    cmd_type = telemetry_pb2.CommandType.THROTTLE_CONSUMPTION
                    cmd_name = "ThrottleConsumptionCommand"
                    target_id = f"REGIONAL-GATEWAY-{trend.region_id}"
                    details = f"Triggered by {trend.anomaly_type}. Throttling region. Rolling Avg: {trend.moving_average:.2f} kW"
                
                elif trend.anomaly_type == "SuspiciousConsumptionPattern":
                    cmd_type = telemetry_pb2.CommandType.THROTTLE_CONSUMPTION
                    cmd_name = "InvestigateConsumptionCommand"
                    target_id = f"REGION-{trend.region_id}-SUSPICIOUS"
                    details = f"Triggered by {trend.anomaly_type}. Flagging for steady high usage: {trend.moving_average:.2f} kW"
                
                elif trend.anomaly_type == "GridInstabilityDetected":
                    cmd_type = telemetry_pb2.CommandType.THROTTLE_CONSUMPTION
                    cmd_name = "GridStabilizationCommand"
                    target_id = f"REGIONAL-GATEWAY-{trend.region_id}"
                    details = f"Grid instability detected. Variance-based anomaly."

                elif trend.anomaly_type == "RapidLoadGrowthDetected":
                    cmd_type = telemetry_pb2.CommandType.THROTTLE_CONSUMPTION
                    cmd_name = "PreventiveLoadControlCommand"
                    target_id = f"REGIONAL-GATEWAY-{trend.region_id}"
                    details = f"Rapid load growth detected. Slope-based anomaly."
                else:
                    cmd_type = telemetry_pb2.CommandType.THROTTLE_CONSUMPTION
                    cmd_name = "GenericTrendCommand"
                    target_id = f"REGIONAL-GATEWAY-{trend.region_id}"
                    details = f"Trend anomaly: {trend.moving_average:.2f} kW"

                command = telemetry_pb2.GridControlCommand(
                    command_id=trend.event_id,
                    meter_id=target_id, 
                    type=cmd_type,
                    details=details
                )

                ack_status = await subject.notify_observers(command)
                if ack_status:
                    async with session_factory() as session:
                        async with session.begin():
                            audit = CommandAuditLog(
                                event_id=command.command_id,
                                target_id=command.meter_id,
                                command_type=cmd_name,
                                details=command.details,
                                ack_received=datetime.utcnow()
                            )
                            await session.merge(audit)
                    logging.info(f"💾 Regional audit log recorded cleanly for transaction {command.command_id}.")

        except Exception as e:
            logging.error(
                f"Error handling downstream action control pipeline: {str(e)}"
            )

            try:
                await route_to_dlq(
                    producer,
                    msg.value,
                    str(e)
                )
            except Exception as dlq_err:
                logging.critical(f"DLQ failed: {dlq_err}")

# --- Fault-Tolerant Kafka Bootstrapping ---
async def start_kafka_client_with_retry(client, label: str):
    backoff = 1.0
    # Objenin kendi sınıf adını alır (AIOKafkaConsumer veya AIOKafkaProducer)
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
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_sessionmaker_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Instantiate our Subject and hook up our gRPC Connection Observer
    control_subject = GridControlSubject()
    mock_engine_observer = MockEngineChannelObserver(MOCK_ENGINE_CONTROL_ADDR)
    control_subject.register_observer(mock_engine_observer)
    logging.info("Observer pattern connection plane registries initialized successfully.")

    # Initialize consumer listening concurrently to multiple topics
    consumer = AIOKafkaConsumer(
        ALERT_TOPIC, TREND_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="action-gateway-group"
    )
    producer = AIOKafkaProducer(
    bootstrap_servers=KAFKA_BROKER
)
    
    # Yeni eklenen otonom onarım bağlantı fonksiyonumuz
    await start_kafka_client_with_retry(consumer, "Action Gateway Service")
    await start_kafka_client_with_retry(producer, "Action Gateway Service")

    logging.info(f"Action Gateway Service active. Monitoring topics: ['{ALERT_TOPIC}', '{TREND_TOPIC}']...")

    try:
        await handle_incoming_pipeline(consumer, control_subject, async_sessionmaker_factory, producer)
    finally:
        await consumer.stop()
        await producer.stop()
        await engine.dispose()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass