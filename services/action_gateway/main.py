import asyncio
import logging
from datetime import datetime
import os
import uuid
import grpc
from aiokafka import AIOKafkaConsumer
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
    ack_received: Mapped[bool] = mapped_column(DateTime, nullable=True)


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
                        logging.info(f"🔄 [Retry Loop] Dispatching command {command.command_id} to meter {command.meter_id}...")
                        # Fire command over gRPC stub with a strict timeout window
                        ack: telemetry_pb2.CommandAcknowledgement = await stub.ExecuteCommand(command, timeout=2.0)
                        
                        if ack.success:
                            logging.info(f"✅ [ACK Received] Meter {ack.meter_id} verified execution of command {ack.command_id}.")
                            return True
                    except grpc.RpcError as grpc_err:
                        logging.warning(f"⚠️ [Delivery Failure] Connection drop or timeout: {grpc_err.details()}. Retrying...")
                    
                    # Exponential Backoff up to a reasonable cap
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
        # Broadcasts command requests through managed connection observers
        for observer in self._observers:
            success = await observer.update(command)
            if success:
                return True
        return False


# --- Idempotency & Database Verification Helper ---
async def is_event_duplicate(session_factory: async_sessionmaker, event_id: str) -> bool:
    async with session_factory() as session:
        result = await session.execute(
            session.query(CommandAuditLog).filter(CommandAuditLog.event_id == event_id)
        )
        return result.scalar() is not None


# --- Event Processing Pipelines ---
async def handle_incoming_pipeline(consumer: AIOKafkaConsumer, subject: GridControlSubject, session_factory: async_sessionmaker):
    async for msg in consumer:
        try:
            # Route processing based on the source message topic origin
            if msg.topic == ALERT_TOPIC:
                alert = telemetry_pb2.EmergencyAlertEvent()
                alert.ParseFromString(msg.value)
                
                logging.warning(f"🚨 [Action Gateway] Consumed Alert Event: {alert.alert_type} from meter {alert.meter_id}")
                
                # Check Idempotency Record Cache in DB
                async with session_factory() as session:
                    existing = await session.get(CommandAuditLog, alert.event_id)
                    if existing:
                        logging.warning(f"⏭️ [Idempotency Block] Event {alert.event_id} already processed. Skipping duplicate command.")
                        continue

                # Map to report specific command definitions
                cmd_type = telemetry_pb2.CommandType.CUT_POWER if alert.alert_type == "VoltageSpikeDetected" else telemetry_pb2.CommandType.RESTART_METER
                cmd_name = "CutPowerCommand" if cmd_type == telemetry_pb2.CommandType.CUT_POWER else "RestartMeterCommand"

                command = telemetry_pb2.GridControlCommand(
                    command_id=alert.event_id,
                    meter_id=alert.meter_id,
                    type=cmd_type,
                    details=f"Triggered by immediate real-time safety breach value: {alert.trigger_value}"
                )

                # Orchestrate control plane broadcast through our active Observers
                ack_status = await subject.notify_observers(command)
                
                if ack_status:
                    # Persist permanent operational tracking log inside PostgreSQL
                    async with session_factory() as session:
                        async with session.begin():
                            audit = CommandAuditLog(
                                event_id=command.command_id,
                                target_id=command.meter_id,
                                command_type=cmd_name,
                                details=command.details
                            )
                            await session.merge(audit)
                    logging.info(f"💾 Command audit log recorded cleanly for transaction {command.command_id}.")

            elif msg.topic == TREND_TOPIC:
                trend = telemetry_pb2.TrendRegionEvent()
                trend.ParseFromString(msg.value)
                
                logging.warning(f"📈 [Action Gateway] Consumed Regional Event: {trend.anomaly_type} for region {trend.region_id}")
                
                async with session_factory() as session:
                    if await session.get(CommandAuditLog, trend.event_id):
                        continue

                # Map regional trends to ThrottleConsumptionCommand
                command = telemetry_pb2.GridControlCommand(
                    command_id=trend.event_id,
                    meter_id=f"REGIONAL-GATEWAY-{trend.region_id}", # Route command to a group/regional context
                    type=telemetry_pb2.CommandType.THROTTLE_CONSUMPTION,
                    details=f"Triggered by {trend.anomaly_type}. Global Moving Average: {trend.moving_average:.2f} kW"
                )

                ack_status = await subject.notify_observers(command)
                if ack_status:
                    async with session_factory() as session:
                        async with session.begin():
                            audit = CommandAuditLog(
                                event_id=command.command_id,
                                target_id=command.meter_id,
                                command_type="ThrottleConsumptionCommand",
                                details=command.details
                            )
                            await session.merge(audit)

        except Exception as e:
            logging.error(f"Error handling downstream action control pipeline: {str(e)}")

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
    await consumer.start()
    logging.info(f"Action Gateway Service active. Monitoring topics: ['{ALERT_TOPIC}', '{TREND_TOPIC}']...")

    try:
        await handle_incoming_pipeline(consumer, control_subject, async_sessionmaker_factory)
    finally:
        await consumer.stop()
        await engine.dispose()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass