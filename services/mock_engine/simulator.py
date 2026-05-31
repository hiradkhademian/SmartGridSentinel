import asyncio
import logging
import random
import time
import grpc
import os

import telemetry_pb2
import telemetry_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
INGESTION_SERVER_ADDR = os.getenv("INGESTION_SERVER_ADDR", "localhost:50051")
CONTROL_PORT = os.getenv("CONTROL_PORT", "[::]:50052")
MOCK_METER_IDS = ["METER-01H", "METER-02H", "METER-03H", "METER-04H"]
# Cihazların anlık durumlarını takip eden state (durum) sözlükleri
meter_power_status = {meter_id: True for meter_id in MOCK_METER_IDS}
meter_throttle_status = {meter_id: False for meter_id in MOCK_METER_IDS} # 🟢 YENİ: Throttle durumu

# --- Otonom Görevler (Auto-Recovery & Auto-Unthrottle) ---
async def schedule_auto_recovery(meter_id: str, delay_seconds: int = 30):
    logging.info(f"⏳ [{meter_id}] Auto-recovery scheduled. Power will be restored in {delay_seconds} seconds.")
    await asyncio.sleep(delay_seconds)
    if not meter_power_status.get(meter_id, True):
        meter_power_status[meter_id] = True
        logging.info(f"♻️ [{meter_id}] Auto-recovery triggered! Power restored.")

async def schedule_auto_unthrottle(meter_id: str, delay_seconds: int = 45):
    """Bölgesel yük hafiflediğinde cihazı normal tüketime döndürür."""
    await asyncio.sleep(delay_seconds)
    if meter_throttle_status.get(meter_id, False):
        meter_throttle_status[meter_id] = False
        logging.info(f"🟢 [{meter_id}] Throttle released. Normal consumption resuming.")

# --- gRPC Server Implementation ---
class MockEngineControlServicer(telemetry_pb2_grpc.MockEngineControlServiceServicer):
    async def ExecuteCommand(
        self, 
        request: telemetry_pb2.GridControlCommand, 
        context: grpc.aio.ServicerContext
    ) -> telemetry_pb2.CommandAcknowledgement:
        
        cmd_name = telemetry_pb2.CommandType.Name(request.type)
        
        # 🟢 YENİ: Daha iyi observability için Meta Log
        logging.info(f"📋 COMMAND_META | meter={request.meter_id} | type={cmd_name} | ts={time.time()}")
        logging.critical(
            f"⚡ [HARDWARE LAYER] Mechanical instruction received for {request.meter_id}! "
            f"Reason: {request.details}"
        )
        
        # 🟢 GÜNCELLENDİ: Tüm komut setleri (CUT, RESTART, THROTTLE)
        if request.type == telemetry_pb2.CommandType.CUT_POWER:
            meter_power_status[request.meter_id] = False
            logging.info(f"🛑 [{request.meter_id}] Power physically cut. Telemetry stream halted.")
            asyncio.create_task(schedule_auto_recovery(request.meter_id, 30))
            
        elif request.type == telemetry_pb2.CommandType.RESTART_METER:
            meter_power_status[request.meter_id] = True
            logging.info(f"✅ [{request.meter_id}] Power restored. Telemetry stream resuming.")
            
        elif request.type == telemetry_pb2.CommandType.THROTTLE_CONSUMPTION:
            meter_throttle_status[request.meter_id] = True
            logging.warning(f"📉 [{request.meter_id}] Consumption throttled by grid control system.")
            asyncio.create_task(schedule_auto_unthrottle(request.meter_id, 45))

        await asyncio.sleep(0.1) # Simulate hardware mechanical delay
        
        return telemetry_pb2.CommandAcknowledgement(
            command_id=request.command_id,
            meter_id=request.meter_id,
            success=True,
            ack_timestamp=int(time.time() * 1000)
        )

async def start_control_server():
    server = grpc.aio.server()
    telemetry_pb2_grpc.add_MockEngineControlServiceServicer_to_server(
        MockEngineControlServicer(), server
    )
    server.add_insecure_port(CONTROL_PORT)
    logging.info(f"Mock Engine Control listening for incoming commands on {CONTROL_PORT}...")
    await server.start()
    await server.wait_for_termination()

# --- Client Path Streaming Outbound Telemetry Data ---
async def run_mock_engine(original_meter_id: str, stub: telemetry_pb2_grpc.TelemetryIngestionServiceStub):
    logging.info(f"Starting telemetry outbound streaming for {original_meter_id}")
    backoff = 2.0 
    
    while True:
        if not meter_power_status.get(original_meter_id, True):
            await asyncio.sleep(2)
            continue
            
        try:
            meter_id = original_meter_id
            base_voltage = random.uniform(220.0, 235.0)
            current = random.uniform(5.0, 14.0)
            
            # 🟢 YENİ: Eğere cihaz Throttled durumdaysa akımı %40 düşür
            if meter_throttle_status.get(original_meter_id, False):
                current *= 0.6 
            
            # 🟢 GÜNCELLENDİ: Daha agresif ve çeşitli DLQ Chaos Enjeksiyonu (%2)
            if random.random() < 0.02:
                logging.warning(f"🧪 [CHAOS TEST] Injecting deliberately corrupted packet for DLQ validation!")
                # Geçersiz formatlar veya boş ID'ler
                meter_id = random.choice(["CORRUPT_METER", "INVALID_!@#", ""]) 
                base_voltage = -999.0 
            
            # 🟢 GÜNCELLENDİ: Anomali isimleri Action Gateway ile birebir eşitlendi
            elif random.random() < 0.05: 
                anomaly_type = random.choice([
                    "VoltageSpikeDetected", 
                    "CurrentSpikeDetected", 
                    "PowerSpikeDetected", 
                    "BlackoutDetected"
                ])
                
                if anomaly_type == "VoltageSpikeDetected":
                    base_voltage = random.uniform(242.0, 255.0)
                    logging.warning(f"⚠️ [{meter_id}] Simulating Voltage Spike: {base_voltage:.2f}V")
                    
                elif anomaly_type == "CurrentSpikeDetected":
                    current = random.uniform(16.0, 19.0)
                    logging.warning(f"⚠️ [{meter_id}] Simulating Current Spike: {current:.2f}A")
                    
                elif anomaly_type == "PowerSpikeDetected":
                    base_voltage = 238.0 
                    current = 17.0 
                    logging.warning(f"⚠️ [{meter_id}] Simulating Power Spike: {(base_voltage * current):.2f}W")
                    
                elif anomaly_type == "BlackoutDetected":
                    base_voltage = random.uniform(150.0, 190.0)
                    logging.warning(f"⚠️ [{meter_id}] Simulating Blackout: {base_voltage:.2f}V")

            consumption_rate = (base_voltage * current) / 1000.0
            packet = telemetry_pb2.TelemetryPacket(
                meter_id=meter_id,
                voltage=base_voltage,
                current=current,
                consumption_rate=consumption_rate,
                timestamp=int(time.time() * 1000)
            )

            response = await stub.StreamTelemetry(packet)
            if not response.success:
                logging.error(f"[{meter_id}] Rejected by Ingestion: {response.message}")
                
            backoff = 2.0 

        except grpc.RpcError as e:
            logging.error(f"[{original_meter_id}] Outbound telemetry path disconnected: {e.details()}. Retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)
            continue 
        
        await asyncio.sleep(2)

async def run_client_pipeline():
    await asyncio.sleep(1.0) 
    async with grpc.aio.insecure_channel(INGESTION_SERVER_ADDR) as channel:
        stub = telemetry_pb2_grpc.TelemetryIngestionServiceStub(channel)
        tasks = [run_mock_engine(meter_id, stub) for meter_id in MOCK_METER_IDS]
        await asyncio.gather(*tasks)

async def main():
    await asyncio.gather(
        start_control_server(),
        run_client_pipeline()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Mock Engine shutdown.")