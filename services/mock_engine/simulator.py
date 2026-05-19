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

# --- gRPC Server Implementation to Receive Downstream Control Side Effects ---
class MockEngineControlServicer(telemetry_pb2_grpc.MockEngineControlServiceServicer):
    async def ExecuteCommand(
        self, 
        request: telemetry_pb2.GridControlCommand, 
        context: grpc.aio.ServicerContext
    ) -> telemetry_pb2.CommandAcknowledgement:
        
        # Translate command type representations enum
        cmd_name = telemetry_pb2.CommandType.Name(request.type)
        logging.critical(
            f"⚡ [HARDWARE LAYER] Mechanical instruction received for {request.meter_id}! "
            f"Execution Action: {cmd_name} | Reason: {request.details}"
        )
        
        # Simulate local network latency or processing delays
        await asyncio.sleep(0.1)
        
        # Return mandatory execution confirmation acknowledgment packet
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
async def run_mock_engine(meter_id: str, stub: telemetry_pb2_grpc.TelemetryIngestionServiceStub):
    logging.info(f"Starting telemetry outbound streaming for {meter_id}")
    while True:
        try:
            base_voltage = random.uniform(220.0, 235.0)
            current = random.uniform(5.0, 15.0)
            
            if random.random() < 0.04: # Inject overvoltage triggers
                base_voltage = random.uniform(242.0, 255.0)
                logging.warning(f"⚠️ [{meter_id}] Simulating anomaly threshold breach: {base_voltage:.2f}V")

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
                logging.error(f"[{meter_id}] Rejected: {response.message}")

        except grpc.RpcError as e:
            logging.error(f"[{meter_id}] Outbound telemetry path disconnected.")
        
        await asyncio.sleep(2)

async def run_client_pipeline():
    await asyncio.sleep(1.0) # Give the local control server a headstart
    async with grpc.aio.insecure_channel(INGESTION_SERVER_ADDR) as channel:
        stub = telemetry_pb2_grpc.TelemetryIngestionServiceStub(channel)
        tasks = [run_mock_engine(meter_id, stub) for meter_id in MOCK_METER_IDS]
        await asyncio.gather(*tasks)

async def main():
    # Co-operatively manage both server task loops and telemetry streaming pipelines concurrently
    await asyncio.gather(
        start_control_server(),
        run_client_pipeline()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Mock Engine shutdown.")