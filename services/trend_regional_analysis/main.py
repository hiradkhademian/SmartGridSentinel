import asyncio
import logging
import os
import uuid
import time
import math
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis

# Import our updated compiled protobuf definitions
import telemetry_pb2

def calculate_slope(values):
    """Basit doğrusal eğim hesaplaması."""
    if len(values) < 2: return 0.0
    x = list(range(len(values)))
    n = len(values)
    sum_x = sum(x)
    sum_y = sum(values)
    sum_xy = sum(i * v for i, v in zip(x, values))
    sum_xx = sum(i * i for i in x)
    return (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x**2) if (n * sum_xx - sum_x**2) != 0 else 0.0

def calculate_variance(values):
    """Varyans hesaplaması."""
    if len(values) < 2: return 0.0
    mean = sum(values) / len(values)
    return sum((v - mean) ** 2 for v in values) / len(values)

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
    region_id = "ZONE-ALPHA"
    region_window_key = f"region:{region_id}:consumption_window"
    # BİREYSEL TAKİP İÇİN YENİ ANAHTAR
    meter_window_key = f"meter:{event.meter_id}:consumption_window"

    # 1. Pipeline atomic window mutations
    async with redis_client.pipeline(transaction=True) as pipe:
        # BÖLGESEL PENCEREYİ GÜNCELLE
        pipe.lpush(region_window_key, event.consumption_rate)
        pipe.ltrim(region_window_key, 0, SLIDING_WINDOW_SIZE - 1)
        pipe.lrange(region_window_key, 0, -1)
        
        # BİREYSEL PENCEREYİ GÜNCELLE
        pipe.lpush(meter_window_key, event.consumption_rate)
        pipe.ltrim(meter_window_key, 0, SLIDING_WINDOW_SIZE - 1)
        pipe.lrange(meter_window_key, 0, -1)
        
        # Pipeline'ı tek seferde çalıştır (Redis'e tek gidiş-dönüş maliyeti)
        results = await pipe.execute()

    # Redis'ten dönen 6 işlemlik sonucun 3.sü bölgesel, 6.sı bireysel verilerdir
    region_raw_values = results[2]
    meter_raw_values = results[5]

    # 2. BÖLGESEL (Makro) Hareketli Ortalama Hesabı
    region_values = [float(val.decode('utf-8')) for val in region_raw_values]
    region_rolling_avg = sum(region_values) / len(region_values) if region_values else 0
    region_slope = calculate_slope(region_values)
    region_variance = calculate_variance(region_values)
   
    # 3. BİREYSEL (Mikro) Hareketli Ortalama Hesabı
    meter_values = [float(val.decode('utf-8')) for val in meter_raw_values]
    meter_rolling_avg = sum(meter_values) / len(meter_values)
    
    logging.info(
        f"📈 [Trend Engine] Region: {region_id} | Meter: {event.meter_id} | "
        f"Rate: {event.consumption_rate:.2f}kW | "
        f"Meter Avg: {meter_rolling_avg:.2f}kW | Region Avg: {region_rolling_avg:.2f}kW | "
        f"Slope: {region_slope:.4f} | Var: {region_variance:.4f}"
    )

    # 3. Anomali Kontrolleri ve Kafka Olayı (Event) Üretimi
    # En tehlikeli/kritik durumdan en hafif duruma doğru sıralanmış bir Karar Ağacı
    trend_anomaly_event = None

    # Öncelik 1: Gerçekleşmiş Bölgesel Aşırı Yük (En kritik sınır aşımı)
    if region_rolling_avg > REGIONAL_OVERLOAD_THRESHOLD:
        logging.warning(f"⚠️ [Trend Engine] Regional OVERLOAD in {region_id}! Avg: {region_rolling_avg:.2f}kW")
        trend_anomaly_event = telemetry_pb2.TrendRegionEvent(
            event_id=str(uuid.uuid4()),
            region_id=region_id,
            anomaly_type="RegionalOverloadDetected",
            moving_average=region_rolling_avg,
            timestamp=int(time.time() * 1000)
        )
        
    # Öncelik 2: Şebeke Kararsızlığı (Yüksek varyans, cihaz ömürlerini tehdit eder)
    elif region_variance > 1.5:
        logging.error(f"⚡ [Trend Engine] GridInstabilityDetected in {region_id}! Variance: {region_variance:.2f}")
        trend_anomaly_event = telemetry_pb2.TrendRegionEvent(
            event_id=str(uuid.uuid4()),
            region_id=region_id,
            anomaly_type="GridInstabilityDetected",
            moving_average=region_rolling_avg, 
            timestamp=int(time.time() * 1000)
        )
        
    # Öncelik 3: Kestirimci Uyarı / Hızlı Yük Artışı (Tehlikeye doğru gidiş)
    elif region_slope > 0.5:
        logging.warning(f"🚀 [Trend Engine] Predictive Alert: Rapid load growth in {region_id}!")
        trend_anomaly_event = telemetry_pb2.TrendRegionEvent(
            event_id=str(uuid.uuid4()),
            region_id=region_id,
            anomaly_type="RapidLoadGrowthDetected",
            moving_average=region_rolling_avg,
            timestamp=int(time.time() * 1000)
        )

    # Öncelik 4: Bireysel Şüpheli Tüketim (Bölge güvende ama tek bir sayaç çok akım çekiyor)
    elif meter_rolling_avg > 3.0:
        logging.warning(f"🕵️ [Trend Engine] Suspicious Consumption on {event.meter_id}! Avg: {meter_rolling_avg:.2f}kW")
        trend_anomaly_event = telemetry_pb2.TrendRegionEvent(
            event_id=str(uuid.uuid4()),
            region_id=region_id,
            anomaly_type="SuspiciousConsumptionPattern",
            moving_average=meter_rolling_avg,
            timestamp=int(time.time() * 1000)
        )

    # 4. Eğer bir alarm oluştuysa (Event None değilse) Kafka'ya fırlat
    if trend_anomaly_event:
        await producer.send_and_wait(
            TREND_TOPIC,
            trend_anomaly_event.SerializeToString()
        )

        logging.info(
            f"📤 Dispatched {trend_anomaly_event.anomaly_type} "
            f"(ID: {trend_anomaly_event.event_id}) to '{TREND_TOPIC}'"
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