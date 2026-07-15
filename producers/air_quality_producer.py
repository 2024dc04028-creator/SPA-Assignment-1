"""
UrbanPulse — Task B.2 : air_quality producer
=============================================
Requirements satisfied:
  * AT-LEAST-ONCE semantics  : acks="all" + retries, and we BLOCK on each send
    to confirm the broker stored it; if it fails we retry manually. This means a
    message is never silently lost (it may, in rare cases, be delivered twice —
    that is exactly what "at-least-once" means).
  * Simulated sensor failure : ~5% of events have a NULL aqi. The producer
    detects these, LOGS a warning, and still forwards them (the DLQ stage in
    Task B.5 will quarantine them). Nothing crashes.

Run from project root:   python producers/air_quality_producer.py
Ctrl+C to stop.
"""
import sys, os, json, time, random, signal, logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import settings

# ---- logging: prints a clean timestamped line for every notable event -------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("air_quality_producer")

ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5", "Z6"]
SENSORS = [f"AQ-{z}-{n}" for z in ZONES for n in (1, 2)]   # 2 sensors per zone
EVENTS_PER_SECOND = 10            # demo rate (real ~60/s)
NULL_AQI_RATE = 0.05              # 5% faulty readings
MAX_SEND_RETRIES = 3             # manual retry attempts per message

running = True
def stop(*_):
    global running
    running = False
signal.signal(signal.SIGINT, stop)


def make_event():
    zone = random.choice(ZONES)
    sensor = random.choice([s for s in SENSORS if s.startswith(f"AQ-{zone}-")])
    # Occasionally push AQI into the hazardous range so Task C alerts fire
    aqi = random.choices(
        population=[random.randint(20, 140),    # good/moderate
                    random.randint(151, 290),   # unhealthy
                    random.randint(301, 450)],   # hazardous (triggers emergency)
        weights=[0.7, 0.2, 0.1],
    )[0]

    event = {
        "sensor_id": sensor,
        "zone": zone,
        "pm25": round(random.uniform(10, 220), 1),
        "pm10": round(random.uniform(20, 350), 1),
        "no2":  round(random.uniform(5, 120), 1),
        "aqi":  aqi,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Simulated sensor failure: 5% of readings lose their AQI value
    if random.random() < NULL_AQI_RATE:
        event["aqi"] = None
    return event


def send_at_least_once(producer, event):
    """
    Send ONE event with at-least-once guarantees.
    We block on the broker acknowledgement (future.get) and retry on failure.
    Returns True if the broker confirmed storage.
    """
    for attempt in range(1, MAX_SEND_RETRIES + 1):
        try:
            future = producer.send(settings.TOPIC_AIR_QUALITY,
                                   key=event["sensor_id"].encode("utf-8"),
                                   value=event)
            # Block until the broker confirms (or raises) -> this is what makes
            # the delivery guarantee real instead of fire-and-forget.
            md = future.get(timeout=10)
            return True
        except KafkaError as e:
            log.warning(f"send failed (attempt {attempt}/{MAX_SEND_RETRIES}) "
                        f"for {event['sensor_id']}: {e}")
            time.sleep(0.5 * attempt)   # back off a little before retrying
    log.error(f"GAVE UP sending event from {event['sensor_id']} after "
              f"{MAX_SEND_RETRIES} attempts")
    return False


def main():
    producer = KafkaProducer(
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",      # all in-sync replicas must store it -> no data loss
        retries=5,       # client-level retries on top of our manual retries
    )

    log.info(f"Producing air quality to '{settings.TOPIC_AIR_QUALITY}' "
             f"at ~{EVENTS_PER_SECOND}/s. Ctrl+C to stop.")
    sent, nulls = 0, 0
    delay = 1.0 / EVENTS_PER_SECOND
    while running:
        event = make_event()
        if event["aqi"] is None:
            nulls += 1
            log.warning(f"NULL AQI from sensor {event['sensor_id']} in zone "
                        f"{event['zone']} -> forwarding for DLQ handling")
        if send_at_least_once(producer, event):
            sent += 1
            if sent % 50 == 0:
                log.info(f"confirmed {sent} events ({nulls} had null AQI)")
        time.sleep(delay)

    log.info("Flushing...")
    producer.flush()
    producer.close()
    log.info(f"Stopped. Confirmed sent: {sent}, of which null AQI: {nulls}")


if __name__ == "__main__":
    main()
