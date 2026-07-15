"""
UrbanPulse — supporting producer : smart_meters
===============================================
Needed for Task C Part II (Spark ward-level energy aggregation).
Keyed by ward_id so all readings for a ward stay ordered together.

Run:  python producers/smart_meters_producer.py   (Ctrl+C to stop)
"""
import sys, os, json, time, random, signal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timezone
from kafka import KafkaProducer
from config import settings

WARDS = [f"W{n:02d}" for n in range(1, 9)]     # 8 wards
METERS_PER_WARD = 5
EVENTS_PER_SECOND = 20

meters = [f"M-{w}-{i+1}" for w in WARDS for i in range(METERS_PER_WARD)]

running = True
def stop(*_):
    global running
    running = False
signal.signal(signal.SIGINT, stop)


def make_event():
    meter = random.choice(meters)
    ward = meter.split("-")[1]
    return {
        "meter_id": meter,
        "ward_id": ward,
        "kwh_reading": round(random.uniform(0.2, 6.5), 3),
        "voltage": round(random.uniform(218, 245), 1),
        "power_factor": round(random.uniform(0.80, 0.99), 3),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )
    print(f"Producing smart meter readings to '{settings.TOPIC_SMART_METERS}'. Ctrl+C to stop.")
    sent = 0
    delay = 1.0 / EVENTS_PER_SECOND
    while running:
        ev = make_event()
        producer.send(settings.TOPIC_SMART_METERS, key=ev["ward_id"], value=ev)
        sent += 1
        if sent % 100 == 0:
            print(f"  sent {sent}  (last ward {ev['ward_id']}, {ev['kwh_reading']} kWh)")
        time.sleep(delay)
    producer.flush(); producer.close()
    print(f"Stopped. Total sent: {sent}")


if __name__ == "__main__":
    main()
