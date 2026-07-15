"""
UrbanPulse — supporting producer : traffic_signals
==================================================
Not one of the two *required* producers, but needed so that:
  * Task B.3 (priority consumers) has a stream to read, and
  * Task C Flink "traffic gridlock" detection has data.

A few junctions are deliberately kept congested (avg_wait_sec > 180) for several
cycles in a row so the gridlock alert fires.

Run:  python producers/traffic_signals_producer.py   (Ctrl+C to stop)
"""
import sys, os, json, time, random, signal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timezone
from kafka import KafkaProducer
from config import settings

ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5", "Z6"]
JUNCTIONS = [f"J{n:02d}" for n in range(1, 13)]      # 12 junctions
PHASES = ["RED", "GREEN", "AMBER"]
EVENTS_PER_SECOND = 8

# Two junctions are "always congested" so gridlock detection has a target
CONGESTED = {"J01", "J07"}

running = True
def stop(*_):
    global running
    running = False
signal.signal(signal.SIGINT, stop)


def make_event(junction):
    if junction in CONGESTED:
        wait = random.uniform(185, 260)     # above the 180s gridlock threshold
    else:
        wait = random.uniform(20, 170)
    return {
        "junction_id": junction,
        "zone": random.choice(ZONES),
        "vehicle_count": random.randint(5, 120),
        "avg_wait_sec": round(wait, 1),
        "signal_phase": random.choice(PHASES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )
    print(f"Producing traffic signals to '{settings.TOPIC_TRAFFIC_SIGNALS}'. Ctrl+C to stop.")
    sent = 0
    delay = 1.0 / EVENTS_PER_SECOND
    while running:
        j = random.choice(JUNCTIONS)
        ev = make_event(j)
        producer.send(settings.TOPIC_TRAFFIC_SIGNALS, key=j, value=ev)
        sent += 1
        if sent % 50 == 0:
            print(f"  sent {sent}  (last junction {j}, wait {ev['avg_wait_sec']}s)")
        time.sleep(delay)
    producer.flush(); producer.close()
    print(f"Stopped. Total sent: {sent}")


if __name__ == "__main__":
    main()
