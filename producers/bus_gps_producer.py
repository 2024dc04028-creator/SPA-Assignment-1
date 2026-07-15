"""
UrbanPulse — Task B.2 : bus_gps producer
=========================================
Requirements satisfied:
  * Uses route_id as the Kafka KEY  -> guarantees ordering of positions per route
    (all messages with the same key always land on the same partition, and Kafka
     preserves order WITHIN a partition).
  * Emits realistic moving-bus events as JSON.

Run from project root:   python producers/bus_gps_producer.py

Press Ctrl+C to stop.

Beginner note on "ordering per route":
  Kafka only promises order inside a single partition. If we let Kafka pick a
  random partition per message, two updates for the same bus could be processed
  out of order. By setting key=route_id, every update for a route is pinned to
  one partition, so "bus moved from A->B->C" stays in that order.
"""
import sys, os, json, time, random, signal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timezone
from kafka import KafkaProducer
from config import settings

# ---- simulation setup -------------------------------------------------------
ROUTES = ["R101", "R102", "R103", "R104", "R105", "R106", "R107", "R108"]
BUSES_PER_ROUTE = 3            # keep small for a laptop demo
EVENTS_PER_SECOND = 40        # demo rate (real city ~2,400/s); raise if you like

# A rough lat/lon "centre" for MetroConnect (Bengaluru-ish) so coordinates look real
CITY_LAT, CITY_LON = 12.9716, 77.5946

# Build a little fleet: each bus has a position it slowly walks around
fleet = []
for r in ROUTES:
    for i in range(BUSES_PER_ROUTE):
        fleet.append({
            "bus_id": f"{r}-B{i+1}",
            "route_id": r,
            "lat": CITY_LAT + random.uniform(-0.05, 0.05),
            "lon": CITY_LON + random.uniform(-0.05, 0.05),
        })

# Deliberately put two buses on route R101 VERY close together so the
# "bus bunching" detector in Task C has something to find.
fleet[0]["lat"] = CITY_LAT
fleet[0]["lon"] = CITY_LON
fleet[1]["lat"] = CITY_LAT + 0.0005      # ~55 metres away -> within 200 m
fleet[1]["lon"] = CITY_LON

running = True
def stop(*_):
    global running
    running = False
signal.signal(signal.SIGINT, stop)


def make_event(bus):
    # # Walk the bus a tiny bit so positions change over time
    # bus["lat"] += random.uniform(-0.0008, 0.0008)
    # bus["lon"] += random.uniform(-0.0008, 0.0008)

    # Keep the two R101 demo buses close so "bus bunching" can actually fire;
    # every other bus still wanders freely.

    if bus["bus_id"] == "R101-B1":            # anchor: drifts only slightly
        bus["lat"] += random.uniform(-0.0001, 0.0001)
        bus["lon"] += random.uniform(-0.0001, 0.0001)
    elif bus["bus_id"] == "R101-B2":          # follower: shadows the anchor ~55 m away
        bus["lat"] = fleet[0]["lat"] + 0.0005
        bus["lon"] = fleet[0]["lon"]
    else:
        bus["lat"] += random.uniform(-0.0008, 0.0008)
        bus["lon"] += random.uniform(-0.0008, 0.0008)
        
    return {
        "bus_id": bus["bus_id"],
        "route_id": bus["route_id"],
        "lat": round(bus["lat"], 6),
        "lon": round(bus["lon"], 6),
        "speed_kmh": round(random.uniform(0, 55), 1),
        "occupancy_pct": random.randint(10, 100),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        # turn python dict -> JSON bytes
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # the KEY is route_id, also sent as bytes
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",          # wait for all in-sync replicas (reliable)
        retries=3,
    )

    print(f"Producing bus GPS to '{settings.TOPIC_BUS_GPS}' "
          f"at ~{EVENTS_PER_SECOND}/s. Ctrl+C to stop.\n")
    sent = 0
    delay = 1.0 / EVENTS_PER_SECOND
    while running:
        bus = random.choice(fleet)
        event = make_event(bus)
        # KEY = route_id  => ordering guarantee per route
        producer.send(settings.TOPIC_BUS_GPS, key=event["route_id"], value=event)
        sent += 1
        if sent % 100 == 0:
            print(f"  sent {sent} events  (last: {event['bus_id']} "
                  f"route {event['route_id']})")
        time.sleep(delay)

    print("\nFlushing remaining messages...")
    producer.flush()
    producer.close()
    print(f"Stopped. Total sent: {sent}")


if __name__ == "__main__":
    main()
