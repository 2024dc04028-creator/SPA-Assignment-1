"""
UrbanPulse — generic topic viewer (testing helper)
==================================================
Print messages from ANY topic so you can SEE what your pipelines produce.

Run examples:
    python consumers/view_topic.py urbanpulse.incidents
    python consumers/view_topic.py urbanpulse.health_advisories
    python consumers/view_topic.py urbanpulse.bus_gps_enriched
    python consumers/view_topic.py urbanpulse.ward_energy_summary

Add 'earliest' to read from the very beginning:
    python consumers/view_topic.py urbanpulse.incidents earliest
"""
import sys, os, json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka import KafkaConsumer
from config import settings

if len(sys.argv) < 2:
    print("Usage: python consumers/view_topic.py <topic> [earliest|latest]")
    sys.exit(1)

topic = sys.argv[1]
offset = sys.argv[2] if len(sys.argv) > 2 else "latest"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=settings.BOOTSTRAP_SERVERS,
    auto_offset_reset=offset,
    value_deserializer=lambda b: b.decode("utf-8"),
)
print(f"Viewing '{topic}' (from {offset}). Ctrl+C to stop.\n")
try:
    for msg in consumer:
        try:
            pretty = json.dumps(json.loads(msg.value))
        except Exception:
            pretty = msg.value
        print(f"[p{msg.partition}@{msg.offset}] {pretty}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
