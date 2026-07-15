"""
UrbanPulse — Task B.3 : STANDARD_PRIORITY consumer (analytics dashboard)
========================================================================
Run THREE copies of this in THREE terminals. They all share the group
"STANDARD_PRIORITY", so Kafka splits the 3 partitions one-per-consumer.

To DEMONSTRATE that the HIGH group is unaffected when this group falls behind,
start these with the --slow flag, which makes each message take ~0.4s to
"process". The analytics group's lag will balloon while the HIGH group stays ~0.

Run (three terminals):
    python consumers/priority_consumer_standard.py --slow
    python consumers/priority_consumer_standard.py --slow
    python consumers/priority_consumer_standard.py --slow

Without --slow they keep up normally.
"""
import sys, os, json, time, signal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka import KafkaConsumer
from config import settings

SLOW = "--slow" in sys.argv
SLOW_SECONDS = 0.4    # simulated heavy analytics work per message

running = True
def stop(*_):
    global running
    running = False
signal.signal(signal.SIGINT, stop)


def main():
    consumer = KafkaConsumer(
        settings.TOPIC_TRAFFIC_SIGNALS,
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        group_id="STANDARD_PRIORITY",                  # <- shared group
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        enable_auto_commit=True,
    )
    mode = "SLOW (simulating overload)" if SLOW else "normal"
    print(f"[STD] analytics consumer started in {mode} mode. "
          f"Kafka will assign it some partitions. Ctrl+C to stop.\n")
    n = 0
    for msg in consumer:
        if not running:
            break
        n += 1
        if SLOW:
            time.sleep(SLOW_SECONDS)        # fall behind on purpose
        if n % 20 == 0:
            print(f"[STD pid={os.getpid()}] processed {n} "
                  f"(partition {msg.partition})")
    consumer.close()
    print(f"[STD pid={os.getpid()}] stopped after {n} messages")


if __name__ == "__main__":
    main()
