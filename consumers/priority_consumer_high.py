"""
UrbanPulse — Task B.3 : HIGH_PRIORITY consumer (signal control)
===============================================================
ONE consumer in the group "HIGH_PRIORITY". Because it is the only member of its
group, Kafka assigns ALL 3 partitions of urbanpulse.traffic_signals to it, so it
sees every message. It processes instantly (signal control must be real-time).

This group has its OWN offsets, completely independent of the STANDARD group, so
even if the analytics consumers fall behind, this one stays current.

Run:  python consumers/priority_consumer_high.py
"""
import sys, os, json, signal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka import KafkaConsumer
from config import settings

running = True
def stop(*_):
    global running
    running = False
signal.signal(signal.SIGINT, stop)


def main():
    consumer = KafkaConsumer(
        settings.TOPIC_TRAFFIC_SIGNALS,
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        group_id="HIGH_PRIORITY",                      # <- its own group
        auto_offset_reset="latest",                    # only new messages
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        enable_auto_commit=True,
    )
    print("[HIGH] signal-control consumer started. Reads ALL partitions, "
          "processes instantly. Ctrl+C to stop.\n")
    n = 0
    for msg in consumer:
        if not running:
            break
        ev = msg.value
        n += 1
        # "Signal control" reaction: if a junction is jammed, we'd adapt timing.
        if ev.get("avg_wait_sec", 0) > settings.GRIDLOCK_WAIT_SECONDS:
            print(f"[HIGH] ADAPT signal at {ev['junction_id']} "
                  f"(wait {ev['avg_wait_sec']}s)  p{msg.partition}@{msg.offset}")
        if n % 100 == 0:
            print(f"[HIGH] processed {n} messages (kept up in real time)")
    consumer.close()
    print(f"[HIGH] stopped after {n} messages")


if __name__ == "__main__":
    main()
