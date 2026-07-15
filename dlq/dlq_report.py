"""
UrbanPulse — Task B.5 : 5-minute DLQ report
===========================================
Reads urbanpulse.dlq for a fixed window (default 5 minutes) and prints the
distribution of error types, then saves a small report to output/dlq_report.txt.

Run:  python dlq/dlq_report.py
      python dlq/dlq_report.py 60      # custom window in seconds (here 60s)
"""
import sys, os, json, time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from collections import Counter
from datetime import datetime
from kafka import KafkaConsumer
from config import settings

WINDOW_SECONDS = int(sys.argv[1]) if len(sys.argv) > 1 else 300   # 5 minutes


def main():
    consumer = KafkaConsumer(
        settings.TOPIC_DLQ,
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        group_id="dlq-report",
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=2000,     # don't block forever between messages
    )

    print(f"Collecting DLQ messages for {WINDOW_SECONDS}s "
          f"(start your producers + dlq_validator first)...\n")
    by_reason = Counter()
    by_topic = Counter()
    total = 0
    end = time.time() + WINDOW_SECONDS
    while time.time() < end:
        for msg in consumer:
            by_reason[msg.value.get("error_reason", "UNKNOWN")] += 1
            by_topic[msg.value.get("source_topic", "UNKNOWN")] += 1
            total += 1
            if time.time() >= end:
                break

    lines = []
    lines.append("=" * 50)
    lines.append("UrbanPulse — DLQ Report")
    lines.append(f"Generated : {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"Window    : {WINDOW_SECONDS} seconds")
    lines.append(f"Total bad records: {total}")
    lines.append("-" * 50)
    lines.append("Error type distribution:")
    for reason, count in by_reason.most_common():
        pct = (count / total * 100) if total else 0
        lines.append(f"   {reason:<18} {count:>6}  ({pct:5.1f}%)")
    lines.append("-" * 50)
    lines.append("By source topic:")
    for topic, count in by_topic.most_common():
        lines.append(f"   {topic:<28} {count:>6}")
    lines.append("=" * 50)

    report = "\n".join(lines)
    print("\n" + report)

    os.makedirs(settings.OUTPUT_DIR, exist_ok=True)
    out = os.path.join(settings.OUTPUT_DIR, "dlq_report.txt")
    with open(out, "w") as f:
        f.write(report + "\n")
    print(f"\nSaved -> {out}")
    consumer.close()


if __name__ == "__main__":
    main()
