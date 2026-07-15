"""
UrbanPulse — Task B.3 : consumer-group LAG monitor
==================================================
This is the PROOF for the assignment. It prints, every few seconds, the total
"lag" (how many messages each group still has to read) for both groups.

While the STANDARD group runs in --slow mode you should see:
    HIGH_PRIORITY      lag ~ 0           (keeps up in real time)
    STANDARD_PRIORITY  lag = growing...  (falling behind)

Run it in its own terminal alongside the producers and consumers:
    python consumers/lag_monitor.py
"""
import sys, os, time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
from config import settings

GROUPS = ["HIGH_PRIORITY", "STANDARD_PRIORITY"]
TOPIC = settings.TOPIC_TRAFFIC_SIGNALS


def group_lag(admin, end_consumer, group_id):
    """Total lag for a group = sum over partitions of (latest offset - committed)."""
    try:
        committed = admin.list_consumer_group_offsets(group_id)
    except Exception:
        return None
    if not committed:
        return None

    tps = list(committed.keys())
    end_offsets = end_consumer.end_offsets(tps)
    total = 0
    for tp, meta in committed.items():
        latest = end_offsets.get(tp, 0)
        total += max(0, latest - meta.offset)
    return total


def main():
    admin = KafkaAdminClient(bootstrap_servers=settings.BOOTSTRAP_SERVERS,
                             client_id="urbanpulse-lag")
    # a plain consumer just to query the latest (end) offsets
    end_consumer = KafkaConsumer(bootstrap_servers=settings.BOOTSTRAP_SERVERS)

    print("Monitoring consumer-group lag (Ctrl+C to stop)\n")
    print(f"{'time':<10}{'HIGH_PRIORITY':>18}{'STANDARD_PRIORITY':>22}")
    print("-" * 50)
    try:
        while True:
            row = time.strftime("%H:%M:%S")
            cells = []
            for g in GROUPS:
                lag = group_lag(admin, end_consumer, g)
                cells.append("n/a" if lag is None else str(lag))
            print(f"{row:<10}{cells[0]:>18}{cells[1]:>22}")
            time.sleep(3)
    except KeyboardInterrupt:
        pass
    finally:
        end_consumer.close()
        admin.close()
        print("\nStopped lag monitor.")


if __name__ == "__main__":
    main()
