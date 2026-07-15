"""
UrbanPulse — Task B.1 : create all Kafka topics
================================================
Run this ONCE after the cluster is up (see EXECUTION_GUIDE.md).

It creates every topic with:
  * a justified PARTITION COUNT  (parallelism vs. event rate)
  * a RETENTION POLICY           (how long messages are kept)
  * replication factor 3         (one copy on each of the 3 brokers = fault tolerant)

Run it from the project root:   python setup/create_topics.py
"""

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config import settings

# Handy constants so retention is readable instead of a wall of zeros
HOUR_MS = 60 * 60 * 1000
DAY_MS  = 24 * HOUR_MS

# -----------------------------------------------------------------------------
# Topic plan.  (partitions, replication, retention_ms, why)
# -----------------------------------------------------------------------------
# PARTITION JUSTIFICATION
#   Partitions = how much PARALLELISM a topic allows. More partitions => more
#   consumers can read in parallel, but also more overhead. Rule of thumb we use:
#   roughly match partitions to expected consumer parallelism and event rate.
#
#   bus_gps         ~2,400 ev/s  -> highest volume, keyed by route_id  -> 6 partitions
#   traffic_signals   ~380 ev/s  -> needs a 3-consumer STANDARD group   -> 3 partitions
#   air_quality        ~60 ev/s  -> low volume, simple processing       -> 2 partitions
#   smart_meters    ~1,100 ev/s  -> high volume, keyed by ward_id       -> 4 partitions
#
# RETENTION JUSTIFICATION (from the assignment)
#   bus_gps        24 hours  -> accident-investigation replay window
#   air_quality    90 days   -> pollution trend analysis
#   smart_meters   365 days  -> regulatory energy audits
#   traffic_signals 7 days   -> not specified; 7 days is enough for tuning/debug
# -----------------------------------------------------------------------------
TOPIC_PLAN = [
    # name,                          partitions, replication, retention_ms,          reason
    (settings.TOPIC_BUS_GPS,          6, 3, 1  * DAY_MS,   "24h replay for accident investigation"),
    (settings.TOPIC_TRAFFIC_SIGNALS,  3, 3, 7  * DAY_MS,   "3 partitions for the 3-consumer standard group"),
    (settings.TOPIC_AIR_QUALITY,      2, 3, 90 * DAY_MS,   "90d pollution trend analysis"),
    (settings.TOPIC_SMART_METERS,     4, 3, 365 * DAY_MS,  "365d regulatory energy audit"),

    # Derived/output topics used by later tasks (sensible short retention)
    (settings.TOPIC_BUS_GPS_ENRICHED, 6, 3, 1  * DAY_MS,   "enriched ETA feed"),
    (settings.TOPIC_INCIDENTS,        3, 3, 30 * DAY_MS,   "incident audit trail"),
    (settings.TOPIC_WARD_ENERGY,      4, 3, 90 * DAY_MS,   "ward energy summaries"),
    (settings.TOPIC_HEALTH_ADVISORY,  2, 3, 90 * DAY_MS,   "health advisories"),
    (settings.TOPIC_DLQ,              3, 3, 14 * DAY_MS,   "dead-letter queue for bad records"),
]


def main():
    print("Connecting to Kafka brokers:", settings.BOOTSTRAP_SERVERS)
    admin = KafkaAdminClient(
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        client_id="urbanpulse-topic-admin",
    )

    new_topics = []
    for name, parts, repl, retention_ms, reason in TOPIC_PLAN:
        new_topics.append(
            NewTopic(
                name=name,
                num_partitions=parts,
                replication_factor=repl,
                topic_configs={"retention.ms": str(retention_ms)},
            )
        )

    print("\nCreating topics:\n" + "-" * 70)
    for name, parts, repl, retention_ms, reason in TOPIC_PLAN:
        print(f"  {name:<34} parts={parts} repl={repl} "
              f"retention={retention_ms // DAY_MS}d  ({reason})")
    print("-" * 70)

    # Create each topic; if it already exists we just say so (idempotent re-runs)
    for nt in new_topics:
        try:
            admin.create_topics([nt])
            print(f"  created  -> {nt.name}")
        except TopicAlreadyExistsError:
            print(f"  exists   -> {nt.name} (skipped)")
        except Exception as e:
            # kafka-python raises a batch error if ANY topic exists; handle gracefully
            print(f"  note     -> {nt.name}: {e}")

    admin.close()
    print("\nDone. Verify with:  python setup/verify_cluster.py")


if __name__ == "__main__":
    main()
