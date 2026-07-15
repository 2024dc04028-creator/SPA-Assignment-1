#!/usr/bin/env bash
# =============================================================================
# UrbanPulse — Task B.1 (ALTERNATIVE to create_topics.py)
# =============================================================================
# This does the SAME thing as setup/create_topics.py but uses Kafka's built-in
# command-line tool INSIDE the docker container. Use whichever you prefer.
#
# Run from the project root:   bash setup/create_topics.sh
# =============================================================================
set -e
B=kafka1:19092   # talk to broker 1 on the internal docker network

create () {  # name partitions retention_ms reason
  echo ">> creating $1  (partitions=$2, retention=$3 ms) -- $4"
  docker exec urbanpulse-kafka1 kafka-topics \
    --bootstrap-server "$B" \
    --create --if-not-exists \
    --topic "$1" \
    --partitions "$2" \
    --replication-factor 3 \
    --config retention.ms="$3"
}

HOUR=3600000
DAY=$((24 * HOUR))

create urbanpulse.bus_gps          6 $((1   * DAY)) "24h accident replay"
create urbanpulse.traffic_signals  3 $((7   * DAY)) "3 partitions for 3-consumer standard group"
create urbanpulse.air_quality      2 $((90  * DAY)) "90d pollution trends"
create urbanpulse.smart_meters     4 $((365 * DAY)) "365d energy audit"
create urbanpulse.bus_gps_enriched 6 $((1   * DAY)) "enriched ETA feed"
create urbanpulse.incidents        3 $((30  * DAY)) "incident audit trail"
create urbanpulse.ward_energy_summary 4 $((90 * DAY)) "ward energy summaries"
create urbanpulse.health_advisories   2 $((90 * DAY)) "health advisories"
create urbanpulse.dlq              3 $((14  * DAY)) "dead-letter queue"

echo ""
echo "All topics created. List them with:"
echo "  docker exec urbanpulse-kafka1 kafka-topics --bootstrap-server $B --list"
