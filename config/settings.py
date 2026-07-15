"""
UrbanPulse — central configuration
-----------------------------------
Every Python script in this project imports its settings from here, so you
only ever change connection details in ONE place.

Beginner note:
- "Bootstrap servers" are simply the addresses where our Kafka brokers listen.
- Because we run 3 brokers in Docker, the client can connect to any of them
  and Kafka figures out the rest automatically.
"""

# ---------------------------------------------------------------------------
# Kafka connection
# ---------------------------------------------------------------------------
# These three addresses are the three brokers we start in docker-compose.yml.
# A producer/consumer only needs ONE of them to connect, but listing all three
# means the client can still connect if one broker is down.
BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]

# ---------------------------------------------------------------------------
# Topic names (kept in one place so nobody mistypes them)
# ---------------------------------------------------------------------------
TOPIC_BUS_GPS         = "urbanpulse.bus_gps"
TOPIC_TRAFFIC_SIGNALS = "urbanpulse.traffic_signals"
TOPIC_AIR_QUALITY     = "urbanpulse.air_quality"
TOPIC_SMART_METERS    = "urbanpulse.smart_meters"

# Derived / output topics produced by our processing jobs
TOPIC_BUS_GPS_ENRICHED = "urbanpulse.bus_gps_enriched"
TOPIC_INCIDENTS        = "urbanpulse.incidents"
TOPIC_WARD_ENERGY      = "urbanpulse.ward_energy_summary"
TOPIC_HEALTH_ADVISORY  = "urbanpulse.health_advisories"
TOPIC_DLQ              = "urbanpulse.dlq"

# ---------------------------------------------------------------------------
# Static reference data (the "lookup tables" used in enrichment joins)
# ---------------------------------------------------------------------------
import os
_HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(_HERE, "..", "data"))

ROUTE_SCHEDULE_CSV = os.path.join(DATA_DIR, "route_schedule.csv")
ZONE_PROFILE_CSV   = os.path.join(DATA_DIR, "zone_profile.csv")

# Where Spark writes its Parquet history (created automatically at runtime)
OUTPUT_DIR   = os.path.abspath(os.path.join(_HERE, "..", "output"))
PARQUET_DIR  = os.path.join(OUTPUT_DIR, "ward_energy_parquet")
CHECKPOINT_DIR = os.path.join(OUTPUT_DIR, "checkpoints")

# ---------------------------------------------------------------------------
# Business thresholds (single source of truth for the alert rules)
# ---------------------------------------------------------------------------
AQI_HAZARDOUS_THRESHOLD = 300   # AQI emergency  (Task C, Flink)
AQI_UNHEALTHY_THRESHOLD = 150   # health advisory (Task C, SQL)
GRIDLOCK_WAIT_SECONDS   = 180   # traffic gridlock
GRIDLOCK_CYCLES         = 3     # consecutive cycles
BUNCHING_DISTANCE_M     = 200   # bus bunching distance in metres
BUNCHING_DURATION_SEC   = 300   # 5 minutes
