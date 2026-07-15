"""
UrbanPulse — shared PyFlink helpers
===================================
Used by the three Flink jobs. Keeps the fiddly setup in ONE place.

The most common beginner problem with PyFlink is the Kafka connector JAR. PyFlink
ships without it, so you must download ONE jar and drop it in  flink/lib/ .
See EXECUTION_GUIDE.md, section "Flink setup", for the exact download command.
This helper finds that jar automatically.
"""
import os, glob, math
from pathlib import Path

# Folder where you place the Kafka connector jar (flink/lib/)
LIB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib")


def kafka_connector_jar_uri():
    """Return file URI of the flink-sql-connector-kafka jar."""
    matches = glob.glob(os.path.join(LIB_DIR, "flink-sql-connector-kafka*.jar"))
    if not matches:
        raise FileNotFoundError(
            "No Kafka connector jar found in flink/lib/"
        )

    # pathlib generates a correct URI on Windows/macOS/Linux
    return Path(matches[0]).resolve().as_uri()


def haversine_m(lat1, lon1, lat2, lon2):
    """Distance between two lat/lon points in METRES (used for bus bunching)."""
    R = 6_371_000  # earth radius in metres
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def iso_to_millis(ts_iso):
    """Convert an ISO timestamp string to epoch milliseconds (for event time)."""
    from datetime import datetime
    # handles the trailing 'Z' or +00:00 produced by our producers
    ts_iso = ts_iso.replace("Z", "+00:00")
    return int(datetime.fromisoformat(ts_iso).timestamp() * 1000)
