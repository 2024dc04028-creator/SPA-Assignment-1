"""
UrbanPulse — Task B.5 : Dead-Letter Queue (DLQ) validator
=========================================================
Reads the raw air_quality and bus_gps streams, validates each record, and:
  * GOOD records  -> counted (in a real system, forwarded to a 'clean' topic)
  * BAD records   -> published to urbanpulse.dlq with an 'error_reason' field

Validation rules (as required by the assignment):
  * null values             -> e.g. aqi is None
  * out-of-range AQI        -> aqi outside 0..500
  * impossible GPS coords   -> lat outside -90..90 or lon outside -180..180

Run:  python dlq/dlq_validator.py    (Ctrl+C to stop)
Then, in another terminal, run the report:  python dlq/dlq_report.py
"""
import sys, os, json, signal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from config import settings

running = True
def stop(*_):
    global running
    running = False
signal.signal(signal.SIGINT, stop)


def validate(topic, record):
    """Return an error reason string if invalid, else None."""
    if topic == settings.TOPIC_AIR_QUALITY:
        if record.get("aqi") is None:
            return "NULL_AQI"
        if not (0 <= record["aqi"] <= 500):
            return "AQI_OUT_OF_RANGE"
    elif topic == settings.TOPIC_BUS_GPS:
        lat, lon = record.get("lat"), record.get("lon")
        if lat is None or lon is None:
            return "NULL_GPS"
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            return "IMPOSSIBLE_GPS"
    return None


def main():
    consumer = KafkaConsumer(
        settings.TOPIC_AIR_QUALITY, settings.TOPIC_BUS_GPS,
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        group_id="dlq-validator",
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    print(f"DLQ validator running. Bad records -> '{settings.TOPIC_DLQ}'. "
          f"Ctrl+C to stop.\n")
    good = bad = 0
    for msg in consumer:
        if not running:
            break
        reason = validate(msg.topic, msg.value)
        if reason is None:
            good += 1
        else:
            bad += 1
            dlq_msg = {
                "error_reason": reason,
                "source_topic": msg.topic,
                "source_partition": msg.partition,
                "source_offset": msg.offset,
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
                "original": msg.value,
            }
            producer.send(settings.TOPIC_DLQ, value=dlq_msg)
            print(f"  DLQ <- {reason:<16} from {msg.topic}")
        if (good + bad) % 200 == 0:
            print(f"  totals: good={good}  bad={bad}")

    producer.flush(); producer.close(); consumer.close()
    print(f"Stopped. good={good}  bad={bad}")


if __name__ == "__main__":
    main()
