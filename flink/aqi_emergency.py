"""
UrbanPulse — Task C Part I (a) : AQI EMERGENCY (PyFlink DataStream)
==================================================================
Rule: any air-quality sensor reporting AQI > 300 (Hazardous) -> emit an alert
within 2 minutes. Because this is a streaming filter, alerts are emitted in
(sub-)seconds, comfortably inside the 2-minute target.

Reads : urbanpulse.air_quality
Writes: urbanpulse.incidents

Run:  python flink/aqi_emergency.py
(Make sure the Kafka connector jar is in flink/lib/ — see EXECUTION_GUIDE.md.)
"""
import sys, os, json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyflink.common import WatermarkStrategy, Types, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)

from config import settings
from flink._flink_common import kafka_connector_jar_uri, iso_to_millis

BOOTSTRAP = ",".join(settings.BOOTSTRAP_SERVERS)


class AqiTsAssigner(TimestampAssigner):
    """Tell Flink to use each reading's own 'timestamp' as the event time."""
    def extract_timestamp(self, value, record_timestamp):
        try:
            return iso_to_millis(json.loads(value)["timestamp"])
        except Exception:
            return record_timestamp


def to_alert(raw):
    """Map a hazardous reading to an incident alert; return None to drop."""
    try:
        ev = json.loads(raw)
    except Exception:
        return None
    if ev.get("aqi") is None:
        return None
    if ev["aqi"] > settings.AQI_HAZARDOUS_THRESHOLD:
        alert = {
            "incident_type": "AQI_EMERGENCY",
            "sensor_id": ev.get("sensor_id"),
            "zone": ev.get("zone"),
            "aqi": ev["aqi"],
            "detected_from_timestamp": ev.get("timestamp"),
        }
        return json.dumps(alert)
    return None


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(kafka_connector_jar_uri())
    env.set_parallelism(1)   # simple & deterministic for a laptop demo

    source = (KafkaSource.builder()
              .set_bootstrap_servers(BOOTSTRAP)
              .set_topics(settings.TOPIC_AIR_QUALITY)
              .set_group_id("flink-aqi-emergency")
              .set_starting_offsets(KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(SimpleStringSchema())
              .build())

    wm = (WatermarkStrategy
          .for_bounded_out_of_orderness(Duration.of_seconds(5))
          .with_timestamp_assigner(AqiTsAssigner()))

    sink = (KafkaSink.builder()
            .set_bootstrap_servers(BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(settings.TOPIC_INCIDENTS)
                .set_value_serialization_schema(SimpleStringSchema())
                .build())
            .build())

    stream = env.from_source(source, wm, "air_quality_source")
    alerts = (stream
              .map(to_alert, output_type=Types.STRING())
              .filter(lambda x: x is not None))
    alerts.print()              # also show alerts in the console
    alerts.sink_to(sink)

    print("Flink AQI Emergency job running. AQI > 300 -> urbanpulse.incidents")
    env.execute("UrbanPulse-AQI-Emergency")


if __name__ == "__main__":
    main()
