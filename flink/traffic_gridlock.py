"""
UrbanPulse — Task C Part I (b) : TRAFFIC GRIDLOCK (PyFlink DataStream)
=====================================================================
Rule: a junction's average wait time exceeds 180 seconds for 3 CONSECUTIVE
signal cycles -> emit a gridlock alert with junction_id and zone.

How it works:
  * keyBy(junction_id)            -> each junction has its own state
  * KeyedProcessFunction          -> we keep a small counter in ValueState
  * counter++ when wait > 180     -> reset to 0 when it drops below
  * counter reaches 3             -> emit alert, reset counter

Reads : urbanpulse.traffic_signals
Writes: urbanpulse.incidents
Run:  python flink/traffic_gridlock.py
"""
import sys, os, json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyflink.common import WatermarkStrategy, Types, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)

from config import settings
from flink._flink_common import kafka_connector_jar_uri, iso_to_millis

BOOTSTRAP = ",".join(settings.BOOTSTRAP_SERVERS)


class TsAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            return iso_to_millis(json.loads(value)["timestamp"])
        except Exception:
            return record_timestamp


class GridlockDetector(KeyedProcessFunction):
    """Counts consecutive over-threshold cycles per junction."""

    def open(self, ctx):
        # ValueState = a single value remembered per key (per junction)
        self.streak = ctx.get_state(
            ValueStateDescriptor("congestion_streak", Types.INT()))

    def process_element(self, raw, ctx):
        try:
            ev = json.loads(raw)
        except Exception:
            return
        wait = ev.get("avg_wait_sec", 0) or 0
        current = self.streak.value() or 0

        if wait > settings.GRIDLOCK_WAIT_SECONDS:
            current += 1
        else:
            current = 0
        self.streak.update(current)

        if current >= settings.GRIDLOCK_CYCLES:
            alert = {
                "incident_type": "TRAFFIC_GRIDLOCK",
                "junction_id": ev.get("junction_id"),
                "zone": ev.get("zone"),
                "avg_wait_sec": wait,
                "consecutive_cycles": current,
                "detected_from_timestamp": ev.get("timestamp"),
            }
            self.streak.update(0)        # reset so we don't spam every cycle
            yield json.dumps(alert)


def key_by_junction(raw):
    try:
        return json.loads(raw).get("junction_id", "UNKNOWN")
    except Exception:
        return "UNKNOWN"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(kafka_connector_jar_uri())
    env.set_parallelism(1)

    source = (KafkaSource.builder()
              .set_bootstrap_servers(BOOTSTRAP)
              .set_topics(settings.TOPIC_TRAFFIC_SIGNALS)
              .set_group_id("flink-gridlock")
              .set_starting_offsets(KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(SimpleStringSchema())
              .build())

    wm = (WatermarkStrategy
          .for_bounded_out_of_orderness(Duration.of_seconds(5))
          .with_timestamp_assigner(TsAssigner()))

    sink = (KafkaSink.builder()
            .set_bootstrap_servers(BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(settings.TOPIC_INCIDENTS)
                .set_value_serialization_schema(SimpleStringSchema())
                .build())
            .build())

    stream = env.from_source(source, wm, "traffic_signals_source")
    alerts = (stream
              .key_by(key_by_junction, key_type=Types.STRING())
              .process(GridlockDetector(), output_type=Types.STRING()))
    alerts.print()
    alerts.sink_to(sink)

    print("Flink Traffic Gridlock job running. "
          ">180s for 3 cycles -> urbanpulse.incidents")
    env.execute("UrbanPulse-Traffic-Gridlock")


if __name__ == "__main__":
    main()
