"""
UrbanPulse — Task C Part I (c) : BUS BUNCHING (PyFlink DataStream)
=================================================================
Rule: two buses on the SAME route_id stay within 200 metres of each other for
MORE than 5 minutes -> emit a bunching alert with both bus IDs.

How it works:
  * keyBy(route_id)               -> all buses on a route share one keyed state
  * MapState "positions"          -> latest {lat,lon,ts} for each bus on the route
  * MapState "pairs"              -> for each close pair, WHEN they first got close
  * if a pair is within 200 m and has been close for > 5 minutes -> alert

Reads : urbanpulse.bus_gps
Writes: urbanpulse.incidents
Run:  python flink/bus_bunching.py

Tip: the bus_gps_producer deliberately starts two R101 buses close together so
you can see this fire. Lower BUNCHING_DURATION_SEC in config/settings.py if you
don't want to wait 5 minutes during a demo.
"""
import sys, os, json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyflink.common import WatermarkStrategy, Types, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)

from config import settings
from flink._flink_common import kafka_connector_jar_uri, iso_to_millis, haversine_m

BOOTSTRAP = ",".join(settings.BOOTSTRAP_SERVERS)
BUNCH_MS = settings.BUNCHING_DURATION_SEC * 1000


class TsAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            return iso_to_millis(json.loads(value)["timestamp"])
        except Exception:
            return record_timestamp


class BunchingDetector(KeyedProcessFunction):

    def open(self, ctx):
        # latest position per bus on this route (value stored as JSON string)
        self.positions = ctx.get_map_state(
            MapStateDescriptor("positions", Types.STRING(), Types.STRING()))
        # for each close pair: epoch-ms when they first became close
        self.pairs = ctx.get_map_state(
            MapStateDescriptor("pairs", Types.STRING(), Types.LONG()))

    def process_element(self, raw, ctx):
        try:
            ev = json.loads(raw)
        except Exception:
            return
        bus_id = ev.get("bus_id")
        lat, lon = ev.get("lat"), ev.get("lon")
        ts = iso_to_millis(ev["timestamp"]) if ev.get("timestamp") else 0
        if bus_id is None or lat is None or lon is None:
            return

        # remember this bus's latest position
        self.positions.put(bus_id, json.dumps({"lat": lat, "lon": lon, "ts": ts}))

        # snapshot the other buses so we don't modify state while iterating
        others = [(b, json.loads(v)) for b, v in self.positions.items()
                  if b != bus_id]

        for other_id, pos in others:
            dist = haversine_m(lat, lon, pos["lat"], pos["lon"])
            pair = "|".join(sorted([bus_id, other_id]))

            if dist <= settings.BUNCHING_DISTANCE_M:
                if not self.pairs.contains(pair):
                    self.pairs.put(pair, ts)          # first time they got close
                else:
                    first = self.pairs.get(pair)
                    if ts - first > BUNCH_MS:
                        a, b = pair.split("|")
                        alert = {
                            "incident_type": "BUS_BUNCHING",
                            "route_id": ev.get("route_id"),
                            "bus_a": a,
                            "bus_b": b,
                            "distance_m": round(dist, 1),
                            "minutes_close": round((ts - first) / 60000, 1),
                            "detected_from_timestamp": ev.get("timestamp"),
                        }
                        self.pairs.put(pair, ts)       # cooldown: restart timer
                        yield json.dumps(alert)
            else:
                if self.pairs.contains(pair):
                    self.pairs.remove(pair)            # they separated; reset


def key_by_route(raw):
    try:
        return json.loads(raw).get("route_id", "UNKNOWN")
    except Exception:
        return "UNKNOWN"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(kafka_connector_jar_uri())
    env.set_parallelism(1)

    source = (KafkaSource.builder()
              .set_bootstrap_servers(BOOTSTRAP)
              .set_topics(settings.TOPIC_BUS_GPS)
              .set_group_id("flink-bunching")
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

    stream = env.from_source(source, wm, "bus_gps_source")
    alerts = (stream
              .key_by(key_by_route, key_type=Types.STRING())
              .process(BunchingDetector(), output_type=Types.STRING()))
    alerts.print()
    alerts.sink_to(sink)

    print("Flink Bus Bunching job running. "
          "two buses <200m for >5min -> urbanpulse.incidents")
    env.execute("UrbanPulse-Bus-Bunching")


if __name__ == "__main__":
    main()
