"""
UrbanPulse — Task B.4 : real-time enrichment (stream-table join)
================================================================
GOAL: join the live bus_gps stream with the static route_schedule "table" to add
scheduled_arrival_time, route_name and terminal -> the basis of the ETA service.

IMPORTANT (read this for your report/viva):
  Kafka Streams is a JAVA/Scala library — it has NO Python API. The assignment is
  to be done in Python, so this file implements the SAME pattern in Python:
      * route_schedule.csv is loaded into a dictionary  = the "KTable"
        (a KTable is just the latest value per key; a CSV lookup table is exactly that)
      * we consume the bus_gps stream                    = the "KStream"
      * for each GPS event we look up its route_id        = the stream-table JOIN
      * we publish the joined record to a new topic       = enriched output
  (If your course allows the "Faust" library, it offers a Kafka-Streams-like API
   in Python; this hand-rolled version avoids extra dependencies and is clearer.)

Run:  python enrichment/bus_gps_enrichment.py
"""
import sys, os, csv, json, signal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka import KafkaConsumer, KafkaProducer
from config import settings

running = True
def stop(*_):
    global running
    running = False
signal.signal(signal.SIGINT, stop)


def load_route_ktable(path):
    """Load route_schedule.csv into {route_id: {...}} — this is our KTable."""
    table = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            table[row["route_id"]] = {
                "route_name": row["route_name"],
                "scheduled_arrival_time": row["scheduled_arrival_time"],
                "terminal": row["terminal"],
            }
    print(f"Loaded KTable with {len(table)} routes from {os.path.basename(path)}")
    return table


def main():
    ktable = load_route_ktable(settings.ROUTE_SCHEDULE_CSV)

    consumer = KafkaConsumer(
        settings.TOPIC_BUS_GPS,
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        group_id="enrichment-eta",
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )

    print(f"Enriching '{settings.TOPIC_BUS_GPS}' -> "
          f"'{settings.TOPIC_BUS_GPS_ENRICHED}'. Ctrl+C to stop.\n")
    n = 0
    for msg in consumer:
        if not running:
            break
        gps = msg.value
        route = ktable.get(gps.get("route_id"))
        # THE JOIN: combine the stream event with its route-table row
        enriched = dict(gps)
        if route:
            enriched.update(route)          # adds the 3 schedule fields
        else:
            enriched.update({"route_name": "UNKNOWN",
                             "scheduled_arrival_time": None,
                             "terminal": "UNKNOWN"})
        producer.send(settings.TOPIC_BUS_GPS_ENRICHED,
                      key=enriched["route_id"], value=enriched)
        n += 1
        if n % 100 == 0:
            print(f"  enriched {n}  (e.g. {enriched['bus_id']} on "
                  f"'{enriched['route_name']}' -> {enriched['terminal']})")

    producer.flush(); producer.close(); consumer.close()
    print(f"Stopped after enriching {n} events")


if __name__ == "__main__":
    main()
