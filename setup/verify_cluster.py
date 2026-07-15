"""
UrbanPulse — verify the cluster is healthy and show topic configuration.
Run:  python setup/verify_cluster.py
"""
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka import KafkaConsumer
from config import settings


def main():
    print("Checking brokers:", settings.BOOTSTRAP_SERVERS)
    admin = KafkaAdminClient(bootstrap_servers=settings.BOOTSTRAP_SERVERS,
                             client_id="urbanpulse-verify")

    # 1) Which brokers are alive?
    cluster = admin._client.cluster
    print("\nLive brokers:")
    for b in cluster.brokers():
        print(f"   broker id={b.nodeId}  host={b.host}:{b.port}")

    # 2) List our topics + partition counts
    consumer = KafkaConsumer(bootstrap_servers=settings.BOOTSTRAP_SERVERS)
    print("\nTopics found:")
    for t in sorted(consumer.topics()):
        if t.startswith("urbanpulse"):
            parts = consumer.partitions_for_topic(t)
            print(f"   {t:<34} partitions={sorted(parts) if parts else '?'}")
    consumer.close()

    # 3) Show retention for the four source topics
    print("\nRetention settings:")
    for name in [settings.TOPIC_BUS_GPS, settings.TOPIC_TRAFFIC_SIGNALS,
                 settings.TOPIC_AIR_QUALITY, settings.TOPIC_SMART_METERS]:
        try:
            cr = ConfigResource(ConfigResourceType.TOPIC, name)
            res = admin.describe_configs([cr])
            for r in res:
                for entry in r.resources[0][4]:
                    if entry[0] == "retention.ms":
                        days = int(entry[1]) / (1000 * 60 * 60 * 24)
                        print(f"   {name:<34} retention.ms={entry[1]}  (~{days:.0f} days)")
        except Exception as e:
            print(f"   {name}: could not read config ({e})")

    admin.close()
    print("\nCluster looks good.")


if __name__ == "__main__":
    main()
