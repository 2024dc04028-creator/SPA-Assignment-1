"""
UrbanPulse — Task C Part II : AQI health advisory (Streaming SQL)
================================================================
A STREAMING SQL query that:
  (a) computes a 10-minute ROLLING average AQI per zone
      (implemented as a 10-min window sliding every 1 min),
  (b) JOINS with the static zone_profile table (zone name, population, schools),
  (c) filters for rolling_avg_aqi > 150 (Unhealthy),
  and writes the enriched advisory to urbanpulse.health_advisories.

Output mode: UPDATE (as required) — each trigger emits the rows whose rolling
average changed.

Reads : urbanpulse.air_quality  +  data/zone_profile.csv
Run:  python spark/aqi_health_advisory.py
"""
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_json, struct
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)
from config import settings

BOOTSTRAP = ",".join(settings.BOOTSTRAP_SERVERS)
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def main():
    spark = (SparkSession.builder
             .appName("UrbanPulse-AQI-Advisory")
             .config("spark.jars.packages", KAFKA_PKG)
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # --- static reference table (the JOIN side) ---
    zone_profile = (spark.read
                    .option("header", True)
                    .option("inferSchema", True)
                    .csv("file://" + settings.ZONE_PROFILE_CSV))
    zone_profile.createOrReplaceTempView("zone_profile")

    # --- streaming AQI source ---
    aqi_schema = StructType([
        StructField("sensor_id", StringType()),
        StructField("zone", StringType()),
        StructField("pm25", DoubleType()),
        StructField("pm10", DoubleType()),
        StructField("no2", DoubleType()),
        StructField("aqi", IntegerType()),
        StructField("timestamp", StringType()),
    ])

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", settings.TOPIC_AIR_QUALITY)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.selectExpr("CAST(value AS STRING) AS json")
                 .select(from_json(col("json"), aqi_schema).alias("d"))
                 .select("d.*")
                 .where(col("aqi").isNotNull())          # ignore the null-AQI faults
                 .withColumn("event_time", to_timestamp(col("timestamp")))
                 .withWatermark("event_time", "5 minutes"))
    parsed.createOrReplaceTempView("aqi_stream")

    # (a) 10-minute rolling average per zone, in SQL
    rolling = spark.sql("""
        SELECT zone,
               window(event_time, '10 minutes', '1 minute') AS w,
               avg(aqi)  AS rolling_avg_aqi,
               count(*)  AS readings
        FROM aqi_stream
        GROUP BY zone, window(event_time, '10 minutes', '1 minute')
    """)
    rolling.createOrReplaceTempView("rolling_aqi")

    # (b) join with static zone profile  +  (c) filter > 150, in SQL
    advisory = spark.sql("""
        SELECT r.zone,
               z.zone_name,
               z.population,
               z.num_schools,
               r.w.start AS window_start,
               r.w.end   AS window_end,
               round(r.rolling_avg_aqi, 1) AS rolling_avg_aqi,
               r.readings,
               'UNHEALTHY' AS advisory_level
        FROM rolling_aqi r
        JOIN zone_profile z ON r.zone = z.zone
        WHERE r.rolling_avg_aqi > 150
    """)

    # to Kafka: value must be a single string column
    out = advisory.select(to_json(struct("*")).alias("value"))

    os.makedirs(settings.CHECKPOINT_DIR, exist_ok=True)
    query = (out.writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP)
             .option("topic", settings.TOPIC_HEALTH_ADVISORY)
             .option("checkpointLocation",
                     os.path.join(settings.CHECKPOINT_DIR, "aqi_advisory"))
             .outputMode("update")               # <- required Update mode
             .start())

    print("Spark AQI advisory job running (Update mode). "
          "rolling avg > 150 -> urbanpulse.health_advisories. Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
