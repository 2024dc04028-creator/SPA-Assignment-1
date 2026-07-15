"""
UrbanPulse — Task C Part II : Ward energy summary (Spark Structured Streaming)
=============================================================================
For each ward_id, per 15-minute tumbling window, compute:
    total_kwh_consumed   = sum(kwh_reading)
    avg_power_factor     = avg(power_factor)
    peak_voltage         = max(voltage)
Output goes to BOTH:
    * Kafka topic  urbanpulse.ward_energy_summary   (for live dashboards)
    * Parquet      output/ward_energy_parquet/, partitioned by ward_id & date
                                                  (for historical trend analysis)

Reads : urbanpulse.smart_meters
Run:  python spark/ward_energy_summary.py

DEMO TIP: 15-minute windows mean you'd wait 15+ minutes to see output. To see
results quickly, run with a shorter window:
    WINDOW_MINUTES=1 python spark/ward_energy_summary.py
"""
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum as _sum, avg as _avg, max as _max,
    to_timestamp, to_json, struct, to_date,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)
from config import settings

BOOTSTRAP = ",".join(settings.BOOTSTRAP_SERVERS)
WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "15"))

# spark-sql-kafka package (auto-downloaded on first run; needs internet once)
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def main():
    spark = (SparkSession.builder
             .appName("UrbanPulse-Ward-Energy")
             .config("spark.jars.packages", KAFKA_PKG)
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Schema of the JSON inside each Kafka message value
    schema = StructType([
        StructField("meter_id", StringType()),
        StructField("ward_id", StringType()),
        StructField("kwh_reading", DoubleType()),
        StructField("voltage", DoubleType()),
        StructField("power_factor", DoubleType()),
        StructField("timestamp", StringType()),
    ])

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", settings.TOPIC_SMART_METERS)
           .option("startingOffsets", "latest")
           .load())

    # value is bytes -> string -> parsed JSON columns
    parsed = (raw.selectExpr("CAST(value AS STRING) AS json")
                 .select(from_json(col("json"), schema).alias("d"))
                 .select("d.*")
                 .withColumn("event_time", to_timestamp(col("timestamp"))))

    agg = (parsed
           .withWatermark("event_time", "5 minutes")
           .groupBy(window(col("event_time"), f"{WINDOW_MINUTES} minutes"),
                    col("ward_id"))
           .agg(_sum("kwh_reading").alias("total_kwh_consumed"),
                _avg("power_factor").alias("avg_power_factor"),
                _max("voltage").alias("peak_voltage")))

    out = agg.select(
        col("ward_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_kwh_consumed"),
        col("avg_power_factor"),
        col("peak_voltage"),
    )

    os.makedirs(settings.CHECKPOINT_DIR, exist_ok=True)

    def write_batch(batch_df, batch_id):
        """Write each finished batch to BOTH Kafka and partitioned Parquet."""
        if batch_df.rdd.isEmpty():
            return
        batch_df = batch_df.withColumn("date", to_date(col("window_start")))

        # 1) Parquet, partitioned by ward_id and date (for history)
        (batch_df.write
            .mode("append")
            .partitionBy("ward_id", "date")
            .parquet(settings.PARQUET_DIR))

        # 2) Kafka topic (for live dashboards)
        (batch_df
            .select(to_json(struct("*")).alias("value"))
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("topic", settings.TOPIC_WARD_ENERGY)
            .save())

        print(f"  batch {batch_id}: wrote {batch_df.count()} ward-windows "
              f"to Kafka + Parquet")

    query = (out.writeStream
             .outputMode("append")           # windows emitted once they close
             .foreachBatch(write_batch)
             .option("checkpointLocation",
                     os.path.join(settings.CHECKPOINT_DIR, "ward_energy"))
             .start())

    print(f"Spark ward-energy job running ({WINDOW_MINUTES}-min windows). "
          f"Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
