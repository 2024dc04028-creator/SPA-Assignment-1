# Task C — Flink vs Spark Comparison (1 page)

UrbanPulse runs two very different processing jobs. The right engine is different for
each, and the choice falls naturally out of four factors: **state size, latency
requirement, recovery time objective (RTO), and operational complexity.**

## Bus Bunching Detection → **Apache Flink**

Bunching means *two buses on the same route within 200 m for more than 5 minutes*. This
is an inherently **event-time, per-key, stateful** problem.

- **State size — small but fine-grained.** State is per `route_id`: the recent
  positions of each bus and any active "too-close-since" timer. It is small in volume
  but needs millisecond-level, per-event updates. Flink's keyed state (`MapState` /
  `ValueState`) is built exactly for this — state is partitioned by key and updated on
  every single event.
- **Latency — must be low and event-driven.** Bunching must surface inside the city's
  real-time SLA. Flink is a true event-at-a-time streaming engine: each GPS reading is
  processed as it arrives, so a bunching condition is detected the moment the 5-minute
  threshold is crossed, using event-time watermarks to stay correct under out-of-order
  GPS data.
- **RTO — fast.** Flink checkpoints keyed state to durable storage and resumes from the
  last checkpoint, so a failed job recovers its per-route timers without replaying from
  zero — important when the output drives operational response.
- **Operational complexity — justified.** Flink's per-key timer + state model is more to
  learn, but it is the only one of the two that expresses "same key, sustained spatial
  condition, over event time" cleanly. Doing this in micro-batch Spark would mean
  awkward self-joins and window gymnastics.

**Verdict: Flink.** Low-latency, event-time, fine-grained per-key state with timers is
its home turf, and the incident-response SLA demands it.

## Ward Energy Aggregation → **Apache Spark Structured Streaming**

This job computes, per `ward_id` per **15-minute tumbling window**, the total kWh, average
power factor, and peak voltage, and writes to both Kafka and partitioned Parquet.

- **State size — larger, but coarse.** State is one set of running aggregates per ward
  per open window. It is bounded and only needs updating once per micro-batch, not per
  event. Spark's micro-batch model holds this comfortably.
- **Latency — relaxed.** These aggregates feed councillor dashboards and reports, which
  tolerate minute-scale latency. A 15-minute window does not benefit from
  event-at-a-time processing; micro-batches every few seconds are more than enough.
- **RTO — relaxed, and reprocessing matters more.** If the job fails, dashboards can lag
  briefly. More importantly, Spark reads the same data to (re)produce historical Parquet
  partitioned by `ward_id`/`date`, so reports can be **recomputed deterministically** —
  the Lambda batch-layer property the government mandate needs.
- **Operational complexity — lower for this shape.** Windowed aggregation with watermarks
  and a `foreachBatch` sink to Parquet + Kafka is concise, declarative Spark SQL. It also
  reuses the same Spark skills the team uses for offline analytics.

**Verdict: Spark.** Windowed batch aggregation over relaxed latency, with first-class
partitioned Parquet output for auditable reprocessing, is exactly what Structured
Streaming does well.

## Summary

| Factor | Bus Bunching (Flink) | Ward Energy (Spark) |
|---|---|---|
| State size | Small, fine-grained, per-bus timers | Bounded aggregates per ward/window |
| Latency requirement | Real-time, event-at-a-time | Minute-scale, micro-batch is fine |
| RTO | Fast resume from keyed checkpoints | Relaxed; recompute from Parquet |
| Operational complexity | Higher, but the only clean fit | Lower; declarative windowed SQL |

This split is precisely why UrbanPulse adopts a **Lambda** architecture: Flink as the
speed layer for incident detection, Spark as the batch layer for auditable aggregates.
