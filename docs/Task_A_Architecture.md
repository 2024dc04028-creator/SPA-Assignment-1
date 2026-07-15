# Task A — Architecture & Design

UrbanPulse — Real-Time Urban Operations Intelligence Platform
City: MetroConnect (fictitious tier-1 city, 4.2M population)
Mandate: Smart Cities Mission, open-source-first, budget-constrained.

---

## A.1 Labelled Architecture Diagram

The full labelled diagram is in `docs/architecture_diagram.svg` (also embedded in the
final PDF report). It shows the five layers below. A text walkthrough follows so the
design intent is clear even without the image.

**Layer 1 — Data Sources (4 streams)**
- Bus GPS — ~2,400 events/s — `bus_id, route_id, lat, lon, speed_kmh, occupancy_pct, timestamp`
- Traffic Signals — ~380 events/s — `junction_id, zone, vehicle_count, avg_wait_sec, signal_phase, timestamp`
- Air Quality — ~60 events/s — `sensor_id, zone, pm25, pm10, no2, aqi, timestamp`
- Smart Meters — ~1,100 events/s — `meter_id, ward_id, kwh_reading, voltage, power_factor, timestamp`

**Layer 2 — Ingestion (Apache Kafka, 3-broker cluster + Zookeeper)**
One topic per stream. Producers publish JSON. A validation step routes malformed
events to a dead-letter queue (`urbanpulse.dlq`). A real-time enrichment job joins
bus GPS with the static route schedule and republishes to `urbanpulse.bus_gps_enriched`.

**Layer 3 — Processing (Lambda architecture: speed layer + batch layer)**
- *Speed layer (Apache Flink / PyFlink):* sub-2-minute incident detection — AQI
  emergencies, traffic gridlock, bus bunching — using keyed state and event-time
  watermarks. Output to `urbanpulse.incidents`.
- *Batch / micro-batch layer (Apache Spark Structured Streaming):* 15-minute
  tumbling-window ward energy aggregates and 10-minute rolling AQI advisories, which
  feed councillor dashboards and government reports. Output to
  `urbanpulse.ward_energy_summary`, `urbanpulse.health_advisories`, and Parquet.

**Layer 4 — Storage (technology choice per data class)**

| Data class | Technology | Why |
|---|---|---|
| Time-series sensor data (AQI, signals, meters) | **TimescaleDB** (PostgreSQL extension) | Native time partitioning + continuous aggregates; SQL that ward officers' BI tools already speak; open-source; self-hosted on city servers. |
| Geospatial bus positions | **PostgreSQL + PostGIS** | First-class geospatial types and distance queries (e.g. "buses within 200 m") needed for ETA and bunching context; open-source. |
| Historical AQI records | **Apache Parquet on MinIO** (S3-compatible object store) | Columnar, compressed, cheap long-term retention for 90-day+ pollution-trend analysis; queryable by Spark; self-hostable for data sovereignty. |
| Councillor report aggregates | **PostgreSQL** | Small, relational, audited summary tables; trivially exported to PDF/Excel for state-government submissions. |

For the runnable demo on a single laptop we use the lightweight equivalents that ship
in the repo (partitioned **Parquet** files for historical/aggregate data and **Kafka
topics** as the live store) so a beginner can run everything without standing up four
databases. The production technologies above are what the design specifies.

**Layer 5 — Serving**
- Live operations dashboard (Flask API in `serving/dashboard_api.py`, port 5000)
- Advisory API (health advisories endpoint)
- Signal control interface (consumes `urbanpulse.incidents` HIGH_PRIORITY group)

---

## A.2 Lambda vs Kappa — Evaluation Matrix

Every cell is grounded in UrbanPulse's actual requirements, not generic theory.

| Criterion | Lambda | Kappa |
|---|---|---|
| **Latency** | Speed layer (Flink) already delivers the sub-2-min AQI alert and 90-s signal adaptation. Batch layer adds latency only for reports, which tolerate it. Meets every SLA. | Single streaming path also meets the sub-2-min SLAs. No latency disadvantage for the live use cases. |
| **Fault Tolerance** | Two independent paths: if the Spark batch job fails, real-time incident detection keeps running, and vice-versa. Failure of reporting never blocks emergency alerts. | One pipeline is a single point of failure for *both* alerts and reports; a bad deploy can take down emergency detection and councillor reporting together. |
| **Operational Complexity** | Higher: two engines (Flink + Spark), two codebases, two failure modes to operate. This is the real cost of Lambda for a small city team. | Lower: one engine, one mental model, one deployment. Attractive for a lean ops team. |
| **Reprocessing Capability** | Batch layer is purpose-built for reprocessing: re-run a month of Parquet to correct a councillor report after a late sensor calibration fix. Natural fit. | Reprocessing means replaying Kafka from the retention window. Smart-meter audit needs 365 days — keeping a year of raw events in Kafka purely to reprocess is expensive and operationally heavy. |
| **Cost** | Two clusters to size and run, but the batch layer can run on cheap, schedulable capacity; raw history lives in cheap Parquet/object storage, not Kafka. | Must retain very long Kafka history (365-day meter audit) to preserve reprocessing — high broker-storage cost, which dominates at MetroConnect's volumes (~3,900 events/s aggregate). |
| **Compliance with Govt Reporting Mandate** | Batch layer produces deterministic, re-runnable, auditable monthly/weekly aggregates from immutable Parquet — exactly what state-government submission and audit require. Strong fit. | Report figures derived from a replayed stream are harder to reproduce identically months later for audit; weaker compliance story. |

### A.2 Conclusion — Architecture Choice: **Lambda**

UrbanPulse is deliberately a *contested* case because it has a genuine dual mandate:
hard real-time operational response **and** long-horizon, auditable government
reporting (including a 365-day smart-meter regulatory audit). Kappa wins on simplicity,
but it loses decisively on the two requirements that matter most here — **cheap,
auditable, long-horizon reprocessing** for compliance, and **failure isolation** so a
reporting bug can never silence an AQI emergency alert. The 365-day audit retention in
particular makes a Kappa "replay from Kafka" model prohibitively expensive.

We therefore choose **Lambda**: Flink as the speed layer for incident detection, Spark
as the batch/micro-batch layer for ward aggregates and compliance reports, with
immutable Parquet as the historical system of record. We accept the higher operational
complexity as the price of regulatory auditability and fault isolation, and mitigate it
by keeping both layers in the same Kafka-centric, open-source toolchain.

---

## A.3 Architecture Readiness Checklist (Government Smart-City Deployment)

A deployment is "ready" only when every item below is satisfied. 16 items, covering the
four mandated themes (data sovereignty, open-source mandate, disaster recovery
RPO < 15 min / RTO < 30 min, and non-technical ward-officer accessibility).

**Data Sovereignty**
1. ☐ All Kafka brokers, processing engines, and databases run on city-owned servers inside the municipal data centre; no managed cloud service stores citizen data.
2. ☐ No data egress to external networks; object storage (MinIO) and databases are bound to the internal VLAN with egress firewall rules denying outbound traffic.
3. ☐ Backups and Parquet snapshots are written only to city-controlled storage; off-site DR replica is a second city/state-government facility, not a public cloud.
4. ☐ Access to raw streams is role-restricted; an audit log records who read or exported citizen-linked data (meter/ward level).

**Open-Source Mandate**
5. ☐ Every core component is OSS with a permissive/Apache-style licence: Kafka, Zookeeper, Flink, Spark, PostgreSQL/PostGIS, TimescaleDB, MinIO, Parquet, Flask.
6. ☐ No proprietary connectors or paid enterprise add-ons are on the critical path; any vendor tooling is optional and replaceable.
7. ☐ Build/run is reproducible from source and pinned versions (see `requirements.txt`, `docker-compose.yml`); no closed binaries required to operate.

**Disaster Recovery (RPO < 15 min, RTO < 30 min)**
8. ☐ Kafka topics use replication factor 3 across brokers so a single broker loss causes zero data loss and no downtime.
9. ☐ Continuous/near-continuous replication or ≤15-min snapshotting of Parquet and databases to the DR site guarantees **RPO < 15 min**.
10. ☐ A documented, rehearsed failover runbook (promote DR brokers, repoint producers/consumers, restart Flink/Spark from last checkpoint) is provable to complete in **under 30 min (RTO)**.
11. ☐ Flink and Spark run with checkpointing enabled to durable storage so stream jobs resume from the last consistent state, not from scratch, after failover.
12. ☐ DR drills are scheduled (e.g. quarterly) and the measured RPO/RTO are recorded against the targets.

**Accessibility for Non-Technical Ward Officers**
13. ☐ Dashboards present plain-language status (e.g. "AQI: Hazardous in Zone 3") with colour coding, not raw JSON or query consoles.
14. ☐ Advisory and incident views are usable on a standard browser/mobile with no install, login is single-sign-on, and no command line is ever required of a ward officer.
15. ☐ Dashboards meet basic accessibility (legible font sizes, colour-blind-safe palette, regional-language labels) so any ward officer can read them.
16. ☐ A one-page "what each alert means and who to call" guide ships with the dashboard so non-technical staff can act on an incident without engineering help.
