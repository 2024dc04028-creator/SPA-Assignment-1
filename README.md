# UrbanPulse

**Real-Time Urban Operations Intelligence & Smart Traffic Management Platform**
for *MetroConnect* (fictitious 4.2M-population tier-1 city) — Smart Cities Mission.

A beginner-friendly, all-Python streaming data-engineering project built on **Apache Kafka + Zookeeper**, **Apache Flink (PyFlink)**, **Apache Spark (PySpark)**, and **Streamlit**.

> **New here? Read [`EXECUTION_GUIDE.md`](EXECUTION_GUIDE.md) first.** It walks you through every installation and execution step. The complete project documentation is available in **docs/UrbanPulse_Report.pdf**.

---

# Architecture (Lambda)

```
4 City Data Streams
        │
        ▼
Kafka (3 Brokers)
        │
 ┌──────┴─────────┐
 │                │
 ▼                ▼
Apache Flink      Apache Spark
Speed Layer       Batch Layer
 │                │
 └──────┬─────────┘
        ▼
Storage Layer
(TimescaleDB / PostGIS / Parquet)
        │
        ▼
Streamlit Dashboard
```

The UrbanPulse platform follows a **Lambda Architecture** to support both low-latency event processing and historical analytics.

The Streamlit dashboard serves as the presentation layer by consuming Kafka output topics and displaying live operational insights for city administrators.

See:

```
docs/architecture_diagram.svg
```

---

# Repository Structure

```
urbanpulse/
├── EXECUTION_GUIDE.md          ← Setup & execution guide
├── docker-compose.yml          ← Kafka (3 brokers) + ZooKeeper + Kafka UI
├── requirements.txt            ← Python dependencies
├── streamlit_app.py            ← Streamlit Dashboard
├── config/
│   └── settings.py             ← Central configuration
├── data/
├── setup/
├── producers/
├── consumers/
├── enrichment/
├── dlq/
├── flink/
├── spark/
├── stream_lit/                    
└── docs/
```

---

# Assignment Mapping

| Task | Description | Location |
|------|-------------|----------|
| Task A | Lambda Architecture Design | docs/ |
| Task B.1 | Kafka Cluster | docker-compose.yml, setup/ |
| Task B.2 | Producers | producers/ |
| Task B.3 | Priority Consumers | consumers/ |
| Task B.4 | Stream Enrichment | enrichment/ |
| Task B.5 | Dead Letter Queue | dlq/ |
| Task C.1 | Flink Incident Detection | flink/ |
| Task C.2 | Spark Streaming Analytics | spark/ |
| Serving Layer | Streamlit Dashboard | streamlit_app.py |

---

# Technology Stack

- Python 3.11
- Apache Kafka
- Apache ZooKeeper
- Apache Flink (PyFlink)
- Apache Spark Structured Streaming
- Streamlit
- Docker
- Pandas

---

# Quick Start

## 1. Start Kafka

```bash
docker compose up -d
```

## 2. Activate Environment

```bash
python3.11 -m venv .venv

source .venv/bin/activate
```

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## 4. Create Topics

```bash
python setup/create_topics.py
```

## 5. Start Producers

Example

```bash
python producers/bus_gps_producer.py

python producers/air_quality_producer.py
```

## 6. Run Flink / Spark Jobs

```bash
python flink/aqi_emergency.py

python spark/ward_energy_summary.py
```

## 7. Launch Dashboard

```bash
streamlit run streamlit_app.py
```

Open

```
http://localhost:8501
```

---

# Dashboard Features

The Streamlit dashboard provides:

- 🚨 Live Incident Monitoring
- 🌍 AQI Health Advisories
- ⚡ Ward Energy Summary
- 📊 Live Metrics
- 📈 Auto-refresh every 5 seconds
- 📋 Interactive Tables

---

# Notes

- Python **3.11** is required.
- Kafka runs entirely inside Docker.
- All Python applications execute natively in VS Code.
- Apache Flink and Apache Spark require Java (JDK 11 or 17).
- The dashboard automatically refreshes every five seconds.
- Kafka output topics are consumed directly by the Streamlit dashboard.
- No additional web server is required.

---

# Project Deliverables

✔ Lambda Architecture

✔ Kafka Multi-Source Streaming

✔ Apache Flink Incident Detection

✔ Apache Spark Structured Streaming

✔ Dead Letter Queue

✔ Stream Enrichment

✔ Streamlit Live Dashboard

✔ Documentation

✔ Video Demonstration
