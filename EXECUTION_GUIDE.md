# UrbanPulse — Execution Guide

This guide is written for a **beginner**. It tells you exactly what to install, what to
type in a **Terminal**, and what to run from **VS Code**.
Follow it top to bottom the first time. After setup, you only repeat Section 6.

> Mental model: **Kafka runs inside Docker** (so you don't install Java just for Kafka).
> **All the Python apps run on your Mac** in a VS Code terminal. Flink and Spark are the
> only Python apps that *also* need a Java runtime — we install that with a normal `.pkg`
> installer.

---

## 0. What you'll end up running

| Piece | Where it runs | How you start it |
|---|---|---|
| Kafka (3 brokers) + Zookeeper + Kafka-UI | Docker | one command |
| Producers / consumers / enrichment / DLQ | Python | VS Code terminal |
| Flink incident detection | Python (needs Java) | VS Code terminal |
| Spark ward analytics | Python (needs Java) | VS Code terminal |
| Dashboard API | Streamlit | VS Code terminal, open in browser |

---

## 1. Install the tools (one-time)

### 1a. Docker Desktop (runs Kafka for you)
1. Go to <https://www.docker.com/products/docker-desktop/>.
2. Download **Docker Desktop for Mac** — pick **Apple Silicon** (M1/M2/M3) or **Intel**.
3. Open the downloaded `.dmg` and drag **Docker** into **Applications**.
4. Launch Docker Desktop and let it finish starting (the whale icon in the menu bar
   stops animating). Accept the default settings.
5. Verify in Terminal:
   ```bash
   docker --version
   docker compose version
   ```
   Both should print a version number.

### 1b. Python 3.11 
PyFlink does **not** support Python 3.12+ yet, so we use **3.11**.
1. Go to <https://www.python.org/downloads/macos/>.
2. Download the latest **Python 3.11.x** "macOS 64-bit universal2 installer" (`.pkg`).
3. Run the `.pkg` and click through with defaults.
4. Verify:
   ```bash
   python3.11 --version
   ```
   It should print `Python 3.11.x`. (If `python3.11` isn't found, close and reopen
   Terminal, or use the full path `/usr/local/bin/python3.11`.)

### 1c. Java 11 (only needed for Flink & Spark — via Temurin .pkg, NOT Homebrew)
1. Go to <https://adoptium.net/temurin/releases/?version=11>.
2. Choose: **Operating System = macOS**, **Architecture = aarch64** (Apple Silicon) or
   **x64** (Intel), **Package Type = JDK**, **Version = 11**.
3. Download the **`.pkg`** installer and run it (defaults are fine).
4. Verify:
   ```bash
   java -version
   ```
   It should mention `openjdk version "11..."` (Temurin).
5. Tell your shell where Java is (so Flink/Spark can find it). Add this line to
   `~/.zshrc`:
   ```bash
   export JAVA_HOME=$(/usr/libexec/java_home -v 11)
   ```
   Then reload: `source ~/.zshrc` and check `echo $JAVA_HOME` prints a path.

### 1d. VS Code
You already have it. Just install the **Python** extension by Microsoft if you haven't
(Extensions sidebar → search "Python" → Install). That's the only extension required.

---

## 2. Open the project & create a virtual environment

A "virtual environment" (venv) is a private folder of Python packages just for this
project, so nothing clashes with the rest of your system.

1. In VS Code: **File → Open Folder…** and choose the `urbanpulse` folder.
2. Open a terminal **inside VS Code**: menu **Terminal → New Terminal**.
3. Create and activate the venv **using Python 3.11**:
   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate
   ```
   Your prompt now starts with `(.venv)`. (On every future terminal, re-run the
   `source .venv/bin/activate` line first.)
4. Tell VS Code to use it: press **Cmd+Shift+P** → "Python: Select Interpreter" → pick
   the one inside `.venv`.

---

## 3. Install the Python packages

With `(.venv)` active:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```
This installs `kafka-python`, `apache-flink`, `pyspark`, `streamlit`, and `pandas`.

> **If `apache-flink` fails to install:** you are almost certainly not on Python 3.11.
> Check `python --version` inside the venv. Recreate the venv with `python3.11` (Section 2).
>
> **If you see a `kafka-python` error on a newer Python:** install the maintained fork
> instead — `pip install kafka-python-ng` — the code works with either.

### Test the install worked (no Kafka needed yet)
```bash
python -c "import kafka, streamlit, pandas; print('core OK')"
python -c "import pyflink; print('flink OK')"
python -c "import pyspark; print('spark OK')"
```
Three "OK" lines means you're ready.

---

## 4. One extra download for Flink & Spark: the Kafka connector JARs

Flink and Spark talk to Kafka through a small Java "connector" file. This is the one
manual download; do it once.

### 4a. Flink connector JAR
1. Create the folder (in the VS Code terminal):
   ```bash
   mkdir -p flink/lib
   ```
2. Download the connector that matches Flink 1.19 from Maven Central:
   <https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar>
3. Save the `.jar` into the `flink/lib/` folder. That's it — `flink/_flink_common.py`
   finds it automatically.

### 4b. Spark connector
Spark downloads its connector automatically the first time you run a Spark job **(needs
internet on that first run)** — it's the `--packages` line already wired into the Spark
scripts. No manual step, just don't be surprised by a one-time download.

---

## 5. Start Kafka (Docker) and create the topics

### 5a. Start the cluster (Terminal — leave this running)
From the project folder:
```bash
docker compose up -d
```
Wait ~30–60 seconds for the brokers to settle. Check the dashboard:
- Open <http://localhost:8080> — this is **Kafka-UI**. You should see 3 brokers.

### 5b. Create the topics (VS Code terminal, venv active)
```bash
python setup/create_topics.py
python setup/verify_cluster.py
```
`verify_cluster.py` should list all topics with their partition counts and retention.
Refresh <http://localhost:8080> to see the topics too.

---

## 6. Run the pipeline — what goes in which terminal

Each long-running program needs **its own terminal tab** (in VS Code, click the **+** in
the terminal panel to add tabs). In **every** new tab, first run:
```bash
source .venv/bin/activate
```

### TASK B — Kafka ingestion

**B.2 Producers** (each in its own tab):
```bash
python producers/bus_gps_producer.py
python producers/air_quality_producer.py
```
Optional supporting streams (needed for Task C gridlock & ward energy):
```bash
python producers/traffic_signals_producer.py
python producers/smart_meters_producer.py
```
The air-quality producer prints a line whenever it injects/logs a 5% null-AQI event.

**B.3 Priority consumers** — open **four** tabs:
```bash
python consumers/priority_consumer_high.py            # tab 1: HIGH_PRIORITY (1 consumer)
python consumers/priority_consumer_standard.py --slow # tab 2
python consumers/priority_consumer_standard.py --slow # tab 3
python consumers/priority_consumer_standard.py --slow # tab 4
```
Then in a fifth tab watch the lag:
```bash
python consumers/lag_monitor.py
```
You'll see **HIGH_PRIORITY lag stays near 0** while **STANDARD_PRIORITY lag grows** —
that's the proof required by the brief. (Make sure the traffic_signals producer is
running.)

**B.4 Enrichment** (needs bus_gps producer running):
```bash
python enrichment/bus_gps_enrichment.py
```
Peek at the result in another tab:
```bash
python consumers/view_topic.py urbanpulse.bus_gps_enriched
```

**B.5 Dead-letter queue:**
```bash
python dlq/dlq_validator.py        # tab 1: routes bad events to the DLQ
python dlq/dlq_report.py           # tab 2: prints + saves the 5-min report
```
The report is also saved to `output/dlq_report.txt`.

### TASK C — Processing (this is the hardest part; make sure Section 1c & 4a are done)

**C.1 Flink incident detection** — run any/all (each in its own tab). Keep the relevant
producers running so there's data to detect:
```bash
python flink/aqi_emergency.py      # needs air_quality producer
python flink/traffic_gridlock.py   # needs traffic_signals producer
python flink/bus_bunching.py       # needs bus_gps producer
```
Watch the alerts land:
```bash
python consumers/view_topic.py urbanpulse.incidents
```

**C.2 Spark ward analytics** — run (each in its own tab; needs smart_meters / air_quality
producers). The first run downloads the Spark-Kafka package (needs internet once):
```bash
python spark/ward_energy_summary.py     # → ward_energy_summary topic + Parquet
python spark/aqi_health_advisory.py     # → health_advisories topic (Update mode)
```
Parquet output appears under `output/ward_energy_parquet/` partitioned by
`ward_id`/`date`.

### Serving layer
```bash
streamlit run streamlit_app.py
```
Open <http://localhost:8501> to see live incidents, advisories, and ward energy.

---

## 7. Recommended order for a clean end-to-end demo / video

1. `docker compose up -d` → show Kafka-UI (3 brokers).
2. `python setup/create_topics.py` then `verify_cluster.py` → show topics + retention (B.1).
3. Start all four producers (B.2).
4. Show priority consumers + `lag_monitor.py` → HIGH ~0 vs STANDARD growing (B.3).
5. Run enrichment, view enriched topic (B.4).
6. Run DLQ validator + report (B.5).
7. Run the three Flink detectors, view `urbanpulse.incidents` (C.1).
8. Run the two Spark jobs, show Parquet folders + `health_advisories` (C.2).
9. Open the dashboard at :8501 (serving layer).
10. Walk through `docs/UrbanPulse_Report.pdf` for the design reasoning (Task A + comparisons).

---

## 8. Shutting down

- Stop any Python app with **Ctrl+C** in its terminal tab.
- Stop Kafka: `docker compose down` (add `-v` to also wipe the Kafka data volumes).

---

## 9. Troubleshooting quick table

| Symptom | Fix |
|---|---|
| `NoBrokersAvailable` | Kafka not up yet. Wait, confirm `docker compose ps` shows brokers "Up", check :8080. |
| `pip install apache-flink` fails | You're not on Python 3.11. Recreate the venv with `python3.11` (Section 2). |
| Flink job: "connector jar not found" | The JAR isn't in `flink/lib/`. Redo Section 4a; filename must start `flink-sql-connector-kafka`. |
| Flink/Spark: `JAVA_HOME is not set` | Redo Section 1c step 5, then `source ~/.zshrc`. |
| Spark first run hangs on "resolving packages" | It's downloading the Kafka package — needs internet once; let it finish. |
| `kafka-python` import error on Python 3.12 | You should be on 3.11. If stuck, `pip install kafka-python-ng`. |
| Port 8080/9092/5000 already in use | Another app is using it. Quit that app, or change the port in `docker-compose.yml` / the script. |

---

## 10. What was and wasn't tested

All 22 Python modules compile cleanly (`python -m py_compile`). The pipelines were built
against the configuration in this guide. Because Kafka, a JDK, and the connector JARs
can't run in the authoring sandbox, the **live end-to-end run happens on your Mac** by
following the steps above — that's exactly what this guide is for, and what your video
walkthrough should capture.
