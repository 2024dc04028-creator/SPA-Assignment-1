import json
import threading
from collections import deque

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh

from config import settings

import os

print("="*60)
print("STREAMLIT PID:", os.getpid())
print("="*60)


st.set_page_config(
    page_title="UrbanPulse Smart City Dashboard",
    page_icon="🚦",
    layout="wide",
)

# Auto refresh every 5 seconds
st_autorefresh(interval=5000, key="urbanpulse_refresh")

incidents = deque(maxlen=100)
advisories = deque(maxlen=100)
ward_energy = deque(maxlen=100)


import os
import json
from kafka import KafkaConsumer


def consume(topic, buffer):
    print(f"\nStarting consumer for: {topic}")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=settings.BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    print(f"Consumer connected to {topic}")

    count = 0

    for msg in consumer:
        count += 1

        data = json.loads(msg.value)
        buffer.appendleft(data)

        if count % 100 == 0:
            print(
                f"{topic} | received={count} | "
                f"buffer={len(buffer)} | "
                f"id(buffer)={id(buffer)} | "
                f"pid={os.getpid()}"
            )


@st.cache_resource
def start_consumers():
    print("=" * 60)
    print("Starting background consumer threads")
    print("=" * 60)

    incidents = deque(maxlen=100)
    advisories = deque(maxlen=100)
    ward_energy = deque(maxlen=100)

    threads = []

    for topic, buf in [
        (settings.TOPIC_INCIDENTS, incidents),
        (settings.TOPIC_HEALTH_ADVISORY, advisories),
        (settings.TOPIC_WARD_ENERGY, ward_energy),
    ]:
        t = threading.Thread(
            target=consume,
            args=(topic, buf),
            daemon=True,
        )
        t.start()
        threads.append(t)

    print("All consumer threads started.")

    return incidents, advisories, ward_energy, threads


incidents, advisories, ward_energy, consumer_threads = start_consumers()


##############################################################

st.title("🚦 UrbanPulse")
st.subheader("Real-Time Urban Operations Intelligence Platform")

#c1, c2, c3 = st.columns(3)

#st.write("Incidents length:", len(incidents))
#st.write(incidents)

#c1.metric("Incidents", len(incidents))
#c2.metric("Health Advisories", len(advisories))
#c3.metric("Ward Energy Records", len(ward_energy))

st.divider()

tab1, tab2, tab3 = st.tabs(
    [
        "🚨 Incidents",
        "🌍 AQI Advisories",
        "⚡ Ward Energy",
    ]
)

##############################################################
# INCIDENTS
##############################################################

with tab1:

    st.header("Live Incidents")

    if incidents:

        df = pd.DataFrame(list(incidents))

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
        )

    else:
        st.info("Waiting for incidents...")

##############################################################
# ADVISORIES
##############################################################

with tab2:

    st.header("Health Advisories")

    if advisories:

        df = pd.DataFrame(list(advisories))

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
        )

    else:
        st.info("Waiting for health advisories...")

##############################################################
# ENERGY
##############################################################

with tab3:

    st.header("Ward Energy Summary")

    if ward_energy:

        df = pd.DataFrame(list(ward_energy))

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
        )

        numeric = df.select_dtypes(include="number")

        if not numeric.empty:
            st.line_chart(numeric)

    else:
        st.info("Waiting for ward energy summary...")

st.divider()

st.caption(
    "UrbanPulse Smart City Dashboard | "
    "Apache Kafka + Apache Flink + Apache Spark + Streamlit"
)