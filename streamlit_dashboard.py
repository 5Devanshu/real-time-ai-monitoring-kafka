import streamlit as st
import pandas as pd
from elasticsearch import Elasticsearch
import time

# Elasticsearch connection details
ES_HOST = "localhost"
ES_PORT = 9200
ES_USER = "elastic"
ES_PASSWORD = "changeme" # Replace with your Elasticsearch password
ES_INDEX = "monitoring_data"

@st.cache_resource
def get_es_client():
    """
    Establishes and caches the Elasticsearch client connection.
    """
    es_client = Elasticsearch(
        hosts=[{'host': ES_HOST, 'port': ES_PORT, 'scheme': 'http'}],
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False
    )
    if not es_client.ping():
        st.error("Could not connect to Elasticsearch! Please ensure it is running and accessible.")
        return None
    return es_client

def fetch_data_from_elasticsearch(es_client, index, query_body=None, size=1000):
    """
    Fetches data from Elasticsearch.
    """
    if es_client is None:
        return pd.DataFrame()

    try:
        response = es_client.search(
            index=index,
            body=query_body if query_body else {"query": {"match_all": {}}},
            size=size,
            sort=[{"timestamp": {"order": "desc"}}]
        )
        hits = response['hits']['hits']
        data = [hit['_source'] for hit in hits]
        df = pd.DataFrame(data)
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp').sort_index()
        return df
    except Exception as e:
        st.error(f"Error fetching data from Elasticsearch: {e}")
        return pd.DataFrame()

st.set_page_config(layout="wide")
st.title("Real-Time AI Monitoring Dashboard")

es = get_es_client()

if es:
    st.sidebar.header("Filter Options")
    selected_severity = st.sidebar.multiselect(
        "Select Severity",
        ["INFO", "WARNING", "ERROR", "CRITICAL", "UNKNOWN"],
        default=["ERROR", "CRITICAL", "WARNING"]
    )
    selected_service = st.sidebar.multiselect(
        "Select Service Name",
        ["CBS", "CSI", "auth-service", "data-ingestion", "analytics-engine", "frontend-api", "user-management", "unknown"],
        default=[]
    )
    selected_event_type = st.sidebar.multiselect(
        "Select Event Type",
        ["WINDOWS_LOG", "CPU_SPIKE", "MEMORY_LEAK", "DISK_OVERLOAD", "NETWORK_LATENCY", "APPLICATION_ERROR", "SERVICE_UNAVAILABLE", "UNPARSED_LOG"],
        default=["WINDOWS_LOG"]
    )
    
    # Auto-refresh functionality
    refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 5, 60, 10)
    st.sidebar.write(f"Dashboard refreshes every {refresh_rate} seconds.")

    # Construct Elasticsearch query
    query = {
        "query": {
            "bool": {
                "must": []
            }
        },
        "sort": [{"timestamp": {"order": "desc"}}]
    }

    if selected_severity:
        query['query']['bool']['must'].append({"terms": {"severity.keyword": selected_severity}})
    if selected_service:
        query['query']['bool']['must'].append({"terms": {"service_name.keyword": selected_service}})
    if selected_event_type:
        query['query']['bool']['must'].append({"terms": {"event_type.keyword": selected_event_type}})

    # Live update section
    st.header("Live Monitoring Data")
    placeholder = st.empty()

    while True:
        df = fetch_data_from_elasticsearch(es, ES_INDEX, query_body=query, size=500)
        
        with placeholder.container():
            st.subheader(f"Latest {len(df)} Events")
            if not df.empty:
                # Display metrics
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Events", len(df))
                col2.metric("Critical Events", df[df['severity'] == 'CRITICAL'].shape[0])
                col3.metric("Error Events", df[df['severity'] == 'ERROR'].shape[0])
                col4.metric("Anomaly Events (AI)", df[df['ai_category'] == 'ANOMALY'].shape[0] + df[df['ai_category'] == 'CRITICAL_ANOMALY'].shape[0])

                st.markdown("---")

                # Display data table
                st.dataframe(df)

                st.markdown("---")

                # Basic charts
                st.subheader("Event Type Distribution")
                st.bar_chart(df['event_type'].value_counts())

                st.subheader("Severity Distribution")
                st.bar_chart(df['severity'].value_counts())

                st.subheader("Events Over Time")
                if 'timestamp' in df.columns:
                    # Group by minute and count events
                    events_over_time = df.resample('1T')['event_id'].count().rename("event_count")
                    st.line_chart(events_over_time)
                else:
                    st.warning("Timestamp column not found for time series chart.")

            else:
                st.info("No data available from Elasticsearch for the selected filters.")
        
        time.sleep(refresh_rate)

else:
    st.warning("Dashboard is not connected to Elasticsearch. Please check your Elasticsearch setup.")
