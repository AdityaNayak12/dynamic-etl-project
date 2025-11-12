import streamlit as st
import pandas as pd
import requests

API_URL = "http://localhost:8000"

st.title("📊 Dynamic ETL Dashboard")

tab1, tab2 = st.tabs(["Schemas", "Batches"])

with tab1:
    st.header("Schema Evolution")
    try:
        data = requests.get(f"{API_URL}/schemas").json()
        df = pd.DataFrame(data["schemas"])
        if not df.empty:
            st.dataframe(df)
        else:
            st.info("No schemas yet. Upload data first.")
    except Exception as e:
        st.error(f"Error: {e}")

with tab2:
    st.header("Batches Processed")
    try:
        data = requests.get(f"{API_URL}/batches").json()
        df = pd.DataFrame(data["batches"])
        if not df.empty:
            st.dataframe(df)
        else:
            st.info("No batches yet.")
    except Exception as e:
        st.error(f"Error: {e}")
