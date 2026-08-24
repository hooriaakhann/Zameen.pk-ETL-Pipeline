import glob
import re

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Zameen.pk Property Dashboard", layout="wide")
st.title("Zameen.pk Property Listings Dashboard")


def load_parquet_data():
    files = glob.glob("parquet_output/*.parquet")
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(file) for file in files), ignore_index=True)


def parse_price(value):
    if not isinstance(value, str):
        return None

    text = value.replace(",", "").strip().lower()
    match = re.search(r"([0-9.]+)", text)
    if not match:
        return None

    number = float(match.group(1))
    if "crore" in text:
        return number * 10_000_000
    if "lakh" in text or "lac" in text:
        return number * 100_000
    return number


df = load_parquet_data()

if df.empty:
    st.info("No Parquet data found yet. Run the producer and Spark pipeline first.")
else:
    df["price_pkr"] = df["price"].apply(parse_price)
    df["bedrooms"] = pd.to_numeric(df["bedrooms"], errors="coerce")
    df["bathrooms"] = pd.to_numeric(df["bathrooms"], errors="coerce")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Listings", f"{len(df):,}")
    col2.metric("Average Price", f"PKR {df['price_pkr'].mean():,.0f}")
    col3.metric("Median Price", f"PKR {df['price_pkr'].median():,.0f}")

    st.subheader("Listings by Location")
    location_counts = df["location"].value_counts().head(10)
    st.bar_chart(location_counts)

    st.subheader("Bedroom Distribution")
    bedroom_counts = df["bedrooms"].dropna().value_counts().sort_index()
    st.bar_chart(bedroom_counts)

    st.subheader("Property Listings")
    display_columns = [
        "title",
        "price",
        "location",
        "area",
        "bedrooms",
        "bathrooms",
        "source",
    ]
    st.dataframe(df[display_columns], use_container_width=True)
