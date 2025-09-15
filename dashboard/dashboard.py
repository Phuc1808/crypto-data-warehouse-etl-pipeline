import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def get_connection():
    engine = create_engine(
        "postgresql+psycopg2://postgres:18082005@postgres:5432/crypto_dw"
    )
    return engine

def load_data():
    query = """
        SELECT 
            m.trade_time,
            m.price,
            m.market_cap,
            m.volume,
            d.symbol,
            d.name
        FROM mart.fact_market AS m
        JOIN mart.dim_coin AS d
        ON m.symbol = d.symbol
        ORDER BY m.trade_time
    """
    engine = get_connection()
    df = pd.read_sql(query, engine)
    return df

def main():
    st.set_page_config(page_title="Crypto Dashboard", layout="wide")
    st.title("Crypto Data Dashboard")

    df = load_data()

    coin_list = df["symbol"].unique().tolist()
    selected_coin = st.sidebar.selectbox("Select Coin", coin_list, index=0)
    df_coin = df[df["symbol"] == selected_coin]

    col1, col2, col3 = st.columns(3)
    col1.metric("Average Price", f"{df_coin['price'].mean():,.2f}USD")
    col2.metric("Average Market Cap", f"{df_coin['market_cap'].mean():,.0f}USD")
    col3.metric("Average Volume", f"{df_coin['volume'].mean():,.0f}USD")

    st.markdown("---")

    st.subheader(f"Price Trend - {selected_coin}")
    st.line_chart(df_coin.set_index("trade_time")["price"])

    st.subheader(f"Trading Volume - {selected_coin}")
    st.bar_chart(df_coin.set_index("trade_time")["volume"])

if __name__ == "__main__":
    main()
