import pandas as pd
import psycopg2

def insert_func(**kwargs):
    conn = psycopg2.connect(
        host="postgres",
        database="crypto_dw",   
        user="postgres",        
        password="18082005",
        port=5432
    )
    cur = conn.cursor()

    cur.execute(
        """
        CREATE SCHEMA IF NOT EXISTS mart
        """
    )
    conn.commit()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mart.dim_coin (
            symbol TEXT PRIMARY KEY,
            name TEXT
        )
        """
    )
    conn.commit()

    cur.execute(
        """
        INSERT INTO mart.dim_coin (symbol , name)
        SELECT DISTINCT
            UPPER(TRIM(coin_id)) AS symbol,
            UPPER(TRIM(coin_id)) AS name
        FROM staging.market_clean
        ON CONFLICT (symbol) DO NOTHING
        """
    )
    conn.commit()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mart.fact_market (
            market_id SERIAL PRIMARY KEY,
            symbol TEXT REFERENCES mart.dim_coin(symbol),
            trade_time TIMESTAMP,
            price NUMERIC,
            market_cap NUMERIC,
            volume NUMERIC
        )
        """
    )
    conn.commit()

    cur.execute(
        """
        INSERT INTO mart.fact_market (symbol, trade_time, price, market_cap, volume)
        SELECT d.symbol, mc.trade_time, mc.price, mc.market_cap, mc.volume
        FROM staging.market_clean AS mc
        JOIN mart.dim_coin AS d
            ON UPPER(TRIM(mc.coin_id)) = d.symbol
        """
    )
    conn.commit()

    cur.close()
    conn.close()

    print("Done")

if __name__ == "__main__":
    insert_func()