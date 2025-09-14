import pandas as pd
import psycopg2

def clean_func(**kwargs):
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
        CREATE SCHEMA IF NOT EXISTS staging
        """
    )
    conn.commit()

    cur.execute(
        """
        DROP TABLE IF EXISTS staging.market_clean
        """
    )
    conn.commit()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS staging.market_clean (
            coin_id TEXT,
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
        INSERT INTO staging.market_clean (coin_id, trade_time, price, market_cap, volume)
        WITH clean_table_1 AS (
            SELECT * , ROW_NUMBER() OVER (PARTITION BY coin_id, trade_time ORDER BY coin_id) AS n
            FROM raw.market
        )
        SELECT coin_id, trade_time, price, market_cap, volume
        FROM clean_table_1
        WHERE n = 1
        """
    )
    conn.commit()

    cur.close()
    conn.close()

    print("Done")

if __name__ == "__main__":
    clean_func()