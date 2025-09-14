import pandas as pd
import psycopg2

def load_raw_func(**kwargs):
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
        CREATE SCHEMA IF NOT EXISTS raw
        """
    )
    conn.commit()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.market (
            coin_id TEXT,
            trade_time TIMESTAMP,
            price NUMERIC,
            market_cap NUMERIC,
            volume NUMERIC
        )
        """
    )
    conn.commit()

    file_path = "/opt/airflow/data/processed/market_extracted.csv"
    df = pd.read_csv(file_path)
    for ind, row in df.iterrows():
        cur.execute("""
            INSERT INTO raw.market (coin_id, trade_time, price, market_cap, volume)
            VALUES (%s, %s, %s, %s, %s);
        """, (row['coin_id'], row['trade_time'], row['price'], row['market_cap'], row['volume']))
    conn.commit()

    cur.close()
    conn.close()
    
    print("Done")

if __name__ == "__main__":
    load_raw_func()