import pandas as pd
import os
import json

raw_dir = "/opt/airflow/data/raw"
processed_dir = "/opt/airflow/data/processed"

def extract_func(**kwargs):
    files = ["BTC.json","ETH.json","XRP.json"]
    all_data = []
    for i in files :
        path = os.path.join(raw_dir,i)
        with open(path,"r") as data_in_file :
            data = json.load(data_in_file)

            prices = pd.DataFrame(data["prices"],columns = ["timestamp","price"])
            market_caps = pd.DataFrame(data["market_caps"],columns = ["timestamp","market_cap"])
            volumes = pd.DataFrame(data["total_volumes"],columns = ["timestamp","volume"])

            df = prices.merge(market_caps,on = "timestamp").merge(volumes,on = "timestamp")

            df["trade_time"] = pd.to_datetime(df["timestamp"],unit = "ms")
            df["coin_id"] = i.replace(".json","")

            df = df[["coin_id","trade_time","price","market_cap","volume"]]

            all_data.append(df)

    final_df = pd.concat(all_data,ignore_index= True)

    os.makedirs(processed_dir,exist_ok= True)
    final_path = os.path.join(processed_dir,"market_extracted.csv")
    final_df.to_csv(final_path,index= False)

    print("Done")

if __name__ == "__main__":
    extract_func()