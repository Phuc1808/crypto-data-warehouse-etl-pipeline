Project : Crypto Data Warehouse & ETL Pipeline

Overview : 
Dự án này xây dựng ETL pipeline để thu thập và xử lí dữ liệu tiền mã hóa (BTC,ETH,XRP) từ file JSON , sau đó lưu trữ vào PostgresSQL Data Warehouse với các schema chuẩn (raw,staging,mart). Pipeline được orchestrated bằng Apache Airflow và triển khai bằng Docker Compose.
Architecture : 
                        JSON files (BTC, ETH, XRP)
                                                    |
                                                    v
                                        Extract (Python)
                                                   |
                                                   v
                                           Load → raw
                                                   |
                                                   v
                                     Transform & Clean → staging
                                                   |
                                                   v
                                         Data Modeling  → mart
                                                  |
                                                  v
                                          Dashboard
Tech Stack : 
* Python (pandas,psycopg2,SQLAlchemy)
* PostgreSQL (schema)
* Apache Airflow (task orchestration, DAG scheduling)
* Docker Compose (containerization)
* Streamlit (dashboard)
Project Structure : 


Crypto_Data_Warehouse_&_ETL_Pipeline/
├── dags/                     
│   ├── dag_workflow.py    
│   └── tasks/                
│       ├── task1.py
│       ├── task2.py
│       ├── task3.py
│       └── task4.py
├── dashboard/                 
│   └── dashboard.py    
├── data/
│   ├── raw/             
│   │   ├── BTC.json
│   │   ├── ETH.json
│   │   └── XRP.json
│   └── processed/            
│       └── market_extracted.csv
├── logs/                      
├── postgres_data/            
├── docker-compose.yml    
├── requirements.txt         
└── README.md            


How to run : 
1. Clone repo :
git clone https://github.com/Phuc1808/crypto-data-warehouse-etl-pipeline.git
cd crypto-data-warehouse-etl-pipeline
2. Start services with Docker : 
        docker-compose up -d
3. Access Airflow UI : 
        URL: http://127.0.0.1:8082
4. Trigger DAG : 
        Bật DAG crypto_dag (bật trigger thủ công)
5. Access dashboard : 
        URL: http://127.0.0.1:8502
