# Assigment

## Deploy a or more containers to serve Airflow and ETL (using Python, Pandas), can connect to the datasource, cloud
1. Crawl Dien May Xanh data from <https://www.dienmayxanh.com/{category}> (*)
2. Transform data and Save to local folder
3. Upload data from local folder to Azure Blob Storage

(*) Available categories:
1. Tivi
2. Tu-lanh
3. May-lanh
4. Gia-dung
5. Loc-nuoc
6. Tu-dong
7. Loa
8. Quat-dieu-hoa
9. Noi-chien
10. Bep-dien
11. May-say
12. Dien-thoai

Config the specific category to crawl by modify .env file *(which I didn't commit to github because of information security)*
but you will provide informations for you to config your own .env file
```text
AIRFLOW_USER=<your-airflow-user>
AIRFLOW_PASS=<your-airflow-pass>
AIRFLOW_UID=1000
URL=https://www.dienmayxanh.com/
CATEGORY=may-lanh
SUFFIX="#c=2002&o=13&pi=1000"                       // suffix in url to get full product from page
CONTAINER_NAME=<your-blob-storage-container>        
WASB_CONN_ID=<your-connection-id>                   // guide below
```
## Project Structure
```text
├── dags/
│   ├── __init__.py
│   └── etl.py                  # script for AIRFLOW DAGs
├── data/
├── img/
├── logs/
├── plugins/
├── docker-compose.yaml
├── Dockerfile                      
├── README.md
├── airflow-webserver.pid
├── airflow.cfg
├── requirements.txt
├── setup.py
├── src
│   ├── __init__.py
│   └── crawler.py              # selenium source for crawling
└── webserver_config.py
```
## Setting for Environment
### Generate SAS Blob URL 
![image alt text](<img/get_SAS.png>)
### Setting connection to Azure Blob Storage
![image alt text](<img/connection-setting.png>)
1. Connection Id: Name your connection id and write it into your .env
2. Blob Storage Login: your storage account name
3. SAS token: Token that you have generated above

## Run project with Docker

```bash
docker compose up
```
