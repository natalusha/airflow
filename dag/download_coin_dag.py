from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from datetime import datetime, timedelta
import json
from airflow.hooks.base import BaseHook
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import boto3

ENDPOINT_URL = "http://10.34.120.222:9000"
AWS_ASCCESS_KEY = "minioadmin"
AWS_ASCCESS_SECRET_KEY = "minioadmin"

DEFAULT_ARGS = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": "30-05-2022",
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "download_coin_dag", default_args=DEFAULT_ARGS, schedule_interval="*/1 * * * *"
)


def _download_coin_data(ts_nodash, **_):
    services = {
        "bitfinex": "https://api.bitfinex.com/v1/trades/btcusd?limit_trades=500",
        "bitmex": "https://www.bitmex.com/api/v1/trade?symbol=XBTUSD&count=500&reverse=true",
        "poloniex": "https://poloniex.com/public?command=returnTradeHistory&currencyPair=USDT_BTC",
    }
    s3 = boto3.resource(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=AWS_ASCCESS_KEY,
        aws_secret_access_key=AWS_ASCCESS_SECRET_KEY,
    )
    for k in services:
        url = services[k]
        response = requests.get(url)
        data = response.json()
        s3.Object("apirowdatalake", f"{k}/{ts_nodash}-{k}.json").put(
            Body=(bytes(json.dumps(data).encode("UTF-8")))
        )


download_coin_data = PythonOperator(
    task_id="download_coin_data",
    python_callable=_download_coin_data,
    dag=dag,
)
