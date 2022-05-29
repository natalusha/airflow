from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime, timedelta, tzinfo
import pytz
import json
import pandas as pd
import boto3
from dateutil import tz
from io import BytesIO, StringIO
from pyarrow import Table, parquet as pq
from sqlalchemy import create_engine, text

CON_PSQL = "postgresql://postgres:root@172.26.64.1:5433/stock_data"
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

dag = DAG("read_and_format_dag", default_args=DEFAULT_ARGS, schedule_interval="@hourly")


def dataframe_to_s3(s3_resource, input_datafame, bucket_name, filepath, format):
    if format == "parquet":
        out_buffer = BytesIO()
        input_datafame.to_parquet(out_buffer, index=False)

    elif format == "csv":
        out_buffer = StringIO()
        input_datafame.to_parquet(out_buffer, index=False)
    s3_resource.Object(bucket_name, filepath).put(Body=out_buffer.getvalue())


def s3_to_dataframe(s3_resource, bucket_name, filepath, format):
    if format == "parquet":
        buf = s3_resource.Object(bucket_name, filepath).get()["Body"].read()
        df2 = pd.read_parquet(BytesIO(buf), engine="pyarrow")

    elif format == "csv":
        buf = s3_resource.Object(bucket_name, filepath).get()["Body"].read()
        df2 = pd.read_parquet(StringIO(buf), engine="pyarrow")
    return df2


def same_df(prefix: str, df, col_order: list):
    for col in df.columns:
        if col not in col_order:
            df.drop(col, inplace=True, axis=1)
    df = df[col_order]
    df.columns = ["date", "price", "amount", "type"]
    df["stock exchange"] = prefix
    df2 = df.drop_duplicates()
    return df2


def concat_dfs(
    s3,
    out_bucket: str,
    prefix: str,
):

    objects = s3.Bucket(out_bucket).objects.all().filter(Prefix=f"{prefix}/")
    dfs = []
    try:
        for obj in objects:
            from_zone = tz.tzutc()
            to_zone = tz.tzlocal()
            last_date_utc = obj.last_modified.replace(tzinfo=from_zone)
            central = last_date_utc.astimezone(to_zone)
            last_date_central = central.replace(tzinfo=None)
            cur_date = datetime.now()
            diff_minutes = ((cur_date - last_date_central).total_seconds()) / 60
            if diff_minutes <= 60:
                obj_str = obj.get()["Body"].read()
                df = pd.read_json(BytesIO(obj_str))
                dfs.append(df)
        df = pd.concat(dfs)
        return df
    except ValueError as er:
        print(er)


def _read_and_format():
    s3 = boto3.resource(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=AWS_ASCCESS_KEY,
        aws_secret_access_key=AWS_ASCCESS_SECRET_KEY,
    )

    df_bitm = concat_dfs(
        s3,
        "apirowdatalake",
        "bitmex",
    )
    dft1 = same_df("bitmex", df_bitm, ["timestamp", "price", "homeNotional", "side"])
    df_bitfin = concat_dfs(
        s3,
        "apirowdatalake",
        "bitfinex",
    )
    dft2 = same_df("bitfinex", df_bitfin, ["timestamp", "price", "amount", "type"])

    df_polon = concat_dfs(
        s3,
        "apirowdatalake",
        "poloniex",
    )
    dft3 = same_df(
        "poloniex",
        df_polon,
        ["date", "rate", "amount", "type"],
    )
    vertical_concat = pd.concat([dft1, dft2, dft3], axis=0)


def _insert_parq():
    s3 = boto3.resource(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=AWS_ASCCESS_KEY,
        aws_secret_access_key=AWS_ASCCESS_SECRET_KEY,
    )
    res = s3_to_dataframe(s3, "prettydata", "result.parquet", "parquet")
    engine = create_engine(CON_PSQL)
    connection = engine.connect()
    truncate_query = text("TRUNCATE TABLE bitcoin_data")
    connection.execution_options(autocommit=True).execute(truncate_query)
    t = res.to_sql("bitcoin_data", con=engine, index=False, if_exists="append")


download_coin_data = PythonOperator(
    task_id="read_and_format",
    python_callable=_read_and_format,
    dag=dag,
)
insert_parq = PythonOperator(
    task_id="insert_parq",
    python_callable=_insert_parq,
    dag=dag,
)
download_coin_data >> insert_parq
