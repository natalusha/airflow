from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd

CON_PSQL = "postgresql://postgres:root@172.26.64.1:5433/stock_data"


def select_sql_df(credentials: str, sql_queries: dict):
    l = []
    s = ""
    # read in your SQL query results using pandas
    for k in sql_queries:
        dataframe = pd.read_sql(
            sql_queries[k],
            con=credentials,
        )
        html = dataframe.to_html()
        pretty_html = f"""<p>Statictics of {k} of bitcoins, which were selled and bought during this day</p>
        {html}"""
        l.append(pretty_html)
    s = " ".join(l)
    return s


def send_email_basic(sender, receiver, email_subject, htmls):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = sender  # Enter  address
    receiver_email = receiver  # Enter receiver address
    password = "6duV1Am6r1kU"  # Enter  gmail password

    email_html = f"""<html>
    <body>
        <p>Hello!</p>
        {htmls}
        <br>
    </body>
    </html>"""

    message = MIMEMultipart("multipart")

    # Turn these into plain/html MIMEText objects
    part2 = MIMEText(email_html, "html")

    message.attach(part2)
    message["Subject"] = email_subject
    message["From"] = sender_email

    for i, val in enumerate(receiver):
        message["To"] = val

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())


def init_email():
    sender = "humla.ikigai@gmail.com"  # add the sender gmail address here
    recipients = ["natalyholubtsova@gmail.com"]  # add your e-mail recipients here
    subject = (
        "Bitcoin statistics per day"  # add the subject of the e-mail you'd like here
    )
    htmls = select_sql_df(
        CON_PSQL,
        {
            "max deal": "SELECT * FROM max_deal",
            "processed amount": "SELECT * FROM sum_amount",
        },
    )
    send_email_basic(sender, recipients, subject, htmls)


default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": "30-05-2022",
    "email": ["airflow@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
}

dag = DAG(
    "send_email_with_stat",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=5),
    schedule_interval="@daily",
    catchup=False,
)

t1 = PythonOperator(task_id="send_email", python_callable=init_email, dag=dag)
