# airflow DAG goes here
"""
DS3022 Data Project 2 — Airflow DAG
Author: <Your Name>
Description:
    Airflow DAG that replicates the Prefect flow to fetch, assemble,
    and submit the phrase.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import boto3
import httpx
import time

AWS_REGION = "us-east-1"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
UVA_ID = "dxg9tt"

sqs = boto3.client("sqs", region_name=AWS_REGION)
log = LoggingMixin().log  # Airflow-provided logger

# ---------------------------------------------------------------------
# Task Functions
# ---------------------------------------------------------------------

def get_sqs_url(**context):
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVA_ID}"
    response = httpx.post(url).json()
    sqs_url = response["sqs_url"]
    context["ti"].xcom_push(key="sqs_url", value=sqs_url)
    log.info(f"Populated queue at {sqs_url}")

def wait_for_all_messages(**context):
    sqs_url = context["ti"].xcom_pull(key="sqs_url")
    while True:
        attrs = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )
        counts = [int(v) for v in attrs["Attributes"].values()]
        total = sum(counts)
        log.info(f"Queue status — total: {total}/21")
        if total >= 21:
            log.info("All messages ready for collection.")
            break
        log.info("Waiting 30 seconds for delayed messages...")
        time.sleep(30)

def collect_messages(**context):
    sqs_url = context["ti"].xcom_pull(key="sqs_url")
    pairs = []

    while True:
        response = sqs.receive_message(
            QueueUrl=sqs_url,
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )
        if "Messages" not in response:
            break
        for msg in response["Messages"]:
            attrs = msg["MessageAttributes"]
            order_no = attrs["order_no"]["StringValue"]
            word = attrs["word"]["StringValue"]
            pairs.append((order_no, word))
            sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=msg["ReceiptHandle"])
            log.info(f"Fetched and deleted message #{order_no}: '{word}'")

    context["ti"].xcom_push(key="pairs", value=pairs)
    log.info(f"Collected {len(pairs)} messages.")

def assemble_phrase(**context):
    pairs = context["ti"].xcom_pull(key="pairs")
    phrase = " ".join(word for _, word in sorted(pairs, key=lambda x: int(x[0])))
    context["ti"].xcom_push(key="phrase", value=phrase)
    log.info(f"Reassembled phrase: '{phrase}'")

def send_solution(**context):
    phrase = context["ti"].xcom_pull(key="phrase")
    response = sqs.send_message(
        QueueUrl=SUBMIT_QUEUE_URL,
        MessageBody="submission",
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": UVA_ID},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": "airflow"},
        },
    )
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        log.info("✅ Airflow submission successful (HTTP 200).")
    else:
        log.warning(f"⚠️ Airflow submission returned {status}.")

# ---------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------

default_args = {
    "owner": "student",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dp2_airflow_dag",
    default_args=default_args,
    description="DS3022 Data Project 2 — Airflow Version",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["DS3022", "DataPipeline"],
) as dag:

    t1 = PythonOperator(task_id="get_sqs_url", python_callable=get_sqs_url, provide_context=True)
    t2 = PythonOperator(task_id="wait_for_all_messages", python_callable=wait_for_all_messages, provide_context=True)
    t3 = PythonOperator(task_id="collect_messages", python_callable=collect_messages, provide_context=True)
    t4 = PythonOperator(task_id="assemble_phrase", python_callable=assemble_phrase, provide_context=True)
    t5 = PythonOperator(task_id="send_solution", python_callable=send_solution, provide_context=True)

    t1 >> t2 >> t3 >> t4 >> t5
