
# prefect flow goes here
import time
import boto3
import httpx
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta

AWS_REGION = "us-east-1"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
sqs = boto3.client("sqs", region_name=AWS_REGION)

# ---------------------------------------------------------------------
# Prefect Tasks
# ---------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=30, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=10))
def get_sqs_url(uva_id: str) -> str:
    """Calls the API to populate the SQS queue and return its URL."""
    logger = get_run_logger()
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"
    logger.info(f"Requesting queue for {uva_id}")
    response = httpx.post(url)
    response.raise_for_status()
    sqs_url = response.json()["sqs_url"]
    logger.info(f"Queue ready at {sqs_url}")
    return sqs_url


@task(retries=5, retry_delay_seconds=60)
def wait_for_all_messages(sqs_url: str, expected_count: int = 21):
    """Waits until at least one message is visible in the SQS queue."""
    logger = get_run_logger()
    while True:
        attributes = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )["Attributes"]
        counts = [int(attributes[k]) for k in [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"
        ]]
        total = sum(counts)
        logger.info(f"Queue status — total: {total}/{expected_count}")
        if total >= 1:
            logger.info("At least one message is now visible. Starting collection.")
            break
        logger.info("Waiting 15 seconds for messages to appear...")
        time.sleep(15)


@task(retries=3, retry_delay_seconds=20)
def collect_messages(sqs_url: str, expected_count: int = 21) -> list:
    """Retrieves, parses, and deletes messages from the queue until all expected messages are collected."""
    logger = get_run_logger()
    pairs = []

    while len(pairs) < expected_count:
        response = sqs.receive_message(
            QueueUrl=sqs_url,
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )

        messages = response.get("Messages", [])
        if not messages:
            logger.info(f"No new messages available. Collected {len(pairs)}/{expected_count} so far. Waiting 15 seconds...")
            time.sleep(15)
            continue

        for msg in messages:
            attrs = msg["MessageAttributes"]
            order_no = attrs["order_no"]["StringValue"]
            word = attrs["word"]["StringValue"]
            pairs.append((order_no, word))
            sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=msg["ReceiptHandle"])
            logger.info(f"Received and deleted message #{order_no}: '{word}'")

    logger.info(f"Collected all {len(pairs)} messages.")
    return pairs


@task
def assemble_phrase(pairs: list) -> str:
    """Sorts message fragments and assembles the final phrase."""
    logger = get_run_logger()
    sorted_pairs = sorted(pairs, key=lambda x: int(x[0]))
    phrase = " ".join(word for _, word in sorted_pairs)
    logger.info(f"Assembled phrase: '{phrase}'")
    return phrase


@task
def send_solution(uva_id: str, phrase: str, platform: str = "prefect"):
    """Sends the completed phrase to the submission queue."""
    logger = get_run_logger()
    response = sqs.send_message(
        QueueUrl=SUBMIT_QUEUE_URL,
        MessageBody="submission",
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": uva_id},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform},
        },
    )
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        logger.info("✅ Submission received (HTTP 200).")
    else:
        logger.warning(f"⚠️ Submission returned status {status}.")


# ---------------------------------------------------------------------
# Prefect Flow
# ---------------------------------------------------------------------

@flow(name="DP2 Prefect Flow", log_prints=True)
def dp2_prefect_flow(uva_id: str):
    logger = get_run_logger()
    logger.info("Starting DS3022 Prefect pipeline...")

    sqs_url = get_sqs_url(uva_id)
    wait_for_all_messages(sqs_url)
    pairs = collect_messages(sqs_url)
    phrase = assemble_phrase(pairs)
    send_solution(uva_id, phrase, "prefect")

    logger.info("Pipeline completed successfully.")


if __name__ == "__main__":
    dp2_prefect_flow(uva_id="dxg9tt")
