import os
import json
import time
import pika
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_URL  = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE_NAME = "user_events"
QUEUE_NAME    = "task_service_user_events"


def handle_user_created(payload: dict):
    logger.info(f"[EVENT] New user created -> id={payload.get('user_id')} username={payload.get('username')}")


def handle_user_deleted(payload: dict):
    logger.info(f"[EVENT] User deleted -> id={payload.get('user_id')}")


def on_message(channel, method, properties, body):
    try:
        payload     = json.loads(body)
        routing_key = method.routing_key
        logger.info(f"Received event [{routing_key}]: {payload}")
        if routing_key == "user.created":
            handle_user_created(payload)
        elif routing_key == "user.deleted":
            handle_user_deleted(payload)
        else:
            logger.warning(f"Unknown routing key: {routing_key}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def start_consumer():
    while True:
        try:
            logger.info("Connecting to RabbitMQ...")
            params  = pika.URLParameters(RABBITMQ_URL)
            conn    = pika.BlockingConnection(params)
            channel = conn.channel()
            channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key="user.*")
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
            logger.info(f"Listening on [{QUEUE_NAME}]...")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            logger.warning("RabbitMQ not ready, retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Consumer error: {e}, retrying in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    start_consumer()
