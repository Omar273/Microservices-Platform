import os
import json
import pika
import logging

logger = logging.getLogger(__name__)

RABBITMQ_URL  = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE_NAME = "user_events"


def get_connection():
    params = pika.URLParameters(RABBITMQ_URL)
    params.heartbeat = 30
    return pika.BlockingConnection(params)


def publish_event(routing_key: str, payload: dict):
    try:
        conn    = get_connection()
        channel = conn.channel()
        channel.exchange_declare(
            exchange=EXCHANGE_NAME,
            exchange_type="topic",
            durable=True
        )
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json"
            )
        )
        logger.info(f"Published event [{routing_key}]: {payload}")
        conn.close()
    except Exception as e:
        logger.error(f"Failed to publish event [{routing_key}]: {e}")
