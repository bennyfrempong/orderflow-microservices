import pika
import json
import os
import time
import logging

logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')


def get_connection(retries=5, delay=3):
    """Establish a RabbitMQ connection with retry logic."""
    for attempt in range(retries):
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            logger.info("Connected to RabbitMQ")
            return connection
        except Exception as e:
            logger.warning(f"RabbitMQ connection attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    raise ConnectionError("Could not connect to RabbitMQ after multiple attempts")


def publish_event(exchange, routing_key, payload):
    """Publish a JSON-encoded event to the given exchange and routing key."""
    connection = get_connection()
    channel = connection.channel()

    # Declare exchange as durable so it survives RabbitMQ restarts
    channel.exchange_declare(exchange=exchange, exchange_type='topic', durable=True)

    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=2,          # Persist message to disk
            content_type='application/json'
        )
    )

    connection.close()
    logger.info(f"Published [{routing_key}] to exchange [{exchange}]: {payload}")


def publish_order_created(order_data):
    """Publish an OrderCreated event after a new order is written to DB."""
    publish_event(
        exchange='orders',
        routing_key='order.created',
        payload={
            'event': 'OrderCreated',
            'order_id': order_data['id'],
            'product_id': order_data['product_id'],
            'quantity': order_data['quantity'],
            'customer_email': order_data['customer_email']
        }
    )
