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
            return connection
        except Exception as e:
            logger.warning(f"RabbitMQ connection attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    raise ConnectionError("Could not connect to RabbitMQ after multiple attempts")


def publish_event(exchange, routing_key, payload):
    """Publish a JSON-encoded event to RabbitMQ."""
    connection = get_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange, exchange_type='topic', durable=True)
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type='application/json'
        )
    )
    connection.close()
    logger.info(f"Published [{routing_key}] to exchange [{exchange}]: {payload}")


def publish_inventory_reserved(order_id, product_id, quantity, customer_email):
    """Happy path — stock was available and has been decremented."""
    publish_event(
        exchange='inventory',
        routing_key='inventory.reserved',
        payload={
            'event': 'InventoryReserved',
            'order_id': order_id,
            'product_id': product_id,
            'quantity': quantity,
            'customer_email': customer_email
        }
    )


def publish_out_of_stock(order_id, product_id, quantity_requested, customer_email):
    """Failure path — triggers Saga compensation in the Order Service."""
    publish_event(
        exchange='inventory',
        routing_key='inventory.out_of_stock',
        payload={
            'event': 'OutOfStock',
            'order_id': order_id,
            'product_id': product_id,
            'quantity_requested': quantity_requested,
            'customer_email': customer_email
        }
    )
