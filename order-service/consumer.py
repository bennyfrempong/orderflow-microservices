import pika
import json
import os
import time
import logging

logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')


def get_connection(retries=10, delay=3):
    """Establish a RabbitMQ connection with retry logic."""
    for attempt in range(retries):
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            logger.info("[Consumer] Connected to RabbitMQ")
            return connection
        except Exception as e:
            logger.warning(f"[Consumer] Connection attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    raise ConnectionError("[Consumer] Could not connect to RabbitMQ")


def handle_out_of_stock(ch, method, properties, body):
    """
    Saga compensation handler.
    When Inventory reports OutOfStock, cancel the corresponding order.
    This is the compensating transaction in the choreography-based saga.
    """
    try:
        payload = json.loads(body)
        order_id = payload.get('order_id')
        logger.info(f"[Saga] Received OutOfStock for order {order_id} — initiating compensation")

        # Import here to avoid circular imports at module level
        from models import db, Order

        order = Order.query.get(order_id)
        if order:
            order.status = 'cancelled'
            db.session.commit()
            logger.info(f"[Saga] Order {order_id} successfully cancelled")
        else:
            logger.warning(f"[Saga] Order {order_id} not found — nothing to cancel")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"[Saga] Error processing OutOfStock event: {e}")
        # Nack without requeue — send to dead-letter queue if configured
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_inventory_reserved(ch, method, properties, body):
    """
    When Inventory confirms reservation, mark the order as confirmed.
    """
    try:
        payload = json.loads(body)
        order_id = payload.get('order_id')
        logger.info(f"[Consumer] Received InventoryReserved for order {order_id} — confirming")

        from models import db, Order

        order = Order.query.get(order_id)
        if order:
            order.status = 'confirmed'
            db.session.commit()
            logger.info(f"[Consumer] Order {order_id} confirmed")
        else:
            logger.warning(f"[Consumer] Order {order_id} not found")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"[Consumer] Error processing InventoryReserved event: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_consumer(app):
    """
    Start consuming inventory events inside the Flask app context.
    Listens for:
      - inventory.out_of_stock  → cancel order (Saga compensation)
      - inventory.reserved      → confirm order
    """
    with app.app_context():
        connection = get_connection()
        channel = connection.channel()

        # Declare the inventory exchange
        channel.exchange_declare(exchange='inventory', exchange_type='topic', durable=True)

        # Dead-letter exchange for failed messages
        channel.exchange_declare(exchange='inventory.dlx', exchange_type='direct', durable=True)
        channel.queue_declare(queue='order_service.dlq', durable=True)
        channel.queue_bind(exchange='inventory.dlx', queue='order_service.dlq', routing_key='order_service.dlq')

        # Queue for OutOfStock events
        channel.queue_declare(
            queue='order_service.out_of_stock',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'inventory.dlx',
                'x-dead-letter-routing-key': 'order_service.dlq'
            }
        )
        channel.queue_bind(
            exchange='inventory',
            queue='order_service.out_of_stock',
            routing_key='inventory.out_of_stock'
        )

        # Queue for InventoryReserved events
        channel.queue_declare(
            queue='order_service.inventory_reserved',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'inventory.dlx',
                'x-dead-letter-routing-key': 'order_service.dlq'
            }
        )
        channel.queue_bind(
            exchange='inventory',
            queue='order_service.inventory_reserved',
            routing_key='inventory.reserved'
        )

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='order_service.out_of_stock', on_message_callback=handle_out_of_stock)
        channel.basic_consume(queue='order_service.inventory_reserved', on_message_callback=handle_inventory_reserved)

        logger.info("[Order Service Consumer] Listening for inventory events...")
        channel.start_consuming()
