import pika
import json
import os
import time
import logging
from publisher import publish_inventory_reserved, publish_out_of_stock

logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')


def get_connection(retries=10, delay=3):
    """Establish a RabbitMQ connection with retry logic."""
    for attempt in range(retries):
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            logger.info("[Inventory Consumer] Connected to RabbitMQ")
            return connection
        except Exception as e:
            logger.warning(f"[Inventory Consumer] Attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    raise ConnectionError("[Inventory Consumer] Could not connect to RabbitMQ")


def handle_order_created(ch, method, properties, body):
    """
    Core business logic for inventory reservation.

    Flow:
      1. Parse the OrderCreated event
      2. Look up the product in this service's DB
      3a. Sufficient stock → decrement + publish InventoryReserved
      3b. No stock / not found → publish OutOfStock (triggers Saga compensation)
    """
    try:
        payload = json.loads(body)
        order_id      = payload['order_id']
        product_id    = payload['product_id']
        quantity      = payload['quantity']
        customer_email = payload['customer_email']

        logger.info(
            f"[Inventory] OrderCreated received: order={order_id}, "
            f"product={product_id}, qty={quantity}"
        )

        from models import db, Product

        product = Product.query.get(product_id)

        if not product:
            logger.warning(f"[Inventory] Product '{product_id}' not found — publishing OutOfStock")
            publish_out_of_stock(order_id, product_id, quantity, customer_email)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if product.stock_quantity >= quantity:
            # ── Happy path ──────────────────────────────────────────────────
            product.stock_quantity -= quantity
            db.session.commit()
            logger.info(
                f"[Inventory] Reserved {quantity}x '{product_id}'. "
                f"Remaining stock: {product.stock_quantity}"
            )
            publish_inventory_reserved(order_id, product_id, quantity, customer_email)
        else:
            # ── Sad path — Saga compensation triggered ───────────────────────
            logger.warning(
                f"[Inventory] Insufficient stock for '{product_id}': "
                f"requested={quantity}, available={product.stock_quantity}"
            )
            publish_out_of_stock(order_id, product_id, quantity, customer_email)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"[Inventory] Error processing OrderCreated event: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_consumer(app):
    """
    Start consuming OrderCreated events inside the Flask app context.
    Binds to the 'orders' exchange with routing key 'order.created'.
    """
    with app.app_context():
        connection = get_connection()
        channel = connection.channel()

        # Declare the orders exchange (must match Order Service declaration)
        channel.exchange_declare(exchange='orders', exchange_type='topic', durable=True)

        # Dead-letter exchange for failed messages
        channel.exchange_declare(exchange='orders.dlx', exchange_type='direct', durable=True)
        channel.queue_declare(queue='inventory_service.dlq', durable=True)
        channel.queue_bind(
            exchange='orders.dlx',
            queue='inventory_service.dlq',
            routing_key='inventory_service.dlq'
        )

        # Main processing queue
        channel.queue_declare(
            queue='inventory_service.order_created',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'orders.dlx',
                'x-dead-letter-routing-key': 'inventory_service.dlq'
            }
        )
        channel.queue_bind(
            exchange='orders',
            queue='inventory_service.order_created',
            routing_key='order.created'
        )

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue='inventory_service.order_created',
            on_message_callback=handle_order_created
        )

        logger.info("[Inventory Consumer] Listening for OrderCreated events...")
        channel.start_consuming()
