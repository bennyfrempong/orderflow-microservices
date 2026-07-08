import pika
import json
import os
import time
import logging

logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')
MAX_RETRIES = 3  # Max processing attempts before sending to DLQ


def get_connection(retries=10, delay=3):
    """Establish a RabbitMQ connection with retry logic."""
    for attempt in range(retries):
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            logger.info("[Notification Consumer] Connected to RabbitMQ")
            return connection
        except Exception as e:
            logger.warning(f"[Notification] Connection attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    raise ConnectionError("[Notification] Could not connect to RabbitMQ")


def get_retry_count(properties):
    """Read the x-death header to determine how many times a message has been retried."""
    if properties and properties.headers:
        x_death = properties.headers.get('x-death', [])
        if x_death:
            return sum(entry.get('count', 0) for entry in x_death)
    return 0


def handle_inventory_reserved(ch, method, properties, body):
    """
    Send a confirmation notification when inventory is successfully reserved.
    Simulates sending an email — in production, call an email API here.
    """
    try:
        payload = json.loads(body)
        order_id       = payload.get('order_id')
        product_id     = payload.get('product_id')
        quantity       = payload.get('quantity')
        customer_email = payload.get('customer_email')

        # ── Simulated email send ───────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("📧 [NOTIFICATION] Confirmation email sent")
        logger.info(f"   To:      {customer_email}")
        logger.info(f"   Order:   #{order_id}")
        logger.info(f"   Product: {product_id}  (qty: {quantity})")
        logger.info(f"   Status:  ✅ CONFIRMED — your item will be shipped shortly")
        logger.info("=" * 60)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        retry_count = get_retry_count(properties)
        logger.error(f"[Notification] Error handling InventoryReserved (attempt {retry_count + 1}): {e}")

        if retry_count < MAX_RETRIES:
            logger.warning(f"[Notification] Requeuing message (retry {retry_count + 1}/{MAX_RETRIES})")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            logger.error(f"[Notification] Max retries reached — sending to DLQ")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_out_of_stock(ch, method, properties, body):
    """
    Send a rejection notification when inventory reports out-of-stock.
    Saga compensation has already cancelled the order at this point.
    """
    try:
        payload = json.loads(body)
        order_id           = payload.get('order_id')
        product_id         = payload.get('product_id')
        quantity_requested = payload.get('quantity_requested')
        customer_email     = payload.get('customer_email')

        # ── Simulated rejection email ──────────────────────────────────────
        logger.info("=" * 60)
        logger.info("📧 [NOTIFICATION] Rejection email sent")
        logger.info(f"   To:        {customer_email}")
        logger.info(f"   Order:     #{order_id}")
        logger.info(f"   Product:   {product_id}  (requested qty: {quantity_requested})")
        logger.info(f"   Status:    ❌ REJECTED — item is currently out of stock")
        logger.info(f"   Action:    Your order has been cancelled. No charge made.")
        logger.info("=" * 60)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        retry_count = get_retry_count(properties)
        logger.error(f"[Notification] Error handling OutOfStock (attempt {retry_count + 1}): {e}")

        if retry_count < MAX_RETRIES:
            logger.warning(f"[Notification] Requeuing message (retry {retry_count + 1}/{MAX_RETRIES})")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            logger.error(f"[Notification] Max retries reached — sending to DLQ")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_consumer(app):
    """
    Start consuming inventory events.
    Listens for:
      - inventory.reserved    → send confirmation notification
      - inventory.out_of_stock → send rejection notification
    Both queues backed by a dead-letter queue for failed messages.
    """
    with app.app_context():
        connection = get_connection()
        channel = connection.channel()

        # Declare the inventory exchange (must match Inventory Service)
        channel.exchange_declare(exchange='inventory', exchange_type='topic', durable=True)

        # Dead-letter exchange & queue
        channel.exchange_declare(exchange='notification.dlx', exchange_type='direct', durable=True)
        channel.queue_declare(queue='notification_service.dlq', durable=True)
        channel.queue_bind(
            exchange='notification.dlx',
            queue='notification_service.dlq',
            routing_key='notification_service.dlq'
        )

        dlq_args = {
            'x-dead-letter-exchange': 'notification.dlx',
            'x-dead-letter-routing-key': 'notification_service.dlq'
        }

        # Queue for InventoryReserved events
        channel.queue_declare(
            queue='notification_service.inventory_reserved',
            durable=True,
            arguments=dlq_args
        )
        channel.queue_bind(
            exchange='inventory',
            queue='notification_service.inventory_reserved',
            routing_key='inventory.reserved'
        )

        # Queue for OutOfStock events
        channel.queue_declare(
            queue='notification_service.out_of_stock',
            durable=True,
            arguments=dlq_args
        )
        channel.queue_bind(
            exchange='inventory',
            queue='notification_service.out_of_stock',
            routing_key='inventory.out_of_stock'
        )

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue='notification_service.inventory_reserved',
            on_message_callback=handle_inventory_reserved
        )
        channel.basic_consume(
            queue='notification_service.out_of_stock',
            on_message_callback=handle_out_of_stock
        )

        logger.info("[Notification Service] Listening for InventoryReserved and OutOfStock events...")
        channel.start_consuming()
