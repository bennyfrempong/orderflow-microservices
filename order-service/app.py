import os
import threading
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from models import db, Order
from publisher import publish_order_created

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def create_app():
    app = Flask(__name__)

    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv(
        'DATABASE_URL', 'sqlite:///orders.db'
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)
    CORS(app)

    with app.app_context():
        db_uri = app.config['SQLALCHEMY_DATABASE_URI']
        if 'sqlite' in db_uri:
            db_path = db_uri.replace('sqlite:///', '')
            os.makedirs(os.path.dirname(db_path), exist_ok=True) if os.path.dirname(db_path) else None
        db.create_all()
        logger.info("Database tables created")

    # ── Routes ────────────────────────────────────────────────────────────────

    @app.route('/health', methods=['GET'])
    def health():
        return jsonify({'status': 'ok', 'service': 'order-service'}), 200

    @app.route('/orders', methods=['POST'])
    def create_order():
        data = request.get_json(silent=True)

        if not data:
            return jsonify({'error': 'Request body must be valid JSON'}), 400

        required_fields = ['product_id', 'quantity', 'customer_email']
        missing = [f for f in required_fields if f not in data or data[f] is None]
        if missing:
            return jsonify({'error': f'Missing required fields: {", ".join(missing)}'}), 400

        if not isinstance(data['quantity'], int) or data['quantity'] <= 0:
            return jsonify({'error': 'quantity must be a positive integer'}), 400

        if '@' not in str(data['customer_email']):
            return jsonify({'error': 'customer_email must be a valid email address'}), 400

        if not str(data['product_id']).strip():
            return jsonify({'error': 'product_id cannot be empty'}), 400

        order = Order(
            product_id=str(data['product_id']).strip(),
            quantity=data['quantity'],
            customer_email=str(data['customer_email']).strip(),
            status='pending'
        )
        db.session.add(order)
        db.session.commit()
        logger.info(f"Order {order.id} created with status 'pending'")

        try:
            publish_order_created(order.to_dict())
        except Exception as e:
            logger.error(f"Failed to publish OrderCreated for order {order.id}: {e}")

        return jsonify(order.to_dict()), 201

    @app.route('/orders/<int:order_id>', methods=['GET'])
    def get_order(order_id):
        order = Order.query.get(order_id)
        if not order:
            return jsonify({'error': f'Order {order_id} not found'}), 404
        return jsonify(order.to_dict()), 200

    @app.route('/orders', methods=['GET'])
    def list_orders():
        orders = Order.query.order_by(Order.created_at.desc()).all()
        return jsonify([o.to_dict() for o in orders]), 200

    return app


# ── Background Saga Consumer ──────────────────────────────────────────────────

def start_saga_consumer():
    import time
    time.sleep(8)
    try:
        from consumer import start_consumer
        logger.info("Starting Saga consumer thread...")
        start_consumer(app)
    except Exception as e:
        logger.error(f"Saga consumer crashed: {e}")


# ── Entry Point ───────────────────────────────────────────────────────────────

app = create_app()

if __name__ == '__main__':
    consumer_thread = threading.Thread(target=start_saga_consumer, daemon=True)
    consumer_thread.start()

    app.run(host='0.0.0.0', port=5001, debug=False)
