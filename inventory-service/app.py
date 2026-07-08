import os
import threading
import logging
from flask import Flask, jsonify
from flask_cors import CORS
from models import db, Product, seed_products

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def create_app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv(
        'DATABASE_URL', 'sqlite:///inventory.db'
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)
    CORS(app)

    with app.app_context():
        db_uri = app.config['SQLALCHEMY_DATABASE_URI']
        if 'sqlite' in db_uri:
            db_path = db_uri.replace('sqlite:///', '')
            if os.path.dirname(db_path):
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
        db.create_all()
        seed_products(db)
        logger.info("Inventory database initialized and seeded")

    # ── Routes ────────────────────────────────────────────────────────────────

    @app.route('/health', methods=['GET'])
    def health():
        return jsonify({'status': 'ok', 'service': 'inventory-service'}), 200

    @app.route('/products', methods=['GET'])
    def list_products():
        products = Product.query.all()
        return jsonify([p.to_dict() for p in products]), 200

    @app.route('/products/<product_id>', methods=['GET'])
    def get_product(product_id):
        product = Product.query.get(product_id)
        if not product:
            return jsonify({'error': f'Product {product_id} not found'}), 404
        return jsonify(product.to_dict()), 200

    return app


# ── Background Consumer ───────────────────────────────────────────────────────

def start_inventory_consumer():
    import time
    time.sleep(8)
    try:
        from consumer import start_consumer
        logger.info("Starting Inventory consumer thread...")
        start_consumer(app)
    except Exception as e:
        logger.error(f"Inventory consumer crashed: {e}")


# ── Entry Point ───────────────────────────────────────────────────────────────

app = create_app()

if __name__ == '__main__':
    consumer_thread = threading.Thread(target=start_inventory_consumer, daemon=True)
    consumer_thread.start()

    app.run(host='0.0.0.0', port=5002, debug=False)
