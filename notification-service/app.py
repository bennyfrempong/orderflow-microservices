import os
import threading
import logging
from flask import Flask, jsonify

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def create_app():
    app = Flask(__name__)

    # ── Routes ────────────────────────────────────────────────────────────────

    @app.route('/health', methods=['GET'])
    def health():
        return jsonify({'status': 'ok', 'service': 'notification-service'}), 200

    return app


# ── Background Consumer ───────────────────────────────────────────────────────

def start_notification_consumer():
    import time
    time.sleep(8)
    try:
        from consumer import start_consumer
        logger.info("Starting Notification consumer thread...")
        start_consumer(app)
    except Exception as e:
        logger.error(f"Notification consumer crashed: {e}")


# ── Entry Point ───────────────────────────────────────────────────────────────

app = create_app()

if __name__ == '__main__':
    consumer_thread = threading.Thread(target=start_notification_consumer, daemon=True)
    consumer_thread.start()

    app.run(host='0.0.0.0', port=5003, debug=False)
