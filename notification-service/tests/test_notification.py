import pytest
import json
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app import create_app
from consumer import handle_inventory_reserved, handle_out_of_stock, get_retry_count


@pytest.fixture
def app():
    test_app = create_app()
    test_app.config['TESTING'] = True
    return test_app


@pytest.fixture
def client(app):
    return app.test_client()


# ── Helper ────────────────────────────────────────────────────────────────────

def make_message(payload, retry_count=0):
    """Build mock RabbitMQ channel, method, and properties for a test message."""
    mock_ch = MagicMock()
    mock_method = MagicMock()
    mock_properties = MagicMock()

    if retry_count > 0:
        mock_properties.headers = {
            'x-death': [{'count': retry_count}]
        }
    else:
        mock_properties.headers = {}

    body = json.dumps(payload).encode()
    return mock_ch, mock_method, mock_properties, body


# ── InventoryReserved Tests ───────────────────────────────────────────────────

class TestInventoryReservedHandler:

    def test_confirmation_acks_message(self, app):
        mock_ch, mock_method, mock_props, body = make_message({
            'order_id': 1,
            'product_id': 'PROD-001',
            'quantity': 2,
            'customer_email': 'confirmed@example.com'
        })
        with app.app_context():
            handle_inventory_reserved(mock_ch, mock_method, mock_props, body)
        mock_ch.basic_ack.assert_called_once()
        mock_ch.basic_nack.assert_not_called()

    def test_confirmation_logs_email(self, app, caplog):
        import logging
        mock_ch, mock_method, mock_props, body = make_message({
            'order_id': 2,
            'product_id': 'PROD-002',
            'quantity': 1,
            'customer_email': 'buyer@example.com'
        })
        with app.app_context():
            with caplog.at_level(logging.INFO):
                handle_inventory_reserved(mock_ch, mock_method, mock_props, body)

        log_text = caplog.text
        assert 'buyer@example.com' in log_text
        assert 'CONFIRMED' in log_text


# ── OutOfStock Tests ──────────────────────────────────────────────────────────

class TestOutOfStockHandler:

    def test_rejection_acks_message(self, app):
        mock_ch, mock_method, mock_props, body = make_message({
            'order_id': 3,
            'product_id': 'PROD-005',
            'quantity_requested': 1,
            'customer_email': 'rejected@example.com'
        })
        with app.app_context():
            handle_out_of_stock(mock_ch, mock_method, mock_props, body)
        mock_ch.basic_ack.assert_called_once()
        mock_ch.basic_nack.assert_not_called()

    def test_rejection_logs_email(self, app, caplog):
        import logging
        mock_ch, mock_method, mock_props, body = make_message({
            'order_id': 4,
            'product_id': 'PROD-005',
            'quantity_requested': 10,
            'customer_email': 'sad@example.com'
        })
        with app.app_context():
            with caplog.at_level(logging.INFO):
                handle_out_of_stock(mock_ch, mock_method, mock_props, body)

        log_text = caplog.text
        assert 'sad@example.com' in log_text
        assert 'REJECTED' in log_text


# ── Retry Logic Tests ─────────────────────────────────────────────────────────

class TestRetryLogic:

    def test_retry_count_zero_when_no_headers(self):
        mock_props = MagicMock()
        mock_props.headers = {}
        assert get_retry_count(mock_props) == 0

    def test_retry_count_reads_x_death_header(self):
        mock_props = MagicMock()
        mock_props.headers = {'x-death': [{'count': 2}]}
        assert get_retry_count(mock_props) == 2

    def test_retry_count_none_properties(self):
        assert get_retry_count(None) == 0

    def test_nacks_with_requeue_on_error_below_max(self, app):
        """On processing error before max retries — requeue the message."""
        mock_ch, mock_method, mock_props, _ = make_message(
            {'bad': 'payload'},  # Missing required fields → triggers exception
            retry_count=1
        )
        with app.app_context():
            handle_inventory_reserved(mock_ch, mock_method, mock_props, b'not valid json{{{')
        mock_ch.basic_nack.assert_called_once_with(
            delivery_tag=mock_method.delivery_tag, requeue=True
        )

    def test_nacks_without_requeue_at_max_retries(self, app):
        """After max retries — send to DLQ (requeue=False)."""
        mock_ch, mock_method, mock_props, _ = make_message(
            {},
            retry_count=3  # At MAX_RETRIES
        )
        with app.app_context():
            handle_inventory_reserved(mock_ch, mock_method, mock_props, b'not valid json{{{')
        mock_ch.basic_nack.assert_called_once_with(
            delivery_tag=mock_method.delivery_tag, requeue=False
        )


# ── Health Endpoint ───────────────────────────────────────────────────────────

class TestHealthEndpoint:

    def test_health_returns_ok(self, client):
        res = client.get('/health')
        assert res.status_code == 200
        assert res.get_json()['status'] == 'ok'
        assert res.get_json()['service'] == 'notification-service'
