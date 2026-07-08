import pytest
from unittest.mock import patch, MagicMock
import sys
import os

# Ensure the order-service root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app import create_app
from models import db, Order


@pytest.fixture
def app():
    """Create a test Flask app with an in-memory SQLite database."""
    test_app = create_app()
    test_app.config['TESTING'] = True
    test_app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    with test_app.app_context():
        db.create_all()
        yield test_app
        db.drop_all()


@pytest.fixture
def client(app):
    return app.test_client()


# ── Validation Tests ──────────────────────────────────────────────────────────

class TestOrderValidation:

    def test_missing_product_id(self, client):
        res = client.post('/orders', json={
            'quantity': 2,
            'customer_email': 'test@example.com'
        })
        assert res.status_code == 400
        assert 'product_id' in res.get_json()['error']

    def test_missing_quantity(self, client):
        res = client.post('/orders', json={
            'product_id': 'PROD-001',
            'customer_email': 'test@example.com'
        })
        assert res.status_code == 400
        assert 'quantity' in res.get_json()['error']

    def test_missing_email(self, client):
        res = client.post('/orders', json={
            'product_id': 'PROD-001',
            'quantity': 2
        })
        assert res.status_code == 400
        assert 'customer_email' in res.get_json()['error']

    def test_zero_quantity_rejected(self, client):
        res = client.post('/orders', json={
            'product_id': 'PROD-001',
            'quantity': 0,
            'customer_email': 'test@example.com'
        })
        assert res.status_code == 400
        assert 'quantity' in res.get_json()['error']

    def test_negative_quantity_rejected(self, client):
        res = client.post('/orders', json={
            'product_id': 'PROD-001',
            'quantity': -5,
            'customer_email': 'test@example.com'
        })
        assert res.status_code == 400

    def test_invalid_email_rejected(self, client):
        res = client.post('/orders', json={
            'product_id': 'PROD-001',
            'quantity': 1,
            'customer_email': 'not-an-email'
        })
        assert res.status_code == 400
        assert 'email' in res.get_json()['error']

    def test_empty_body_rejected(self, client):
        res = client.post('/orders', content_type='application/json', data='')
        assert res.status_code == 400


# ── DB Write Tests ────────────────────────────────────────────────────────────

class TestOrderCreation:

    @patch('app.publish_order_created')
    def test_valid_order_returns_201(self, mock_publish, client):
        res = client.post('/orders', json={
            'product_id': 'PROD-001',
            'quantity': 3,
            'customer_email': 'user@example.com'
        })
        assert res.status_code == 201
        data = res.get_json()
        assert data['product_id'] == 'PROD-001'
        assert data['quantity'] == 3
        assert data['status'] == 'pending'
        assert 'id' in data

    @patch('app.publish_order_created')
    def test_order_written_to_db(self, mock_publish, client, app):
        client.post('/orders', json={
            'product_id': 'PROD-002',
            'quantity': 1,
            'customer_email': 'buyer@example.com'
        })
        with app.app_context():
            order = Order.query.first()
            assert order is not None
            assert order.status == 'pending'
            assert order.product_id == 'PROD-002'

    @patch('app.publish_order_created')
    def test_event_published_on_order_creation(self, mock_publish, client):
        client.post('/orders', json={
            'product_id': 'PROD-003',
            'quantity': 2,
            'customer_email': 'someone@example.com'
        })
        mock_publish.assert_called_once()
        call_args = mock_publish.call_args[0][0]
        assert call_args['product_id'] == 'PROD-003'
        assert call_args['quantity'] == 2

    @patch('app.publish_order_created', side_effect=Exception("RabbitMQ unavailable"))
    def test_order_saved_even_if_publish_fails(self, mock_publish, client, app):
        """Order must persist even if RabbitMQ is down."""
        res = client.post('/orders', json={
            'product_id': 'PROD-004',
            'quantity': 1,
            'customer_email': 'resilient@example.com'
        })
        assert res.status_code == 201
        with app.app_context():
            assert Order.query.count() == 1


# ── GET Order Tests ───────────────────────────────────────────────────────────

class TestGetOrder:

    @patch('app.publish_order_created')
    def test_get_existing_order(self, mock_publish, client):
        post_res = client.post('/orders', json={
            'product_id': 'PROD-005',
            'quantity': 1,
            'customer_email': 'get@example.com'
        })
        order_id = post_res.get_json()['id']

        get_res = client.get(f'/orders/{order_id}')
        assert get_res.status_code == 200
        assert get_res.get_json()['id'] == order_id

    def test_get_nonexistent_order_returns_404(self, client):
        res = client.get('/orders/99999')
        assert res.status_code == 404


# ── Saga Compensation Tests ───────────────────────────────────────────────────

class TestSagaCompensation:

    @patch('app.publish_order_created')
    def test_out_of_stock_cancels_order(self, mock_publish, client, app):
        """Simulate the Saga: order is created then cancelled via OutOfStock event."""
        from consumer import handle_out_of_stock
        import json

        # Create an order
        res = client.post('/orders', json={
            'product_id': 'PROD-OOS',
            'quantity': 100,
            'customer_email': 'saga@example.com'
        })
        order_id = res.get_json()['id']

        # Simulate OutOfStock message arriving from Inventory Service
        with app.app_context():
            mock_ch = MagicMock()
            mock_method = MagicMock()
            body = json.dumps({'event': 'OutOfStock', 'order_id': order_id}).encode()
            handle_out_of_stock(mock_ch, mock_method, None, body)

            order = Order.query.get(order_id)
            assert order.status == 'cancelled'
            mock_ch.basic_ack.assert_called_once()
