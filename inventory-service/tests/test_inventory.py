import pytest
from unittest.mock import patch, MagicMock, call
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app import create_app
from models import db, Product, seed_products


@pytest.fixture
def app():
    test_app = create_app()
    test_app.config['TESTING'] = True
    test_app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    with test_app.app_context():
        db.create_all()
        seed_products(db)
        yield test_app
        db.drop_all()


@pytest.fixture
def client(app):
    return app.test_client()


# ── Seed Data Tests ───────────────────────────────────────────────────────────

class TestSeedData:

    def test_products_seeded(self, app):
        with app.app_context():
            assert Product.query.count() == 5

    def test_out_of_stock_product_exists(self, app):
        with app.app_context():
            oos = Product.query.get('PROD-005')
            assert oos is not None
            assert oos.stock_quantity == 0

    def test_seed_does_not_duplicate(self, app):
        with app.app_context():
            seed_products(db)  # Call again — should be a no-op
            assert Product.query.count() == 5


# ── Stock Check Logic Tests ───────────────────────────────────────────────────

class TestStockLogic:

    @patch('consumer.publish_inventory_reserved')
    @patch('consumer.publish_out_of_stock')
    def test_sufficient_stock_publishes_reserved(self, mock_oos, mock_reserved, app):
        from consumer import handle_order_created

        with app.app_context():
            mock_ch = MagicMock()
            mock_method = MagicMock()
            body = json.dumps({
                'order_id': 1,
                'product_id': 'PROD-001',
                'quantity': 5,
                'customer_email': 'test@example.com'
            }).encode()

            handle_order_created(mock_ch, mock_method, None, body)

            mock_reserved.assert_called_once_with(1, 'PROD-001', 5, 'test@example.com')
            mock_oos.assert_not_called()
            mock_ch.basic_ack.assert_called_once()

    @patch('consumer.publish_inventory_reserved')
    @patch('consumer.publish_out_of_stock')
    def test_stock_decremented_on_reservation(self, mock_oos, mock_reserved, app):
        from consumer import handle_order_created

        with app.app_context():
            product_before = Product.query.get('PROD-001')
            initial_stock = product_before.stock_quantity

            mock_ch = MagicMock()
            mock_method = MagicMock()
            body = json.dumps({
                'order_id': 2,
                'product_id': 'PROD-001',
                'quantity': 10,
                'customer_email': 'buyer@example.com'
            }).encode()

            handle_order_created(mock_ch, mock_method, None, body)

            product_after = Product.query.get('PROD-001')
            assert product_after.stock_quantity == initial_stock - 10

    @patch('consumer.publish_inventory_reserved')
    @patch('consumer.publish_out_of_stock')
    def test_zero_stock_publishes_out_of_stock(self, mock_oos, mock_reserved, app):
        from consumer import handle_order_created

        with app.app_context():
            mock_ch = MagicMock()
            mock_method = MagicMock()
            body = json.dumps({
                'order_id': 3,
                'product_id': 'PROD-005',   # 0 stock
                'quantity': 1,
                'customer_email': 'sad@example.com'
            }).encode()

            handle_order_created(mock_ch, mock_method, None, body)

            mock_oos.assert_called_once_with(3, 'PROD-005', 1, 'sad@example.com')
            mock_reserved.assert_not_called()

    @patch('consumer.publish_inventory_reserved')
    @patch('consumer.publish_out_of_stock')
    def test_exceeds_stock_publishes_out_of_stock(self, mock_oos, mock_reserved, app):
        from consumer import handle_order_created

        with app.app_context():
            mock_ch = MagicMock()
            mock_method = MagicMock()
            body = json.dumps({
                'order_id': 4,
                'product_id': 'PROD-004',  # only 5 in stock
                'quantity': 999,
                'customer_email': 'greedy@example.com'
            }).encode()

            handle_order_created(mock_ch, mock_method, None, body)

            mock_oos.assert_called_once()
            mock_reserved.assert_not_called()

    @patch('consumer.publish_inventory_reserved')
    @patch('consumer.publish_out_of_stock')
    def test_unknown_product_publishes_out_of_stock(self, mock_oos, mock_reserved, app):
        from consumer import handle_order_created

        with app.app_context():
            mock_ch = MagicMock()
            mock_method = MagicMock()
            body = json.dumps({
                'order_id': 5,
                'product_id': 'PROD-GHOST',
                'quantity': 1,
                'customer_email': 'ghost@example.com'
            }).encode()

            handle_order_created(mock_ch, mock_method, None, body)

            mock_oos.assert_called_once()
            mock_reserved.assert_not_called()

    @patch('consumer.publish_inventory_reserved')
    @patch('consumer.publish_out_of_stock')
    def test_stock_not_decremented_on_oos(self, mock_oos, mock_reserved, app):
        """Stock must remain unchanged when out-of-stock path is taken."""
        from consumer import handle_order_created

        with app.app_context():
            product_before = Product.query.get('PROD-005')
            stock_before = product_before.stock_quantity  # 0

            mock_ch = MagicMock()
            mock_method = MagicMock()
            body = json.dumps({
                'order_id': 6,
                'product_id': 'PROD-005',
                'quantity': 1,
                'customer_email': 'test@example.com'
            }).encode()

            handle_order_created(mock_ch, mock_method, None, body)

            product_after = Product.query.get('PROD-005')
            assert product_after.stock_quantity == stock_before  # unchanged


# ── API Endpoint Tests ────────────────────────────────────────────────────────

class TestInventoryAPI:

    def test_health_check(self, client):
        res = client.get('/health')
        assert res.status_code == 200
        assert res.get_json()['status'] == 'ok'

    def test_list_products_returns_all(self, client):
        res = client.get('/products')
        assert res.status_code == 200
        assert len(res.get_json()) == 5

    def test_get_existing_product(self, client):
        res = client.get('/products/PROD-001')
        assert res.status_code == 200
        data = res.get_json()
        assert data['id'] == 'PROD-001'
        assert data['stock_quantity'] == 50

    def test_get_nonexistent_product_returns_404(self, client):
        res = client.get('/products/PROD-NONE')
        assert res.status_code == 404
