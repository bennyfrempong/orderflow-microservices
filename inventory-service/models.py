from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Product(db.Model):
    __tablename__ = 'products'

    id = db.Column(db.String(50), primary_key=True)   # e.g. "PROD-001"
    name = db.Column(db.String(100), nullable=False)
    stock_quantity = db.Column(db.Integer, nullable=False, default=0)

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'stock_quantity': self.stock_quantity
        }


def seed_products(db):
    """Populate the inventory DB with sample products on first run."""
    if Product.query.count() == 0:
        products = [
            Product(id='PROD-001', name='Wireless Headphones',  stock_quantity=50),
            Product(id='PROD-002', name='Mechanical Keyboard',  stock_quantity=25),
            Product(id='PROD-003', name='USB-C Hub',            stock_quantity=10),
            Product(id='PROD-004', name='Laptop Stand',         stock_quantity=5),
            Product(id='PROD-005', name='Out of Stock Item',    stock_quantity=0),  # Demo failure path
        ]
        for p in products:
            db.session.add(p)
        db.session.commit()
