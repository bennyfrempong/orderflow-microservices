from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()


class Order(db.Model):
    __tablename__ = 'orders'

    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.String(50), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    customer_email = db.Column(db.String(120), nullable=False)
    status = db.Column(db.String(20), nullable=False, default='pending')
    # pending -> confirmed (InventoryReserved)
    # pending -> cancelled (OutOfStock — Saga compensation)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'product_id': self.product_id,
            'quantity': self.quantity,
            'customer_email': self.customer_email,
            'status': self.status,
            'created_at': self.created_at.isoformat()
        }
