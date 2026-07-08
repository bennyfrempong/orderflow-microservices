FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (better Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory for SQLite volume mount
RUN mkdir -p /app/data

EXPOSE 5001

CMD ["python", "app.py"]
