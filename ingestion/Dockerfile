# Base image with Python
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Kafka (librdkafka)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# You'll need: requests, confluent-kafka, python-dotenv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY producer.py .

# Run the producer
CMD ["python", "producer.py"]