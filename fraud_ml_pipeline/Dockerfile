# Fraud Detection ML Pipeline
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Generate data and train model if not present
RUN python data/generate_dataset.py || true

# Expose API port
EXPOSE 8000

# Run the API
CMD ["uvicorn", "api.scoring_service:app", "--host", "0.0.0.0", "--port", "8000"]
