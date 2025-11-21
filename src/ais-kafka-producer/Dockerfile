FROM python:3.12-slim

WORKDIR /app

COPY producer.py .
COPY requirements.txt .
COPY .env .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "producer.py"]
