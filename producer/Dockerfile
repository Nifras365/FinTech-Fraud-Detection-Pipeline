FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY producer /app/producer

WORKDIR /app/producer

CMD ["python", "main.py"]
