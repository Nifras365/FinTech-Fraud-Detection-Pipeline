FROM bitnami/spark:3.5.1

USER root

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY spark /app/spark

WORKDIR /app/spark

CMD ["python", "fraud_detection.py"]
