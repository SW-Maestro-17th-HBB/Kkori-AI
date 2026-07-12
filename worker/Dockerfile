#   docker build -f worker/Dockerfile -t kkori-worker .
FROM python:3.12-slim

WORKDIR /app

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY worker/src/ src/

CMD ["python", "-m", "src.main"]
