FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ shared/
COPY ledger_node/ ledger_node/
EXPOSE 8000 50051
CMD ["python", "-m", "shared.serve", "ledger_node.main:app"]
