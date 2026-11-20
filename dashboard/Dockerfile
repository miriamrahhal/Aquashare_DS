FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# shared/ has the signing code; scripts/ and ledger_node/ are used by the "Check the rules" scenario
COPY shared/ shared/
COPY ledger_node/ ledger_node/
COPY scripts/ scripts/
COPY dashboard/ dashboard/
EXPOSE 8000
CMD ["uvicorn", "dashboard.main:app", "--host", "0.0.0.0", "--port", "8000"]
