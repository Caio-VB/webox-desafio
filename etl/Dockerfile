FROM python:3.12-slim

WORKDIR /app

# Dependências básicas para compilar/rodar psycopg2 e pandas direito
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    python-dotenv \
    openpyxl

COPY main.py .

CMD ["python", "main.py"]
