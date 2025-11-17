import os
from pathlib import Path

# URL do banco
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://webox:weboxpass@localhost:5432/weboxdb",
)

# Nome da tabela principal de faturamento
TABLE_NAME = os.getenv("FATURAMENTO_TABLE", "faturamento")

# Diretório de entrada dos arquivos
INBOX_DIR = Path(os.getenv("INBOX_DIR", "/data/inbox"))

# Cliente padrão (fallback) caso não consiga extrair do nome do arquivo
CLIENTE_ID_DEFAULT = os.getenv("CLIENTE_ID", "cliente_demo")

# Intervalo entre varreduras (segundos)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))

# Colunas reservadas da tabela (metadados internos)
RESERVED_COLS = {"id", "cliente_id", "arquivo_nome", "linha_numero", "created_at"}
