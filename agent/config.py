import os

# Configuração de banco
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurada no ambiente.")

TABLE_NAME = os.getenv("FATURAMENTO_TABLE", "faturamento")

# Config de LLM
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
USE_LLM = bool(OPENAI_API_KEY)

# Limites
MAX_ROWS = 10000
MAX_COLS = 30
MAX_ROWS_FOR_LLM = MAX_ROWS
