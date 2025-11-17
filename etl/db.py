from typing import List, Dict

from sqlalchemy import create_engine, text

from config import DATABASE_URL, TABLE_NAME

# Engine global
engine = create_engine(DATABASE_URL)


def get_processados() -> set[str]:
    """
    Busca no banco os arquivos já processados com sucesso (qualquer cliente).
    Assim evitamos processar o mesmo arquivo várias vezes.
    """
    sql = text(
        """
        SELECT DISTINCT arquivo_nome
        FROM etl_jobs
        WHERE status = 'success'
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql)
        nomes = {row[0] for row in result.fetchall()}
    return nomes


def get_existing_columns(table_name: str = TABLE_NAME) -> set[str]:
    """
    Busca as colunas já existentes na tabela de faturamento.
    """
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :table
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql, {"table": table_name})
        return {row[0] for row in result.fetchall()}
