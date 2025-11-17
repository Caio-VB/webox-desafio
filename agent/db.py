from typing import List, Dict

from sqlalchemy import create_engine, text

from config import DATABASE_URL, TABLE_NAME
from models import Row

# Engine global
engine = create_engine(DATABASE_URL)


def get_table_schema(table_name: str = TABLE_NAME) -> List[Dict[str, str]]:
    sql = text(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = :table
        ORDER BY ordinal_position
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql, {"table": table_name})
        return [{"name": row[0], "type": row[1]} for row in result.fetchall()]


def db_mcp_tool(sql: str) -> List[Row]:
    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.mappings().all()
    return [dict(r) for r in rows]
