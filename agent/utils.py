from typing import Dict, Any, List

from config import MAX_ROWS, MAX_COLS
from db import db_mcp_tool


def is_safe_sql(sql: str) -> bool:
    return sql.strip().lower().startswith("select")


def enforce_sql_limits(sql: str) -> str:
    sql_clean = sql.strip().rstrip(";")
    if "limit" in sql_clean.lower():
        return sql_clean + ";"
    return sql_clean + f" LIMIT {MAX_ROWS};"


def run_queries(queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    for q in queries:
        sql = enforce_sql_limits(q["sql"])
        if not is_safe_sql(sql):
            results.append({
                **q,
                "rows": [],
                "error": "SQL insegura (não começa com SELECT)."
            })
            continue

        try:
            rows = db_mcp_tool(sql)
            if rows and len(rows[0]) > MAX_COLS:
                results.append({
                    **q,
                    "rows": [],
                    "error": f"SQL retornou muitas colunas ({len(rows[0])})."
                })
                continue

            results.append({
                **q,
                "rows": rows,
                "error": None
            })
        except Exception as e:
            results.append({
                **q,
                "rows": [],
                "error": f"Erro ao executar SQL: {e}"
            })

    return results
