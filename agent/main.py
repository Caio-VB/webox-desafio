import os
from typing import Tuple, Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# Conexão com o Postgres (dentro da rede Docker, host = "db")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://webox:weboxpass@db:5432/weboxdb",
)

engine = create_engine(DATABASE_URL)

app = FastAPI(title="WeBox Agent Service")


class AskRequest(BaseModel):
    question: str
    cliente_id: str = "cliente_demo"


class AgentResponse(BaseModel):
    answer: str
    debug_sql: str | None = None


def decide_sql(question: str, cliente_id: str) -> Tuple[str, Dict[str, Any]]:
    """
    Aqui é o 'miolo' do agente.
    Por enquanto, fazemos uma lógica simples baseada em palavras-chave.
    Em produção, este pedaço seria substituído por um LLM (OpenAI, etc.)
    que gera a SQL dinamicamente.

    Exemplos suportados:
    - "Quanto eu faturei"
    - "Qual meu faturamento total"
    """
    q = question.lower()

    # Caso 1: faturamento total por cliente
    if "faturei" in q or "faturamento" in q:
        sql = """
        SELECT COALESCE(SUM(valor_total), 0) AS total
        FROM faturamento
        WHERE cliente_id = :cliente_id
        """
        params = {"cliente_id": cliente_id}
        return sql, params

    # Caso padrão: apenas contar linhas
    sql = """
    SELECT COUNT(*) AS qtd
    FROM faturamento
    WHERE cliente_id = :cliente_id
    """
    params = {"cliente_id": cliente_id}
    return sql, params


def build_answer(question: str, row: Dict[str, Any], sql: str, cliente_id: str) -> str:
    """
    Monta uma resposta em linguagem natural com base no resultado da query.
    """
    if "total" in row:
        total = float(row["total"] or 0)
        return (
            f"Para a pergunta '{question}', o faturamento total do cliente {cliente_id} "
            f"é de R$ {total:.2f}."
        )

    if "qtd" in row:
        qtd = int(row["qtd"] or 0)
        return (
            f"Para a pergunta '{question}', encontrei {qtd} registros de faturamento "
            f"para o cliente {cliente_id}."
        )

    return f"Não consegui interpretar bem a pergunta: '{question}'. SQL usada: {sql}"


@app.post("/run-agent", response_model=AgentResponse)
def run_agent(req: AskRequest):
    """
    Agent-service:
    - recebe question + cliente_id
    - decide SQL
    - consulta Postgres
    - devolve resposta em linguagem natural + SQL usada
    """
    sql, params = decide_sql(req.question, req.cliente_id)

    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql), params)
            row = result.mappings().first() or {}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao executar SQL: {e}",
        )

    answer = build_answer(req.question, dict(row), sql, req.cliente_id)
    return AgentResponse(answer=answer, debug_sql=sql)
