import os

from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# No container, o host do banco é "db"
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://webox:weboxpass@db:5432/weboxdb",
)

engine = create_engine(DATABASE_URL)

app = FastAPI(title="WeBox Faturamento API")


class AskRequest(BaseModel):
    question: str
    cliente_id: str = "cliente_demo"


class AskResponse(BaseModel):
    answer: str
    debug_sql: str | None = None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):
    """
    Implementação TEMPORÁRIA:
    - Se a pergunta falar de 'faturamento' ou 'faturei',
      fazemos uma soma simples no banco.
    - Depois vamos trocar isso para chamar o agent-service (LLM).
    """
    q_lower = req.question.lower()
    sql = None
    answer = "API está funcionando, mas o agente de IA ainda não foi conectado."

    if "faturei" in q_lower or "faturamento" in q_lower:
        sql = """
        SELECT COALESCE(SUM(valor_total), 0) AS total
        FROM faturamento
        WHERE cliente_id = :cliente_id
        """
        with engine.begin() as conn:
            result = conn.execute(text(sql), {"cliente_id": req.cliente_id})
            total = result.scalar() or 0

        answer = (
            f"O faturamento total do cliente {req.cliente_id} "
            f"é de R$ {float(total):.2f}."
        )

    return AskResponse(answer=answer, debug_sql=sql)
