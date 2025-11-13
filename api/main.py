import os

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

AGENT_URL = os.getenv("AGENT_URL", "http://agent:9000/run-agent")

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
    API fina: só repassa a pergunta para o agent-service.
    O agent-service é quem decide SQL, consulta o banco e monta a resposta.
    """
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(
                AGENT_URL,
                json=req.model_dump(),
            )
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Erro ao chamar agent-service: {e}",
        )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Agent-service retornou erro: {resp.text}",
        )

    data = resp.json()
    # Garantir campos esperados
    return AskResponse(
        answer=data.get("answer", ""),
        debug_sql=data.get("debug_sql"),
    )
