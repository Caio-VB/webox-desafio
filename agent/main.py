from fastapi import FastAPI, HTTPException

from config import USE_LLM
from models import AskRequest, AgentResponse
from llm import llm_generate_queries, llm_generate_answer
from utils import run_queries

app = FastAPI(title="WeBox Agent Service")

@app.post("/run-agent", response_model=AgentResponse)
def run_agent(req: AskRequest):
    if not USE_LLM:
        msg = (
            "Desculpe, o módulo de IA não está disponível no momento, "
            "então não consigo gerar uma resposta baseada nos seus dados. "
            "Tente novamente mais tarde ou habilite a OPENAI_API_KEY."
        )
        return AgentResponse(answer=msg, debug_sql=None)

    try:
        queries = llm_generate_queries(req.question)
        results = run_queries(queries)
        answer = llm_generate_answer(req.question, results)

        debug_sql_blocks = [f"-- {q['title']}\n{q['sql']}" for q in queries]
        debug_sql = "\n\n".join(debug_sql_blocks) if debug_sql_blocks else None

        return AgentResponse(answer=answer, debug_sql=debug_sql)

    except HTTPException:
        raise
    except Exception as e:
        return AgentResponse(
            answer=f"Não foi possível concluir a análise desta pergunta: {e}",
            debug_sql=None,
        )
