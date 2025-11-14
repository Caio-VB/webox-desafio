import os
import json
from typing import Tuple, Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# --- Configuração de banco (tool MCP de banco, na prática) ---

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://webox:weboxpass@db:5432/weboxdb",
)

engine = create_engine(DATABASE_URL)

# --- Opcional: LLM (OpenAI) para geração de SQL ---

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
USE_LLM = bool(OPENAI_API_KEY)

if USE_LLM:
    from openai import OpenAI

    client = OpenAI(api_key=OPENAI_API_KEY)

app = FastAPI(title="WeBox Agent Service")


class AskRequest(BaseModel):
    question: str
    cliente_id: str = "cliente_demo"


class AgentResponse(BaseModel):
    answer: str
    debug_sql: str | None = None


# ----------------------------------------------------------------
# Camada de decisão de SQL
# ----------------------------------------------------------------


def decide_sql(question: str, cliente_id: str) -> Tuple[str, Dict[str, Any]]:
    """
    Usa um LLM (OpenAI) para gerar SQL a partir da pergunta.
    Aqui estamos simulando o comportamento de um agente A2A/MCP
    que decide qual query disparar.
    """
    system_prompt = """
Você é um assistente que gera consultas SQL para um banco PostgreSQL.

Esquema relevante:

Tabela faturamento:
- id (serial, PK)
- cliente_id (text)
- arquivo_nome (text)
- linha_numero (integer)
- data_emissao (date)
- data_vencimento (date)
- valor_total (numeric)
- status (text)
- raw (jsonb)
- created_at (timestamp)

Regras:
- Sempre filtre por cliente_id usando parâmetro nomeado :cliente_id.
- Nunca use DELETE ou UPDATE, apenas SELECT.
- Responda apenas com a SQL, sem explicação, sem markdown.
    """.strip()

    user_prompt = f"Pergunta do usuário: {question}\nGere apenas a SQL correspondente."

    # Deixa o erro "subir" e ser tratado pelo run_agent
    chat = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )
    sql = chat.choices[0].message.content.strip()

    # --- Sanitização do retorno do LLM ---

    # 1) remover cercas de markdown ```...``` se existirem
    if sql.startswith("```"):
        lines = sql.splitlines()

        # remove primeira linha (``` ou ```sql)
        if lines:
            lines = lines[1:]

        # remove última linha se for ```
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]

        sql = "\n".join(lines).strip()

    # 2) normalizar estilo de parâmetro para :cliente_id
    sql = sql.replace("%(cliente_id)s", ":cliente_id")

    params = {"cliente_id": cliente_id}
    return sql, params


# ----------------------------------------------------------------
# Camada "tool" de banco (pensar como uma MCP tool)
# ----------------------------------------------------------------

def db_mcp_tool(sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Esta função encapsula a chamada ao banco e representa,
    conceitualmente, uma tool de banco de dados no MCP/A2A.

    Em um cenário com MCP, esta função seria a implementação
    do "ferramenta de banco", e o agente chamaria essa tool.
    """
    with engine.begin() as conn:
        result = conn.execute(text(sql), params)
        row = result.mappings().first() or {}
    return dict(row)


def build_answer(question: str, row: Dict[str, Any], sql: str, cliente_id: str) -> str:
    """
    Monta uma resposta em linguagem natural com base no resultado da query.
    Cobre:
      - total (mês passado / total geral)
      - total_faturado (quando LLM usa esse alias)
      - qtd (contagem de registros)
      - top5 (cinco maiores notas)
      - último ciclo (total + qtd + período)
    """
    # Caso: cinco maiores notas (top5)
    if "top5" in row:
        top5 = row["top5"]
        if top5 is None:
            return (
                f"Para a pergunta '{question}', não encontrei notas fiscais "
                f"para o cliente {cliente_id} com esse filtro."
            )

        # Se vier como string, tenta converter de JSON
        if isinstance(top5, str):
            try:
                top5 = json.loads(top5)
            except Exception:
                return (
                    f"Para a pergunta '{question}', obtive o resultado: {row}. "
                    f"(SQL usada: {sql})"
                )

        if not top5:
            return (
                f"Para a pergunta '{question}', não encontrei notas fiscais "
                f"para o cliente {cliente_id}."
            )

        linhas = []
        for i, nf in enumerate(top5, start=1):
            data = nf.get("data_emissao")
            valor = nf.get("valor_total")
            status_nf = nf.get("status")
            linhas.append(
                f"{i}. data={data}, valor=R$ {float(valor):.2f}, status={status_nf}"
            )
        lista = "\n".join(linhas)
        return (
            f"Para a pergunta '{question}', estas são as cinco maiores notas fiscais "
            f"do cliente {cliente_id}:\n{lista}"
        )

    # Caso: último ciclo (total + qtd + período)
    if "total" in row and "inicio" in row and "fim" in row:
        qtd = int(row.get("qtd") or 0)
        total = float(row.get("total") or 0)
        inicio = row.get("inicio")
        fim = row.get("fim")

        if qtd == 0:
            return (
                f"Para a pergunta '{question}', não encontrei faturamento recente "
                f"para o cliente {cliente_id}."
            )

        return (
            f"Para a pergunta '{question}', no último ciclo de faturamento do cliente "
            f"{cliente_id} (de {inicio} até {fim}), o faturamento foi de "
            f"R$ {total:.2f} em {qtd} registros."
        )

    # Caso: total (geral, mês passado, etc.)
    if "total" in row:
        total = row["total"]
        if total is None:
            return (
                f"Para a pergunta '{question}', não encontrei faturamento "
                f"para o cliente {cliente_id} com esse filtro."
            )
        total = float(total)
        return (
            f"Para a pergunta '{question}', o faturamento total do cliente "
            f"{cliente_id} é de R$ {total:.2f}."
        )

    # Caso: total_faturado (quando LLM gera esse alias)
    if "total_faturado" in row:
        total = row["total_faturado"]
        if total is None:
            return (
                f"Para a pergunta '{question}', não encontrei faturamento "
                f"para o cliente {cliente_id} com esse filtro."
            )
        total = float(total)
        return (
            f"Para a pergunta '{question}', o faturamento total do cliente "
            f"{cliente_id} é de R$ {total:.2f}."
        )

    # Caso: qtd (contagem)
    if "qtd" in row:
        qtd = int(row["qtd"] or 0)
        return (
            f"Para a pergunta '{question}', encontrei {qtd} registros de faturamento "
            f"para o cliente {cliente_id}."
        )

    # Fallback genérico
    return (
        f"Para a pergunta '{question}', obtive o resultado: {row}. "
        f"(SQL usada: {sql})"
    )


# ----------------------------------------------------------------
# Endpoint do agente
# ----------------------------------------------------------------

@app.post("/run-agent", response_model=AgentResponse)
def run_agent(req: AskRequest):
    """
    Agent-service:
    - recebe question + cliente_id
    - se LLM estiver disponível, gera SQL e chama a "tool" de banco
    - se LLM não estiver disponível, responde de forma elegante
    """
    if not USE_LLM:
        # Modo "sem IA": arquitetura está no ar, mas o agente está desativado.
        mensagem = (
            "Desculpe, o módulo de IA não está disponível no momento, "
            "então não consigo gerar uma resposta baseada nos seus dados. "
            "Tente novamente mais tarde ou habilite a OPENAI_API_KEY."
        )
        return AgentResponse(answer=mensagem, debug_sql=None)

    # Modo normal: IA ativa, gera SQL e consulta banco
    try:
        sql, params = decide_sql(req.question, req.cliente_id)
    except Exception as e:
        # Se der algum erro estranho na decisão de SQL, falha de forma clara
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao decidir SQL com o agente de IA: {e}",
        )

    try:
        row = db_mcp_tool(sql, params)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao executar SQL: {e}",
        )

    answer = build_answer(req.question, row, sql, req.cliente_id)
    return AgentResponse(answer=answer, debug_sql=sql)
