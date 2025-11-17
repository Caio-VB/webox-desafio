import os
import json
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# ==========================
# Configuração básica
# ==========================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurada no ambiente.")

engine = create_engine(DATABASE_URL)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
USE_LLM = bool(OPENAI_API_KEY)

if USE_LLM:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

TABLE_NAME = os.getenv("FATURAMENTO_TABLE", "faturamento")

app = FastAPI(title="WeBox Agent Service")

MAX_ROWS = 100
MAX_COLS = 30
MAX_ROWS_FOR_LLM = MAX_ROWS
Row = Dict[str, Any]


class AskRequest(BaseModel):
    question: str


class AgentResponse(BaseModel):
    answer: str
    debug_sql: str | None = None


# ==========================
# Introspecção simples
# ==========================

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


# ==========================
# Helpers de SQL
# ==========================

def is_safe_sql(sql: str) -> bool:
    return sql.strip().lower().startswith("select")


def enforce_sql_limits(sql: str) -> str:
    sql_clean = sql.strip().rstrip(";")
    if "limit" in sql_clean.lower():
        return sql_clean + ";"
    return sql_clean + f" LIMIT {MAX_ROWS};"


def db_mcp_tool(sql: str) -> List[Row]:
    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.mappings().all()
    return [dict(r) for r in rows]


# ==========================
# 1) LLM gera 1–5 SQLs
# ==========================

def llm_generate_queries(question: str) -> List[Dict[str, Any]]:
    if not USE_LLM:
        raise RuntimeError("LLM não configurado (OPENAI_API_KEY ausente).")

    schema = get_table_schema(TABLE_NAME)
    schema_lines = [f"- {c['name']} ({c['type']})" for c in schema]
    schema_desc = "\n".join(schema_lines) if schema_lines else "(sem colunas)"

    system_prompt = """
Você é um assistente que planeja análises de faturamento E gera SQL para PostgreSQL.

TAREFA:
- Receber uma pergunta em linguagem natural.
- Decidir de 1 a 5 subconsultas SQL que, juntas, ajudem a responder à pergunta.
- Gerar apenas consultas SELECT válidas para PostgreSQL.

REGRAS:
1) Use APENAS a tabela informada (TABLE_NAME).
2) Use APENAS as colunas listadas no esquema.
3) NÃO invente colunas ou tabelas.
4) NÃO use SELECT *; escolha colunas relevantes.
5) NÃO use DELETE/UPDATE/INSERT/DDL.
6) MÁXIMO 100 linhas por consulta (use LIMIT se necessário).
7) Para perguntas amplas (relatório, visão geral, ano inteiro).
8) Faça sempre consultas AGREGADAS (SUM/COUNT/AVG, GROUP BY), para evitar que retorne apenas amostras de conjutnos maiores.
9) Se a pergunta mencionar "faturamento total" de um período (ano/mês),
   inclua pelo menos UMA consulta que retorne o total consolidado desse período
   em uma única linha, usando SUM em colunas como valor_bruto/valor_liquido
   e apelidos claros, por exemplo:
   - faturamento_bruto_total
   - faturamento_liquido_total
   NÃO dependa de somar várias linhas fora do banco.
10) Responda SEMPRE com JSON válido, sem markdown.

FORMATO OBRIGATÓRIO:
{
  "queries": [
    {
      "id": "q1",
      "title": "Título curto",
      "sql": "SELECT ...",
      "purpose": "Objetivo desta query em 1 frase"
    }
  ]
}
""".strip()

    user_prompt = f"""
TABLE_NAME: {TABLE_NAME}

Esquema da tabela:
{schema_desc}

Pergunta do usuário:
"{question}"

Gere de 1 a 5 consultas seguindo o formato JSON especificado.
""".strip()

    chat = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )

    content = chat.choices[0].message.content.strip()

    # Remove ```json ...``` se vier
    if content.startswith("```"):
        lines = content.splitlines()
        lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        content = "\n".join(lines).strip()

    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Falha ao parsear JSON de queries: {e}")

    queries = data.get("queries")
    if not isinstance(queries, list) or not queries:
        raise HTTPException(status_code=500, detail="LLM não retornou 'queries' válidas.")

    # Normalização básica
    normalized = []
    for i, q in enumerate(queries[:5], start=1):
        sql = (q.get("sql") or "").strip()
        if not sql:
            continue
        if sql.startswith("```"):
            lines = sql.splitlines()
            lines = lines[1:]
            if lines and lines[-1].strip().startswith("```"):
                lines = lines[:-1]
            sql = "\n".join(lines).strip()

        normalized.append({
            "id": q.get("id") or f"q{i}",
            "title": q.get("title") or f"Query {i}",
            "purpose": q.get("purpose") or "",
            "sql": sql,
        })

    if not normalized:
        raise HTTPException(status_code=500, detail="Nenhuma SQL válida gerada pelo LLM.")

    return normalized


# ==========================
# 2) Executar SQLs
# ==========================

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


# ==========================
# 3) LLM gera resposta final
# ==========================

def llm_generate_answer(question: str, query_results: List[Dict[str, Any]]) -> str:
    if not USE_LLM:
        raise RuntimeError("LLM não configurado (OPENAI_API_KEY ausente).")

    payload = {
        "question": question,
        "queries": []
    }

    for q in query_results:
        rows = q.get("rows") or []
        payload["queries"].append({
            "id": q["id"],
            "title": q["title"],
            "purpose": q.get("purpose", ""),
            "sql": q["sql"],
            "error": q.get("error"),
            "total_rows": len(rows),
            "rows": rows[:MAX_ROWS_FOR_LLM],
        })

    payload_json = json.dumps(payload, ensure_ascii=False, default=str)

    system_prompt = """
Você é um assistente de negócios que responde perguntas sobre faturamento
com base em resultados de consultas SQL.

TAREFA:
- Receber a pergunta original do usuário e o resultado de 1 a 5 consultas SQL.
- Se a pergunta for direta e objetiva, responda de forma curta (até 3 frases).
- Se a pergunta pedir visão geral, relatório, resumo anual/mensal ou análise ampla,
  escreva um pequeno relatório estruturado em texto corrido (3 a 8 parágrafos),
  abordando os principais pontos (ex.: visão geral, por mês, por status, por cliente).

REGRAS NUMÉRICAS (MUITO IMPORTANTES):
1) NÃO invente números. Use APENAS números que já apareçam nas linhas do JSON.
2) VOCÊ ESTÁ PROIBIDO de fazer qualquer conta (somar, subtrair, multiplicar,
   dividir, calcular médias, percentuais ou totais a partir de várias linhas).
   - Se os dados trazem apenas valores mensais, você NÃO pode dizer
     "faturamento total do ano foi X", a menos que exista uma linha explícita
     com esse total anual.
3) Você pode:
   - repetir números exatamente como aparecem nas linhas;
   - comparar qualitativamente (ex.: "setembro foi o maior mês entre os listados");
   - citar valores de colunas agregadas já prontas (ex.: uma coluna chamada
     faturamento_bruto_total, faturamento_liquido_total, etc.).
4) Se a pergunta pedir um total consolidado que NÃO exista em nenhuma linha
   (por exemplo, soma do ano inteiro), explique que os dados recebidos são
   agregados por outra granularidade (ex.: por mês) e não trazem esse total pronto.
5) Se alguma query tiver erro, ignore-a ou mencione rapidamente que aquela visão
   não foi possível.
6) Escreva sempre em português, tom profissional, direto.
7) Não use markdown.
""".strip()

    user_prompt = f"""
Dados de entrada (JSON):

{payload_json}

Com base nisso, responda à pergunta original do usuário de forma adequada
(curta ou em formato de mini-relatório, conforme o tipo de pergunta),
sempre respeitando as REGRAS NUMÉRICAS acima.
""".strip()

    chat = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )

    return chat.choices[0].message.content.strip()


# ==========================
# Endpoint
# ==========================

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
