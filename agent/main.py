import os
import re
import json
from typing import Tuple, Dict, Any, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# ============================================================
# Configuração básica
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurada no ambiente.")

engine = create_engine(DATABASE_URL)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
USE_LLM = bool(OPENAI_API_KEY)

if USE_LLM:
    from openai import OpenAI

    client = OpenAI(api_key=OPENAI_API_KEY)

app = FastAPI(title="WeBox Agent Service")


class AskRequest(BaseModel):
    question: str


class AgentResponse(BaseModel):
    answer: str
    debug_sql: str | None = None


# Limites de segurança / contexto
MAX_COLS = 30
MAX_ROWS = 100
MAX_SQL_ATTEMPTS = 3
MAX_ROWS_FOR_LLM = MAX_ROWS

Row = Dict[str, Any]


# ============================================================
# Introspecção do banco (schema + exemplos)
# ============================================================

def get_table_schema(table_name: str = "faturamento") -> List[Dict[str, str]]:
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


def get_column_samples(
    table_name: str,
    schema: List[Dict[str, str]],
    max_samples: int = 5,
) -> Dict[str, List[str]]:
    samples: Dict[str, List[str]] = {}
    text_types = {"character varying", "text", "varchar", "char"}

    with engine.begin() as conn:
        for col in schema:
            col_name = col["name"]
            col_type = col["type"]

            if col_type not in text_types:
                continue

            sql = text(
                f"""
                SELECT DISTINCT "{col_name}"
                FROM {table_name}
                WHERE "{col_name}" IS NOT NULL
                LIMIT :limit
                """
            )
            result = conn.execute(sql, {"limit": max_samples})
            values = [str(row[0]) for row in result.fetchall() if row[0] is not None]

            if values:
                samples[col_name] = values

    return samples


# ============================================================
# Helpers de SQL
# ============================================================

def is_safe_sql(sql: str) -> bool:
    """
    Regra simples: só aceita SELECT (guard-rail extra).
    """
    s = sql.strip().lower()
    return s.startswith("select")


def analyze_sql_shape(sql: str) -> Tuple[bool, bool, int | None]:
    """
    Analisa a forma da query:

    - has_aggregate: True se parecer agregada (SUM/AVG/COUNT/MIN/MAX ou GROUP BY)
    - has_limit: True se há LIMIT
    - limit_value: valor numérico do LIMIT, se existir
    """
    s = sql.lower()
    has_aggregate_fn = bool(re.search(r"\b(sum|avg|count|min|max)\s*\(", s))
    has_group_by = "group by" in s
    has_aggregate = has_aggregate_fn or has_group_by

    m = re.search(r"\blimit\s+(\d+)", s)
    has_limit = m is not None
    limit_value = int(m.group(1)) if m else None

    return has_aggregate, has_limit, limit_value


def normalize_sql_for_postgres(sql: str) -> str:
    """
    Corrige 'GROUP BY ... WITH ROLLUP' (MySQL) para 'GROUP BY ROLLUP (...)' (Postgres).
    """

    def repl_rollup(match: re.Match) -> str:
        cols = match.group(1).strip().rstrip(",")
        return f"GROUP BY ROLLUP ({cols})"

    pattern_rollup = re.compile(
        r"group\s+by\s+([a-zA-Z0-9_\",.\s]+?)\s+with\s+rollup",
        re.IGNORECASE,
    )
    fixed = pattern_rollup.sub(repl_rollup, sql)
    return fixed


# ============================================================
# Geração de SQL via LLM
# ============================================================

def decide_sql(
    question: str,
    previous_sql: str | None = None,
    previous_issue: str | None = None,
) -> Tuple[str, Dict[str, Any]]:
    if not USE_LLM:
        raise RuntimeError("LLM não configurado (OPENAI_API_KEY ausente).")

    table_name = os.getenv("FATURAMENTO_TABLE", "faturamento")

    schema = get_table_schema(table_name)
    samples = get_column_samples(table_name, schema, max_samples=5)

    col_lines = []
    for col in schema:
        name = col["name"]
        col_type = col["type"]
        line = f"- {name} ({col_type})"
        if name in samples:
            example_values = ", ".join(samples[name][:5])
            line += f" | exemplos de valores: {example_values}"
        col_lines.append(line)

    schema_description = "\n".join(col_lines) if col_lines else "(sem colunas?)"

    system_prompt = """
Você é um assistente que gera consultas SQL para um banco PostgreSQL.

REGRAS MUITO IMPORTANTES (NÃO QUEBRE):

1) Use APENAS a tabela informada pelo sistema.
2) Use APENAS as colunas listadas no esquema.
3) Use APENAS valores de texto que apareçam nas amostras fornecidas.
4) Use colunas numéricas adequadas para valores (ex.: com 'valor', 'total').
5) Use colunas de status/situação para títulos em aberto/pago/vencido.
6) Não invente colunas nem valores.
7) NÃO use SELECT *; selecione apenas as colunas necessárias.
8) NÃO use DELETE, UPDATE, INSERT ou DDL; apenas SELECT.
9) A consulta NÃO pode retornar mais do que 30 colunas.
10) A consulta NÃO pode retornar mais do que 100 linhas.
11) Para relatórios ou períodos amplos (ano, mês), PREFIRA consultas AGREGADAS
    (GROUP BY, SUM, COUNT, AVG).
12) Use APENAS sintaxe PostgreSQL. Para totais:
    - Use GROUP BY ROLLUP (...) ou GROUPING SETS.
    - NUNCA use 'WITH ROLLUP' nem 'WITH CUBE'.
13) Responda APENAS com a SQL, sem explicações, sem markdown.
""".strip()

    feedback_block = ""
    if previous_sql and previous_issue:
        feedback_block = f"""
A consulta SQL ANTERIOR foi:

{previous_sql}

Problema detectado:
{previous_issue}

Gere AGORA uma NOVA consulta que:
- respeite os limites (≤ 30 colunas, ≤ 100 linhas),
- responda à mesma pergunta do usuário,
- use agregações quando houver muitas linhas,
- use apenas sintaxe válida de PostgreSQL.
""".strip()

    user_prompt = f"""
Tabela disponível: {table_name}.

Esquema da tabela (colunas, tipos e exemplos de valores):

{schema_description}

Pergunta (subpergunta) do usuário:
{question}

{feedback_block}

Com base SOMENTE nas colunas listadas e exemplos fornecidos,
gere uma ÚNICA consulta SQL em PostgreSQL que responda à pergunta
e respeite os limites de no máximo 30 colunas e 100 linhas.
""".strip()

    chat = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )
    sql = chat.choices[0].message.content.strip()

    # Sanitização de bloco ```sql ... ```
    if sql.startswith("```"):
        lines = sql.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        sql = "\n".join(lines).strip()

    params: Dict[str, Any] = {}
    return sql, params


# ============================================================
# Plano de análise (fixo para relatórios de ano)
# ============================================================

def build_fixed_plan_for_question(question: str) -> List[Dict[str, Any]]:
    """
    Para perguntas claramente de relatório/ano, monta 3 subperguntas.
    Caso contrário, apenas 1 subpergunta igual à pergunta (pergunta simples).
    """
    q_lower = question.lower()

    mentions_report = "relatório" in q_lower or "relatorio" in q_lower
    broad_how_was = "como foi" in q_lower
    mentions_year = re.search(r"\b20\d{2}\b", q_lower) is not None

    is_year_report = mentions_report or (broad_how_was and mentions_year)

    if not is_year_report:
        return [{
            "id": "q1",
            "title": "Pergunta principal",
            "question": question,
            "goal": "Responder diretamente à pergunta do usuário"
        }]

    # Para relatório de ano, 3 subperguntas estratégias (todas agregadas)
    return [
        {
            "id": "q1",
            "title": "Visão geral mensal do faturamento",
            "question": (
                "Calcule, para o ano de 2024, o faturamento total por mês, "
                "incluindo quantidade de notas, soma de valor_bruto, soma de valor_liquido, "
                "soma de valor_imposto e ticket_medio_estimado médio por mês."
            ),
            "goal": "Entender a evolução mensal do faturamento em 2024."
        },
        {
            "id": "q2",
            "title": "Faturamento por status do título",
            "question": (
                "Para o ano de 2024, agrupe por status_titulo e retorne, por status, "
                "a quantidade de notas e a soma de valor_liquido."
            ),
            "goal": "Entender quanto está faturado por status (pago, aberto, vencido, etc.)."
        },
        {
            "id": "q3",
            "title": "Faturamento por cluster ou segmento de cliente",
            "question": (
                "Para o ano de 2024, agrupe por cluster_cliente ou segmento_cliente "
                "(o que existir na tabela) e retorne a quantidade de notas e a soma de valor_liquido. "
                "Se existirem as duas colunas, prefira agrupar por cluster_cliente."
            ),
            "goal": "Entender concentração de receita por perfil de cliente em 2024."
        },
    ]


# ============================================================
# Execução de SQL
# ============================================================

def enforce_sql_limits(sql: str) -> str:
    sql_clean = sql.strip().rstrip(";")
    s_lower = sql_clean.lower()

    if re.search(r"\blimit\b", s_lower):
        return sql_clean + ";"

    return sql_clean + f" LIMIT {MAX_ROWS};"


def db_mcp_tool(sql: str, params: Dict[str, Any]) -> List[Row]:
    with engine.begin() as conn:
        result = conn.execute(text(sql), params)
        rows = result.mappings().all()
    return [dict(r) for r in rows]


def plan_and_run_sql_with_limits(question: str) -> Tuple[str, List[Row]]:
    previous_sql: str | None = None
    previous_issue: str | None = None

    for _ in range(1, MAX_SQL_ATTEMPTS + 1):
        sql, params = decide_sql(question, previous_sql, previous_issue)
        sql = normalize_sql_for_postgres(sql)
        sql = enforce_sql_limits(sql)

        if not is_safe_sql(sql):
            raise HTTPException(
                status_code=400,
                detail="A consulta gerada pela IA foi considerada insegura."
            )

        has_aggregate, has_limit, limit_value = analyze_sql_shape(sql)
        rows = db_mcp_tool(sql, params)

        num_rows = len(rows)
        num_cols = len(rows[0]) if rows else 0

        # Se não é agregada, tem LIMIT e bateu exatamente o teto: provavelmente amostra
        if (not has_aggregate) and has_limit and num_rows == MAX_ROWS:
            previous_sql = sql
            previous_issue = (
                "a consulta retornou 100 linhas de detalhe usando LIMIT, "
                "provavelmente como amostra de um conjunto maior. "
                "Gere agora uma consulta AGREGADA (GROUP BY com SUM/COUNT/AVG, etc.) "
                "que responda a mesma pergunta sem depender de amostra."
            )
            continue

        if num_rows <= MAX_ROWS and num_cols <= MAX_COLS:
            return sql, rows

        previous_sql = sql
        previous_issue = (
            f"A consulta retornou {num_cols} colunas e {num_rows} linhas (resultado muito grande). "
            "Gere uma nova consulta MAIS AGREGADA e com MENOS colunas, "
            "respeitando o limite de no máximo 30 colunas e 100 linhas."
        )

    raise HTTPException(
        status_code=500,
        detail=(
            "Não foi possível gerar uma consulta SQL que respeitasse o limite "
            "de no máximo 30 colunas e 100 linhas após várias tentativas."
        ),
    )


def run_analysis_pipeline(question: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Usa plano fixo:
    - Se pergunta simples => 1 subpergunta (a própria pergunta).
    - Se pergunta de relatório de ano => 3 subperguntas estratégicas.
    """
    plan = build_fixed_plan_for_question(question)
    query_results: List[Dict[str, Any]] = []

    for item in plan:
        sub_q = item.get("question") or question
        try:
            sql, rows = plan_and_run_sql_with_limits(sub_q)
            query_results.append({
                "id": item.get("id"),
                "title": item.get("title"),
                "goal": item.get("goal"),
                "subquestion": sub_q,
                "sql": sql,
                "total_rows": len(rows),
                "rows": rows,
                "error": None,
            })
        except HTTPException as e:
            query_results.append({
                "id": item.get("id"),
                "title": item.get("title"),
                "goal": item.get("goal"),
                "subquestion": sub_q,
                "sql": None,
                "total_rows": 0,
                "rows": [],
                "error": str(e.detail),
            })

    if all(r["error"] is not None for r in query_results):
        raise HTTPException(
            status_code=500,
            detail="Nenhuma consulta SQL pôde ser executada com sucesso para o plano de análise.",
        )

    return plan, query_results


# ============================================================
# Respostas em linguagem natural
# ============================================================

def generate_simple_answer_with_llm(question: str, sql: str, rows: List[Row]) -> str:
    """
    Para perguntas simples (apenas 1 subquery): resposta direta, curta.
    Sempre usando apenas os valores retornados pelo banco.
    """
    if not USE_LLM:
        raise RuntimeError("LLM não configurado (OPENAI_API_KEY ausente).")

    total_rows = len(rows)
    truncated_rows = rows[:MAX_ROWS_FOR_LLM]
    rows_json = json.dumps(truncated_rows, default=str, ensure_ascii=False)

    system_prompt = """
Você é um assistente de negócios que responde perguntas objetivas
sobre resultados de consultas SQL de faturamento.

REGRAS:

1) NÃO invente números. Use APENAS os valores numéricos que aparecem
   nas linhas do JSON.
2) NÃO crie totais que não estejam explicitamente em alguma linha/coluna.
3) Se a SQL já traz um total (ex.: soma, count, etc.), você pode usá-lo.
4) Se os dados estiverem agregados por mês/cliente/etc., responda usando
   esses valores (por exemplo, maior mês, menor mês), SEM inventar valor
   adicional.
5) Responda em no máximo 3 frases, em português, bem direto ao ponto.
6) Não repita a SQL. Não use markdown.
""".strip()

    user_prompt = f"""
Pergunta do usuário:
{question}

SQL executada:
{sql}

Total de linhas retornadas pela SQL: {total_rows}
Quantidade de linhas enviadas ao modelo: {len(truncated_rows)}

Resultado da SQL (lista de linhas em JSON):
{rows_json}

Responda de forma direta e curta à pergunta do usuário,
usando apenas os números presentes nos dados acima.
""".strip()

    chat = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )

    answer = chat.choices[0].message.content.strip()
    return answer


def generate_report_with_llm(
    question: str,
    plan: List[Dict[str, Any]],
    query_results: List[Dict[str, Any]],
) -> str:
    if not USE_LLM:
        raise RuntimeError("LLM não configurado (OPENAI_API_KEY ausente).")

    payload = {
        "user_question": question,
        "plan": plan,
        "queries": []
    }

    for qr in query_results:
        rows = qr.get("rows") or []
        payload["queries"].append({
            "id": qr.get("id"),
            "title": qr.get("title"),
            "goal": qr.get("goal"),
            "subquestion": qr.get("subquestion"),
            "sql": qr.get("sql"),
            "total_rows": qr.get("total_rows", len(rows)),
            "rows": rows,
            "error": qr.get("error"),
        })

    payload_json = json.dumps(payload, default=str, ensure_ascii=False)

    system_prompt = """
Você é um assistente de negócios que escreve RELATÓRIOS ESTRUTURADOS
sobre faturamento, com base em resultados de consultas SQL.

REGRAS:

1) NÃO invente números. Use APENAS os valores numéricos presentes nas linhas.
2) NÃO crie totais que não estejam em alguma linha da entrada.
3) Se faltar número para algo, comente qualitativamente, sem chutar valor.
4) Se alguma subpergunta tiver erro, mencione brevemente que aquela visão
   não pôde ser analisada.
5) Estruture como relatório em texto simples (sem markdown), por exemplo:
   Visão geral, Análise por mês, Análise por status, Análise por cliente,
   Conclusões.
6) Escreva em português, tom profissional e objetivo.
7) Entre 4 e 10 parágrafos.
""".strip()

    user_prompt = f"""
Aqui está o PLANO de análise e os RESULTADOS das consultas em JSON:

{payload_json}

Tarefa:
- Monte um RELATÓRIO estruturado, em português, que responda à pergunta
  original do usuário:
  "{question}"

- Use apenas os números presentes nas linhas do JSON.
- Não repita SQL.
- Não use markdown.
""".strip()

    chat = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )

    answer = chat.choices[0].message.content.strip()
    return answer


def build_answer(question: str, plan_or_rows, query_results: List[Dict[str, Any]] | None = None) -> str:
    return (
        "Os dados foram consultados no banco com sucesso, mas o módulo de IA "
        "que gera a resposta em linguagem natural não está disponível no momento. "
        "Tente novamente mais tarde."
    )


# ============================================================
# Endpoint do agente
# ============================================================

@app.post("/run-agent", response_model=AgentResponse)
def run_agent(req: AskRequest):
    if not USE_LLM:
        mensagem = (
            "Desculpe, o módulo de IA não está disponível no momento, "
            "então não consigo gerar uma resposta baseada nos seus dados. "
            "Tente novamente mais tarde ou habilite a OPENAI_API_KEY."
        )
        return AgentResponse(answer=mensagem, debug_sql=None)

    # 1) Monta plano (fixo) e executa as queries
    try:
        plan, query_results = run_analysis_pipeline(req.question)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro no pipeline de análise com o agente de IA: {e}",
        )

    # 2) Escolhe se é resposta simples ou relatório
    try:
        if len(plan) == 1:
            # Pergunta simples => usa só a primeira query
            qr = query_results[0]
            if qr.get("error"):
                raise HTTPException(status_code=500, detail=qr["error"])
            sql = qr.get("sql")
            rows = qr.get("rows") or []
            answer = generate_simple_answer_with_llm(req.question, sql, rows)
            debug_sql = sql
        else:
            # Pergunta ampla => relatório estruturado
            answer = generate_report_with_llm(req.question, plan, query_results)

            debug_blocks: List[str] = []
            for qr in query_results:
                title = qr.get("title") or qr.get("id") or "subpergunta"
                sql = qr.get("sql")
                err = qr.get("error")
                if sql:
                    debug_blocks.append(f"-- {title}\n{sql}")
                elif err:
                    debug_blocks.append(f"-- {title} (erro)\n-- {err}")
            debug_sql = "\n\n".join(debug_blocks) if debug_blocks else None

    except Exception as e:
        print(f"[AGENT] Erro ao gerar resposta com LLM, usando fallback: {e}")
        if len(plan) == 1:
            answer = build_answer(req.question, query_results[0].get("rows") or [])
            debug_sql = query_results[0].get("sql")
        else:
            answer = build_answer(req.question, plan, query_results)
            debug_sql = None

    return AgentResponse(answer=answer, debug_sql=debug_sql)
