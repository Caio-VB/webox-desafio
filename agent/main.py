import os
import json
from typing import Tuple, Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# --- Configuração de banco (tool MCP de banco, na prática) ---

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurada no ambiente.")

engine = create_engine(DATABASE_URL)

# --- LLM (OpenAI) para geração de SQL e respostas ---

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


# ----------------------------------------------------------------
# Camada de introspecção do banco
# ----------------------------------------------------------------


def get_table_schema(table_name: str = "faturamento") -> list[dict]:
    """
    Lê o schema real da tabela no Postgres (colunas e tipos).
    Não assume nada além do nome da tabela.
    """
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
    schema: list[dict],
    max_samples: int = 5,
) -> dict[str, list[str]]:
    """
    Para cada coluna textual, busca alguns valores distintos
    para dar contexto ao LLM.

    Não assume nomes específicos de colunas (status, cliente, etc.);
    tudo vem do schema real.
    """
    samples: dict[str, list[str]] = {}

    text_types = {
        "character varying",
        "text",
        "varchar",
        "char",
    }

    with engine.begin() as conn:
        for col in schema:
            col_name = col["name"]
            col_type = col["type"]

            if col_type not in text_types:
                continue

            # col_name vem do information_schema, não de input do usuário.
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


# ----------------------------------------------------------------
# Camada de decisão de SQL
# ----------------------------------------------------------------


DANGEROUS_KEYWORDS = [
    "insert", "update", "delete", "drop", "alter",
    "truncate", "create", "grant", "revoke", "execute", "copy"
]

def is_safe_sql(sql: str) -> bool:
    s = sql.strip().lower()

    # só permite SELECT
    if not s.startswith("select"):
        return False

    # só uma instrução (no máximo um ';' no final)
    if ";" in s[:-1]:
        return False

    # corta comentários pra não esconder coisa
    for kw in DANGEROUS_KEYWORDS:
        if kw in s:
            return False

    return True



def decide_sql(question: str) -> Tuple[str, Dict[str, Any]]:
    """
    Usa um LLM (OpenAI) para gerar SQL a partir da pergunta.

    Não assume nomes específicos de colunas de negócio.
    Em vez disso:
      - lê o schema real da tabela de faturamento (todas as colunas),
      - lê alguns valores de exemplo para colunas textuais,
      - passa isso tudo para o LLM escolher quais colunas/valores usar.
    """
    if not USE_LLM:
        raise RuntimeError("LLM não configurado (OPENAI_API_KEY ausente).")

    table_name = os.getenv("FATURAMENTO_TABLE", "faturamento")

    # 1) Schema real da tabela
    schema = get_table_schema(table_name)

    # 2) Amostras de valores das colunas textuais
    samples = get_column_samples(table_name, schema, max_samples=5)

    # 3) Monta descrição para o prompt
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

Você tem acesso a UMA tabela principal que representa dados de faturamento
consolidados a partir de arquivos Excel.

Regras MUITO IMPORTANTES (NÃO QUEBRE):

1) NÃO invente nomes de tabelas. Use APENAS a tabela informada pelo sistema.
2) NÃO invente nomes de colunas. Use APENAS as colunas listadas no esquema.
3) Para filtrar por valores de texto (por exemplo, status, situação, cliente),
   use APENAS valores que apareçam nas amostras fornecidas pelo sistema.
   - Copie o valor exatamente como está:
     * mesmas maiúsculas/minúsculas,
     * mesmos underscores,
     * sem traduzir, sem “normalizar”.
   - Exemplo: se o valor de exemplo é "EM_ABERTO", use exatamente 'EM_ABERTO'.
4) Para perguntas sobre "quanto eu faturei", "total faturado", etc.,
   escolha uma coluna NUMÉRICA adequada com base no nome e nos exemplos.
   Exemplo: colunas cujos nomes contenham "valor", "total", "faturamento".
5) Para perguntas sobre "quanto tenho a receber" ou "em aberto",
   escolha uma coluna TEXTUAL de status/situação e valores que representem
   "não pago / em aberto / pendente", COM BASE nas amostras fornecidas.
6) Se a pergunta mencionar um cliente ou identificador (ex.: "XPTO"),
   e houver uma coluna textual compatível com isso (pelo nome ou pelos valores),
   use essa coluna em um WHERE apropriado.
7) Se você NÃO encontrar colunas/valores suficientes para responder,
   NÃO invente nada. Nesse caso, gere uma SQL exploratória que ajude
   o usuário a entender os dados, por exemplo:
   - SELECT DISTINCT alguma_coluna FROM tabela;
   - ou uma agregação simples por colunas relevantes.
8) Não use SELECT *; selecione apenas as colunas necessárias.
9) Não use DELETE, UPDATE ou INSERT; apenas SELECT.
10) Responda APENAS com a SQL, sem explicações, sem markdown.
""".strip()

    user_prompt = f"""
A tabela disponível se chama {table_name}.

Esquema da tabela (colunas, tipos e exemplos de valores):

{schema_description}

Pergunta do usuário:
{question}

Com base SOMENTE nas colunas listadas acima e nos valores de exemplo
informados, gere uma única consulta SQL em PostgreSQL que responda à pergunta.

Lembre-se:
- não invente colunas;
- não invente valores: use apenas os exemplos fornecidos (quando precisar filtrar);
- se não houver informação suficiente, gere uma SQL exploratória útil.
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

    # Sanitização (``` etc.)
    if sql.startswith("```"):
        lines = sql.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        sql = "\n".join(lines).strip()

    params: Dict[str, Any] = {}
    return sql, params


# ----------------------------------------------------------------
# Camada "tool" de banco (pensar como uma MCP tool)
# ----------------------------------------------------------------


def enforce_sql_limits(sql: str) -> str:
    s = sql.strip().lower()
    if s.startswith("select *") and " limit " not in s:
        return sql.rstrip(" ;") + " LIMIT 500;"
    return sql


def db_mcp_tool(sql: str, params: Dict[str, Any]) -> list[Dict[str, Any]]:
    """
    Esta função encapsula a chamada ao banco e representa,
    conceitualmente, uma tool de banco de dados no MCP/A2A.

    Em um cenário com MCP, esta função seria a implementação
    da "ferramenta de banco", e o agente chamaria essa tool.
    """
    with engine.begin() as conn:
        result = conn.execute(text(sql), params)
        rows = result.mappings().all()
    return [dict(r) for r in rows]


def generate_answer_with_llm(question: str, sql: str, rows: list[Dict[str, Any]]) -> str:
    """
    Usa o LLM para transformar o resultado da query em uma resposta de negócio.

    Para evitar estouro de contexto, só mandamos um subconjunto das linhas
    e informamos o total de registros encontrados.
    """
    if not USE_LLM:
        raise RuntimeError("LLM não configurado (OPENAI_API_KEY ausente).")

    total_rows = len(rows)

    # Limite de linhas que vamos mandar para o modelo
    MAX_ROWS_FOR_LLM = 100
    truncated_rows = rows[:MAX_ROWS_FOR_LLM]

    rows_json = json.dumps(truncated_rows, default=str, ensure_ascii=False)

    system_prompt = """
Você é um assistente de negócios que explica resultados de consultas SQL
sobre uma tabela de faturamento.

Você recebe:
- a pergunta original do usuário (em português),
- a SQL que foi executada em um banco PostgreSQL,
- o resultado dessa SQL em formato JSON (APENAS UMA AMOSTRA das linhas),
- o total de linhas retornadas pela SQL.

Seu trabalho:
- Interpretar a pergunta e o resultado.
- Usar principalmente campos numéricos e de negócio (por exemplo valores,
  datas, status, cliente) para construir a análise.
- Se a pergunta pede totais, fale de totais e médias (quando fizer sentido).
- Se houver muitas linhas (total_linhas >> amostra), faça um resumo agregado:
  por exemplo, total do período, quantidade de notas, médias por cliente ou status, etc.
- Se a lista estiver vazia, explique que não há dados que atendam ao filtro.
- Monte uma resposta em português, clara e objetiva (2 a 6 frases).
- Não repita a SQL na resposta final.
- Não use markdown, apenas texto simples.
""".strip()

    user_prompt = f"""
Pergunta do usuário:
{question}

SQL executada:
{sql}

Total de linhas retornadas pela SQL: {total_rows}
Quantidade de linhas na amostra enviada ao modelo: {len(truncated_rows)}

Resultado da SQL (amostra em JSON - lista de linhas):
{rows_json}

Com base nisso, responda ao usuário de forma objetiva, em português,
resumindo o que aconteceu nesse conjunto de dados. Se fizer sentido,
mencione volume de notas, valores relevantes, status (pago, aberto, vencido)
e qualquer padrão importante que apareça na amostra.
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


def build_answer(question: str, rows: list[Dict[str, Any]], sql: str) -> str:
    """
    Fallback bem genérico, usado só se a LLM de resposta falhar.
    """
    return (
        "Desculpe, o módulo de IA não está disponível no momento, "
        "então não consigo gerar uma resposta baseada nos seus dados. "
        "Tente novamente mais tarde ou habilite a OPENAI_API_KEY."
    )


# ----------------------------------------------------------------
# Endpoint do agente
# ----------------------------------------------------------------


@app.post("/run-agent", response_model=AgentResponse)
def run_agent(req: AskRequest):
    """
    Agent-service:
    - recebe question
    - se LLM estiver disponível, gera SQL e chama a "tool" de banco
    - em seguida, usa o LLM novamente para traduzir o resultado em resposta de negócio
    """
    if not USE_LLM:
        mensagem = (
            "Desculpe, o módulo de IA não está disponível no momento, "
            "então não consigo gerar uma resposta baseada nos seus dados. "
            "Tente novamente mais tarde ou habilite a OPENAI_API_KEY."
        )
        return AgentResponse(answer=mensagem, debug_sql=None)

    # 1) Gera SQL a partir da pergunta
    try:
        sql, params = decide_sql(req.question)
        sql = enforce_sql_limits(sql)
        if not is_safe_sql(sql):
            raise HTTPException(
                status_code=400,
                detail="A consulta gerada pela IA foi considerada insegura."
            )
        rows = db_mcp_tool(sql, params)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao decidir SQL com o agente de IA: {e}",
        )

    # 2) Executa a SQL via tool de banco (MCP-like)
    try:
        rows = db_mcp_tool(sql, params)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao executar SQL: {e}",
        )

    # 3) Usa LLM para gerar a resposta de negócio
    try:
        answer = generate_answer_with_llm(req.question, sql, rows)
    except Exception as e:
        print(f"[AGENT] Erro ao gerar resposta com LLM, usando fallback: {e}")
        answer = build_answer(req.question, rows, sql)

    return AgentResponse(answer=answer, debug_sql=sql)
