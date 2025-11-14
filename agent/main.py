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


def get_raw_samples(
    table_name: str = "faturamento",
    max_samples: int = 5,
) -> list[dict]:
    """
    Busca algumas amostras reais do campo raw (JSONB) para dar contexto ao LLM.
    Não assume nada sobre as chaves; apenas devolve o JSON como dict.
    """
    sql = text(
        f"""
        SELECT raw
        FROM {table_name}
        WHERE raw IS NOT NULL
        LIMIT :limit
        """
    )
    samples: list[dict] = []
    with engine.begin() as conn:
        result = conn.execute(sql, {"limit": max_samples})
        for row in result.fetchall():
            raw_value = row[0]
            if isinstance(raw_value, str):
                try:
                    samples.append(json.loads(raw_value))
                except Exception:
                    # Se não conseguir parsear, ignora essa linha
                    continue
            elif isinstance(raw_value, dict):
                samples.append(raw_value)
            else:
                # Último recurso: tenta serializar e reabrir
                try:
                    samples.append(json.loads(json.dumps(raw_value, default=str)))
                except Exception:
                    continue
    return samples


# ----------------------------------------------------------------
# Camada de decisão de SQL
# ----------------------------------------------------------------


def decide_sql(question: str) -> Tuple[str, Dict[str, Any]]:
    """
    Usa um LLM (OpenAI) para gerar SQL a partir da pergunta.

    Não assume nomes específicos de colunas de negócio.
    Em vez disso:
      - lê o schema real da tabela de faturamento (colunas fixas),
      - lê alguns exemplos reais do campo raw (JSONB),
      - passa isso tudo para o LLM escolher quais chaves/valores usar.
    """
    if not USE_LLM:
        raise RuntimeError("LLM não configurado (OPENAI_API_KEY ausente).")

    table_name = os.getenv("FATURAMENTO_TABLE", "faturamento")

    # 1) Schema real da tabela (colunas fixas)
    schema = get_table_schema(table_name)

    col_lines = []
    for col in schema:
        name = col["name"]
        col_type = col["type"]
        col_lines.append(f"- {name} ({col_type})")
    schema_description = "\n".join(col_lines) if col_lines else "(sem colunas?)"

    # 2) Amostras reais do raw (JSONB)
    raw_samples = get_raw_samples(table_name, max_samples=5)
    raw_samples_json = json.dumps(raw_samples, ensure_ascii=False, default=str)

    system_prompt = """
Você é um assistente que gera consultas SQL para um banco PostgreSQL.

Você tem acesso a UMA tabela principal que representa dados de faturamento,
consolidados a partir de arquivos Excel.

A tabela tem algumas colunas fixas (como cliente_id, arquivo_nome, etc.)
e uma coluna chamada raw, que é um JSONB contendo todas as colunas originais
da planilha (datas, valores, status, etc.).

REGRAS MUITO IMPORTANTES (NÃO QUEBRE):

1) NÃO invente nomes de tabelas. Use APENAS a tabela informada pelo sistema.
2) NÃO invente nomes de colunas SQL. Use APENAS as colunas listadas no schema
   (por exemplo: id, cliente_id, arquivo_nome, linha_numero, raw, created_at).
3) Para acessar campos internos do JSONB, use SEMPRE a sintaxe:
   raw->>'nome_da_chave'
   e, quando precisar de número, faça cast, por exemplo:
   (raw->>'valor_total')::numeric
   (raw->>'data_emissao')::date
4) NÃO invente nomes de chaves do JSON. Use APENAS chaves que apareçam nos
   exemplos reais do campo raw fornecidos pelo sistema.
5) Quando precisar comparar um valor de texto (por exemplo, status),
   COPIE o valor EXATAMENTE como ele aparece nos exemplos:
   - mesmas maiúsculas/minúsculas,
   - mesmos underscores e espaços,
   - sem traduzir nem “normalizar” nada.
   Ex.: se o exemplo tiver "EM_ABERTO", use exatamente 'EM_ABERTO' na SQL.
6) Se a pergunta falar "em aberto", "pendente", etc., procure nas amostras
   de raw valores que signifiquem "não pago / em aberto" e use esses valores
   literalmente no WHERE.
7) Se você NÃO encontrar nas amostras nenhuma chave/valor compatível com o
   que a pergunta precisa, NÃO invente.
   Nesse caso, gere uma SQL exploratória que ajude o usuário a entender
   os dados, por exemplo:
   SELECT DISTINCT raw->>'status' AS status FROM faturamento;
   ou algo semelhante, usando chaves que existam nas amostras.
8) Para perguntas sobre "quanto eu faturei", "quanto tenho a receber",
   "total faturado" etc., escolha chaves numéricas dentro de raw
   (por exemplo, algo como "valor_total", "valor", etc.), com base nas
   amostras, e faça SUM com cast para numeric.
9) Não use SELECT *; selecione apenas as colunas necessárias.
10) Não use DELETE, UPDATE ou INSERT; apenas SELECT.
11) Responda APENAS com a SQL, sem explicações, sem markdown.
""".strip()

    user_prompt = f"""
A tabela disponível se chama {table_name}.

Esquema da tabela (colunas fixas):

{schema_description}

Alguns exemplos reais do campo raw (JSONB), extraídos da própria tabela:

{raw_samples_json}

Pergunta do usuário:
{question}

Com base SOMENTE nas colunas listadas no schema e nas chaves/valores que
aparecem nos exemplos de raw acima, gere uma única consulta SQL em PostgreSQL
que responda à pergunta.

Lembre-se:
- não invente chaves nem valores;
- copie os valores exatamente como aparecem nas amostras;
- se não houver informação suficiente, gere uma SQL exploratória que ajude a
  inspecionar os dados relevantes (por exemplo, DISTINCT de algum campo).
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

    rows: lista de linhas retornadas pela SQL (cada linha é um dict).
    Pode estar vazia (nenhum resultado), ter uma linha (agregação)
    ou várias linhas (lista, top N, etc.).
    """
    if not USE_LLM:
        raise RuntimeError("LLM não configurado (OPENAI_API_KEY ausente).")

    system_prompt = """
Você é um assistente de negócios que explica resultados de consultas SQL
sobre uma tabela de faturamento.

Você recebe:
- a pergunta original do usuário (em português),
- a SQL que foi executada em um banco PostgreSQL,
- o resultado dessa SQL em formato JSON (uma lista de objetos, cada objeto é uma linha).

Seu trabalho:
- Interpretar a pergunta e o resultado.
- Se a pergunta pede totais, use os campos numéricos agregados (por exemplo, SUM).
- Se a pergunta pede listas (como "cinco maiores notas"), descreva os principais itens.
- Se a lista estiver vazia, explique que não há dados que atendam ao filtro.
- Monte uma resposta em português, clara e curta (2 a 5 frases no máximo).
- Não repita a SQL na resposta final.
- Não use markdown, apenas texto simples.
""".strip()

    rows_json = json.dumps(rows, default=str, ensure_ascii=False)

    user_prompt = f"""
Pergunta do usuário:
{question}

SQL executada:
{sql}

Resultado da SQL (JSON - lista de linhas):
{rows_json}

Com base nisso, responda ao usuário de forma objetiva, em português.
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
        f"Para a pergunta '{question}', obtive {len(rows)} linha(s) como resultado. "
        f"Primeiras linhas: {rows[:3]}. "
        f"(SQL usada: {sql})"
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
