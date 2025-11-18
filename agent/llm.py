import json
from typing import Dict, Any, List

from fastapi import HTTPException

from config import TABLE_NAME, MAX_ROWS_FOR_LLM, USE_LLM, OPENAI_API_KEY
from db import get_table_schema

if USE_LLM:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)


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
6) Para perguntas amplas (relatório, visão geral, ano inteiro).
7) Faça sempre consultas AGREGADAS (SUM/COUNT/AVG, GROUP BY), para evitar que retorne apenas amostras de conjutnos maiores, a menos que o usuário peça uma lista.
8) Se a pergunta mencionar "faturamento total" de um período (ano/mês),
   inclua pelo menos UMA consulta que retorne o total consolidado desse período
   em uma única linha, usando SUM em colunas como valor_bruto/valor_liquido
   e apelidos claros, por exemplo:
   - faturamento_bruto_total
   - faturamento_liquido_total
   NÃO dependa de somar várias linhas fora do banco.
9) Responda SEMPRE com JSON válido, sem markdown.

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

    normalized: List[Dict[str, Any]] = []
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
- Se a pergunta pedir uma lista, devolva a lista (ex: quais foram as notas emitidas para o cliente x?).

REGRAS NUMÉRICAS (MUITO IMPORTANTES):
1) NÃO invente números. Use APENAS números que já apareçam nas linhas do JSON.
2) VOCÊ ESTÁ PROIBIDO de fazer qualquer conta (somar, subtrair, multiplicar,
   dividir, calcular médias, percentuais ou totais a partir de várias linhas).
   - Se os dados trazem apenas valores mensais, você NÃO pode dizer
     "faturamento total do ano foi X", a menos que exista uma linha explícita
     com esse total anual.
3) Você pode:
   - repetir números exatamente como aparecem nas linhas, mas tratando quando nescessário, por exmplo quando for moeda;
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
8) Se a pergunta pedir uma lista, você deve exibir tudo o que foi retornado do SQL e não apenas uma amostra.
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
