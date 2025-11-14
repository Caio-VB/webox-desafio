import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_integer_dtype,
    is_float_dtype,
)

from sqlalchemy import create_engine, text
import time

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://webox:weboxpass@localhost:5432/weboxdb",
)

engine = create_engine(DATABASE_URL)

# Nomes de colunas reservadas para metadados da tabela
RESERVED_COLS = {"id", "cliente_id", "arquivo_nome", "linha_numero", "created_at"}


def extract_cliente_id(file_path: Path, default_cliente_id: str) -> str:
    """
    Extrai o cliente_id do nome do arquivo.

    Convenção:
      <cliente_id>__qualquer_coisa.xlsx

    Exemplos:
      cliente_acme__faturamento_2025-01.xlsx -> cliente_acme
      grupo_x__jan2025.xlsx                  -> grupo_x

    Se não encontrar "__" no nome, usa o default_cliente_id.
    """
    name = file_path.stem  # nome sem extensão
    if "__" in name:
        return name.split("__", 1)[0]
    return default_cliente_id


def get_processados() -> set[str]:
    """
    Busca no banco os arquivos já processados com sucesso (qualquer cliente).
    Assim evitamos processar o mesmo arquivo várias vezes.
    """
    sql = text(
        """
        SELECT DISTINCT arquivo_nome
        FROM etl_jobs
        WHERE status = 'success'
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql)
        nomes = {row[0] for row in result.fetchall()}
    return nomes


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de colunas para snake_case, sem acentos."""
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .str.replace(r"[^0-9a-zA-Z]+", "_", regex=True)
    )
    return df


def rename_reserved_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se a planilha trouxer colunas com nomes reservados (cliente_id, arquivo_nome, etc.),
    renomeia essas colunas para evitar conflito com os metadados da tabela.
    Exemplo: cliente_id -> cliente_id_excel
    """
    df = df.copy()
    renames = {}

    for col in df.columns:
        if col in RESERVED_COLS:
            new_name = f"{col}_excel"
            suffix = 2
            # Garante que o nome novo não colida com outras colunas
            while new_name in df.columns or new_name in RESERVED_COLS:
                new_name = f"{col}_excel_{suffix}"
                suffix += 1
            renames[col] = new_name

    if renames:
        print(f"[ETL] Renomeando colunas reservadas da planilha: {renames}")
        df = df.rename(columns=renames)

    return df


def get_existing_columns(table_name: str = "faturamento") -> set[str]:
    """
    Busca as colunas já existentes na tabela faturamento.
    """
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :table
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql, {"table": table_name})
        return {row[0] for row in result.fetchall()}


def infer_column_type(series: pd.Series) -> str:
    """
    Inferir tipo SQL básico (DATE, NUMERIC, TEXT) com heurística simples.

    Regras:
    - Se o dtype já é datetime -> DATE
    - Se é int/float -> NUMERIC
    - Se é texto/objeto -> tenta DATE, depois NUMERIC
    """
    s = series.dropna()

    if s.empty:
        return "TEXT"

    # 1) Se já é datetime em pandas, vai como DATE
    if is_datetime64_any_dtype(s):
        return "DATE"

    # 2) Se é numérico (int/float), não tem conversa: NUMERIC
    if is_integer_dtype(s) or is_float_dtype(s):
        return "NUMERIC"

    # 3) Para colunas de texto/objeto: tenta data, depois número
    #    (e só se >90% dos valores forem válidos)
    try:
        s_dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if s_dt.notna().mean() > 0.9:
            return "DATE"
    except Exception:
        pass

    try:
        s_num = pd.to_numeric(s, errors="coerce")
        if s_num.notna().mean() > 0.9:
            return "NUMERIC"
    except Exception:
        pass

    # 4) Default
    return "TEXT"


def ensure_columns_exist(df: pd.DataFrame, table_name: str = "faturamento"):
    """
    Garante que todas as colunas do DataFrame existam na tabela.
    Faz ALTER TABLE ADD COLUMN se faltar alguma.
    """
    existing = get_existing_columns(table_name)

    # TODAS as colunas vindas do Excel (já normalizadas e com reservadas renomeadas)
    excel_cols = list(df.columns)
    novas = [c for c in excel_cols if c not in existing]

    if not novas:
        print("[ETL] Nenhuma coluna nova para criar.")
        return

    print(f"[ETL] Criando {len(novas)} coluna(s) nova(s) na tabela {table_name}: {novas}")

    with engine.begin() as conn:
        for col in novas:
            col_type = infer_column_type(df[col])
            # nomes já foram normalizados, mas ainda assim usamos aspas
            alter_sql = text(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {col_type};')
            print(f"[ETL] ALTER TABLE: adicionando coluna {col} ({col_type})")
            conn.execute(alter_sql)


def run_etl_for_file(file_path: Path, cliente_id: str):
    started_at = datetime.now(timezone.utc)
    arquivo_nome = file_path.name
    status_job = "success"
    rows_imported = 0
    error_message: str | None = None

    print(f"[ETL] Iniciando processamento de {arquivo_nome} para cliente {cliente_id}")

    try:
        # 1) Lê o Excel e normaliza colunas
        df = pd.read_excel(file_path)
        df = normalize_columns(df)
        df = rename_reserved_columns(df)

        if df.empty:
            raise ValueError("Planilha sem linhas de dados.")

        # 2) Garante que as colunas do Excel existam na tabela (DDL dinâmico)
        ensure_columns_exist(df, table_name="faturamento")

        # 3) Prepara INSERT com todas as colunas: fixas + do Excel
        excel_cols = list(df.columns)
        all_cols = ["cliente_id", "arquivo_nome", "linha_numero"] + excel_cols

        columns_sql = ", ".join(f'"{c}"' for c in all_cols)
        values_sql = ", ".join(f":{c}" for c in all_cols)

        insert_sql = text(
            f"""
            INSERT INTO faturamento (
                {columns_sql}
            )
            VALUES (
                {values_sql}
            )
            """
        )

        # 4) Insere linha a linha
        with engine.begin() as conn:
            for idx, row in df.iterrows():
                rec: dict[str, object] = {
                    # cliente_id lógico (do nome do arquivo)
                    "cliente_id": cliente_id,
                    "arquivo_nome": arquivo_nome,
                    "linha_numero": idx + 1,
                }

                for col in excel_cols:
                    rec[col] = row.get(col)

                conn.execute(insert_sql, rec)

            rows_imported = len(df)
            print(f"[ETL] Inseridas {rows_imported} linhas em faturamento.")

    except Exception as e:
        status_job = "fail"
        error_message = str(e)
        print(f"[ETL] Erro ao processar {arquivo_nome}: {error_message}")

    finally:
        finished_at = datetime.now(timezone.utc)

        # 5) Registra job em etl_jobs
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO etl_jobs (
                        arquivo_nome,
                        cliente_id,
                        status,
                        rows_imported,
                        started_at,
                        finished_at,
                        error_message
                    )
                    VALUES (
                        :arquivo_nome,
                        :cliente_id,
                        :status,
                        :rows_imported,
                        :started_at,
                        :finished_at,
                        :error_message
                    )
                    """
                ),
                {
                    "arquivo_nome": arquivo_nome,
                    "cliente_id": cliente_id,
                    "status": status_job,
                    "rows_imported": rows_imported,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "error_message": error_message,
                },
            )

        if status_job == "success":
            print(f"[ETL] Job registrado com sucesso em etl_jobs.")
        else:
            print(f"[ETL] Job com falha registrado em etl_jobs.")


def main():
    inbox_dir = Path(os.getenv("INBOX_DIR", "/data/inbox"))
    default_cliente_id = os.getenv("CLIENTE_ID", "cliente_demo")
    poll_interval = int(os.getenv("POLL_INTERVAL", "30"))

    print(f"[ETL] Iniciando watcher em {inbox_dir}")
    print(f"[ETL] Cliente padrão (fallback): {default_cliente_id}")
    print(f"[ETL] Intervalo de varredura: {poll_interval} segundos")

    while True:
        try:
            if not inbox_dir.exists():
                print(f"[ETL] Diretório {inbox_dir} não existe, aguardando...")
            else:
                todos_arquivos = list(inbox_dir.glob("*.xlsx"))
                if not todos_arquivos:
                    print(f"[ETL] Nenhum .xlsx encontrado em {inbox_dir}")

                processados = get_processados()
                novos = [f for f in todos_arquivos if f.name not in processados]

                if novos:
                    print(f"[ETL] Encontrados {len(novos)} arquivos novos.")
                else:
                    print("[ETL] Nenhum arquivo novo para processar.")

                for file in novos:
                    cliente_id = extract_cliente_id(file, default_cliente_id)
                    print(f"[ETL] Arquivo {file.name} será processado para cliente {cliente_id}")
                    run_etl_for_file(file, cliente_id)

        except Exception as e:
            print(f"[ETL] Erro inesperado no loop principal: {e}")

        print(f"[ETL] Aguardando {poll_interval} segundos para nova varredura...")
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
