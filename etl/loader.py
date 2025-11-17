from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pandas as pd
from sqlalchemy import text

from config import TABLE_NAME
from db import engine, get_existing_columns
from transform import (
    normalize_columns,
    rename_reserved_columns,
    infer_column_type,
)


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


def ensure_columns_exist(df: pd.DataFrame, table_name: str = TABLE_NAME):
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
            alter_sql = text(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {col_type};')
            print(f"[ETL] ALTER TABLE: adicionando coluna {col} ({col_type})")
            conn.execute(alter_sql)


def run_etl_for_file(file_path: Path, cliente_id: str, table_name: str = TABLE_NAME):
    """
    Executa o ETL completo para um único arquivo Excel:
    - lê planilha
    - normaliza colunas
    - garante colunas na tabela
    - insere linhas em {table_name}
    - registra job em etl_jobs
    """
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
        ensure_columns_exist(df, table_name=table_name)

        # 3) Prepara INSERT com todas as colunas: fixas + do Excel
        excel_cols = list(df.columns)
        all_cols = ["cliente_id", "arquivo_nome", "linha_numero"] + excel_cols

        columns_sql = ", ".join(f'"{c}"' for c in all_cols)
        values_sql = ", ".join(f":{c}" for c in all_cols)

        insert_sql = text(
            f"""
            INSERT INTO {table_name} (
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
                rec: Dict[str, object] = {
                    "cliente_id": cliente_id,
                    "arquivo_nome": arquivo_nome,
                    "linha_numero": idx + 1,
                }

                for col in excel_cols:
                    rec[col] = row.get(col)

                conn.execute(insert_sql, rec)

            rows_imported = len(df)
            print(f"[ETL] Inseridas {rows_imported} linhas em {table_name}.")

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
