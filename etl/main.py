import os
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://webox:weboxpass@localhost:5432/weboxdb",
)

engine = create_engine(DATABASE_URL)


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


def run_etl_for_file(file_path: Path, cliente_id: str):
    started_at = datetime.utcnow()
    arquivo_nome = file_path.name
    status = "success"
    rows_imported = 0
    error_message: str | None = None

    print(f"[ETL] Iniciando processamento de {arquivo_nome} para cliente {cliente_id}")

    try:
        df = pd.read_excel(file_path)
        df = normalize_columns(df)

        records = []
        for idx, row in df.iterrows():
            raw = row.to_dict()

            data_emissao = raw.get("data_emissao")
            data_vencimento = raw.get("data_vencimento")
            valor_total = raw.get("valor_total")
            status_nf = raw.get("status") or raw.get("situacao")

            records.append(
                {
                    "cliente_id": cliente_id,
                    "arquivo_nome": arquivo_nome,
                    "linha_numero": idx + 1,
                    "data_emissao": data_emissao,
                    "data_vencimento": data_vencimento,
                    "valor_total": valor_total,
                    "status": status_nf,
                    # Enviamos como JSON string e fazemos CAST no SQL
                    "raw": json.dumps(raw, default=str),
                }
            )

        with engine.begin() as conn:
            insert_sql = text(
                """
                INSERT INTO faturamento (
                    cliente_id,
                    arquivo_nome,
                    linha_numero,
                    data_emissao,
                    data_vencimento,
                    valor_total,
                    status,
                    raw
                )
                VALUES (
                    :cliente_id,
                    :arquivo_nome,
                    :linha_numero,
                    :data_emissao,
                    :data_vencimento,
                    :valor_total,
                    :status,
                    CAST(:raw AS JSONB)
                )
                """
            )

            for rec in records:
                conn.execute(insert_sql, rec)

            rows_imported = len(records)
            print(f"[ETL] Inseridas {rows_imported} linhas em faturamento.")

    except Exception as e:
        status = "fail"
        error_message = str(e)
        print(f"[ETL] Erro ao processar {arquivo_nome}: {error_message}")

    finally:
        finished_at = datetime.utcnow()

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
                    "status": status,
                    "rows_imported": rows_imported,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "error_message": error_message,
                },
            )

        if status == "success":
            print(f"[ETL] Job registrado com sucesso em etl_jobs.")
        else:
            print(f"[ETL] Job com falha registrado em etl_jobs.")


def main():
    inbox_dir = Path(os.getenv("INBOX_DIR", "/data/inbox"))
    cliente_id = os.getenv("CLIENTE_ID", "cliente_demo")

    print(f"[ETL] Lendo arquivos de {inbox_dir}")
    excel_files = list(inbox_dir.glob("*.xlsx"))

    if not excel_files:
        print(f"[ETL] Nenhum arquivo .xlsx encontrado em {inbox_dir}")
        return

    for file in excel_files:
        run_etl_for_file(file, cliente_id)


if __name__ == "__main__":
    main()
