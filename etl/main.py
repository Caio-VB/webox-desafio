import os
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

import time

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://webox:weboxpass@localhost:5432/weboxdb",
)

engine = create_engine(DATABASE_URL)


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


def run_etl_for_file(file_path: Path, cliente_id: str):
    started_at = datetime.now(timezone.utc)
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
        finished_at = datetime.now(timezone.utc)

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
