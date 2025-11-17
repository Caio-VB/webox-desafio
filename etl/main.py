import time
from pathlib import Path

from config import INBOX_DIR, CLIENTE_ID_DEFAULT, POLL_INTERVAL, TABLE_NAME
from db import get_processados
from loader import run_etl_for_file, extract_cliente_id


def main():
    inbox_dir: Path = INBOX_DIR

    print(f"[ETL] Iniciando watcher em {inbox_dir}")
    print(f"[ETL] Cliente padrão (fallback): {CLIENTE_ID_DEFAULT}")
    print(f"[ETL] Intervalo de varredura: {POLL_INTERVAL} segundos")
    print(f"[ETL] Tabela de destino: {TABLE_NAME}")

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
                    print(f"[ETL] Encontrados {len(novos)} arquivo(s) novo(s).")
                else:
                    print("[ETL] Nenhum arquivo novo para processar.")

                for file in novos:
                    cliente_id = extract_cliente_id(file, CLIENTE_ID_DEFAULT)
                    print(f"[ETL] Arquivo {file.name} será processado para cliente {cliente_id}")
                    run_etl_for_file(file, cliente_id, table_name=TABLE_NAME)

        except Exception as e:
            print(f"[ETL] Erro inesperado no loop principal: {e}")

        print(f"[ETL] Aguardando {POLL_INTERVAL} segundos para nova varredura...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
