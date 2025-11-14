-- Tabela-mãe
CREATE TABLE faturamento (
    id SERIAL PRIMARY KEY,
    cliente_id TEXT NOT NULL,
    arquivo_nome TEXT NOT NULL,
    linha_numero INTEGER NOT NULL,
    raw JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);


-- Histórico dos jobs de ETL
CREATE TABLE IF NOT EXISTS etl_jobs (
    id SERIAL PRIMARY KEY,
    arquivo_nome TEXT NOT NULL,
    cliente_id TEXT NOT NULL,
    status TEXT NOT NULL,              -- 'success' ou 'fail'
    rows_imported INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP WITHOUT TIME ZONE,
    error_message TEXT
);
