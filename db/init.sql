-- Tabela-mãe de faturamento
CREATE TABLE IF NOT EXISTS faturamento (
    id SERIAL PRIMARY KEY,
    cliente_id TEXT NOT NULL,
    arquivo_nome TEXT NOT NULL,
    linha_numero INTEGER NOT NULL,

    data_emissao DATE NULL,
    data_vencimento DATE NULL,
    valor_total NUMERIC(18,2) NULL,
    status TEXT NULL,

    raw JSONB NOT NULL,

    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
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
