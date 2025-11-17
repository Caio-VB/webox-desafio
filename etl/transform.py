import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_integer_dtype,
    is_float_dtype,
)

from config import RESERVED_COLS


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
