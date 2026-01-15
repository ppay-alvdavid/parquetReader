import tkinter as tk
from tkinter import filedialog
import pyarrow.parquet as pq
import json
from datetime import datetime, date, timezone
import pandas as pd
from decimal import Decimal
import sys
import os

# ==============================
# CONFIGURAÇÕES
# ==============================
OUTPUT_FILE = "parquet_output.json"
BATCH_SIZE = 50_000


# ==============================
# NORMALIZAÇÃO FORTE (JSON-SAFE)
# ==============================
def normalizar(obj):
    """
    Converte tipos não serializáveis para JSON-safe
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    if isinstance(obj, Decimal):
        # Decide como serializar Decimal:
        # int se possível, senão float
        if obj % 1 == 0:
            return int(obj)
        return float(obj)

    return obj


def normalizar_registro(registro: dict) -> dict:
    """
    Normaliza recursivamente cada registro
    (evita erro de circular reference do json)
    """
    return {k: normalizar(v) for k, v in registro.items()}


# ==============================
# TKINTER — SELEÇÃO DE ARQUIVOS
# ==============================
def selecionar_parquets():
    root = tk.Tk()
    root.withdraw()

    return filedialog.askopenfilenames(
        title="Selecione arquivos Parquet",
        filetypes=[("Parquet files", "*.parquet")]
    )


# ==============================
# LEITURA DEFENSIVA DO PARQUET
# ==============================
def ler_parquet_stream(caminho):
    registros = []

    try:
        pf = pq.ParquetFile(caminho)

        for batch in pf.iter_batches(batch_size=BATCH_SIZE):
            for linha in batch.to_pylist():
                registros.append(normalizar_registro(linha))

    except Exception as e:
        print(f"[ERRO] Falha ao processar {caminho}: {e}", file=sys.stderr)

    return registros


# ==============================
# ESCRITA DO JSON
# ==============================
def salvar_json(dados, caminho_saida):
    with open(caminho_saida, "w", encoding="utf-8") as f:
        json.dump(
            dados,
            f,
            ensure_ascii=False,
            indent=2
        )


# ==============================
# MAIN
# ==============================
def main():
    arquivos = selecionar_parquets()

    if not arquivos:
        print("[INFO] Nenhum arquivo selecionado.")
        return

    resultado = []

    for arquivo in arquivos:
        print(f"[INFO] Lendo: {arquivo}")
        registros = ler_parquet_stream(arquivo)
        resultado.extend(registros)
        print(f"[INFO] Registros acumulados: {len(resultado)}")

    if not resultado:
        print("[WARN] Nenhum dado extraído.")
        return

    saida = {
        "metadata": {
            "total_registros": len(resultado),
            "arquivos_lidos": [os.path.basename(a) for a in arquivos],
            "gerado_em": datetime.now(timezone.utc).isoformat()
        },
        "data": resultado
    }

    salvar_json(saida, OUTPUT_FILE)

    print(f"\n[OK] Arquivo gerado com sucesso: {OUTPUT_FILE}")
    print(f"[OK] Total de registros: {len(resultado)}")


if __name__ == "__main__":
    main()
