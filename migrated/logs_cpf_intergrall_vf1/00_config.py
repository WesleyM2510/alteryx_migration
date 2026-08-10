# Databricks notebook source
# MAGIC %md
# MAGIC # Logs CPF Intergrall VF1 — Config & Helpers
# MAGIC
# MAGIC **Migrated from:** Alteryx workflow `Logs CPF Intergrall VF1.yxmd`
# MAGIC **Migration date:** 2026-08-06
# MAGIC **Medallion tier:** shared across Bronze / Silver / Gold
# MAGIC
# MAGIC Included in the other notebooks with `%run ./00_config`. Holds widgets,
# MAGIC path/table names, the landing reader and the serverless-safe `materialize()`
# MAGIC helper, so the four notebooks share one definition of each.

# COMMAND ----------

dbutils.widgets.text("catalog", "bmg_auditoria", "Catálogo")
dbutils.widgets.text("bronze_schema", "bronze", "Schema Bronze")
dbutils.widgets.text("silver_schema", "silver", "Schema Silver")
dbutils.widgets.text("gold_schema", "gold", "Schema Gold")
dbutils.widgets.text(
    "log_input_path",
    "/Volumes/bmg_auditoria/landing/auditoria/cat099_log_itgl/",
    "Log Intergrall (landing)",
)
dbutils.widgets.text(
    "rh_input_path",
    "/Volumes/bmg_auditoria/landing/auditoria/Base_Funcionarios_Atualizado.xlsx",
    "Base RH (.xlsx)",
)
dbutils.widgets.text(
    "target_volume",
    "/Volumes/bmg_auditoria/gold/resultado",
    "Volume de destino (.xlsx)",
)
dbutils.widgets.text(
    "baseline_path", "", "Baseline Alteryx p/ validação (opcional)"
)

CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.{dbutils.widgets.get('bronze_schema')}"
SILVER = f"{CATALOG}.{dbutils.widgets.get('silver_schema')}"
GOLD = f"{CATALOG}.{dbutils.widgets.get('gold_schema')}"

LOG_INPUT_PATH = dbutils.widgets.get("log_input_path")
RH_INPUT_PATH = dbutils.widgets.get("rh_input_path")
TARGET_VOLUME = dbutils.widgets.get("target_volume").rstrip("/")
BASELINE_PATH = dbutils.widgets.get("baseline_path").strip()

# Bronze — nomes originais preservados
T_BRONZE_LOG = f"{BRONZE}.cat099_log_itgl"
T_BRONZE_RH = f"{BRONZE}.base_funcionarios"
# Silver — cleansed & conformed, snake_case
T_SILVER_LOG = f"{SILVER}.log_itgl_consultas"
T_SILVER_RH = f"{SILVER}.rh_funcionarios"
# Gold — o indicador de auditoria
T_GOLD = f"{GOLD}.logs_cpf_intergrall"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute type — decide `.cache()` vs. temp Delta
# MAGIC
# MAGIC Regra 8 do skill: `.cache()` falha em serverless. Detectado uma vez aqui,
# MAGIC e `materialize()` escolhe o caminho conforme o ambiente.

# COMMAND ----------

def _is_serverless() -> bool:
    """True quando não há um cluster clássico dedicado (serverless / Connect)."""
    try:
        conf = spark.conf
        if conf.get("spark.databricks.clusterUsageTags.clusterId", None) is None:
            return True
        # serverless anuncia-se explicitamente em runtimes recentes
        return conf.get("spark.databricks.clusterUsageTags.clusterType", "") == "serverless"
    except Exception:
        return True  # o caminho seguro: temp Delta funciona em qualquer compute


IS_SERVERLESS = _is_serverless()

_TEMP_TABLES: list[str] = []


def materialize(df, name: str):
    """Quebra a linhagem de um DataFrame reutilizado.

    Em cluster clássico usa `.cache()`; em serverless grava uma tabela Delta
    temporária e relê. Column mapping está ligado porque a base de RH tem
    colunas com espaço e acento (`NOME FUNCIONÁRIO`, `CARGO - NOME`).
    """
    if not IS_SERVERLESS:
        df = df.cache()
        df.count()  # força a materialização enquanto o cache está quente
        return df

    table = f"{SILVER}._tmp_{name}"
    (
        df.write.mode("overwrite")
        .option("delta.columnMapping.mode", "name")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .saveAsTable(table)
    )
    _TEMP_TABLES.append(table)
    return spark.table(table)


def cleanup_temp_tables():
    for t in _TEMP_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    _TEMP_TABLES.clear()


print(f"compute: {'serverless (temp Delta)' if IS_SERVERLESS else 'classic (.cache)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitor de landing
# MAGIC
# MAGIC Despacha por extensão. `.xlsx` lê com pandas e `dtype=str` — CPF e matrícula
# MAGIC têm zeros à esquerda que a inferência de tipo destrói.
# MAGIC
# MAGIC Nota de performance (item 11 do skill): `pd.read_excel` é single-threaded.
# MAGIC Para produção, converter a base de RH para Delta uma vez e ler nativamente;
# MAGIC é exatamente o que a camada Bronze aqui faz.

# COMMAND ----------

import os


def read_landing(path: str, sheet: str | None = None):
    lower = path.lower()

    if lower.endswith((".xlsx", ".xls")):
        import pandas as pd

        pdf = pd.read_excel(path, sheet_name=sheet, dtype=str, engine="openpyxl")
        pdf = pdf.where(pdf.notna(), None)
        pdf = pdf.loc[:, [c for c in pdf.columns if not str(c).startswith("Unnamed:")]]
        pdf.columns = [str(c).strip() for c in pdf.columns]
        return spark.createDataFrame(pdf)

    if lower.endswith(".csv"):
        return spark.read.option("header", True).option("inferSchema", False).csv(path)

    if lower.endswith(".parquet") or os.path.isdir(path):
        try:
            return spark.read.format("delta").load(path)
        except Exception:
            return spark.read.parquet(path)

    raise ValueError(f"Formato de landing não reconhecido: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nomes de saída (Alteryx Tools 19 / 27)
# MAGIC
# MAGIC O Alteryx montava `FULLPATCH` / `FULLPATCH2` / `AssuntoEmail` com `Dia`, `Mês`,
# MAGIC `Ano` e o mês em português. A mesma estrutura de pastas é reproduzida no Volume.

# COMMAND ----------

from datetime import date, timedelta

MESES_PT = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
]

HOJE = date.today()
DIA, MES, ANO = f"{HOJE.day:02d}", f"{HOJE.month:02d}", str(HOJE.year)
FOLDER_MONTH = MESES_PT[HOJE.month - 1]

ASSUNTO_EMAIL = f"Monitoramento CPF Intergrall{DIA}_{MES}_{ANO}"
OUTPUT_DIR = f"{TARGET_VOLUME}/{ANO}/{FOLDER_MONTH}"
OUTPUT_XLSX = f"{OUTPUT_DIR}/{DIA}-{MES}-{ANO}.xlsx"
SHEET_NAME = "Monitoramento CPF Intergrall"
EMAIL_TO = "auditoria.continua@bancobmg.com.br"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for s in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")

print(f"bronze={BRONZE}  silver={SILVER}  gold={GOLD}")
print(f"xlsx destino: {OUTPUT_XLSX}")
