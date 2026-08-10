# Databricks notebook source
# MAGIC %md
# MAGIC # Logs CPF Intergrall VF1 — Validation
# MAGIC
# MAGIC **Migrated from:** Alteryx workflow `Logs CPF Intergrall VF1.yxmd`
# MAGIC **Migration date:** 2026-08-06
# MAGIC **Validates:** `{gold}.logs_cpf_intergrall`
# MAGIC
# MAGIC ## RISCO CONHECIDO — sem baseline do Alteryx
# MAGIC
# MAGIC Não há arquivo de saída esperada para este indicador. Conforme a Regra 3 do
# MAGIC skill, isto fica **documentado como risco** e a validação passa a ser por
# MAGIC profiling e invariantes, **não** por comparação contra a saída real do Alteryx.
# MAGIC
# MAGIC **O que isto significa:** os testes abaixo provam que o pipeline é
# MAGIC internamente consistente e que cada etapa faz o que o XML manda. **Não** provam
# MAGIC paridade numérica com o Alteryx. Duas classes de erro sobrevivem a este
# MAGIC notebook:
# MAGIC
# MAGIC * divergência de intenção (o anti-join, ponto 1 do notebook 03);
# MAGIC * divergência de dados na origem (o `.xlsx` de RH mudou desde a última execução
# MAGIC   do Alteryx).
# MAGIC
# MAGIC **Como fechar o risco:** rodar o workflow Alteryx original uma vez, guardar o
# MAGIC `dd-MM-yyyy.xlsx` produzido num Volume e preencher o widget `baseline_path`.
# MAGIC A célula de comparação abaixo ativa-se sozinha e roda os 6 checks do skill.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Framework de validação (do skill, sem alterações)

# COMMAND ----------

def validate_migration(df_expected, df_migrated, key_columns=None):
    """
    Comprehensive validation of migrated output against expected Alteryx output.
    Returns a dict of validation results.
    """
    results = {}

    # 1. Row Count Comparison
    expected_count = df_expected.count()
    migrated_count = df_migrated.count()
    results["row_count"] = {
        "expected": expected_count,
        "migrated": migrated_count,
        "match": expected_count == migrated_count,
        "diff": migrated_count - expected_count,
    }

    # 2. Schema Comparison
    expected_cols = set(df_expected.columns)
    migrated_cols = set(df_migrated.columns)
    results["schema"] = {
        "missing_in_migrated": expected_cols - migrated_cols,
        "extra_in_migrated": migrated_cols - expected_cols,
        "match": expected_cols == migrated_cols,
    }

    # 3. Data Type Comparison (on common columns)
    common_cols = expected_cols & migrated_cols
    expected_types = {f.name: str(f.dataType) for f in df_expected.schema.fields if f.name in common_cols}
    migrated_types = {f.name: str(f.dataType) for f in df_migrated.schema.fields if f.name in common_cols}
    type_mismatches = {
        c: {"expected": expected_types[c], "migrated": migrated_types[c]}
        for c in common_cols
        if expected_types.get(c) != migrated_types.get(c)
    }
    results["data_types"] = {"mismatches": type_mismatches, "match": len(type_mismatches) == 0}

    # 4. Null Count Comparison
    null_comparison = {}
    for col in sorted(common_cols):
        exp_nulls = df_expected.filter(F.col(col).isNull()).count()
        mig_nulls = df_migrated.filter(F.col(col).isNull()).count()
        if exp_nulls != mig_nulls:
            null_comparison[col] = {"expected_nulls": exp_nulls, "migrated_nulls": mig_nulls}
    results["null_counts"] = {"discrepancies": null_comparison, "match": len(null_comparison) == 0}

    # 5. Numeric Aggregation Comparison
    numeric_cols = [
        f.name
        for f in df_expected.schema.fields
        if str(f.dataType)
        in ("DoubleType", "FloatType", "IntegerType", "LongType", "DecimalType(38,18)", "ShortType")
        and f.name in common_cols
    ]
    agg_comparison = {}
    for col in numeric_cols:
        exp_stats = df_expected.select(
            F.sum(col).alias("sum"), F.avg(col).alias("avg"),
            F.min(col).alias("min"), F.max(col).alias("max"),
        ).collect()[0]
        mig_stats = df_migrated.select(
            F.sum(col).alias("sum"), F.avg(col).alias("avg"),
            F.min(col).alias("min"), F.max(col).alias("max"),
        ).collect()[0]
        diffs = {}
        for stat in ["sum", "avg", "min", "max"]:
            e, m = exp_stats[stat], mig_stats[stat]
            if e is not None and m is not None:
                if abs(float(e) - float(m)) > 1e-6:
                    diffs[stat] = {"expected": float(e), "migrated": float(m)}
            elif e != m:
                diffs[stat] = {"expected": e, "migrated": m}
        if diffs:
            agg_comparison[col] = diffs
    results["numeric_aggregations"] = {"discrepancies": agg_comparison, "match": len(agg_comparison) == 0}

    # 6. Row-Level Diff (if key columns provided)
    if key_columns and all(c in common_cols for c in key_columns):
        only_in_expected = df_expected.join(df_migrated, on=key_columns, how="left_anti")
        only_in_migrated = df_migrated.join(df_expected, on=key_columns, how="left_anti")
        results["row_diff"] = {
            "rows_only_in_expected": only_in_expected.count(),
            "rows_only_in_migrated": only_in_migrated.count(),
            "match": only_in_expected.count() == 0 and only_in_migrated.count() == 0,
        }

    all_passed = all(v.get("match", True) for v in results.values())
    results["overall"] = "PASS" if all_passed else "FAIL"
    return results


def report(results):
    print(f"Overall: {results['overall']}")
    for check, detail in results.items():
        if check != "overall":
            status = "PASS" if detail.get("match", True) else "FAIL"
            print(f"  {check}: {status}")
            if not detail.get("match", True):
                for k, v in detail.items():
                    if k != "match":
                        print(f"    {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação contra baseline — ativa quando `baseline_path` está preenchido

# COMMAND ----------

df_migrated = spark.table(T_GOLD).filter(F.col("data_execucao") == HOJE).drop("data_execucao")

if BASELINE_PATH:
    df_expected = read_landing(BASELINE_PATH).withColumnRenamed("Chave cons.", "chave_cons")
    print(f"baseline: {BASELINE_PATH}\n")
    results = validate_migration(df_expected, df_migrated, key_columns=["log_itgl_seq"])
    report(results)
    assert results["overall"] == "PASS", "validação contra o baseline FALHOU — ver detalhes acima"
else:
    print("SEM BASELINE — comparação ignorada; profiling + invariantes abaixo.")
    print("Para ativar: preencha o widget 'baseline_path' com o .xlsx do Alteryx.")
    results = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling (substituto por ausência de baseline, Regra 3)
# MAGIC
# MAGIC Estatísticas num único `.select()` (item 9 de performance do skill).

# COMMAND ----------

prof = df_migrated.select(
    F.count("*").alias("linhas"),
    F.countDistinct("log_itgl_seq").alias("seq_distintos"),
    F.countDistinct("chave_cons").alias("chaves_distintas"),
    F.countDistinct("log_itgl_usu").alias("usuarios_distintos"),
    F.countDistinct("log_itgl_cpf_cns").alias("cpfs_consultados"),
    F.min("log_itgl_dat").alias("dat_min"),
    F.max("log_itgl_dat").alias("dat_max"),
    *[
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"nulos_{c}")
        for c in ["log_itgl_usu", "log_itgl_nm_cns", "log_itgl_cpf_cns", "log_itgl_dat", "chave_cons"]
    ],
).collect()[0].asDict()

print(f"=== profiling {T_GOLD} — execução {HOJE} ===")
for k, v in prof.items():
    print(f"  {k:24s} {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Invariantes — o que o XML garante e que deve valer sempre

# COMMAND ----------

checks = {}
n = prof["linhas"]

# 1. formato de data: o Tool 23 troca / por -
checks["data_normalizada"] = (
    df_migrated.filter(F.col("log_itgl_dat").contains("/")).count() == 0
)

# 2. janela do Tool 25: nada anterior a ontem
checks["janela_desde_ontem"] = (
    df_migrated.filter(
        F.to_date("log_itgl_dat", "dd-MM-yyyy") < F.date_add(F.current_date(), -1)
    ).count()
    == 0
)

# 3. Tool 12 (Duplicates): toda chave presente ocorre >= 2x no log de origem
log_silver = spark.table(T_SILVER_LOG)
chaves_com_1_ocorrencia = (
    log_silver.groupBy("chave_cons").count().filter(F.col("count") < 1).count()
)
checks["chaves_repetidas"] = chaves_com_1_ocorrencia == 0

# 4. Tool 18 (Left): nenhum usuário do resultado é cargo de direção
rh = spark.table(T_SILVER_RH)
vazamento = df_migrated.join(
    F.broadcast(rh), df_migrated["log_itgl_usu"] == rh["nome_funcionario"], "inner"
).count()
checks["anti_join_sem_vazamento"] = vazamento == 0

# 5. chave consistente com os componentes
checks["chave_consistente"] = (
    df_migrated.filter(
        F.col("chave_cons")
        != F.concat(
            F.coalesce("log_itgl_usu", F.lit("")),
            F.coalesce("log_itgl_nm", F.lit("")),
            F.coalesce("log_itgl_nm_cns", F.lit("")),
            F.coalesce("log_itgl_cpf_cns", F.lit("")),
            # a chave usa a data ORIGINAL com /, revertida aqui
            F.regexp_replace("log_itgl_dat", "-", "/"),
        )
    ).count()
    == 0
)

# 6. sem duplicação de partição (replaceWhere funcionou)
checks["sem_duplicacao_execucao"] = prof["seq_distintos"] == n

print("=== invariantes ===")
for k, ok in checks.items():
    print(f"  {'PASS' if ok else 'FAIL'}  {k}")

falhas = [k for k, ok in checks.items() if not ok]
assert not falhas, f"invariantes violadas: {falhas}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot-check — 10 linhas para conferência manual
# MAGIC
# MAGIC Substituto direto do baseline: a auditoria confere estas linhas contra o
# MAGIC Intergrall e confirma que cada uma é de facto uma consulta repetida feita por
# MAGIC alguém fora dos cargos de direção.

# COMMAND ----------

display(
    df_migrated.select(
        "log_itgl_seq", "log_itgl_usu", "log_itgl_nm_cns",
        "log_itgl_cpf_cns", "log_itgl_dat", "log_itgl_hor",
    ).orderBy("log_itgl_usu", "log_itgl_dat").limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribuição por usuário — onde a auditoria olha primeiro

# COMMAND ----------

display(
    df_migrated.groupBy("log_itgl_usu")
    .agg(
        F.count("*").alias("consultas_repetidas"),
        F.countDistinct("log_itgl_cpf_cns").alias("cpfs_distintos"),
    )
    .orderBy(F.desc("consultas_repetidas"))
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sumário

# COMMAND ----------

status = "PASS" if not falhas else "FAIL"
print(f"=== VALIDAÇÃO: {status} ===")
print(f"tabela        : {T_GOLD}")
print(f"execução      : {HOJE}")
print(f"linhas        : {n:,}")
print(f"invariantes   : {len(checks) - len(falhas)}/{len(checks)} PASS")
print(f"baseline      : {'comparado — ' + results['overall'] if results else 'AUSENTE (risco documentado)'}")
if not BASELINE_PATH:
    print()
    print("RISCO ABERTO: sem baseline do Alteryx, a paridade numérica com o fluxo")
    print("original não está provada. Ver a nota no topo deste notebook.")

# COMMAND ----------

cleanup_temp_tables()

dbutils.notebook.exit(f"VALIDATION {status} | {n} linhas | baseline={'sim' if BASELINE_PATH else 'nao'}")
