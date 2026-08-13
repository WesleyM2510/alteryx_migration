---
name: alteryx-to-pyspark
description: "Migrate, convert, or translate Alteryx Designer workflows to PySpark/Python on Databricks, following medallion architecture with mandatory output validation. Use when: (1) the user asks to migrate/convert/translate an Alteryx workflow, (2) a .yxmd/.yxwz/.yxmc file is provided or referenced, (3) the user describes an Alteryx workflow or names Alteryx tools (Input Data, Join, Summarize, Filter, Formula, Multi-Row Formula, Cross Tab, Append Fields, Unique, Browse), (4) work involves an Alteryx-to-Databricks migration estate or bulk workflow conversion."
---

# Alteryx Migration to PySpark on Databricks

## Skill Overview

This skill provides structured guidance for migrating Alteryx Designer workflows to Python/PySpark code running on Databricks. It enforces medallion architecture best practices, mandatory output validation, and quality assurance checks comparing migrated code output against expected Alteryx output.

**When to use this skill:**
- User asks to migrate, convert, or translate an Alteryx workflow to PySpark/Python
- User provides an Alteryx XML (.yxmd, .yxwz, .yxmc) file or describes an Alteryx workflow
- User mentions Alteryx tool names (e.g., Input Data, Join, Summarize, Filter, Formula)

---

## CRITICAL: Pre-Migration Checklist

Before writing ANY migration code, you MUST verify the following. Do NOT proceed until all items are confirmed.

### 1. Expected Output File (MANDATORY)

Check if the user has provided an expected output file (CSV, Parquet, Delta, Excel, or any tabular format) that represents the correct output of the Alteryx workflow.

**If an expected output file exists:**
- Load it and profile it (row count, column names, dtypes, sample rows, null counts)
- Store it as the validation baseline
- Use it for quality assurance after migration

**If NO expected output file is provided, STOP and ask:**

> I need an expected output file to validate the migrated code against the original Alteryx workflow results. Please provide one of the following:
> - A CSV/Parquet/Excel file with the expected output data
> - A path to an existing Delta table with expected results
> - A sample of the expected output (at minimum: column names, row count, and 5-10 sample rows)
>
> Without this, I cannot guarantee the migration produces correct results.

### 2. Output Save Location (MANDATORY)

Confirm where the migrated notebooks and output data should be saved.

**Ask the user if not provided:**

> Where should I save the migration artifacts? I need:
> 1. **Notebook save path** — e.g., `/Workspace/Users/you@company.com/migrations/workflow_name`
> 2. **Output data location** — a Unity Catalog target: `catalog.schema.table_name` or a Volume path: `/Volumes/catalog/schema/volume/path`
> 3. **Medallion tier** — Which layer does this output belong to? (Bronze / Silver / Gold)

### 3. Source Data Inventory

Identify all input data sources from the Alteryx workflow:
- File paths (CSV, Excel, Parquet, JSON)
- Database connections (SQL Server, Oracle, PostgreSQL, etc.)
- API endpoints
- Alteryx Gallery data connections

Map each source to a Databricks equivalent (Unity Catalog table, Volume file, external connection).

---

## Medallion Architecture Mapping

All migrated code MUST follow the Databricks medallion (multi-hop) architecture. Map Alteryx workflow stages to the appropriate tier.

### Bronze Layer (Raw Ingestion)

**Purpose:** Land raw data as-is from source systems. Minimal transformation.

**Alteryx equivalents → Bronze:**
- Input Data tool → `spark.read` / Auto Loader / `read_files()`
- Connect In-DB tool → JDBC/ODBC reads
- Download tool → API ingestion scripts
- Directory tool → File listing from Volumes

**Best practices:**
- Preserve original column names and types
- Add ingestion metadata: `_ingested_at`, `_source_file`, `_batch_id`
- Store as Delta tables in a `bronze` schema
- Use `COPY INTO` or Auto Loader for incremental file ingestion
- Never apply business logic at this layer

```python
# Bronze pattern
from pyspark.sql import functions as F

df_bronze = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/catalog/schema/volume/raw_files/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

df_bronze.write.mode("append").saveAsTable("catalog.bronze.table_name")
```

### Silver Layer (Cleansed & Conformed)

**Purpose:** Clean, deduplicate, validate, and conform data. This is where most Alteryx transformation logic lives.

**Alteryx equivalents → Silver:**
- Data Cleansing tool → `.dropDuplicates()`, null handling, type casting
- Filter tool → `.filter()` / `.where()`
- Formula tool → `.withColumn()` with expressions
- Multi-Row Formula → Window functions
- Select tool → `.select()`, `.withColumnRenamed()`
- Sort tool → `.orderBy()`
- Sample tool → `.limit()` / `.sample()`
- Unique tool → `.dropDuplicates()`
- Imputation tool → `.fillna()`, `.na.fill()`
- DateTime tool → Date/time functions from `pyspark.sql.functions`
- RegEx tool → `F.regexp_extract()`, `F.regexp_replace()`
- Find Replace tool → `F.regexp_replace()`, `F.when().otherwise()`
- Auto Field tool → Schema enforcement / explicit casting

**Best practices:**
- Apply data quality checks (null rates, value ranges, referential integrity)
- Enforce schema with explicit column types
- Remove exact duplicates
- Standardize column names (snake_case, lowercase)
- Store as Delta tables in a `silver` schema
- Partition by date or high-cardinality business key when appropriate

```python
# Silver pattern
df_silver = (
    spark.read.table("catalog.bronze.table_name")
    .dropDuplicates(["primary_key"])
    .filter(F.col("status").isNotNull())
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumnRenamed("oldName", "new_name")
)

df_silver.write.mode("overwrite").saveAsTable("catalog.silver.table_name")
```

### Gold Layer (Business-Level Aggregates)

**Purpose:** Business-ready datasets optimized for analytics and reporting.

**Alteryx equivalents → Gold:**
- Summarize tool → `.groupBy().agg()`
- Cross Tab tool → Pivot operations
- Join tool → `.join()`
- Union tool → `.unionByName()`
- Append Fields tool → `.crossJoin()`
- Transpose tool → Unpivot with `stack()`
- Weighted Average → Custom aggregations with `.agg()`
- Running Total → Window functions with `F.sum().over()`
- Pearson Correlation → `df.stat.corr()`
- Frequency tool → `.groupBy().count()`
- Reporting tools → Results feed dashboards or BI tools

**Best practices:**
- Align with business glossary / metric definitions
- Optimize for query patterns (Z-ORDER, liquid clustering)
- Store as Delta tables in a `gold` schema
- Document metric calculations as column comments
- These tables serve dashboards, ML features, and ad-hoc analysis

```python
# Gold pattern
df_gold = (
    spark.read.table("catalog.silver.table_name")
    .groupBy("region", "product_category", "event_date")
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("user_id").alias("unique_users"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("order_value").alias("avg_order_value")
    )
)

df_gold.write.mode("overwrite").saveAsTable("catalog.gold.summary_table")
```

---

## Alteryx Tool → PySpark Reference

Comprehensive mapping of Alteryx Designer tools to PySpark equivalents.

### Input / Output Tools

| Alteryx Tool | PySpark Equivalent | Notes |
|---|---|---|
| Input Data (CSV) | `spark.read.csv(path, header=True, inferSchema=True)` | Use `read_files()` in SQL |
| Input Data (Excel) | `spark.read.format("com.crealytics.spark.excel").load()` or pandas then convert | Install `spark-excel` package |
| Input Data (DB) | `spark.read.jdbc(url, table, properties)` | Use Lakehouse Federation for persistent connections |
| Input Data (Parquet) | `spark.read.parquet(path)` | Native Spark support |
| Output Data (CSV) | `df.write.csv(path, header=True)` | Prefer Delta: `.write.saveAsTable()` |
| Output Data (DB) | `df.write.jdbc(url, table, mode, properties)` | Or write to Delta table |
| Browse | `display(df)` or `df.show()` | Use `display()` in Databricks |
| Directory | `dbutils.fs.ls(path)` or `spark.read.format("binaryFile").load(path)` | |

### Preparation Tools

| Alteryx Tool | PySpark Equivalent | Notes |
|---|---|---|
| Select | `df.select("col1", "col2").withColumnRenamed("old", "new")` | Also: `.drop()` to remove columns |
| Filter | `df.filter(F.col("x") > 10)` | Chain multiple with `&` / `|` |
| Formula | `df.withColumn("new_col", <expression>)` | Use `F.when().otherwise()` for conditionals |
| Multi-Row Formula | Window functions: `F.lag()`, `F.lead()`, `F.sum().over()` | Define `Window.partitionBy().orderBy()` |
| Sort | `df.orderBy(F.col("x").desc())` | |
| Sample | `df.sample(fraction=0.1)` or `df.limit(100)` | |
| Unique | `df.dropDuplicates(["key_col"])` | |
| Data Cleansing | `df.na.fill(0)`, `df.na.drop()`, `F.trim()`, `F.lower()` | Combine multiple operations |
| Auto Field | Explicit `.cast()` per column | |
| Imputation | `df.na.fill({"col": value})` or `Imputer` from ML | |
| DateTime | `F.to_date()`, `F.to_timestamp()`, `F.date_add()`, `F.datediff()` | |
| Multi-Field Formula | Loop over columns: `for c in cols: df = df.withColumn(c, expr)` | Use `reduce()` for functional style |
| Generate Rows | `spark.range(n)` with `.withColumn()` | |
| Record ID | `F.monotonically_increasing_id()` | Not guaranteed sequential |
| Running Total | `F.sum("col").over(Window.orderBy("date"))` | |
| Tile | `F.ntile(n).over(Window.orderBy("col"))` | |

### Join Tools

| Alteryx Tool | PySpark Equivalent | Notes |
|---|---|---|
| Join | `df1.join(df2, on="key", how="inner")` | Left/Right unmatched: use `how="left_anti"` |
| Union | `df1.unionByName(df2, allowMissingColumns=True)` | |
| Append Fields | `df1.crossJoin(df2)` | Caution: cartesian product |
| Find Replace | `df.join(lookup_df, on="key").withColumn(...)` | Or use `F.when()` for small mappings |
| Fuzzy Match | Levenshtein: `F.levenshtein(a, b)`, Soundex: `F.soundex()` | For advanced: `spark-nlp` or custom UDF |
| Join Multiple | Chain `.join()` calls | Use broadcast for small tables |

### Transform Tools

| Alteryx Tool | PySpark Equivalent | Notes |
|---|---|---|
| Summarize | `df.groupBy("col").agg(F.sum(), F.avg(), F.count(), ...)` | |
| Cross Tab (Pivot) | `df.groupBy("row").pivot("col").agg(F.sum("val"))` | |
| Transpose (Unpivot) | `stack()` in `selectExpr` | `df.selectExpr("id", "stack(3, 'a', a, 'b', b, 'c', c) as (key, value)")` |
| Count Records | `df.count()` | |
| Weighted Average | `F.sum(F.col("val") * F.col("weight")) / F.sum("weight")` | |
| Pearson Correlation | `df.stat.corr("col1", "col2")` | For matrix: `Correlation.corr(df, "features")` |
| Frequency | `df.groupBy("col").count().withColumn("pct", F.col("count") / df.count())` | |

### Parse / String Tools

| Alteryx Tool | PySpark Equivalent | Notes |
|---|---|---|
| RegEx | `F.regexp_extract()`, `F.regexp_replace()` | Java regex syntax |
| Text To Columns | `F.split("col", "delimiter")` then access with `[0]`, `[1]` | |
| Column To Rows | Explode: `F.explode(F.split("col", ","))` | |
| XML Parse | `spark.read.format("xml")` | Install `spark-xml` package |
| JSON Parse | `F.from_json()`, `F.get_json_object()` | Define schema with StructType |

### Spatial / Macro Tools

| Alteryx Tool | PySpark Equivalent | Notes |
|---|---|---|
| Spatial Match | Use `geopandas` or `sedona` (GeoSpark) | Not native Spark—install separately |
| Trade Area | Haversine UDF or `sedona` buffer | |
| Iterative Macro | `while` loop with convergence check | Avoid collect() in loops |
| Batch Macro | Parameterized function called in a loop or `for` comprehension | |
| Dynamic Input | Parameterize paths: `spark.read.load(path_variable)` | |

---

## Output Quality Validation Framework

**This step is MANDATORY.** After migration, you MUST compare the PySpark output against the expected Alteryx output.

### Validation Procedure

Run ALL of the following checks. Report results to the user.

```python
import pyspark.sql.functions as F

def validate_migration(df_expected, df_migrated, key_columns=None):
    """
    Comprehensive validation of migrated output against expected Alteryx output.
    Returns a dict of validation results.
    """
    results = {}

    # ------------------------------------------------------------------
    # 1. Row Count Comparison
    # ------------------------------------------------------------------
    expected_count = df_expected.count()
    migrated_count = df_migrated.count()
    results["row_count"] = {
        "expected": expected_count,
        "migrated": migrated_count,
        "match": expected_count == migrated_count,
        "diff": migrated_count - expected_count
    }

    # ------------------------------------------------------------------
    # 2. Schema Comparison
    # ------------------------------------------------------------------
    expected_cols = set(df_expected.columns)
    migrated_cols = set(df_migrated.columns)
    results["schema"] = {
        "missing_in_migrated": expected_cols - migrated_cols,
        "extra_in_migrated": migrated_cols - expected_cols,
        "match": expected_cols == migrated_cols
    }

    # ------------------------------------------------------------------
    # 3. Data Type Comparison (on common columns)
    # ------------------------------------------------------------------
    common_cols = expected_cols & migrated_cols
    expected_types = {f.name: str(f.dataType) for f in df_expected.schema.fields if f.name in common_cols}
    migrated_types = {f.name: str(f.dataType) for f in df_migrated.schema.fields if f.name in common_cols}
    type_mismatches = {c: {"expected": expected_types[c], "migrated": migrated_types[c]}
                       for c in common_cols if expected_types.get(c) != migrated_types.get(c)}
    results["data_types"] = {
        "mismatches": type_mismatches,
        "match": len(type_mismatches) == 0
    }

    # ------------------------------------------------------------------
    # 4. Null Count Comparison
    # ------------------------------------------------------------------
    null_comparison = {}
    for col in sorted(common_cols):
        exp_nulls = df_expected.filter(F.col(col).isNull()).count()
        mig_nulls = df_migrated.filter(F.col(col).isNull()).count()
        if exp_nulls != mig_nulls:
            null_comparison[col] = {"expected_nulls": exp_nulls, "migrated_nulls": mig_nulls}
    results["null_counts"] = {
        "discrepancies": null_comparison,
        "match": len(null_comparison) == 0
    }

    # ------------------------------------------------------------------
    # 5. Numeric Aggregation Comparison
    # ------------------------------------------------------------------
    numeric_cols = [f.name for f in df_expected.schema.fields
                    if str(f.dataType) in ("DoubleType", "FloatType", "IntegerType",
                                           "LongType", "DecimalType(38,18)", "ShortType")
                    and f.name in common_cols]
    agg_comparison = {}
    for col in numeric_cols:
        exp_stats = df_expected.select(
            F.sum(col).alias("sum"), F.avg(col).alias("avg"),
            F.min(col).alias("min"), F.max(col).alias("max")
        ).collect()[0]
        mig_stats = df_migrated.select(
            F.sum(col).alias("sum"), F.avg(col).alias("avg"),
            F.min(col).alias("min"), F.max(col).alias("max")
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
    results["numeric_aggregations"] = {
        "discrepancies": agg_comparison,
        "match": len(agg_comparison) == 0
    }

    # ------------------------------------------------------------------
    # 6. Row-Level Diff (if key columns provided)
    # ------------------------------------------------------------------
    if key_columns and all(c in common_cols for c in key_columns):
        only_in_expected = df_expected.join(df_migrated, on=key_columns, how="left_anti")
        only_in_migrated = df_migrated.join(df_expected, on=key_columns, how="left_anti")
        results["row_diff"] = {
            "rows_only_in_expected": only_in_expected.count(),
            "rows_only_in_migrated": only_in_migrated.count(),
            "match": only_in_expected.count() == 0 and only_in_migrated.count() == 0
        }

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    all_passed = all(v.get("match", True) for v in results.values())
    results["overall"] = "PASS" if all_passed else "FAIL"

    return results
```

### How to Use the Validator

```python
# Load expected output (from Alteryx)
df_expected = spark.read.csv("/Volumes/.../expected_output.csv", header=True, inferSchema=True)

# Load migrated output
df_migrated = spark.read.table("catalog.silver.migrated_table")

# Run validation
results = validate_migration(df_expected, df_migrated, key_columns=["id"])

# Report
print(f"Overall: {results['overall']}")
for check, detail in results.items():
    if check != "overall":
        status = "PASS" if detail.get("match", True) else "FAIL"
        print(f"  {check}: {status}")
        if not detail.get("match", True):
            for k, v in detail.items():
                if k != "match":
                    print(f"    {k}: {v}")
```

### Validation Thresholds

| Check | Pass Criteria | Action on Fail |
|---|---|---|
| Row count | Exact match | Investigate filters, joins, dedup logic |
| Schema | All expected columns present | Map missing columns, check renames |
| Data types | Compatible types | Add explicit `.cast()` |
| Null counts | Exact match per column | Review null handling, join types |
| Numeric aggregates | Within ±0.000001 tolerance | Check rounding, casting, formula logic |
| Row-level diff | Zero unmatched rows | Debug join keys, filter conditions |

---

## Migration Workflow (Step-by-Step)

Follow this sequence for every Alteryx migration.

### Phase 1: Discovery & Setup

1. **Parse the Alteryx workflow** — Read the .yxmd XML or user description. Identify all tools, connections, and data flow.
2. **Inventory inputs/outputs** — List every data source and destination.
3. **Confirm expected output file** — If not provided, STOP and ask (see Pre-Migration Checklist).
4. **Confirm save locations** — Notebook path and output table/path. If not provided, STOP and ask.
5. **Map to medallion tiers** — Assign each transformation stage to Bronze, Silver, or Gold.

### Phase 2: Build Bronze Layer

6. **Create ingestion notebook** — Read raw data, add metadata columns, write to bronze Delta table.
7. **Validate bronze** — Confirm row counts match source files.

### Phase 3: Build Silver Layer

8. **Create transformation notebook** — Translate Alteryx preparation/join/transform tools to PySpark.
9. **Handle tool-by-tool** — Use the reference table above. Maintain the same logical order as the Alteryx workflow.
10. **Add data quality checks** — Null rates, value ranges, uniqueness constraints.

### Phase 4: Build Gold Layer

11. **Create aggregation notebook** — Translate Alteryx Summarize, Cross Tab, reporting logic.
12. **Optimize output** — Apply Z-ORDER or liquid clustering on key query columns.

### Phase 5: Validate

13. **Run the validation framework** — Compare migrated output to expected Alteryx output.
14. **Report discrepancies** — Present the validation summary to the user.
15. **Iterate** — Fix any failures and re-validate until all checks pass.

### Phase 6: Optimize & Finalize

16. **Run the Post-Migration Optimization Checklist** — See section below.
17. **Add documentation** — Markdown cells explaining each step, original Alteryx tool mapping.
18. **Clean up** — Remove scratch cells, temporary tables.
19. **Confirm with user** — Present final notebook structure and validation results.

---

## Notebook Structure Template

Every migration should produce notebooks following this structure:

```
📁 migrations/
  📁 <workflow_name>/
    📓 01_bronze_ingestion.py       — Raw data loading
    📓 02_silver_transformation.py  — Cleansing & business logic
    📓 03_gold_aggregation.py       — Business-level aggregates (if applicable)
    📓 04_validation.py             — Output quality checks
    📓 README.md                    — Migration summary & mapping doc
```

### Notebook Header Template

Every notebook should start with:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # [Workflow Name] — [Layer] Layer
# MAGIC
# MAGIC **Migrated from:** Alteryx workflow `<workflow_file_name>.yxmd`
# MAGIC **Migration date:** <date>
# MAGIC **Medallion tier:** Bronze / Silver / Gold
# MAGIC **Source data:** <list of inputs>
# MAGIC **Target table:** `catalog.schema.table`
# MAGIC
# MAGIC ## Alteryx Tool Mapping
# MAGIC | Step | Alteryx Tool | PySpark Operation |
# MAGIC |------|-------------|--------------------|
# MAGIC | 1    | Input Data  | spark.read.csv()   |
# MAGIC | 2    | Filter      | .filter()          |
# MAGIC | ...  | ...         | ...                |
```

---

## Common Pitfalls & Mitigations

| Pitfall | Cause | Mitigation |
|---|---|---|
| Row count mismatch after Join | Alteryx Join outputs L/R/Inner as separate anchors; PySpark join returns one DataFrame | Use `how="left_anti"` separately for unmatched rows |
| Null handling differs | Alteryx treats empty strings and nulls differently | Explicitly handle both: `F.when(F.col(c) == "", None)` |
| Sort order changes results | Alteryx is row-order sensitive; Spark is not | Add explicit `.orderBy()` before row-dependent ops |
| Formula precision loss | Alteryx uses fixed-point; Spark uses floating-point | Use `DecimalType` for financial calculations |
| Multi-Row Formula mismatch | Alteryx processes rows sequentially; Spark window functions are parallel | Verify window frame spec matches Alteryx grouping |
| DateTime parsing errors | Alteryx auto-detects date formats; Spark requires explicit format strings | Use `F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss")` with exact format |
| Duplicate rows after Union | Alteryx Union auto-deduplicates by configuration; PySpark `unionByName` does not | Add `.dropDuplicates()` if Alteryx config had dedup enabled |
| Cross Tab column name issues | Alteryx generates readable pivot column names; Spark may produce names with special chars | Rename pivot columns explicitly |
| `.cache()` fails on serverless | Serverless compute does not support `.cache()` or `.persist()` | Use temp Delta tables with `materialize()` pattern (see Serverless section) |
| Column names with spaces in Delta | Spark columns with spaces/special chars fail on Delta write | Use `delta.columnMapping.mode = "name"` with `minReaderVersion=2`, `minWriterVersion=5` |
| Category values containing '#' | Special chars in data cause type casting failures | Use `try_cast()` instead of `.cast()` to handle gracefully |
| Debug `.count()` triggers full recomputation | Each `.count()` call on an uncached lazy DataFrame re-runs the entire lineage | Remove debug counts, or place them after materialization/caching |

---

## Performance Best Practices

1. **Broadcast small lookup tables** — `F.broadcast(small_df)` for joins where one side is <100MB
2. **Avoid `.collect()` in loops** — Keep data in Spark; only collect final validation metrics
3. **Repartition before writes** — `df.repartition("partition_col")` for large outputs
4. **Use Delta merge for incremental** — `MERGE INTO` instead of full rewrite when appropriate
5. **Cache intermediate DataFrames sparingly** — Only cache when reused multiple times in the same notebook
6. **Prefer `F.expr()` for complex SQL** — Some Alteryx formulas translate more cleanly to SQL expressions
7. **Use Photon-enabled compute** — Leverage Photon for faster Delta operations
8. **Consolidate groupBy operations** — When computing multiple aggregations on the same grouping keys (e.g., Laspeyres numerator + denominator), combine into a single `.groupBy().agg()` call instead of separate groupBys joined together. This eliminates redundant shuffles and joins.
9. **Batch validation statistics** — Compute all mean/median/sum in a single `.select()` instead of looping per column. Each `.collect()` triggers a Spark job.
10. **Use `len(pandas_df)` instead of `spark_df.count()`** — When a pandas DataFrame is already in memory (e.g., from `pd.read_excel`), use `len()` for row counts to avoid triggering Spark materialization.
11. **Convert Excel to Parquet/Delta for production** — `pd.read_excel` is single-threaded and slow for large files. For recurring workflows, convert source Excel to Parquet or Delta once, then read natively in Spark.

---

## Post-Migration Optimization Checklist (MANDATORY)

**After validation passes, ALWAYS run through this checklist before marking the migration as complete.**

Alteryx workflows are inherently sequential and in-memory. A naive 1:1 translation to PySpark often produces correct results but with catastrophic performance due to Spark's lazy evaluation model. The same DataFrame can be recomputed dozens of times without the developer realizing it.

### 1. Identify Reused DataFrames

Scan the notebook for DataFrames that are referenced in multiple downstream operations (joins, unions, aggregations, writes, validations). Each reference triggers full recomputation of the DataFrame's lineage.

**Rule:** If a DataFrame is used in N downstream actions, Spark recomputes it N times unless materialized.

**Example from AU Monthly Inflation migration:** 4 branch DataFrames were each used in 1-5 Fisher index calls. Without materialization, the full Silver pipeline (671K rows) was recomputed ~40 times, inflating runtime from ~3 min to ~20 min.

### 2. Materialize Strategically

**On classic/interactive clusters:** Use `.cache()` or `.persist()` for frequently reused DataFrames.

**On serverless compute:** `.cache()` and `.persist()` are NOT supported. Use this temp Delta table pattern instead:

```python
_TEMP_TABLES = []
def materialize(df, name):
    """Write DataFrame to a temp Delta table and read back to break lineage."""
    table = f"catalog.schema._tmp_{name}"
    df.write.mode("overwrite") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .saveAsTable(table)
    _TEMP_TABLES.append(table)
    return spark.table(table)

def cleanup_temp_tables():
    for t in _TEMP_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    _TEMP_TABLES.clear()
```

**Where to materialize (priority order):**
1. Branch point DataFrames (used by multiple downstream paths)
2. DataFrames after expensive transformations (groupBy, joins on large tables)
3. Final output DataFrames reused in both output write and validation

### 3. Consolidate Aggregations

Alteryx Summarize tools often produce multiple separate aggregations that get joined back together. In PySpark, these should be a single `.groupBy().agg()` call.

**Before (slow — N groupBys + N-1 joins):**
```python
laspeyres = df.groupBy(*cols).agg(F.sum("L_num").alias("L_num")).join(
    df.groupBy(*cols).agg(F.sum("L_den").alias("L_den")), on=cols)
```

**After (fast — 1 groupBy, 0 joins):**
```python
result = df.groupBy(*cols).agg(
    F.sum("L_num").alias("L_num"),
    F.sum("L_den").alias("L_den"),
    F.sum("P_num").alias("P_num"),
    F.sum("P_den").alias("P_den")
)
```

### 4. Minimize Action Calls

Each Spark action (`.count()`, `.collect()`, `.show()`, `.write`) triggers full computation of the DataFrame lineage. Audit the notebook for unnecessary actions:

- Remove debug `.count()` calls or move them after materialization
- Replace `spark_df.count()` with `len(pandas_df)` when a pandas version exists
- Batch multiple statistics into a single `.select(...).collect()` instead of per-column loops

### 5. Add Cleanup Cell

Always add a cleanup cell at the end of the notebook to drop temporary tables:

```python
# Final cell
cleanup_temp_tables()
```

---

## Agent Behavior Rules

1. **NEVER skip validation.** Every migration must end with the validation framework comparing output to expected results.
2. **NEVER assume output location.** Always confirm with the user.
3. **NEVER proceed without expected output.** If the user cannot provide one, document this as a known risk and validate with profiling (row counts, distributions, sample spot-checks) instead.
4. **ALWAYS add the Alteryx tool mapping table** to each notebook as documentation.
5. **ALWAYS follow medallion architecture** unless the user explicitly requests a flat/single-notebook migration.
6. **ALWAYS report validation results** in a clear summary format with PASS/FAIL per check.
7. **ALWAYS run the Post-Migration Optimization Checklist** after validation passes. Correct results with poor performance is an incomplete migration.
8. **ALWAYS check compute type** before using `.cache()` — it fails on serverless. Use temp Delta tables instead.
9. **When in doubt, ask.** Ambiguous Alteryx configurations (e.g., join type, null handling, sort order) should be confirmed with the user rather than assumed.
