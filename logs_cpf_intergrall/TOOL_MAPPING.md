# Logs CPF Intergrall VF1 — Alteryx → Databricks Tool Mapping

**Source workflow:** `Logs CPF Intergrall VF1.yxmd` (Alteryx 2023.2)
**Migration date:** 2026-08-06
**Target platform:** Databricks (SQL + Python, Serverless compute)

---

## Workflow Overview

The Alteryx workflow identifies **repeated CPF lookups** in the Intergrall system
performed by users who are **not** in management positions (Diretoria/Presidência).
It produces a dated `.xlsx` report and sends an email alert to the Continuous
Audit team.

---

## Data Flow

```
Path A — Log processing:
  Tool 9 → 24 → 11 → 12 (Duplicates) → 23 → 25 → 18 (Left input)

Path B — RH processing:
  Tools 13 + 14 → 15 (Union) → 16 → 17 → 18 (Right input)

Path C — Output (after Join):
  Tool 18 (Left out) → 19 → 21 → 20 → splits:
    • xlsx output:         32 → 10
    • email:               22 → 1
    • empty-result branch: 29+28 → 31 → 27 → 30 → 33 → 34
```

---

## Tool-to-Code Mapping

### Path A — Log Processing

| ToolID | Alteryx Tool | Operation | Migrated To | Notes |
|--------|-------------|-----------|-------------|-------|
| 9 | DbFileInput (ODBC Sybase) | `SELECT * FROM cat099_log_itgl WHERE month/year = current` | `01_bronze` cell 5: `DESCRIBE TABLE ${log_input_table}` | Source is now a UC table; month filter moved to Silver |
| 24 | DateTime | Convert `log_itgl_dat` from `dd/MM/yyyy` to Date → `Data convertida` | `02_silver` cell 5: `to_date(log_itgl_dat, 'dd/MM/yyyy') AS data_convertida` | Exact match |
| 11 | Formula | `Chave cons. = [log_itgl_usu]+[log_itgl_nm]+[log_itgl_nm_cns]+[log_itgl_cpf_cns]+[log_itgl_dat]` | `02_silver` cell 5: `concat(coalesce(...))` in CTE | Exact match (with null handling via coalesce) |
| 12 | Unique → Duplicates output | Keep 2nd+ occurrence of each `Chave cons.` | `02_silver` cell 5: `row_number() OVER (PARTITION BY chave_cons ORDER BY log_itgl_seq) WHERE rn >= 2` | Exact match |
| 23 | Formula | `REGEX_replace([log_itgl_dat],'/','-')` | `02_silver` cell 5: `regexp_replace(log_itgl_dat, '/', '-')` | Exact match |
| 25 | Filter | `[Data convertida] >= yesterday` | `02_silver` cell 5: `WHERE data_convertida >= date_add(current_date(), -1)` | Conditional: only when `date_filter='recent'` |

### Path B — RH Processing

| ToolID | Alteryx Tool | Operation | Migrated To | Notes |
|--------|-------------|-----------|-------------|-------|
| 13 | DbFileInput (Excel) | Read `Ativos sheet from SMB share | `01_bronze` cell 7: `read_landing(RH_INPUT_PATH, sheet="INDEX")` | Source changed to single INDEX sheet in UC Volume |
| 14 | DbFileInput (Excel) | Read `DesligadosConsolidado sheet | N/A | Eliminated — single sheet now contains all employees |
| 15 | Union (ByPos) | Combine Ativos + Desligados by position | N/A | Eliminated — single sheet |
| 16 | Formula | `CPF_RET = TrimLeft(Replace(Replace([CPF],'.',''),'-',''),'0')` + `CPF_RST = IF StartsWith(CARGO,'DIR'\|'MEMBRO'\|'PRES') THEN '1'` | `02_silver` cell 7: `regexp_replace(CPF, ...)` + `WHERE RLIKE '^(DIR\|MEMBRO\|PRES)'` | Filter absorbed into WHERE clause |
| 17 | Filter | `[CPF_RST] = "1"` | `02_silver` cell 7: `WHERE upper(coalesce(Cargo, '')) RLIKE '^(DIR\|MEMBRO\|PRES)'` | Merged with Tool 16 logic |

### Path C — Join + Output

| ToolID | Alteryx Tool | Operation | Migrated To | Notes |
|--------|-------------|-----------|-------------|-------|
| 18 | Join → Left output | Anti-join: log records NOT matching RH (`log_itgl_usu = NOME FUNCIONÁRIO`) | `03_gold` cell 7: `LEFT ANTI JOIN ... ON l.log_itgl_usu = r.nome_funcionario` | Exact match |
| 19 | Formula | Compute `Dia`, `Mês`, `Ano`, `Folder Month`, `FULLPATCH`, `FULLPATCH2`, `AssuntoEmail` | `00_config` cell 9: `OUTPUT_DIR`, `OUTPUT_XLSX`, `ASSUNTO_EMAIL`, `MESES_PT` | Paths point to UC Volume instead of SMB |
| 21 | Select | Remove helper fields (`Data convertida`, `System Date`, `Dia`, etc.) | `03_gold` cell 7: SELECT list only includes final columns | Implicit |
| 20 | BlockUntilDone | Synchronization barrier | N/A | Sequential cell execution provides ordering |
| 22 | PortfolioComposerTable | Format data as HTML table for email body | N/A | Replaced by Lakeflow Job notification with link |
| 1 | Email (SendGrid) | Send report with .xlsx attachment to `auditoria.continua@bancobmg.com.br` | Job `email_notifications.on_success` | No attachment; link to run + file in Volume |
| 32 | Select | Remove `AssuntoEmail`, `FULLPATCH2` before xlsx output | Implicit in SQL SELECT | |
| 10 | DbFileOutput (xlsx) | Write xlsx to SMB share at `FULLPATCH` path | `03_gold` cell 11: `pd.ExcelWriter` → UC Volume | Output destination changed |

### Path C — Empty-Result Branch

| ToolID | Alteryx Tool | Operation | Migrated To | Notes |
|--------|-------------|-----------|-------------|-------|
| 28 | TextInput | Static row: `LOG = "Sem registro"` | `03_gold` cell 11: `pd.DataFrame({"LOG": ["Sem registro"]})` | Exact match |
| 29 | AppendFields | Cross-join "Sem registro" with date formulas | `03_gold` cell 11: `if n_resultado == 0` branch | Simplified — no cross-join needed |
| 31 | Select | Pass-through | N/A | Implicit |
| 27 | Formula | Same date/path formulas as Tool 19 (for empty case) | `00_config` cell 9: same variables for both paths | Unified |
| 30 | Filter | `IsEmpty([log_itgl_usu])` — detects empty result | `03_gold` cell 9: `if n_resultado == 0` | Simplified to count check |
| 33 | Select | Remove helper columns | N/A | Implicit |
| 34 | DbFileOutput (xlsx) | Write "Sem registro" xlsx to same FULLPATCH path | `03_gold` cell 11: same code handles empty case | Unified with Tool 10 |

### Events

| Event | Alteryx | Migrated To |
|-------|---------|-------------|
| AfterError | Email "ERRO NA EXECUÇÃO: LOGS INTERGRALL" via SendGrid | Job `email_notifications.on_failure` → `p30178@bancobmg.com.br` |

---

## Key Differences

| Aspect | Alteryx | Databricks |
|--------|---------|------------|
| Log source | ODBC → Sybase (month filter in SQL) | UC table `cat099_log_itgl` (month filter in Silver SQL) |
| RH source | SMB share `\\swap629\...`, 2 Excel sheets + Union | UC Volume, single INDEX sheet |
| Date filter | Hardcoded: yesterday only | Configurable: `date_filter` parameter (`recent` or `full`) |
| Output path | SMB share `\\swap629\Auditoria_Continua$\...` | UC Volume `/Volumes/dtlk-sandbox/micracao_alteryx/excel_files/` |
| Email | SendGrid API with xlsx attachment | Lakeflow Job notification (link to run; file in Volume) |
| Persistence | Only .xlsx file output | Delta Gold table (`logs_cpf_intergrall`) + .xlsx in Volume |
| Error handling | AfterError event → email | Job `on_failure` email notification |
| Execution | Alteryx Server scheduler | Lakeflow Job (cron `0 0 6 * * ?` America/Sao_Paulo) |
| Language | Alteryx visual tools | Databricks SQL (Silver + Gold) + Python (Bronze Excel, xlsx export) |

---

## Migrated Notebooks

| Notebook | Layer | Purpose |
|----------|-------|--------|
| `00_config` | Shared | Widgets, table names, helper functions, output paths |
| `01_bronze_ingestion` | Bronze | Read log table (UC) + ingest RH Excel → Delta |
| `02_silver_transformation` | Silver | SQL: date conversion, chave_cons, duplicates, date filter, RH filtering |
| `03_gold_aggregation` | Gold | SQL: anti-join + table write; Python: .xlsx export |
| `04_validation` | QA | Profiling, invariants, optional baseline comparison |

---

## Job Configuration

* **Job name:** `[dev p30178] [dev] Logs CPF Intergrall — Auditoria Contínua`
* **Schedule:** Daily at 06:00 (America/Sao_Paulo) — currently PAUSED
* **Parameters:**
  - `catalog` = `dtlk-sandbox`
  - `bronze_schema` / `silver_schema` / `gold_schema` = `micracao_alteryx`
  - `log_input_table` = `` `dtlk-sandbox`.micracao_alteryx.cat099_log_itgl ``
  - `rh_input_path` = `/Volumes/dtlk-sandbox/micracao_alteryx/excel_files/Base IGI RH.XLSX`
  - `target_volume` = `/Volumes/dtlk-sandbox/micracao_alteryx/excel_files`
  - `date_filter` = `recent` (default) | `full` (all history)
  - `baseline_path` = (empty — no Alteryx baseline yet)

---

## Open Items

1. **Anti-join direction** — verify with audit team whether the indicator should
   capture lookups BY management (inner join) or OUTSIDE management (anti-join).
2. **Join by name** — `log_itgl_usu = nome_funcionario` is exact text match;
   consider using CPF for robustness.
3. **Month boundary** — on the 1st of the month, the "yesterday" filter crosses
   into the prior month which the month filter excludes. Use `date_filter=full`
   or fix the window logic.
