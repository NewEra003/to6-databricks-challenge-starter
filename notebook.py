# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1 AS connection_check

# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/ulzie.l.rea.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Returns_Initial Table Description
# MAGIC - It has 5 columns (return_id, taxpayer_id, tax_year, income, deduction)
# MAGIC     - return_id is a string and is supplied by upstream table parsed_source table (return_id)
# MAGIC     - taxpayer_id is a string and is supplied by the upstream table parsed_source table (taxpayer_id)
# MAGIC     - tax_year is an int and is supplied by the upstream table parsed_source table (tax_year)
# MAGIC     - income is an int and is supplied by the upstream table parsed_source table (income, return_id)
# MAGIC     - deduction is an int and is supplied by the upstream table parsed_source table (deduction)
# MAGIC
# MAGIC ## Returns_Corrected Table Description
# MAGIC - It has 5 columns (return_id, taxpayer_id, tax_year, income, deduction)
# MAGIC     - return_id is a string and is supplied by upstream table parsed_source table (return_id)
# MAGIC     - taxpayer_id is a string and is supplied by the upstream table parsed_source table (taxpayer_id)
# MAGIC     - tax_year is an int and is supplied by the upstream table parsed_source table (tax_year)
# MAGIC     - income is an int and is supplied by the upstream table parsed_source table (income)
# MAGIC     - deduction is an int and is supplied by the upstream table parsed_source table (deduction)
# MAGIC
# MAGIC ## Difference return_initial and return_corrected
# MAGIC The difference is that income only has one upstream source (parsed_source.income) in return_corrected. While , return_initial has two sources(parsed_source.income and parsed_source.return_id). I also notice that there is duplicate data in return_initial while observing the sample data. There is also a row of data that shows null for income in return_initial. This is all data that looks to be corrected in return_corrected.
# MAGIC
# MAGIC ## Observations in summary
# MAGIC     - Return R01 appears twice.
# MAGIC     - Return R02 is missing income.
# MAGIC     - Returns R03 and R09 have zero income.
# MAGIC     - 2022 has 5 rows but only 4 distinct returns, and this delivery would fail validation.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Full-batch profile: missing values and counts by year
# Full-batch profile of both deliveries, run BEFORE any validation gate.
# Read-only: nothing is filled, dropped or deduplicated here.
import json
from pyspark.sql import functions as F

PROFILE_REQUIRED = ["return_id", "tax_year", "income", "deduction"]
BATCHES = {"returns_initial": INITIAL_TABLE, "returns_corrected": CORRECTED_TABLE}

missing_profile, year_profile = {}, {}
for name, table in BATCHES.items():
    df = spark.table(table)

    # 1. Missing-value count for each required column over every row of the batch
    missing_row = df.agg(
        F.count(F.lit(1)).alias("total_rows"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in PROFILE_REQUIRED],
    ).first().asDict()
    missing_profile[name] = {
        "table": table,
        "total_rows": missing_row["total_rows"],
        "missing_by_column": {c: int(missing_row[c] or 0) for c in PROFILE_REQUIRED},
    }

    # 2. Record counts by tax year over every row (a null year would show as its own group)
    by_year = (
        df.groupBy("tax_year")
          .agg(
              F.count(F.lit(1)).alias("rows"),
              F.countDistinct("return_id").alias("distinct_return_ids"),
              (F.count("return_id") - F.countDistinct("return_id")).alias("duplicate_rows"),
              F.sum(F.when(F.col("income") == 0, 1).otherwise(0)).alias("zero_income_rows"),
          )
          .orderBy("tax_year")
    )
    print(f"{table}: rows by tax year")
    display(by_year)
    year_rows = [r.asDict() for r in by_year.collect()]
    assert sum(r["rows"] for r in year_rows) == missing_row["total_rows"], "year counts must cover the full batch"
    year_profile[name] = {"table": table, "by_tax_year": year_rows}

print("Missing values per required column (full batch):")
display(spark.createDataFrame(
    [(n, p["total_rows"], *[p["missing_by_column"][c] for c in PROFILE_REQUIRED])
     for n, p in missing_profile.items()],
    ["batch", "total_rows", *[f"missing_{c}" for c in PROFILE_REQUIRED]],
))

# Save as inspectable exports so they can be committed with the Git folder
out_dir = repo_root / "outputs"
(out_dir / "profile-missing-values.json").write_text(
    json.dumps(missing_profile, indent=2) + "\n", encoding="utf-8")
(out_dir / "profile-counts-by-year.json").write_text(
    json.dumps(year_profile, indent=2) + "\n", encoding="utf-8")
print("Saved outputs/profile-missing-values.json and outputs/profile-counts-by-year.json")

# COMMAND ----------

# DBTITLE 1,Profile findings and impact
# MAGIC %md
# MAGIC ### Profile findings (from the cell above, full batches)
# MAGIC - **Missing values:** `returns_initial` has 1 missing `income` (R02, 2022); `return_id`, `tax_year` and `deduction` have none. `returns_corrected` has no missing required values.
# MAGIC - **Counts by year:** `returns_initial` has 5 rows / 4 distinct IDs in 2022 (R01 duplicated), 4 / 4 in 2023 and 4 / 4 in 2024. `returns_corrected` has 4 / 4 in every year (12 rows total).
# MAGIC - **Zero income:** one return in 2022 (R03) and one in 2024 (R09) in both batches. These are valid and must stay, with a null ratio.
# MAGIC
# MAGIC **Impact on the next steps:** the missing income and the duplicate in 2022 mean `returns_initial` must fail the completeness and uniqueness gate in section 2. The corrected batch has 4 returns per year, so each split table should get exactly 4 rows. Zero income must not be treated as missing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC The below need to be determine for the batch completeness and uniqueness. The following fields are required, return ID, tax year, income, and deduction.
# MAGIC Each row should have one return.
# MAGIC
# MAGIC Removing duplicates, filling nulls with zero, or dropping rows with missing values anywhere before the feature step.
# MAGIC Checking uniqueness by taxpayer ID. Taxpayers can file in several years, so that ID repeats legitimately.
# MAGIC Treating zero income (R03, R09) as missing. It's valid and those returns must stay.
# MAGIC Relying only on distinct-ID counts. Row count and distinct count need to be compared.
# MAGIC Overwriting the rejection evidence with the corrected run's outputs.    
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 1: validator and gate

# COMMAND ----------

import json
from datetime import datetime, timezone
from pyspark.sql import functions as F, Window

REQUIRED_COLUMNS = ["return_id", "tax_year", "income", "deduction"]
UNIQUE_KEY = "return_id"


class BatchRejected(Exception):
    """Raised when a batch fails completeness or uniqueness checks."""


def validate_batch(table_name):
    """Apply the same completeness and uniqueness rules to any returns batch.
    Read-only: never fills, drops or deduplicates records."""
    df = spark.table(table_name)
    absent = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if absent:
        raise ValueError(f"{table_name} is missing required columns: {absent}")

    checked_at = datetime.now(timezone.utc).isoformat()
    by_id = Window.partitionBy(UNIQUE_KEY)

    missing_fields = F.concat_ws(
        ", ", *[F.when(F.col(c).isNull(), F.lit(c)) for c in REQUIRED_COLUMNS]
    )
    flagged = (
        df.withColumn("id_occurrences", F.count(F.lit(1)).over(by_id))
          .withColumn("missing_fields", missing_fields)
          .withColumn(
              "rejection_reason",
              F.concat_ws(
                  "; ",
                  F.when(F.col("missing_fields") != "",
                         F.concat(F.lit("completeness: missing "), F.col("missing_fields"))),
                  F.when(F.col(UNIQUE_KEY).isNotNull() & (F.col("id_occurrences") > 1),
                         F.concat(F.lit("uniqueness: return_id occurs "),
                                  F.col("id_occurrences").cast("string"), F.lit(" times"))),
              ),
          )
    )

    # Every affected row is kept, including each copy of a duplicate.
    affected_df = (
        flagged.filter(F.col("rejection_reason") != "")
               .withColumn("batch", F.lit(table_name))
               .withColumn("checked_at", F.lit(checked_at))
               .select("batch", *df.columns, "id_occurrences", "rejection_reason", "checked_at")
               .orderBy(UNIQUE_KEY, "tax_year")
    )

    stats = df.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(UNIQUE_KEY).alias("distinct_return_ids"),
        F.sum(F.when(F.greatest(*[F.col(c).isNull().cast("int") for c in REQUIRED_COLUMNS]) == 1, 1)
               .otherwise(0)).alias("rows_missing_required"),
        (F.count(UNIQUE_KEY) - F.countDistinct(UNIQUE_KEY)).alias("duplicate_rows"),
        F.sum(F.when(F.col("income") == 0, 1).otherwise(0)).alias("zero_income_rows"),
    ).first().asDict()

    summary = {
        "batch": table_name,
        "checked_at": checked_at,
        "required_columns": REQUIRED_COLUMNS,
        "unique_key": UNIQUE_KEY,
        **stats,
        "passed": stats["rows_missing_required"] == 0 and stats["duplicate_rows"] == 0,
    }
    return {"df": df, "summary": summary, "affected_df": affected_df,
            "affected": [r.asDict() for r in affected_df.collect()]}


def write_evidence(result, file_name):
    """Save the summary and affected records before the gate decides."""
    out = repo_root / "outputs" / file_name
    out.write_text(json.dumps({"summary": result["summary"],
                               "affected_records": result["affected"]},
                              indent=2, default=str) + "\n", encoding="utf-8")
    return out


def require_valid(result):
    """Gate: call before any feature output is written."""
    s = result["summary"]
    if not s["passed"]:
        reasons = sorted({r["rejection_reason"] for r in result["affected"]})
        raise BatchRejected(f"{s['batch']} rejected: {reasons}")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 2: reject the initial batch

# COMMAND ----------

initial = validate_batch(INITIAL_TABLE)
display(initial["affected_df"])
print(initial["summary"])
print(write_evidence(initial, "initial-rejection.json"))   # evidence first

try:
    require_valid(initial)
    raise AssertionError("Initial batch unexpectedly passed")
except BatchRejected as e:
    initial_rejection = str(e)
    print(initial_rejection)

# Self-check against the known defects
assert sorted(r["return_id"] for r in initial["affected"]) == ["R01", "R01", "R02"]
assert initial["summary"]["row_count"] == 13 and initial["summary"]["distinct_return_ids"] == 12

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 3: same rules on the corrected batch

# COMMAND ----------

corrected = validate_batch(CORRECTED_TABLE)
print(corrected["summary"])
print(write_evidence(corrected, "corrected-validation.json"))
require_valid(corrected)              # stops the notebook here if it fails
accepted_df = corrected["df"]         # the only input for feature output

assert corrected["affected"] == []
assert corrected["summary"]["row_count"] == corrected["summary"]["distinct_return_ids"] == 12

# COMMAND ----------

# MAGIC %md
# MAGIC # Your checks and decision here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

from pyspark.sql import functions as F

# Inputs to this step
FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"
assert OUTPUT_SCHEMA == "training.to6_c3_dbbd558d0c45", OUTPUT_SCHEMA
assert corrected["summary"]["passed"], "Only a validated batch may produce features"

FEATURE_COLUMNS = ["return_id", "tax_year", "deduction_to_income"]

# Build features: zero income stays in with a null ratio
features_df = (
    accepted_df
    .withColumn(
        "deduction_to_income",
        F.when(F.col("income") == 0, F.lit(None).cast("double"))
         .otherwise(F.col("deduction").cast("double") / F.col("income")),
    )
    .select(*FEATURE_COLUMNS)          # drops taxpayer_id, income, deduction
)

# Pre-write checks: every accepted return exactly once
accepted_ids = {r["return_id"] for r in accepted_df.select("return_id").collect()}
assert features_df.count() == len(accepted_ids) == 12
assert features_df.select("return_id").distinct().count() == 12

# Overwrite so reruns replace the table instead of appending copies
(features_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FEATURES_TABLE))

# Read back the saved table
saved = spark.table(FEATURES_TABLE)

# 1. Exact columns, no restricted fields
assert saved.columns == FEATURE_COLUMNS, saved.columns
assert not {"taxpayer_id", "income", "deduction"} & set(saved.columns)

# 2. Every accepted return once, nothing extra
saved_ids = [r["return_id"] for r in saved.select("return_id").collect()]
assert len(saved_ids) == len(set(saved_ids)) == 12
assert set(saved_ids) == accepted_ids

# 3. Saved rows equal the in-memory frame, row for row
assert saved.exceptAll(features_df).count() == 0
assert features_df.exceptAll(saved).count() == 0

# 4. Nonzero case: expected value derived from the source row
src = (spark.table(CORRECTED_TABLE)
       .filter(F.col("return_id") == "R04")
       .select("income", "deduction").first())
expected_r04 = src["deduction"] / src["income"]                  # 59 / 400
assert abs(expected_r04 - 0.1475) < 1e-12                        # hand-derived check
saved_r04 = saved.filter(F.col("return_id") == "R04").first()
assert saved_r04["tax_year"] == 2022
assert abs(saved_r04["deduction_to_income"] - expected_r04) < 1e-12

# 5. Zero-income cases stay in with a null ratio
zero_ids = {r["return_id"] for r in
            spark.table(CORRECTED_TABLE).filter(F.col("income") == 0).select("return_id").collect()}
assert zero_ids == {"R03", "R09"}, zero_ids
zero_saved = saved.filter(F.col("return_id").isin(*zero_ids)).collect()
assert len(zero_saved) == 2
assert all(r["deduction_to_income"] is None for r in zero_saved)

# 6. Only zero-income returns have a null ratio
assert saved.filter(F.col("deduction_to_income").isNull()).count() == len(zero_ids)

display(saved.orderBy("return_id"))

# Small export for outputs/
features_export = [r.asDict() for r in saved.orderBy("return_id").collect()]
(repo_root / "outputs" / "return_features.json").write_text(
    json.dumps(features_export, indent=2) + "\n", encoding="utf-8")
print(f"Saved and verified {FEATURES_TABLE}: {len(features_export)} rows; "
      f"R04 = {saved_r04['deduction_to_income']} (expected {expected_r04}); "
      f"R03, R09 ratio = null")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 1: build and check the year splits

# COMMAND ----------

import hashlib
from pyspark.sql import functions as F

FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"
FEATURE_COLUMNS = ["return_id", "tax_year", "deduction_to_income"]
SPLITS = {
    "features_train": 2022,
    "features_validation": 2023,
    "features_test": 2024,
}

features = spark.table(FEATURES_TABLE)
assert features.columns == FEATURE_COLUMNS, features.columns

# Every feature year must map to exactly one split; no return left unassigned
feature_years = {r["tax_year"] for r in features.select("tax_year").distinct().collect()}
assert feature_years == set(SPLITS.values()), f"Unassigned or missing years: {feature_years}"

# Write each split with overwrite so a rerun replaces it instead of appending
for table, year in SPLITS.items():
    (features.filter(F.col("tax_year") == year)
        .select(*FEATURE_COLUMNS)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{OUTPUT_SCHEMA}.{table}"))

# Read back every split
saved_splits = {t: spark.table(f"{OUTPUT_SCHEMA}.{t}") for t in SPLITS}

for table, year in SPLITS.items():
    df = saved_splits[table]
    assert df.columns == FEATURE_COLUMNS, (table, df.columns)
    # Only its assigned year
    assert df.filter(F.col("tax_year") != year).count() == 0, table
    # No duplicate return IDs within the split
    assert df.count() == df.select("return_id").distinct().count(), table

# Across splits: each accepted return appears exactly once, with identical values
all_splits = None
for df in saved_splits.values():
    all_splits = df if all_splits is None else all_splits.unionByName(df)

assert all_splits.count() == features.count() == 12
assert all_splits.select("return_id").distinct().count() == 12
assert all_splits.exceptAll(features).count() == 0
assert features.exceptAll(all_splits).count() == 0

# Zero-income cases stay in their assigned split with a null ratio
for rid, table in [("R03", "features_train"), ("R09", "features_test")]:
    row = saved_splits[table].filter(F.col("return_id") == rid).first()
    assert row is not None and row["deduction_to_income"] is None, (rid, table)

for table, df in saved_splits.items():
    print(table, SPLITS[table], df.count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 2: save this run's snapshot and exports

# COMMAND ----------

from datetime import datetime, timezone

def table_rows(df):
    return [r.asDict() for r in df.orderBy("return_id").collect()]

snapshot_tables = {"return_features": table_rows(spark.table(FEATURES_TABLE))}
snapshot_tables.update({t: table_rows(df) for t, df in saved_splits.items()})

canonical = json.dumps(snapshot_tables, sort_keys=True, separators=(",", ":"))
run_at = datetime.now(timezone.utc)

try:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    notebook_path = ctx.notebookPath().get()
except Exception:
    notebook_path = "unknown"

snapshot = {
    "run_at_utc": run_at.isoformat(),
    "notebook_path": notebook_path,
    "content_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    "row_counts": {t: len(rows) for t, rows in snapshot_tables.items()},
    "tables": snapshot_tables,
}

runs_dir = repo_root / "outputs" / "runs"
runs_dir.mkdir(parents=True, exist_ok=True)
snap_path = runs_dir / f"run-{run_at.strftime('%Y%m%dT%H%M%SZ')}.json"
snap_path.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")

# Small exports of the saved split tables
for t, rows in snapshot_tables.items():
    if t != "return_features":
        (repo_root / "outputs" / f"{t}.json").write_text(
            json.dumps(rows, indent=2) + "\n", encoding="utf-8")

print(snap_path, snapshot["content_sha256"])

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 3: compare the two most recent complete runs

# COMMAND ----------

snap_files = sorted(runs_dir.glob("run-*.json"))
if len(snap_files) < 2:
    print("Only one run recorded. Run the whole notebook again, then rerun this cell.")
else:
    prev, curr = (json.loads(p.read_text(encoding="utf-8")) for p in snap_files[-2:])
    comparison = {
        "previous_run": {"file": snap_files[-2].name, "run_at_utc": prev["run_at_utc"],
                         "sha256": prev["content_sha256"]},
        "current_run": {"file": snap_files[-1].name, "run_at_utc": curr["run_at_utc"],
                        "sha256": curr["content_sha256"]},
        "per_table": {},
    }
    for t in curr["tables"]:
        a, b = prev["tables"].get(t), curr["tables"][t]
        comparison["per_table"][t] = {
            "same_return_ids": [r["return_id"] for r in a] == [r["return_id"] for r in b],
            "same_values": a == b,
            "rows": len(b),
        }
    comparison["identical"] = (
        prev["content_sha256"] == curr["content_sha256"]
        and all(v["same_values"] for v in comparison["per_table"].values())
    )
    (repo_root / "outputs" / "run-comparison.json").write_text(
        json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(comparison, indent=2))
    assert comparison["identical"], "Runs differ - inspect per_table"