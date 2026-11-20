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

# COMMAND ----------

# MAGIC %md
# MAGIC # Your checks and decision here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# Your implementation and verification here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# Your implementation, rerun comparison and exports here.