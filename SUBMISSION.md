# My dataset submission

Complete this file in Databricks. Keep README.md as the course navigation page.

## Inputs and outputs

Full input, lineage and output table names; what one row represents; ratio definition.

Inputs:
training.to6_challenge3_inputs.returns_initial, the rejected batch
training.to6_challenge3_inputs.returns_corrected, the accepted batch
Lineage: training.to6_challenge3_inputs.source_lineage, upstream source training.to6_challenge3_inputs.parsed_source.
Outputs located in training.to6_c3_dbbd558d0c45: return_features, features_train, features_validation and features_test.
Row meaning: one row is one tax return. The return ID identifies the return. The taxpayer ID identifies the filer, tax year is the year the return is filed, income is the tax filer income, deductions are the deductions on the return, and deduction_to_income is the ratio of deduction to income.
Ratio definition: deduction_to_income is deduction divided by income. It's null when income is zero, and those returns stay in the table.

## Checks and decisions

Source references; initial rejection reference, affected-record export and reasons; replacement
checks; independently derived nonzero expected value; saved-table readback including zero income.

Source references: the catalog descriptions, the sample rows you looked at, and the lineage view.
Initial rejection: the notebook cell where the gate rejected the batch and its message, plus outputs/initial-rejection.json.
Uniqueness: R01 appears twice. Keep both rows.
Completeness: R02 is missing income.
Feature output was not written from this batch.
Replacement checks: the same validator on returns_corrected found 12 rows, 12 distinct IDs and nothing missing. The evidence is outputs/corrected-validation.json.
R02's income of 200 comes from the corrected delivery, not from filling in a value.
The two zero-income returns, R03 and R09, are valid.
Nonzero expected value: R04 is 59 / 400 = 0.1475, worked out from the source row and matched by the saved value.
Read-back:
The saved table has exactly the three required columns and 12 unique returns.
It matches the data that was written, row for row.
R03 and R09 are present with null ratios.

## Two successful runs

For each run: notebook URL and execution timestamp, or Jobs URL/ID. Name the retained exports
or comparison evidence showing identical return IDs and values in the feature and year tables.
Explain how you checked year assignment, one copy of every accepted return and no appended copies.

Per run: the notebook URL and run time. Use UTC to match the snapshot file names.

Evidence files: 
Run 1:
https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866557984

    "file": "run-20261122T220803Z.json",
    "run_at_utc": "2026-11-22T22:08:03.701863+00:00",
    "sha256": "74e5cd2e73423cd00ef066f1bd48960cbae55c55697cd4af20dd3ec0a724253c"

Run 2:
https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866557984

    "file": "run-20261122T221921Z.json",
    "run_at_utc": "2026-11-22T22:19:21.152051+00:00",
    "sha256": "74e5cd2e73423cd00ef066f1bd48960cbae55c55697cd4af20dd3ec0a724253c"

outputs/run-comparison.json showing matching contents and identical IDs and values for all four tables.
Year assignment: each split table holds only its year, and every year in return_features maps to exactly one split.
One copy of each return: the three splits together equal return_features row for row, with 12 rows and 12 unique IDs.
No appended copies: every table is overwritten on each run, and the row counts stay the same between runs.

## Final files and limitations

List final exports under outputs/, distinguishing practice files. State anything not verified.

Final exports:
profile-missing-values.json: missing-value count for each required column over the full initial and corrected batches, computed before validation (initial: income 1, others 0; corrected: all 0)
profile-counts-by-year.json: rows, distinct return IDs, duplicates and zero-income rows by tax year for both full batches (initial 2022: 5 rows / 4 IDs; all other years 4 / 4)
initial-rejection.json and corrected-validation.json
return_features.json, features_train.json, features_validation.json and features_test.json
runs/run-*.json and run-comparison.json
Practice file: connection-practice.json is from the ungraded connection check.

Note Verified:
Accuracy of corrected values: I checked returns_corrected against the completeness and uniqueness rules only. I could not confirm that replacement values, such as R02's income of 200, are correct against the original source documents; they come from the data owner's corrected delivery.
Lineage beyond the supplied view: the upstream source (parsed_source) comes from training.to6_challenge3_inputs.source_lineage and the Catalog descriptions. I did not inspect the upstream parsing or the raw XML.
Model leakage: these checks set the data contract only. They do not show that every source of leakage is gone. Later modelling work must add labels and check which fields are available at prediction time.
Value ranges: amounts are expected to be nonnegative, but the gate enforces only completeness and uniqueness. Either confirm that no negative income or deduction exists, or keep this item in the list.
Floating-point precision: I compared ratios such as R04 = 0.1475 within a tolerance of 1e-12, not as exact decimals.