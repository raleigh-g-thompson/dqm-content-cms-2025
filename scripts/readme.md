# Test Case Comparison Process

This repository provides a workflow for comparing expected and actual results for measure test cases. The process generates a comparison file at `./scripts/comparison/output_results.csv` that summarizes PASS/FAIL for each population result, plus a human-readable `./scripts/comparison/discrepancy_report.md`.

## Workflow Steps

1. **Run the CQL plugin**  
   Execute the CQL plugin to generate actual test results for each measure. This will create files in the `./input/tests/results` directory.

2. **Run the scripts in order:**
   - **Step 1:** `extract_population_expected.py`  
     Parses expected results from MeasureReport files and Measure resources.  
     _Only rerun when MeasureReport expected results change._
   - **Step 2:** `extract_population_actual.py`  
     Parses actual results from the output of the CQL plugin in `./input/tests/results/`.  
     _Rerun whenever these result files change._
   - **Step 3:** `compare_results.py`  
     Compares expected and actual results, producing `./scripts/comparison/output_results.csv` and `./scripts/comparison/discrepancy_report.md`.  
     _Rerun whenever either expected or actual results change._

_NOTES_

- Execute scripts from the project's root directory
  - `python ./scripts/<SCRIPT NAME>`
- Scripts developed/tested using `python 3.12`
- No dependencies beyond the standard library

## Script Descriptions

### `extract_population_expected.py`

- Reads MeasureReport JSON files and corresponding Measure resources.
- Extracts population codes and criteria expressions as display names.
- Outputs a CSV of expected results to `./scripts/comparison/expected_results.csv`.

### `extract_population_actual.py`

- Reads actual result files from `./input/tests/results`, which the CQL plugin writes in two
  forms: per-measure `TestCaseResult-*.json` files, and `{MeasureName}.txt` engine traces.
- **Prefers the JSON whenever it is present**, falling back to traces only when there is no JSON.
  The JSON is the complete format — one file per test case, with every population plus an
  `errors` array. The traces are for humans and the plugin writes them inconsistently: on
  extension 0.9.8 / engine 5.3.0, only 48 of 73 traces contained population lines; the other 25
  were header-only stubs (tool versions and a list of test-case paths, nothing else).
- Override with `--json-results` / `-jr` or `--text-results` / `-txt`, e.g. for an archived
  capture containing only traces.
- Parses population results for each test case, handling boolean and list values. Test cases
  whose JSON carries a non-empty `errors` array are skipped.
- Prints the format it chose, the row count, and a **warning listing any measure that produced
  zero population rows**. Take that warning seriously: a measure contributing nothing appears
  downstream as "Missing Results", which reads identically to the CQL failing to translate.
  Reading stub traces instead of the JSON once turned a healthy 96.29% run into an apparent
  49.12%, with 35 measures reported missing.
- Outputs a CSV of actual results to `./scripts/comparison/actual_results.csv`.

### `compare_results.py`

- Compares expected and actual results by measure name, guid, and population.
- Outputs `./scripts/comparison/output_results.csv` with PASS/FAIL/MISSING for each population cell, and `./scripts/comparison/discrepancy_report.md` grouping the differences per measure.
- Prints the number and percentage of passing and failing test cases to the terminal.

#### QI-Core parity columns (optional)

Knowing a test case fails is not enough to act on it, because there are two very different reasons: the measure is genuinely wrong (QI-Core gets it wrong too — a parity gap), or the QI-Core → USQualityCore migration broke it (QI-Core passes it — a regression). The report cannot tell those apart on its own, so `compare_results.py` takes an optional third input:

- `scripts/comparison/qicore_actual_results.csv` — a copy of the QI-Core project's `scripts/comparison/actual_results.csv`, same `measure_name,guid,population,count` schema. Override the location with `--qicore-actual <path>`.
- **If the file is absent, nothing else changes.** The report renders exactly as it does without it, so a checkout with no QI-Core copy still works. The script prints which file it used, or that none was found.

When it is present, only the *failing* measures and test cases gain QI-Core detail:

| Where | Column | Meaning |
|---|---|---|
| Header `Details` table | `CMS Fail / QI-Core OK` | The one actionable parity count: the migration-regression bucket, in cases and measures |
| "Measures with Discrepancies" | `QI-Core Also Failing` | `n of m` — failing test cases QI-Core does not also pass, of all failing cases on that measure |
| "Measures with Discrepancies" | `CMS Fail / QI-Core OK` | `n of m` — the same denominator read the other way. `0 of m` marks a measure as fully actionable in this repo |
| Missing Results / Missing Populations | `QI-Core` | Status of that test case on QI-Core |
| Mismatched Test Cases | `QI-Core`, `QI-Core Actual` | Status, plus QI-Core's own values, position-aligned with the Population/Expected/Actual lists |

Status is scored against the **same fixture MeasureReport expectations** the UQC columns use, not against the UQC engine's output:

- **PASS** — QI-Core reproduced every expected population. Where the UQC engine fails such a case, the failure is a **migration regression**, not a parity gap.
- **FAIL** — QI-Core disagrees with the fixture, i.e. **parity**: both engines are wrong the same way.
- **MISSING** — QI-Core produced no result for that test case at all. Absence of evidence, not a pass. `QI-Core Actual` shows `MISSING` in that case rather than a blank.

Two things to know before reading the numbers:

- **`MISSING` counts toward `QI-Core Also Failing`.** Read the column as "not reproduced on QI-Core", not "confirmed broken on QI-Core".
- **Its denominator will not match `Fail Count`.** The column counts failing *(test case, group)* pairs, matching the per-measure tables; `Fail Count` counts each test case once across groups. A measure with four groups failing for one patient can read `4 of 4` while contributing 1 to `Fail Count`.

`output_results.csv` is unaffected — QI-Core is a reporting dimension, not a fourth scoring input.

##### The `## QI-Core Parity` section

Every column above is scoped to test cases UQC *already fails*, which is the right scope for triaging a failing measure but the wrong one for asking "how far apart are these two engines, really?". That question needs the passing cases too, so the report adds a `## QI-Core Parity` section between the CQFM exclusions and "Measures with No Discrepancies". It contains:

- **A 3×3 verdict cross-tab** — CMS verdict × QI-Core verdict, over every test case in the expected results. Both verdicts come from one function (`side_status`), so the parity view can never contradict the UQC columns.
- **Total disagreement** — test cases and authored population cells where the two engines' values actually differ.
- **The one actionable bucket** — CMS fails, QI-Core reproduces the fixture — as a per-measure table (cases and cells) plus the test case GUIDs grouped under each measure.

The reverse bucket, cases CMS passes and QI-Core does not, is **counted but not listed**, and gets no header row either. It is not CMS work — on every such case in this corpus the two repositories' fixture expectations agree and only the engine output differs, so it is QI-Core-side staleness (catalogued as I-01) and a reason to refresh the QI-Core snapshot. Listing ~740 test cases of it cost far more report than the information was worth. Its size stays visible in the cross-tab as the `PASS`/`FAIL` and `PASS`/`MISSING` cells, and the total disagreement stays visible in the section's opening line; `qicore_only_failures` is still computed so the parity record stays symmetric, but nothing prints it.

Two properties worth relying on:

- **Disagreement is scored cell-by-cell over the populations the fixture authored**, not by comparing the two population maps for equality. CMS and QI-Core emit different *extra* populations on some measures, and counting those would inflate the total with cells nobody wrote an expectation for.
- **The disagreement total is larger than the two buckets summed.** Two engines can disagree about a case both of them get wrong the same way, so a case in `FAIL`/`FAIL` still counts as a disagreement. Do not expect the buckets to reconcile to the headline number.

**Unit of account.** A test case in this section is one `(measure, GUID, group)` triple — the same unit the per-measure tables use — so the cross-tab totals more than the header's `Total Test Cases`, which counts each patient once per measure. The section states both figures so they can be reconciled by group rather than compared as totals. This is the same `(test case, group)` vs. test case distinction already noted for `QI-Core Also Failing` vs. `Fail Count`; it is not new arithmetic, just a new place it shows up.

Measure names in the bucket table are linked only when the measure also has its own `####` section further down, so the table never emits a dead anchor.

##### Implementation issues (optional)

Some test cases score differently on CMS and QI-Core only because the measure steward's test data misuses FHIR or CQL semantics. These are class `implementation` in `defect-tracking/known-issues.md`; I-67 is an example (a start-only `abatementPeriod` used to mean a resolution date). Neither engine side owes a fix, so these cases shouldn't show up as migration regressions. They are listed by hand in:

- `scripts/comparison/implementation_issues.csv`, with columns `issue_id,measure_name,guid,group,note`. Leave `group` blank to cover every group of the test case. Quote any `note` that contains a comma. The script stops with an error on a missing required column or an unquoted comma.
- **If the file is absent or empty, nothing changes.**

A listed case that the two engines score differently (one `PASS`, the other not) is taken out of both asymmetric buckets and reported in its own subsection, `### Implementation Issues, Not Parity Defects`, with its issue ID, the verdict pair per group, and the note. It is also left out of the header's `CMS Fail / QI-Core OK` row and the per-measure `CMS Fail / QI-Core OK` column. When there are any, the header gains an `Implementation Issues (not parity)` row, and notes in the section and under the summary table explain why the cross-tab and the two columns no longer reconcile. Listed cases that both engines score the same way are left alone.

PASS/FAIL scoring, `output_results.csv`, the cross-tab and the disagreement counts are unchanged: an implementation-issue case that fails still counts as a failure.

To refresh the copy after a QI-Core re-run:

```sh
cp ../dqm-content-qicore-2025/scripts/comparison/actual_results.csv \
   scripts/comparison/qicore_actual_results.csv
```

### `comparison/populations.py`

Support module for the above. It does two things that exist to stop the report inventing differences that are not really there, so it's worth knowing about before you interpret a result:

- **Canonicalises population names.** The expected CSV takes names from the fixture MeasureReport's `code.coding[0].display`, while the engine emits its own spelling. CMS986's "Measure Population Observation" and "Measure Observation" are the same cell under two names; without canonicalisation those rows could never match and every one would read as a failure.
- **Excludes CQFM measure-observation populations from scoring.** CMS1017, CMS871 and CMS986 wire a `measure-observation` to a parameterized CQL function that is meant to be invoked once per member of another population and then aggregated (`cqfm-criteriaReference` + `cqfm-aggregateMethod`). This workflow has no way to perform that computation, so those cells are listed in their own report section instead of being scored — see `defect-tracking/known-issues.md`, I-62. Every other population on those three measures is scored normally.

### `validate_test_fixtures.py`

Sanity-checks the per-measure test case fixtures under `input/tests/measure/` for
internal-reference consistency — the kind of data-authoring bug that produces a silent zero
population instead of an error, and so is easy to misdiagnose as a CQL or engine defect (see
I-07 below).

- **Patient-reference mismatches.** Each test case is scoped to one patient (`context Patient`);
  a resource whose `subject` / `patient` / `beneficiary` / `Task.for` (etc.) points at a
  *different* or non-existent patient is invisible to the measure logic. Auto-discovers every
  such field across a folder's resources and classifies each mismatch.
- **Missing required fields.** Some resource types (`Task.for`, `Encounter.subject`,
  `MedicationAdministration.subject`, `Observation.subject`) need that reference present at all
  for `context Patient` retrieval to work — not just correct. A resource missing it entirely is
  flagged as `MISSING-REQUIRED-FIELD`.
- **This is exactly what was needed to fix CMS816** (I-07, see `defect-tracking/change-log.md`):
  17 of its 28 fixture Encounter resources had no `subject` at all, so the engine silently
  dropped them from every retrieve no matter how correctly their type/status/period were
  authored — 12 cases failed visibly, 5 more carried the same defect without a visible symptom.
  `python3 scripts/validate_test_fixtures.py --measure CMS816FHIRHHHypo --fix-required-fields
  --apply` found and fixed all 19 affected files.

```sh
python3 scripts/validate_test_fixtures.py                                    # report only
python3 scripts/validate_test_fixtures.py --measure CMS104                   # one measure
python3 scripts/validate_test_fixtures.py --json                             # machine output
python3 scripts/validate_test_fixtures.py --fix --apply                      # rewrite CORE fields
python3 scripts/validate_test_fixtures.py --fix-required-fields --apply      # inject missing for/subject
```

No `--apply` run ever commits anything — review with `git diff` first. See the script's own
docstring for the full field taxonomy and the `--fix-profile-ns` migration mode.

## Reproducing Results Without Running CQL

To quickly reproduce the comparison results without running the CQL plugin:

- Extract `./scripts/results-connectathon-2025-09-13.zip` to `./input/tests/results`.
- This will populate the results directory with a captured run, allowing you to run the scripts and regenerate the comparison outputs.

```sh
unzip scripts/results-connectathon-2025-09-13.zip -d input/tests/results
python3 scripts/extract_population_expected.py
python3 scripts/extract_population_actual.py
python3 scripts/compare_results.py
```

## Unit Tests

- Unit tests are provided for the modules with logic worth pinning
  - run them from the root directory: `python3 -m pytest`
  - or individually, e.g. `python3 -m pytest scripts/tests/test_populations.py`
  - or `python3 -m pytest scripts/tests/test_validate_test_fixtures.py`
  - `test_compare_results_qicore.py` covers the QI-Core parity columns, the
    whole-corpus `qi_core_parity` cross-tab, and asserts that a run without the
    QI-Core file produces no QI-Core content.
