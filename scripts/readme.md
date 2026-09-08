# Test Case Comparison Process

This repository provides a workflow for comparing expected and actual results for measure test cases. The process generates a comparison file at `./scripts/comparison/output_results.csv` that summarizes PASS/FAIL for each population result.

> **Note (2026-09):** This file describes the original 3-step manual workflow, which is
> still accurate for steps 1-2 below. Step 3 (`compare_results.py`) now has additional
> siblings that should run alongside it -- see **Recommended: `run_reports.py`** below.
> A full rewrite of this document is planned; until then, treat this note as the
> up-to-date entry point.

## Recommended: `scripts/run_reports.py`

After running `extract_population_expected.py` and `extract_population_actual.py` (steps
1-2 below), run:

```
python3 scripts/run_reports.py
```

instead of calling `compare_results.py` directly. It runs the comparison, regenerates
`defect-tracking/engine-issues.md` from the issue catalog, and verifies neither generated
file has drifted from its source -- all three in one command. Running `compare_results.py`
alone (or editing `scripts/comparison/known_issues.json` without a follow-up regen) is how
`engine-issues.md` forked silently from its source for 11 commits before anyone noticed; see
`scripts/check_generated.py`'s docstring.

To catch that kind of drift (or a test regression) before it's committed, install the local
git hook once per clone:

```
git config core.hooksPath scripts/git-hooks
```

## Workflow Steps

1. **Run the CQL plugin**  
   Execute the CQL plugin to generate actual test results for each measure. This will create files in the `./input/tests/results` directory.

2. **Run the scripts in order:**
   - **Step 1:** `extract_population_expected.py`  
     Parses expected results from MeasureReport files and Measure resources.  
     _Only rerun when MeasureReport expected results change._
   - **Step 2:** `extract_population_actual.py`  
     Parses actual results from the output of the CQL plugin in `./input/tests/results/{MeasureName}.txt`.  
     _Rerun whenever these result files change._
   - **Step 3:** `compare_results.py`  
     Compares expected and actual results, producing the summary file `./scripts/comparison/output_results.csv`.  
     _Rerun whenever either expected or actual results change._

_NOTES_

- Execute scripts from the project's root directory
  - `python ./scripts/<SCRIPT NAME>`
- Scripts developed/tested using `python 3.12`

## Script Descriptions

### `extract_population_expected.py`

- Reads MeasureReport JSON files and corresponding Measure resources.
- Extracts population codes and criteria expressions as display names.
- Outputs a CSV of expected results to `./scripts/comparison/expected_results.csv`.

### `extract_population_actual.py`

- Reads actual result files from `./input/tests/results`.
- Parses population results for each test case, handling boolean and list values.
- Outputs a CSV of actual results to `./scripts/comparison/actual_results.csv`.

### `compare_results.py`

- Compares expected and actual results by measure name, guid, and display name.
- Outputs a summary CSV to `./scripts/comparison/output_results.csv` with PASS/FAIL status for each population.
- Prints the number and percentage of PASS and FAIL items to the terminal.

## Reproducing Results Without Running CQL

To quickly reproduce the comparison results without running the CQL plugin:

- Extract `./scripts/results-connectathon-2025-09-13.zip` to `./input/tests/results`.
- This will populate the results directory with sample data, allowing you to run the scripts and generate `./scripts/comparison/output_results.csv`

## Unit Tests

- Unit tests are provided
  - run tests from the command line in the root directory
    - _Note: Do not include the file extension_
  - example: `python -m scripts.tests.test_extract_population_actual`
