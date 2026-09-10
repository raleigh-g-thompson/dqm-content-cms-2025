# scripts/ — Test Case Comparison & Report Pipeline

This folder regenerates the measure-attribution report suite (`discrepancy_report.md`,
`catalog_issue_details.md`, `defect-tracking/engine-issues.md`,
`defect-tracking/improvement-tracking.md`, and the `actual_results.csv` /
`output_results.csv` workbooks) after a fresh CQL measure execution.

The single entry point is `python3 scripts/run_reports.py` — it runs every step
in the right order and verifies the generated artifacts agree with their own
sources. This README exists to make that command discoverable and to document
the individual scripts you might need to call directly.

## Quick start

After re-running the measures in VS Code (which writes `*.txt` traces or
`TestCaseResult-*.json` files into `input/tests/results/`):

```sh
# Run from the repo root.
python3 scripts/run_reports.py
```

That's it. The script writes a fresh `actual_results.csv` from the engine
output, recompiles the issue catalog, scores expected vs actual, regenerates
both Markdown trackers, appends one line to the run-history log, archives the
report (capped at the 10 newest), and drift-checks the result.

### Install the local pre-commit guard (recommended, one command per clone)

```sh
git config core.hooksPath scripts/git-hooks
```

After that, every commit runs `pytest -q` and `scripts/check_generated.py`
before you can commit — drift between a generated file and its source becomes
a build failure instead of a silent bug.

## What `run_reports.py` does

Six steps, each prints its banner so you can see progress on a long run:

| Step | Calls | Purpose |
|------|-------|---------|
| 0/6 | `extract_population_actual.main()` | turns `input/tests/results/` into `actual_results.csv` (auto-detects `*.txt` vs `TestCaseResult-*.json`) |
| 1/6 | `build_catalog.main()` | compiles `defect-tracking/issues/` into `known_issues.json` |
| 2/6 | `compare_results.main()` | scores actual vs expected, writes the report + diff CSVs, appends to `run-history.jsonl`, archives the report |
| 3/6 | `generate_improvement_tracking.main()` | regens `defect-tracking/improvement-tracking.md` from the run-history log |
| 4/6 | `generate_engine_issues_doc.main()` | regens `defect-tracking/engine-issues.md` from `known_issues.json` |
| 5/6 | `check_generated.run_checks()` | drift-checks every generated artifact against its source — must always pass after steps 0–4 |

## Artifacts produced

| File | Source it is regenerated from | Hand-edit? |
|------|-------------------------------|------------|
| `scripts/comparison/actual_results.csv` | `input/tests/results/` (step 0) | No |
| `scripts/comparison/known_issues.json` | `defect-tracking/issues/` (step 1) | No |
| `scripts/comparison/output_results.csv` | `expected_results.csv` × `actual_results.csv` (step 2) | No |
| `scripts/comparison/discrepancy_report.md` | above + `known_issues.json` (step 2) | No |
| `scripts/comparison/catalog_issue_details.md` | step 2 output (step 2) | No |
| `scripts/comparison/qicore_engine_diff.csv` | `qicore-2025-actual-results.csv` (step 2) | No |
| `scripts/comparison/run-history.jsonl` | appended each run (step 2) | No |
| `scripts/comparison/_archive/discrepancy_report-*.md` | newest copy of the report (step 2); capped at 10 | No |
| `defect-tracking/improvement-tracking.md` | `run-history.jsonl` (step 3) | No |
| `defect-tracking/engine-issues.md` | `known_issues.json` (step 4) | No |
| `scripts/comparison/expected_results.csv` | refreshed only when MeasureReports change (run `extract_population_expected.py` by hand) | No |

If you ever need to regenerate a file, run the step that owns it — never edit
a generated file by hand. `check_generated.py` will catch the divergence.

## `run_reports.py` flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--results-dir` | `./input/tests/results` | where the CQL engine writes traces/JSONs |
| `--skip-extract` | off | bypass step 0; leave `--actual` untouched (useful when the CSV is already up to date or when running tests against a fixture) |
| `--skip-catalog-build` | off | bypass step 1; use the on-disk `known_issues.json` |
| `--skip-drift-check` | off | bypass step 5 (don't do this in CI) |
| `--run-history-path` | `scripts/comparison/run-history.jsonl` | override the append target — tests point this at a temp file to stay hermetic |
| `--expected` | `scripts/comparison/expected_results.csv` | expected rows |
| `--actual` | `scripts/comparison/actual_results.csv` | the CSV step 0 writes and step 2 reads |
| `--output` | `scripts/comparison/output_results.csv` | scored results CSV |
| `--report` | `scripts/comparison/discrepancy_report.md` | markdown report |
| `--known-issues` | `scripts/comparison/known_issues.json` | compiled catalog |
| `--issues-dir` | `defect-tracking/issues` | authored catalog source |
| `--qicore-actual` | `scripts/comparison/qicore-2025-actual-results.csv` | QI-Core engine output |
| `--qicore-diff-csv` | `scripts/comparison/qicore_engine_diff.csv` | where the QI-Core diff is written |
| `--engine-issues-output` | `defect-tracking/engine-issues.md` | where step 4 writes |
| `--improvement-tracking-output` | `defect-tracking/improvement-tracking.md` | where step 3 writes |

Run from the repo root. All defaults are relative to it.

## Standalone scripts

Use these directly when you need to do exactly one step.

### `extract_population_actual.py`

Parses CQL engine output (`*.txt` traces or `TestCaseResult-*.json`) into
`actual_results.csv`. Auto-detects format. Called as step 0 by
`run_reports.py`; useful when you want to inspect the CSV without running the
full report.

```sh
python3 scripts/extract_population_actual.py
python3 scripts/extract_population_actual.py --results-dir ./input/tests/results --output /tmp/actuals.csv
```

### `extract_population_expected.py`

Parses `MeasureReport-*.json` and the corresponding Measure resources into
`expected_results.csv`. **Not part of the daily flow** — only re-run when
MeasureReport expected results change.

### `build_catalog.py`

Compiles the issue catalog (`defect-tracking/issues/*.md` + `cases.csv` +
`_preamble.md` + `_cross_cutting_lessons.md`) into
`scripts/comparison/known_issues.json`. Called as step 1 by `run_reports.py`.

### `compare_results.py`

Scores actual vs expected, writes `output_results.csv` + the discrepancy
report, appends to `run-history.jsonl`, and archives the report.
Called as step 2 by `run_reports.py`.

### `generate_improvement_tracking.py`

Regenerates `defect-tracking/improvement-tracking.md` from the run-history
log. Called as step 3 by `run_reports.py`.

### `generate_engine_issues_doc.py`

Regenerates `defect-tracking/engine-issues.md` from `known_issues.json`.
Called as step 4 by `run_reports.py`.

### `check_generated.py`

Drift-checks all four generated Markdown/JSON artifacts against their own
sources. Called as step 5 by `run_reports.py`; also invoked by the
pre-commit hook.

### QICore subflow (opt-in, separate locked baseline)

- `extract_population_qicore.py` — parses the QI-Core engine bundle
  (`input/tests/results/qicore/`) into `qicore-2025-actual-results.csv`.
  Output is hash-by-row comparable with the locked baseline; `--force` is
  required to overwrite it.
- `audit_qicore_baseline.py` — diffs a fresh QI-Core run against the locked
  baseline and emits a per-measure drift report.

These are NOT part of `run_reports.py`. They are a separate subflow for
comparing CMS vs QI-Core engine output, gated on `--force` so a stray run
can't clobber the locked baseline.

### Utility scripts (used sparingly, named by what they fix)

- `validate_test_fixtures.py` — sanity-checks the per-measure test case
  fixtures under `input/tests/measure/`.
- `fix_cms347_fixture_data.py` — one-shot data repair for CMS347 fixtures.
- `rekey_cms1264_fixtures_to_2026.py` — one-shot rekey of CMS1264 fixtures
  to the 2026 publication year.
- `reattribute_uscore_profiles.py` — one-shot attribution fixup for
  USCore-profile measures.

## Reproducing results without running CQL

If you don't want to fire up the VS Code extension, the connectathon result
bundle reproduces a complete run:

```sh
unzip scripts/results-connectathon-2025-09-13.zip -d input/tests/results
python3 scripts/run_reports.py
```

## Gotchas

- **Run from the repo root.** All default paths (`./input/tests/results`,
  `./scripts/comparison/...`, `./defect-tracking/...`) are relative to it.
  Running from anywhere else will silently fail or write to surprising
  locations.
- **Python 3.12.** Scripts are developed and tested on Python 3.12. Use
  `python3`, not `python`.
- **`*.txt` vs `TestCaseResult-*.json`** is auto-detected by
  `extract_population_actual.py`. If auto-detect picks the wrong format, pass
  `-txt` or `-jr` explicitly.
- **Scaffold measures (`testE*`, `defectHelper`)** are excluded from scoring
  by `is_scaffold_measure()` in `scripts/comparison/attribution.py`. Their
  rows in `expected_results.csv` are intentional repro assets and never
  appear as failures.
- **`expected_results.csv` is static.** Only refresh it via
  `extract_population_expected.py` when MeasureReport expected results
  actually change — a re-run of `run_reports.py` does not touch it.
- **`scripts/comparison/_archive/` is capped.** Newest 10
  `discrepancy_report-*.md` are kept; SHA-256 byte-identical reruns dedupe
  against the existing archive. The folder is gitignored.
- **Never hand-edit a generated file.** `check_generated.py` (and the
  pre-commit hook) catches the divergence and fails the build.
- **Hermetic tests use temp `--run-history-path` and `--skip-extract`.**
  Without those, a `pytest` run would pollute the real
  `scripts/comparison/run-history.jsonl` and overwrite your fixture
  `actual_results.csv`.

## Tests

```sh
# From the repo root:
python3 -m pytest -q
```

The pre-commit hook runs the same command on every commit once you've set
`core.hooksPath` to `scripts/git-hooks` (see Quick start).
