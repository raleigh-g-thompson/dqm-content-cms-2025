# Measure Parity Notes

This document tracks the ongoing effort to fix CQL measure test failures in this repo, why the
work is scoped the way it is, and the status of what's been found and fixed so far. It exists so
that work can resume across sessions (human or AI) without re-deriving context from scratch.

## Why this work exists

The repo is mid-migration from the QICore 6.0.0 model to the new US Quality Core (USQualityCore)
model (see `USQualityCoreUpdateProcess.md`). After that migration, the measure test suite regressed:
`scripts/comparison/discrepancy_report-main-2026-08-20.md` showed 61 of 74 measures failing (up
from 44/73 in the January pre-connectathon baseline).

**RESOLVED 2026-08-23 (user directive): the goal is 100% passing of all measures after
converting to USQualityCore, on this repo's Java engine (Clinical Reasoning / cql Engine).**
This is not an engine-vs-engine comparison exercise — we are finding gaps in the local Java
engine implementation surfaced by the test suite. A failing test means either (a) a migration
regression in the converted CQL or fixtures — fix the conversion — or (b) a genuine gap or
limitation in the Java engine — apply and document the narrowest possible CQL workaround. The
framing below (the "category 1/2/3" bucketing) was this document's original working theory and is
now superseded — kept struck through/inline for history, but do not apply it to new work. It also
resolves the "Open question" that used to live here: every mismatch found so far (#6
`doNotPerform`, #7/#11 wrong patient references, #10/#15 `.onset.toInterval()` vs
`.prevalenceInterval()`, #13 `doNotPerform` again, #14 missing `Claim.item` links, #16/#18) was a
genuine migration regression or an engine limitation with a known workaround.

**What this means practically going forward:**

- The correctness question for any mismatch is: **does the USQualityCore-converted CQL correctly
  reproduce the pre-migration QICore logic when run on the Java engine?** If it does but still
  fails, treat it as an engine gap.
- **Any change that deviates from the original QICore implementation — converted CQL logic or
  test fixtures — must be documented in the changelog below**, including the reason (migration
  regression fix vs. engine-gap workaround).
- `dqm-content-qicore-2025` (`https://github.com/cqframework/dqm-content-qicore-2025`, see the
  memory note saved 2026-08-21) is the actual ground truth to diff a measure's `define`s against
  when in doubt — not the test fixtures' expected `MeasureReport` values in isolation. If a fix
  looks right by CQL-authoring convention but the QICore original did something different on
  purpose, that's worth a second look before applying it.
- The `input/tests/measure/*/MeasureReport-*.json` "expected" values and `cqfm-testCaseDescription`
  extensions describe real clinical intent and remain useful for understanding what a test case is
  trying to exercise. The comparison scripts (`scripts/comparison/compare_results.py` etc.) are
  the coverage signal for "did this fix change what I expected."
- No more "flag category 3, don't touch it" — a mismatch that looks like a genuine CQL bug should
  just be fixed (verified against the QICore source when the fix is non-obvious), not parked.

## Verification loop

Three scripts, run in order from the repo root after CQL/fixture changes and a fresh CQL-engine
test run (`input/tests/results/*.txt` regenerated — this repo doesn't run that engine directly
from a shell tool; it's driven by the VS Code CQL extension test harness):

```sh
python ./scripts/extract_population_expected.py   # only needed if MeasureReport expected results changed
python ./scripts/extract_population_actual.py      # re-run whenever input/tests/results/*.txt changes
python ./scripts/compare_results.py                 # regenerates scripts/comparison/discrepancy_report.md
```

See `scripts/readme.md` for details.

## Where to look

- **Discrepancy reports**: `scripts/comparison/discrepancy_report*.md` — per-measure pass/fail
  breakdown, including which population (Initial Population, Denominator, Numerator, Denominator
  Exclusion/Exception) mismatched, for which test-case GUID, expected vs. actual.
- **Per-test-case trace dumps**: `input/tests/results/<Measure>.txt` — a full dump of every CQL
  `define`'s computed value per test case, in roughly alphabetical-by-name order per patient block
  (blocks are separated by blank lines; `Patient=Patient(id=...)` appears partway through a block,
  not necessarily first — don't assume ordering, split on blank lines and read the whole block).
- **Test fixtures**: `input/tests/measure/<Measure>/<guid>/` — the FHIR resources for one test
  case, plus the expected `MeasureReport-*.json`. The `MeasureReport`'s
  `cqfm-testCaseDescription` extension often states the clinical intent in plain English — read it
  before assuming a mismatch is a bug.
- **Unit-test / repro libraries (engine-defect isolation)**: when demonstrating or isolating an
  engine/translator defect, author a standalone CQL library **prefixed `test`** (e.g.
  `testE15ConditionUnionPrevalence.cql`). Conventions:
  - The test case fixture folder is `input/tests/measure/<CQL basename without .cql>/<patient-id>/`
    (mirrors the production measure layout).
  - Two registration resources are required for the engine to resolve the test library; without
    them the library loads with `id/version null`
    (`Error=Library <name> was included with version null`):
    - `input/resources/library/test<Name>.json` — Library resource with id/version `0.0.000` matching
      the CQL `library <name> version '0.0.000'` declaration.
    - `input/resources/measure/test<Name>.json` — Measure + library reference. The test library
      "becomes the measure" for harness purposes.
  - Canonical examples: `testE15ConditionUnionPrevalence` (FAILING + PASSING defines) and
    `testE15Passing` (PASSING-only isolation), both live in `defect-tracking/_reference` context and
    are referenced from `defect-tracking/engine-issues.md`. New repro: `testE18DateTimeCompare`
    (E-18). Note: this repo has no CI for CQL behavior — these libraries run via the VS Code CQL
    test harness, not automated tests.
- **Engine/translator source** (for anything that looks like an engine-level issue, not a CQL
  issue): `/Users/raleigh.thompson/projects/smile/vs-code-cql/_repo/clinical_quality_language`
  (cql-to-elm translator, cql-engine) and
  `/Users/raleigh.thompson/projects/smile/vs-code-cql/_repo/clinical-reasoning` (FHIR integration
  layer — retrieve providers, code extraction, model resolvers). The CQL language server's output
  channel (in VS Code) shows real translation/evaluation errors and stack traces that the
  `input/tests/results/*.txt` summary lines don't include.
- **ELM/AST**: the VS Code CQL extension can generate ELM (JSON/XML) and an AST view for any
  library — invaluable for confirming exactly what a CQL expression compiled to, rather than
  guessing from the source text.

## Change log

Every change attempted in this effort, in chronological order, with the reasoning behind it and
its outcome. Entries marked **KEPT** are in the working tree (uncommitted unless noted).
Entries marked **REVERTED** were tried, didn't resolve the issue, and were removed — the CQL/repo
state was confirmed clean via `git diff` after reverting. This section is meant to be detailed
enough to share with teammates without needing to re-derive the reasoning.

### 1. Stale `onc` → `astp` profile namespace in test fixtures — KEPT, verified

- **Files**: 2,679 files under `input/tests/measure/`, across 12 measures (led by
  `CMS347FHIRStatinPreventionTxCVD` at 1,129 files, `CMS129FHIRProstCaBoneScanUse` at 459,
  `CMS69FHIRPCSBMIScreenAndFollowUp` at 316, plus `CMS56FHIRFuncStatHipReplacement`,
  `CMS90FHIRFSAforHeartFailure`, `CMS50FHIRReceiptofSpecialistReport`,
  `CMS68FHIRDocumentationCurrentMeds`, `CMS75FHIRChildrenDentalDecay`,
  `CMS74FHIRDentalCariesPrevention`, `CMS165FHIRControllingHighBP`,
  `NHSNGlycemicControlHypoglycemiaInitialPopulation`, `CMS135FHIRACEIorARBorARNIforHF`).
- **Why**: the IG renamed its US Quality Core profile namespace from
  `http://fhir.org/guides/onc/us-quality-core/...` to
  `http://fhir.org/guides/astp/us-quality-core/...`. `usqualitycore-modelinfo-0.1.0-cibuild.xml`
  and every `.cql` library had already moved to `astp` (confirmed 0 remaining `onc` references in
  either), but these 2,679 fixture files still had `meta.profile` and extension URLs on the old
  `onc` path. USQualityCore profile-typed retrieves (e.g. `[USQualityCore.Encounter: ...]`,
  `isEncounterPerformed()`) classify a resource by matching `meta.profile` against the model info's
  declared profile URL — a fixture on the stale URL silently fails that match and the retrieve
  returns empty, with no error.
- **Fix**: bulk find-and-replace `onc` → `astp` in the profile URL, scripted (not hand-edited).
- **Verified**: suite-wide pass rate 85.23% → 87.51% (fail count 3504 → 2962). Confirmed via a
  fresh test run + `compare_results.py`.

### 2. CMS1264FHIRECATREHQR default Measurement Period — KEPT, verified

- **File**: `input/cql/CMS1264FHIRECATREHQR.cql`.
- **Why**: 57 of 58 test cases failing (98%). The `"Measurement Period"` default was
  `Interval[@2026-01-01, @2027-01-01)`, a year behind this measure's own test fixtures (e.g. an
  encounter dated `2027-05-01`) and its expected `MeasureReport.period` (`2027-01-01`–
  `2027-12-31`). No test case in the repo carries a `Parameters` resource, so this CQL default is
  what actually executes — every other measure in the suite uses the 2026 default and has fixtures
  dated accordingly; CMS1264 alone was authored a year ahead without updating its default.
- **Fix**: changed the default to `Interval[@2027-01-01, @2028-01-01)`.
- **Verified**: moved CMS1264 into "Measures with No Discrepancies" — 58/58 pass.

### 3. Invalid UCUM system URI in 3 test fixtures — KEPT, verified

- **Files**:
  `input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-7eb0-47b0-8c77-905944706aa1/Observation-dce97708-c832-464b-a62e-0f15142b6a10.json`,
  `input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/45b1ce40-0f49-4559-8c3b-5c2a8070b0a7/Observation-db3631fe-ff4a-47b7-8380-1d7e13bc119c.json`,
  `input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/7b34e64e-e7fe-402c-9a26-12da90662897/Observation-b13e9323-0c0c-445d-a7b0-d90dad09fc88.json`.
- **Why**: `valueQuantity.system` was `"https://ucum.org"` instead of the correct canonical UCUM
  system URI `"http://unitsofmeasure.org"` (used correctly 1,001 other times across the fixture
  set) — a data typo, not a CQL issue. Caused
  `FHIRHelpers.ToQuantity.InvalidFHIRQuantity: Invalid FHIR Quantity code: mg/dL` and showed up as
  4 "Missing Results" rows in CMS347 (one test case × 4 groups, since all 4 groups reference the
  same fixture).
- **Fix**: corrected all 3 to `http://unitsofmeasure.org`.
- **Verified**: the affected test cases moved from "Missing Results" (execution error) to either
  passing or a real, diagnosable mismatch.

### 4. CMS871FHIRHHHyper missing valueset — KEPT, verified (partially)

- **File added**:
  `input/vocabulary/valueset/external/ValueSet-2.16.840.1.113762.1.4.1196.394-20250227.json`.
- **Why**: `"Hypoglycemics Treatment Medications"`
  (`http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113762.1.4.1196.394`) had no corresponding file
  in `input/vocabulary/valueset/external/`, causing `"Unable to locate ValueSet ..."`. Found a
  fully-expanded, correctly-formed copy already sitting in the IG Publisher's terminology cache
  (`input-cache/txcache/vs-4d4e1cfc-8e1d-45e5-ae0e-f62f71192e14.json` — 238 codes, version
  `20250227`) — evidently resolved once during a prior IG publish but never persisted into the
  repo's committed vocabulary source.
- **Fix**: copied the cached expansion into `input/vocabulary/valueset/external/`, matching the
  naming convention of sibling files (`id` field updated to include the date suffix, e.g.
  `2.16.840.1.113762.1.4.1196.394-20250227`).
- **Also determined** (no code change needed): CMS871's separate
  `"Invalid Interval - the ending boundary (0) must be greater than or equal to the starting
  boundary (1)"` error is a *downstream cascade* of the `Min()`/`DateTimeType` engine bug (see
  external issues below), not an independent defect —
  `hospitalDaysMax10()` (`CMS871FHIRHHHyper.cql:238-241`) calls `Min({...})`, which throws; the
  resulting garbage `Period` then breaks `daysInPeriod()`'s day-count construction
  (`USQualityCoreCommon.cql:219-225`).
- **Verified**: CMS871's missing-result count dropped (5 → 3, matching the number of test cases
  whose failure was solely the missing valueset, not the separate engine bug). The remaining
  failures trace to the external `Min()` issue, not this repo.

### 5. Comparison script — group-scoped denominator gating — KEPT, verified, repo-wide impact

- **File**: `scripts/extract_population_actual.py`,
  `validate_measure_population_counts()`.
- **Why**: for multi-stratum measures — CMS347 has 4 groups, each with its own `Denominator N`
  criteria expression, but *one shared* `Numerator` / `Denominator Exceptions` /
  `Denominator Exclusions` expression referenced by all 4 groups (confirmed directly from the
  Measure resource's population `criteria.expression` mapping in
  `input/resources/measure/CMS347FHIRStatinPreventionTxCVD.json`) — the script reported the raw
  shared boolean as the "actual" value for every group referencing it, without checking whether
  the patient was even a member of *that specific group's* denominator. Concretely: a patient in
  only `Denominator 3` was reported with `Denominator Exception Actual=1` for Groups 1, 2, and 4
  too, groups they were never in. The existing validation logic tried to handle this (there was
  already a `if not numer and not denom: denexc_count = 0` rule) but keyed off `numer`, which is
  *also* a shared, non-group-scoped value in this class of measure — so the rule silently failed to
  fire whenever the shared `Numerator` happened to be `true` from another group's perspective.
- **Fix**: added an explicit, unconditional gate — `if not denom_count: numer_count = numex_count
  = denex_count = denexc_count = 0` — evaluated *before* the existing (single-stratum-oriented)
  adjustment rules, since a patient who isn't in a group's own denominator can never belong to that
  group's numerator, exclusion, or exception population, full stop, regardless of what any shared
  expression says.
- **Verified**: `python ./scripts/extract_population_actual.py && python
  ./scripts/compare_results.py` — CMS347's mismatches dropped 134 → 74. Confirmed via a scripted
  diff against the prior report that CMS347 was the *only* measure in the current dataset whose
  counts changed — no regressions elsewhere. This fix is generic (not CMS347-specific); watch for
  it helping other multi-stratum measures as Phase 2 continues.

### 6. CMS347 CQL bugs: `doNotPerform` and an intent-code typo — KEPT, verified

- **File**: `input/cql/CMS347FHIRStatinPreventionTxCVD.cql`.
- **Why**: `"Statin Therapy Ordered during Measurement Period"` and
  `"Medication Active during the Measurement Period"` retrieved
  `[MedicationRequest: "... Statin Therapy"]` without excluding `doNotPerform = true` records. A
  `MedicationRequest` profiled as `us-quality-core-medicationnotrequested` — i.e. an explicit
  "do NOT perform, contraindicated" record, which is the *correct* representation for a
  denominator-exception case — was being double-counted as an actual statin order, wrongly
  flipping `Numerator` to `true` and suppressing the intended `Denominator Exception`.
  Root-caused via one concrete failing test case (`8b0f2e04-...`, description: "no high statin
  prescribed d/t contraindicated") whose `MedicationRequest` fixture has `"doNotPerform": true`.
  Confirmed `doNotPerform` is a declared element on `USQualityCore.MedicationRequest` in the model
  info, and that this "regular request vs. negation request" distinction is an established pattern
  elsewhere in the repo (`NHSNAcuteCareHospitalMonthlyInitialPopulation1.cql`'s
  `// doNotPerform is false or absent` comments). Also found a typo, `'filter-order'`, which should
  be the real FHIR `MedicationRequest.intent` code `'filler-order'` (confirmed against the correct
  spelling used in `AHAOverall.cql`).
- **Fix**: added `and ( StatinRequest.doNotPerform is null or not StatinRequest.doNotPerform )`
  (and the `ActiveStatin` equivalent) to both retrieves' `where` clauses; fixed the typo.
- **Verified**: CMS347's mismatches dropped 74 → 53 after a fresh test run.

### 7. CMS347 (and 3 other measures) test fixtures reference the wrong patient — KEPT, pending re-verification

- **Files**: 40 resource files fixed — 34 in `CMS347FHIRStatinPreventionTxCVD`, 3 in
  `CMS72FHIRSTKAntithromboticDay2`, 1 in `CMS108FHIRVTEProphylaxis`, 1 in
  `NHSNGlycemicControlHypoglycemiaInitialPopulation`.
- **Why**: of the remaining 53 CMS347 mismatches, every one still showing an
  Initial-Population/Denominator-level discrepancy traced to a resource (`Condition`, `Encounter`,
  `Observation`, `MedicationRequest`, `Procedure`, or `ServiceRequest`) whose `subject.reference`
  pointed at a *different* patient GUID than the one whose test-case folder it lived in — so the
  CQL, scoped to `context Patient`, correctly never saw that resource at all for the intended test
  case. Confirmed by scanning every resource in every test case folder for every measure and
  comparing `subject.reference` against that folder's own `Patient-<guid>.json`: 40 files across 4
  measures had a mismatch. These are data-authoring typos, not CQL bugs — one was a stray leading
  `d` character (`Patient/d759a89b4-...` instead of `Patient/759a89b4-...`), another had a literal
  embedded double-space splitting a GUID in half (`Patient/1d3021bb-b593-4efc-af5b-3  20243bbe9b7`).
  Each test case folder is self-contained with exactly one `Patient-*.json`, so the correct
  reference is unambiguous in every case.
- **Fix**: mechanically corrected each mismatched `subject.reference` to point to its own folder's
  patient GUID. Validated all touched files remain syntactically valid JSON afterward.
- **Verified**: CMS347's mismatches dropped 53 → 48 after a fresh test run. Fewer than the 34
  fixed references might suggest, because fixing a reference sometimes only revealed a *further*,
  separate issue for that same test case rather than making it pass outright (see #8 below — most
  of the newly-exposed failures turned out to be the shared-library parameter-default bug, not
  something wrong with the reference fix itself). `CMS72FHIRSTKAntithromboticDay2`,
  `CMS108FHIRVTEProphylaxis`, and `NHSNGlycemicControlHypoglycemiaInitialPopulation` showed no
  count change — their single fixed files weren't the cause of those measures' reported
  mismatches, but the references were still wrong and worth having fixed regardless.

### 8. Shared libraries missing a default `"Measurement Period"` — SUPERSEDED by #9 below

- **Files (original fix, since superseded)**: `input/cql/PalliativeCare.cql`,
  `input/cql/Hospice.cql`, `input/cql/AdvancedIllnessandFrailty.cql`.
- **Why**: after fix #7 above, 13 CMS347 mismatches remained, and their test-case descriptions
  were overwhelmingly palliative-care- or hospice-related (9 palliative-care cases spanning *all
  four* of `PalliativeCare.cql`'s OR-branches — Condition diagnosis, Encounter, Procedure, and the
  FACIT-Pal Assessment — plus 2 hospice cases). Checked each palliative-care case's underlying data
  individually (codes, valueset membership, dates, `.verified()`/`.isEncounterPerformed()` helper
  functions) and found nothing wrong with any of it. The one thing common to *every* branch of
  *both* libraries: `PalliativeCare.cql` and `Hospice.cql` both declare
  `parameter "Measurement Period" Interval<DateTime>` with **no default value**, unlike every
  other shared library that had one at the time (`AHAOverall.cql`, `CQMCommon.cql`, `VTE.cql`,
  `PCMaternal.cql`, `TJCOverall.cql`, `AdultOutpatientEncounters.cql`, `Antibiotic.cql`) and every
  measure — if the test harness doesn't propagate the calling measure's parameter value into an
  included library, `"Measurement Period"` would evaluate to `null` inside these two libraries,
  breaking every date-scoped check in them uniformly. `AdvancedIllnessandFrailty.cql` had the same
  gap, fixed preemptively.
- **Original fix (now removed, see #9)**: added the standard default to all three files.
- **Superseded**: you pointed out the harness actually supports parameters via
  `input/tests/config.json` (global and library/test-case-scoped), which is the better long-term
  fix — one source of truth instead of ~80 duplicated per-file defaults that can drift. See #9.
  The root-cause diagnosis above (missing default → null parameter → uniform failure) is still the
  reason this mattered; only the *fix location* changed.

### 9. Centralized `"Measurement Period"` via `input/tests/config.json`; removed all per-file CQL defaults — KEPT, pending re-verification

- **Files**: `input/tests/config.json`, plus all 82 `.cql` files that declare a
  `"Measurement Period"` parameter (every measure, plus `AHAOverall`, `CQMCommon`, `VTE`,
  `PCMaternal`, `TJCOverall`, `AdultOutpatientEncounters`, `Antibiotic`, `AdvancedIllnessandFrailty`,
  `Hospice`, `PalliativeCare`).
- **Why**: fix #8 (and CMS1264's earlier fix #2) worked by editing CQL defaults directly, but that
  meant the actual measurement period was duplicated across ~85 files with no single source of
  truth, and — as #8 demonstrated — easy to silently omit in a shared library. The CQL test
  harness supports parameter injection via `input/tests/config.json`, with global, library-scoped,
  and test-case-scoped entries (merge priority: global → library → test case; see
  `.vscode/extensions/cqframework.cql-0.9.9/schemas/cql-config.schema.json` for the schema). Moving
  to this makes the measurement period a single, explicit, version-controlled setting instead of
  an implicit convention repeated in every file.
- **Found and fixed two format bugs in the existing global entry** before proceeding: it was named
  `"MeasurementPeriod"` (no space) with type `"Interval<Date>"` and a date-only literal
  (`Interval[@2026-01-01, @2027-01-01)`), none of which match the CQL declarations
  (`parameter "Measurement Period" Interval<DateTime>`, used with full `DateTime` literals
  everywhere in the repo). Corrected the global entry's `name`/`type`/`value` to match exactly.
- **Added two library-scoped overrides** for the libraries whose correct measurement period
  genuinely differs from the global default (confirmed by checking their own test fixtures/expected
  results, not just their CQL default):
  - `CMS1264FHIRECATREHQR` → `Interval[@2027-01-01T00:00:00.000Z, @2028-01-01T00:00:00.000Z)`
    (matches fix #2 above — this measure's fixtures are dated a year later than everything else).
  - `NHSNAcuteCareHospitalMonthlyInitialPopulation1` → `Interval[@2026-01-01T00:00:00.000Z,
    @2026-02-01T00:00:00.000Z)` (a 1-month window — this is an NHSN *monthly* measure, intentionally
    not annual; its CQL default was already correctly a 1-month period, unlike everything else).
- **Fix**: commented out the `default Interval[...]` line in all 82 files with a note
  (`// Measurement Period default removed; value is now supplied via input/tests/config.json`),
  leaving `parameter "Measurement Period" Interval<DateTime>` declared with no default (valid CQL
  — the parameter becomes required at runtime instead). Left `AdultOutpatientEncounters.cql`'s
  slightly-differently-formatted default (`[@2026-01-01T00:00:00, @2026-12-31T23:59:59]` — no `Z`
  suffix, closed interval) commented out the same way rather than adding an override, since it's
  functionally equivalent to the global default (off by under a second, not a real behavioral
  difference). 5 shared libraries (`AlaraCommonFunctions`, `NHSNHelpers`, `Status`,
  `SupplementalDataElements`, `USQualityCoreCommon`) don't declare this parameter at all and were
  left untouched.
- **Status**: **VERIFIED — confirmed correct, large repo-wide win.** Fresh test run
  (`discrepancy_report-measure-fixes-20260821-1128.md`) vs. the prior report: fail count
  2804 → 2344 (460 fewer failing test cases), measures with any discrepancy 60 → 46 (14 more
  measures now fully passing), **zero regressions** (scripted diff confirmed no measure's
  mismatch count went up). 22 measures improved, most down to 0 mismatches — including several
  that had been stuck since Phase 0/1 (`CMS138FHIRTobaccoScrnCessation`,
  `CMS125FHIRBreastCancerScreen`, `CMS122FHIRDiabetesAssessGT9Pct`,
  `CMS136FHIRChildADHDMedFollowUp`, `CMS130FHIRColorectalCancerScrn`, and more). This confirms the
  harness *does* propagate `config.json` parameters into every library in the dependency graph,
  and that the root-cause diagnosis in #8 (missing/null `"Measurement Period"` breaking
  `Hospice.cql`/`PalliativeCare.cql`/`AdvancedIllnessandFrailty.cql` uniformly) was correct.
  `CMS347FHIRStatinPreventionTxCVD` alone dropped from 48 → 4 mismatches; the 4 remaining are
  distinct, unrelated issues (see next entry), not more instances of this bug.

### 10. CMS347 — 4 lingering, unrelated mismatches (not fixed, parking here)

Not fixed in this pass — noted for whoever picks CMS347 back up. Each is independent; none are
more instances of the config.json fix (#9) or of each other.

- **`"Has Diabetes Diagnosis"` (and likely `"Has ESRD Diagnosis"`, identical pattern) date-window
  bug** — affects test cases `8927dd81-b976-4b7f-a78c-c4215ee8fc9a` and
  `5e65bf6d-6518-44d7-a827-821b59b00cc0`. `input/cql/CMS347FHIRStatinPreventionTxCVD.cql:149-153`
  (`"Has Diabetes Diagnosis"`) uses `DiabetesDiagnosis.onset.toInterval() overlaps day of
  "Measurement Period"`. For a plain `onsetDateTime` (not a `Period`), `.toInterval()` produces a
  zero-width point interval, so a diagnosis with an onset date *before* the measurement period
  (e.g. `2022-12-31`, `clinicalStatus: active`, no abatement) can never "overlap" a 2026 period,
  even though it's an ongoing chronic condition that should count. Compare to
  `"ASCVD Diagnosis or Procedure before End of Measurement Period"` elsewhere in the same file,
  which correctly uses `"starts on or before day of end of Measurement Period"` for this exact
  "diagnosed at any point up through now, still active" pattern.
  `"Has ESRD Diagnosis"` (line 156) uses the identical `overlaps` pattern and is likely affected
  the same way, though not yet confirmed against a failing test case.
- **Allergy-to-statin exception not matching — FIXED, see #11 below.** Was a fixture bug (wrong
  `AllergyIntolerance.patient` reference), not a CQL issue.
- **`"Denominator Exclusions"` matches a resolved/inactive diagnosis** — test case
  `695b64d8-8102-4109-89c2-9ca128d43f4d` (description: "rhabdomyolysis dx last day of prior year
  but no longer 'active' in the MP"), `Denominator Exclusion` expected `0`, actual `1`.
  `input/cql/CMS347FHIRStatinPreventionTxCVD.cql:96-103` (`"Denominator Exclusions"`) checks
  `ExclusionDiagnosis.onset.toInterval() overlaps day of "Measurement Period" and
  ExclusionDiagnosis.isVerified()` but never checks `clinicalStatus` — so a diagnosis that's
  resolved/inactive by the time the measurement period starts still counts as an exclusion. Not
  yet fixed.

### 11. Wrong-patient references in `Claim`/`Coverage`/`AllergyIntolerance` fixtures across 7 measures — KEPT, pending re-verification

- **Files**: 188 fixture files total —
  97 `Claim-*.json` in `CMS72FHIRSTKAntithromboticDay2`, 70 in `CMS104FHIRSTKDCAntithrombotic`,
  2 each in `CMS108FHIRVTEProphylaxis`/`CMS190FHIRVTEProphylaxisICU`, 1 each in
  `CMS1028FHIRPCSevereOBComps`/`CMS1264FHIRECATREHQR`/`CMS71FHIRSTKAnticoagAFFlutter`;
  14 `Coverage-*.json` (`beneficiary` field) across `CMS347`, `CMS104`, `CMS71`; and 1
  `AllergyIntolerance-*.json` (`patient` field) in `CMS347` (the "allergy to statin" case from
  #10, now resolved).
- **Why**: diagnosing `CMS72FHIRSTKAntithromboticDay2` (98 of 158 mismatches, almost all
  `Initial Population` itself failing). Traced the call chain:
  `"Initial Population"` → `TJC."Ischemic Stroke Encounter"` →
  `"Non Elective Inpatient Encounter With Age"` →
  `NonElectiveEncounterWithAge.hasPrincipalDiagnosisOf("Ischemic Stroke")`
  (`input/cql/TJCOverall.cql:36-43`) → `hasPrincipalDiagnosisOf()` / `principalDiagnosis()` /
  `claimDiagnosis()` (`input/cql/CQMCommon.cql:406-428`) — this pattern (from the QICore
  "Principal Diagnosis and Present on Admission" authoring convention) requires a FHIR `Claim`
  resource with a `diagnosis[].type` of `"principal"` linked to the encounter, not just a
  `Condition`. Every affected fixture *has* a correctly-formed `Claim` (right diagnosis code,
  right `sequence`, right `type: principal`) — but `Claim.patient.reference` pointed to one of two
  fixed placeholder GUIDs (`d170a0a8-b5ad-4303-b6df-e304dd5f92ad` or
  `5450abfd-a667-4eb9-9b59-e85feed4865c`) instead of the test case's own patient. Confirmed via
  `find`/`ls` that **neither placeholder GUID exists as an actual test case anywhere in the
  repo** — these are template-generation artifacts (a fixed placeholder patient reference in
  whatever template produced these `Claim` resources, never replaced with the real per-test-case
  patient ID), not sibling-record mix-ups like the CMS347 `subject.reference` bug in #7. Given
  this, swept the *entire* `input/tests/measure/` tree for the same pattern on every
  patient-identity field this repo's resources actually use (`subject`, `patient`, `beneficiary`)
  and fixed everything found, not just CMS72.
- **Fix**: same mechanical correction as #7 — each `Claim`/`Coverage`/`AllergyIntolerance`'s
  patient-identity reference set to `Patient/<its own folder's GUID>`. All touched files validated
  as syntactically correct JSON afterward.
- **Expected impact**: `CMS72`'s fix count (97) almost exactly matches its 98 mismatches, and
  `CMS104`'s (70) almost exactly matches its 69 mismatches (our current #1 and #2 Phase 2
  priorities) — high confidence both largely resolve. Smaller counts in `CMS108`, `CMS190`,
  `CMS1028`, `CMS1264`, `CMS71` may fix 1-2 mismatches each or may be incidental (not yet cross-
  checked against those measures' specific failing test cases).
- **Status**: **VERIFIED.** Fresh test run (`discrepancy_report-measure-fixes-20260821-1159.md`)
  vs. the prior report: fail count 2344 → 1954 (390 fewer), **zero regressions** (scripted diff
  confirmed). `CMS72FHIRSTKAntithromboticDay2` 98 → 13, `CMS104FHIRSTKDCAntithrombotic` 69 → 15 —
  both closely matching the predicted impact. `CMS108FHIRVTEProphylaxis` 23 → 21,
  `CMS190FHIRVTEProphylaxisICU` 26 → 24, `CMS1028FHIRPCSevereOBComps` 4 → 2,
  `CMS347FHIRStatinPreventionTxCVD` 4 → 3 (the allergy-to-statin case resolved, as expected).

### 12. CMS72 `.effective` not converted to an interval before date comparison — KEPT (CMS72 only); CMS190's identical fix REVERTED after a regression

- **File**: `input/cql/CMS72FHIRSTKAntithromboticDay2.cql:132`.
- **Why**: diagnosing CMS72's 8 remaining pure `Denominator Exception` misses (of 13 left after
  #11). `"Reason For Not Administering Antithrombotic"` returns a tuple
  `{ id: ..., authoredOn: MedicationAdm.effective }`, unioned with a sibling branch
  (`"Reason For Not Ordering Antithrombotic"`) whose `authoredOn` is
  `NoAntithromboticOrder.authoredOn` — a plain `FHIR.dateTime`. But
  `MedicationAdministration.effective` is a choice type (`dateTime | Period`), and unlike every
  other Period/dateTime comparison in this codebase, it was never converted via `.toInterval()`
  before being compared with `NoAntithrombotic.authoredOn during day of (...)`. Confirmed the
  affected fixture uses `effectivePeriod` (not `effectiveDateTime`).
- **Fix**: changed to `start of MedicationAdm.effective.toInterval ( )`, matching the established
  convention used everywhere else in this codebase for Period/dateTime choice fields.
- **Verified**: CMS72 improved 13 → 9 in the next test run, consistent with this fix working.
- **CMS190 — REVERTED, do not reapply without further investigation.** Found the identical
  textual pattern in `CMS190FHIRVTEProphylaxisICU.cql:302`
  (`authoredOn: NoMedicationAdm.effective`, inside `"No VTE Prophylaxis Medication Administered Or
  Ordered"`, also `union`'d with sibling branches whose `authoredOn` is plain `FHIR.dateTime` —
  structurally identical to CMS72's case) and applied the same fix. Result: **CMS190 regressed
  24 → 28 mismatches**, almost all newly-broken `Numerator` cases (a population this specific
  `define` doesn't even feed into directly — it feeds `Denominator Exclusion`/`Exception` via
  `"No VTE Prophylaxis Medication Due To Medical Reason ..."`), no engine errors logged. Reverted
  immediately (confirmed clean via `git diff`) rather than dig further, since the safety signal
  was unambiguous and this was the only change touching CMS190 that test run. Root cause of *why*
  a seemingly-safe, spec-aligned fix caused this is **not understood** — the leading theory (type
  mismatch across `union`'d tuple shapes when one branch's `authoredOn` becomes `System.DateTime`
  while siblings stay `FHIR.dateTime`) doesn't fully hold up, since CMS72 has the *identical*
  structural pattern and did not regress. Do not blindly reapply this fix to CMS190 (or assume
  CMS72's fix is definitely safe long-term, since it shares the same risky pattern) without a real
  stack trace or more careful before/after diffing of *all* CMS190 test case results, not just the
  summary counts.
- **Revert verified clean.** Next test run: CMS190 28 → 24 (exactly back to its pre-regression
  count), and it was the *only* measure that changed — confirms the revert was correctly isolated
  with no other side effects.

### 13. CMS104 Numerator missing `doNotPerform` check — KEPT, pending re-verification

- **File**: `input/cql/CMS104FHIRSTKDCAntithrombotic.cql:43-56`.
- **Why**: 9 of CMS104's 15 remaining mismatches (after #11) showed the exact
  `Denominator Exception Expected=1/Actual=0, Numerator Expected=0/Actual=1` signature as the
  CMS347 `doNotPerform` bug (#6). Confirmed: `"Numerator"`'s `["MedicationRequest": "Antithrombotic
  Therapy for Ischemic Stroke"]` retrieve never checked `doNotPerform`, so a discharge
  `MedicationRequest` explicitly marked "do NOT perform, contraindicated" (profile
  `us-quality-core-medicationnotrequested`, `doNotPerform: true`) was being double-counted as an
  actual discharge order. Checked CMS72's analogous `"Numerator"` for the same gap — it retrieves
  `[MedicationAdministration: ...]` with an explicit `status in {'in-progress','completed'}`
  filter, which already excludes `not-done`-status records, so no fix needed there.
- **Fix**: added `and ( DischargeAntithrombotic.doNotPerform is null or not
  DischargeAntithrombotic.doNotPerform )` to the Numerator's `such that` clause.
- **Watch list, not yet fixed**: a repo-wide grep for `[MedicationRequest: ...]` retrieves
  combined with an `'active', 'completed'` status filter but no `doNotPerform` check nearby found
  12 candidate measures that may have the same latent bug, unconfirmed against actual failing
  test data: `CMS1017FHIRHHFI`, `CMS108FHIRVTEProphylaxis`, `CMS1173FHIRDiagnosticDelayVTE`,
  `CMS190FHIRVTEProphylaxisICU`, `CMS22FHIRPCSBPScreeningFollowUp`,
  `CMS2FHIRPCSDepScreenAndFollowUp`, `CMS506FHIRSafeUseofOpioids`,
  `CMS645FHIRBoneDensityPCADTherapy`, `CMS646FHIRIntravesicalBCGTherapy`,
  `CMS71FHIRSTKAnticoagAFFlutter`, `CMS72FHIRSTKAntithromboticDay2` (already checked, not
  affected), `CMS996FHIRAptTxforSTEMI`. Don't blindly patch these — verify against a real failing
  test case first, the way #6 and this entry were confirmed.
- **Status**: **VERIFIED.** CMS104 improved 15 → 7 in the next test run, consistent with this fix
  (plus #14's one `Claim.item` fix) working.

### 14. `Claim.item` missing `.encounter`/`.diagnosisSequence` links — 4 fixed total; follow-up scan closed the rest as false positives (procedure-type claims)

- **Why**: diagnosing CMS104's remaining IP-level mismatches (test case
  `0b1aa8ee-e8bf-49f5-b968-48c5a9702843`, description "Testing do not perform not true"). Unlike
  #7/#11 (wrong `patient`/`beneficiary` reference), this `Claim`'s `patient.reference` was
  correct, but its `item[0]` had **no `encounter` field and no `diagnosisSequence` field at all**.
  `claimDiagnosis()` (`input/cql/CQMCommon.cql:415-421`) requires
  `exists (C.item I where I.encounter.references(E))` to even find the claim, and then
  `claimItem.diagnosisSequence` to select which diagnoses apply — with both missing, the whole
  principal-diagnosis chain (and therefore `Initial Population`) silently resolves to empty for
  that test case, same end symptom as the wrong-reference bugs but a different underlying gap
  (missing required linkage, not a typo'd value).
- **Fixed**: `input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0b1aa8ee-.../Claim-5ca62962....json`
  — added `"diagnosisSequence": [1]` and `"encounter": [{"reference":
  "Encounter/2be30658-0b61-4a07-b87d-bf812d2dafc0"}]` (the inpatient encounter in that folder;
  there's also a separate `EMER` encounter that isn't the right target — picking the correct one
  requires per-case judgment, not a blind mechanical fix like #7/#11).
- **Follow-up scan (2026-08-23) corrected the count**: a repo-wide sweep for `item[]` entries
  missing `encounter` or `diagnosisSequence` flagged 21 fixtures, but **19 are false positives**
  — the CMS108 (12) / CMS190 (7) claims are *procedure-type* claims: they carry
  `claim.procedure[]` (`type: primary`, ICD-10-PCS codes) plus `item.procedureSequence` feeding
  `claimPrincipalProcedure()` (`input/cql/CQMCommon.cql:490`) via `hasPrincipalProcedureOf()`,
  and legitimately have **no `claim.diagnosis[]` at all**. Adding `diagnosisSequence` there would
  dangle. No action needed.
- **Fixed (2026-08-23, 3 real gaps)**:
  - `CMS104FHIRSTKDCAntithrombotic/e84c89f7-.../Claim-5ca62962....json` — added
    `"diagnosisSequence": [1]` (principal SNOMED 111297002 Nonparalytic stroke) and
    `"encounter": [{"reference": "Encounter/78fdcacc-ae1b-445f-af15-caf5304a5851"}]` (the sole
    Encounter in that folder); this test case currently fails IP/D/DenException E=1/A=0.
  - `CMS1017FHIRHHFI/e6d91b78-.../Claim-bd849de3-....json` — added `"diagnosisSequence": [1]`
    (diagnosis seq 1 = W01.0XXA); item already had the encounter link.
  - `CMS1017FHIRHHFI/e6d91b78-.../Claim-4dee9c95-....json` — repaired a **dangling** link:
    item had `"diagnosisSequence": [1, 2]` but the claim's `diagnosis[]` only contains sequence
    2 (M80.00XA); changed to `[2]`. Case passes today; fix keeps it valid without changing
    resolved values.
- **Status**: **VERIFIED (2026-08-23 1747 run).** Delta vs the 1718 baseline is exactly one
  removed mismatch row — CMS104's `e84c89f7` (was IP/D/DenException E=1/A=0) now passes,
  confirming SNOMED 111297002 resolves via the restored principal-diagnosis chain. CMS1017's two
  repaired claims stayed passing as expected. Fail count 840 → 837; no other measure moved.
  This entry is closed: 4 fixed and verified, 19 not-applicable.

### 15. CMS133 `Denominator Exclusions` used a point-in-time onset check instead of `prevalenceInterval()` — KEPT, pending re-verification

- **File**: `input/cql/CMS133FHIRCataracts2040BCVA90Days.cql:270-272`.
- **Why**: 59 of 73 mismatches (80.82%), almost entirely `Denominator Exclusion Expected=1,
  Actual=0`. `"Cataract Surgeries in Patients with Significant Ocular Conditions Impacting the
  Visual Outcome of Surgery"` is one large `with (...)` clause covering ~25 valueset union
  branches (all sharing a single `such that`), checking
  `ComorbidDiagnosis.onset.toInterval ( ) overlaps before day of
  CataractSurgeryPerformed.performed.toInterval ( )`. Confirmed via one failing test case
  (description: "retinal vascular and muscular diagnosis overlapping cataract surgery") that its
  `Condition` fixture has `onsetDateTime: "2024-11-01"` — no abatement, `clinicalStatus: active` —
  a chronic condition that predates the 2026-03 cataract surgery by well over a year. Confirmed
  the diagnosis code (`H34.232`) *is* correctly in the "Retinal Vascular Occlusion" valueset, so
  the retrieve/valueset match isn't the problem. `.onset.toInterval()` on a scalar `onsetDateTime`
  produces a zero-width point interval, which can never "overlap" a surgery period a year+ later
  — same class of bug as #10's `"Has Diabetes Diagnosis"` finding in CMS347: a chronic,
  still-active condition needs the "present at any point up through now" pattern
  (`.prevalenceInterval()`, the convention used everywhere else in this codebase for exactly this
  — e.g. `AHAOverall.cql`, CMS347's `"Has Advanced Illness..."`), not a literal onset-date overlap
  check. Confirmed `ComorbidDiagnosis.isVerified()` (called on the same line, same variable) is a
  locally-defined fluent function typed for
  `Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>`, so `ComorbidDiagnosis`'s
  type is already established as compatible with the Choice type `.prevalenceInterval()` expects
  elsewhere in the codebase.
- **Fix**: changed `ComorbidDiagnosis.onset.toInterval ( )` to
  `ComorbidDiagnosis.prevalenceInterval ( )`.
- **Caution carried over from #12's CMS190 regression**: this looked like a safe, well-precedented
  fix by the same reasoning that seemed sound for #12 — verify carefully against a fresh test run
  rather than assuming it's correct just because the pattern matches prior fixes.
- **Status**: not yet verified against a fresh test run.

### 16. Six more instances of the `.onset.toInterval()` vs `.prevalenceInterval()` bug (#10/#15's pattern) — KEPT, pending re-verification

- **Files fixed** (8 defines across 6 measures):
  - `input/cql/CMS90FHIRFSAforHeartFailure.cql:81` (`"Initial Population"`) — confirmed against
    fixture `17be91ec-117d-4767-8271-f0403f0c8f84` (`Condition.onsetDateTime: 2025-12-31T23:59:00Z`,
    `clinicalStatus: active`, no abatement; Measurement Period is 2026). This one root cause
    explained 34 of 37 total mismatches (91.89%) — every other "Initial Population" conjunct
    (age, outpatient encounters) already passed.
  - `input/cql/CMS142FHIRCommWithDrManagingDiab.cql:92` (`"Diabetic Retinopathy Encounter"`) —
    confirmed against fixture `b85440e4-b902-49cd-b3d6-363ba7a99bce` (`onsetDateTime: 2023-07-01`
    vs. a 2026-07 qualifying encounter). Explains the bulk of 19/32 mismatches.
  - `input/cql/CMS143FHIRPOAGOpticNerveEval.cql:77` (`"Primary Open Angle Glaucoma Encounter"`) —
    byte-identical pattern/structure to CMS142 (same "Diagnosis + Qualifying Encounter" shape,
    same `.isVerified()` call confirming type compatibility). 18/32 mismatches.
  - `input/cql/CMS951FHIRKidneyHealthEval.cql:75` (`"Has Active Diabetes Overlaps Start Of
    Measurement Period"`) and `:88` (`"Has CKD Stage 5 Or ESRD Diagnosis Overlaps Measurement
    Period"`) — both used the same union+manual-`verificationStatus`-check shape as CMS347's
    already-confirmed instances. 44/55 mismatches (80%).
  - `input/cql/CMS157FHIRPainIntensityQuantified.cql:62` (`"Face to Face or Telehealth Encounter
    with Ongoing Chemotherapy"`) and `:79` (`"Radiation Treatment Management During Measurement
    Period with Cancer Diagnosis"`) — confirmed against fixture `b0729673-76ed-4c08-ae06-acd214ad203d`
    (`Condition.onsetDateTime: 2024-11-01`, `active`, vs. 2026 encounters). 40/126 mismatches.
  - `input/cql/CMS129FHIRProstCaBoneScanUse.cql:144` (`"Prostate Cancer Diagnosis"`) — confirmed
    against fixture `56b77354-f6c1-4507-8270-a07de39f0fa9`, whose own
    `cqfm-testCaseDescription` states the intent directly: *"Test case with condition overlapping
    MP with abatement date. Clinical status is resolved. IPPPass due to diagnosis was active for
    part of the MP."* (`onset: 2022-08-17`, `abatement: 2026-08-17`, `status: resolved`) — this is
    the clearest possible confirmation that the CQL needs the onset-through-abatement span
    (`.prevalenceInterval()`), not a zero-width onset point.
  - `input/cql/CMS347FHIRStatinPreventionTxCVD.cql:151` (`"Has Diabetes Diagnosis"`) and `:157`
    (`"Has ESRD Diagnosis"`) — these are the two defines #10 explicitly flagged as still-broken and
    parked; fixing them now closes out that open item.
- **Why** (repo-wide): all 8 are the same anti-pattern as #10/#15 — `.onset.toInterval()` on a
  scalar `onsetDateTime` produces a zero-width point interval, so a chronic/still-active diagnosis
  that predates the comparison window by any margin can never `overlaps` it, even when the
  diagnosis has no abatement (or an abatement that falls inside/after the window) and is clinically
  still relevant. Every fix here was verified either against a concrete failing fixture's
  onset/abatement/clinicalStatus dates and its `cqfm-testCaseDescription`, or (CMS143/CMS951's
  second occurrence) against a byte-identical code shape immediately adjacent to an
  individually-confirmed instance in the same file. Type compatibility with `.prevalenceInterval()`
  was confirmed for each by finding an existing `.isVerified()` or manual `verificationStatus` check
  on the same alias, establishing it as a `Condition`-compatible Choice type — the same check #15
  used.
- **Fix**: changed each `<alias>.onset.toInterval ( )` to `<alias>.prevalenceInterval ( )` in place.
- **Repo-wide sweep performed but NOT acted on** — `grep -rn 'onset.toInterval' input/cql/` turned
  up roughly 80 more hits. Most are legitimate: `starts before`/`starts after`/`starts during`/
  `same day as` comparisons are genuine point-in-time checks where a zero-width interval is exactly
  correct (e.g. "did the diagnosis start during this visit"), not the `overlaps`-against-a-distant-
  window anti-pattern. A smaller set of `overlaps`-style comparisons against a Measurement Period or
  encounter *do* look structurally similar to this bug — notably `CMS131FHIRDiabetesEyeExam.cql:56`
  (checked against fixture `985b5e49-...`, but the onset dates there sit right at the MP boundary
  and the test description didn't clearly disambiguate which of two `Condition` fixtures each
  define resolves to — inconclusive, left unfixed to avoid a #12-style blind-pattern regression),
  `CMS156FHIRHighRiskMedsElderly.cql:169,179` (chronic-diagnosis-in-lookback-year checks — plausible
  but unverified, and this measure's failures are dominated by the external `Min()` engine issue,
  not clearly this bug), and `CMS1173FHIRDiagnosticDelayVTE.cql:159/163/176/180` (Hospice/Palliative
  diagnosis checks — also unverified, and this measure's 62/65 failures are "Missing Results"
  execution errors already attributed to the external `Min()`/`DateTimeType` issue, not mismatches
  from this pattern). None of these were touched. A future pass should trace each individually
  against a real failing fixture before fixing, the same way every fix in this entry was.

### 17. `doNotPerform` gap (fixes #6/#13's pattern) confirmed and fixed in 4 more measures; 2 adjacent bugs found and fixed along the way — KEPT, pending re-verification

- **Context**: fresh discrepancy report (`discrepancy_report-measure-fixes-20260821-1244.md`) showed
  the same "Denominator Exception Expected=1/Actual=0 + Numerator Expected=0/Actual=1" swap (or a
  standalone Denominator Exception miss) across 9 candidate measures — the exact signature of the
  `doNotPerform`-not-excluded bug from #6 (CMS347) and #13 (CMS104). Each was checked individually
  against a real failing fixture before touching anything, per this document's own rule.
- **Confirmed and fixed (the doNotPerform gap itself)**:
  - `input/cql/CMS71FHIRSTKAnticoagAFFlutter.cql` (`"Numerator"`'s `DischargeAnticoagulant` retrieve)
    — byte-identical structure to CMS104's pre-#13 bug (this measure was already on #13's watch
    list). Confirmed via fixture `e20b4e76-...`: a `MedicationRequest` profiled
    `us-quality-core-medicationnotrequested` with `doNotPerform: true` was being double-counted.
  - `input/cql/CMS135FHIRACEIorARBorARNIforHF.cql` (`"Has ACEI or ARB or ARNI Ordered"` and
    `"Is Currently Taking ACEI or ARB or ARNI"`) — confirmed via fixture `d297e68e-...`
    ("...is not prescribed ACE/ARB medication for Patient Reason"): a `MedicationRequest` profiled
    `us-quality-core-medicationnotrequested` matched the plain `[MedicationRequest: ...]` retrieve
    used for both Numerator branches. 6 of this measure's 9 mismatches were this exact pattern; the
    other 3 are a separate, unrelated allergy/intolerance-diagnosis issue (see below, not fixed).
  - `input/cql/CMS144FHIRHFBetaBlockerForLVSD.cql` (`"Has Beta Blocker Therapy for LVSD Ordered"`
    and `"Is Currently Taking Beta Blocker Therapy for LVSD"`) — confirmed via all 3 of this
    measure's mismatched fixtures (`7b8885c5-...`, `07efd4bb-...`, `67779bc6-...`), each carrying a
    `doNotPerform: true` `MedicationRequest`.
  - `input/cql/CMS645FHIRBoneDensityPCADTherapy.cql` (`"Has Baseline DEXA Scan..."`, the
    `DEXAOrdered` `ServiceRequest` retrieve) — same bug, one type over: a `ServiceRequest` profiled
    `us-quality-core-servicenotrequested` with `doNotPerform: true` (confirmed via fixture
    `8c41481d-...`, "Patient refused DEXA at 3 months after ADT") was matched by the plain
    `[ServiceRequest: "DEXA Bone Density..."]` retrieve. Fixed 2 of this measure's 5 mismatches; the
    remaining 2 (`59743016-...`, `05afd17d-...`) are Initial-Population-level failures unrelated to
    this bug, not investigated further.
  - `input/cql/CMS22FHIRPCSBPScreeningFollowUp.cql` — three Numerator-side retrieves
    (`"NonPharmacological Interventions"`, `"Follow up with Rescreen Within 6 Months"`,
    `"Laboratory Test or ECG for Hypertension"`) all had the same `ServiceRequest` gap, confirmed
    via fixture `ad737f80-...` ("...patient declined recommendation to reduce weight" — a
    `ServiceRequest` profiled `us-quality-core-servicenotrequested`, `doNotPerform: true`, matched by
    the unfiltered `[ServiceRequest: "Weight Reduction Recommended"]` retrieve).
- **Adjacent bug #1 found and fixed while diagnosing CMS22**: 6 defines in the same file
  (`"NonPharmacological Intervention Not Ordered"` and 5 more declined-intervention checks, lines
  328/346/357/377/386/400) filtered on `<alias>.reasonCode in "Patient Declined"` for
  `[ServiceNotRequested: ...]`-typed retrieves. `ServiceRequest` has no native `reasonCode` element
  for "why wasn't this done" — that's carried in the `us-quality-core-doNotPerformReason` extension,
  which this codebase already exposes via the `reasonRefused()` fluent function
  (`USQualityCoreCommon.cql:190-191`, already used correctly in `CMS645`/`CMS108`/`CMS190`/`CMS69`).
  Confirmed against fixture `ad737f80-...`, whose `ServiceRequest` carries the reason only in that
  extension, not in a `reasonCode` element (which doesn't exist on this resource type at all — the
  field was simply never being read). Changed all 6 to `.reasonRefused ( )`.
- **Adjacent bug #2 found and fixed while diagnosing CMS646**: `"BCG Not Available Within 6 Months
  After Bladder Cancer Staging"` (`input/cql/CMS646FHIRIntravesicalBCGTherapy.cql:169`) compared
  `BCGNotGiven.effective` (a `MedicationAdministration` choice `dateTime | Period`) directly against
  a temporal-distance operator without `.toInterval()` first — the same class of bug as #12
  (CMS72's `.effective` fix). Confirmed by comparing against this same file's sibling
  `"First BCG Administered"` define three lines down, which does the equivalent comparison
  correctly with `.effective.toInterval ( ) starts ...`. Confirmed against fixture `e648fa70-...`,
  whose own `cqfm-testCaseDescription` self-flags the issue: *"BCG not available during MP. Should
  pass. Note: Issue with Denominator Exception due to negation issues."* Changed to
  `BCGNotGiven.effective.toInterval ( ) starts 6 months or less after day of start of
  FirstBladderCancerStaging.performed.toInterval ( )`, matching the sibling exactly. Fixed 1 of this
  measure's 4 mismatches; the other 3 (`Denominator Exclusion`/`Numerator` misses) are unrelated,
  not investigated.
- **Investigated, root cause found, deliberately NOT fixed (needs a human decision or more care)**:
  - `input/cql/CMS104FHIRSTKDCAntithrombotic.cql` — the 2 remaining mismatches after #13's fix are
    NOT another instance of the doNotPerform gap (that retrieve already has the check). Root cause
    is different: `"Reason For Not Giving Antithrombotic At Discharge"`'s second union branch (the
    `MedicationRequest`-with-`TaskRejected` pattern, lines 79-86) evaluates empty even when the
    `Task`/`MedicationRequest`/valueset/reasonCode data all line up correctly (confirmed via fixture
    `5adc911a-...`, description "task rejected-patient refusal" — the trace dump shows
    `Reason For Not Giving Antithrombotic At Discharge=[]`). Needs a translator/ELM-level trace to
    diagnose further, not a mechanical fix — left alone.**Corroborated on CMS108 (2026-09-08)**: 8
    CMS108 VTE-prophylaxis cases (`182103c1`, `2eff6dbd`, `3c854f27`, `525e73f2`, `5f739500`,
    `91ff5f1a`, `d205878e`, `ff814452`) show the same `MedicationRequest`-with-`TaskRejected`
    rejection arm (`CMS108FHIRVTEProphylaxis.cql` lines 343-358) evaluating empty despite correct
    data/valueset/profile. These were previously swept into E-17 (positive profile-retrieve
    mislabel); reclassified to E-12 and tracked on `known_issues.json` / `engine-issues.md`.
  - `input/cql/CMS72FHIRSTKAntithromboticDay2.cql` — the 5 remaining `Denominator Exception`
    mismatches are NOT the doNotPerform gap (confirmed: the relevant retrieves already use the
    `MedicationNotRequested`/`MedicationAdministrationNotDone` negation-specific types, which don't
    need the exclusion). Root cause looks like a date-window/`calendarDayOfOrDayAfter()` logic issue
    instead (fixture `ab024aef-...`, "antithrombotic is not ordered due to ref but = 1 day after
    start of ED visit") — not investigated deeply enough to fix confidently; left alone.
  - `input/cql/CMS2FHIRPCSDepScreenAndFollowUp.cql` — all 8 mismatches trace to `"Denominator
    Exceptions"` being hardcoded to `false` with an inline `TODO` comment ("Need to reassess how we
    are representing given no ObservationCancelled profile"). That profile now *does* exist in
    `usqualitycore-modelinfo-0.1.0-cibuild.xml` (confirmed), so the TODO's blocking condition may no
    longer hold — but re-enabling this is a real design/implementation task (writing the
    `ObservationCancelled`-based exception logic and verifying it against fixtures), not a
    mechanical one-line fix. Flagged for a human decision, not attempted.
  - `input/cql/CMS996FHIRAptTxforSTEMI.cql` — the 4 `Denominator Exception` mismatches trace to two
    defines comparing a choice-typed field (`ProcedureNotDone.performed`, `Medication
    AdministrationNotDone.effective`) directly against a temporal operator with no `.toInterval()` —
    looks like #12/adjacent-bug-#2's pattern at first glance, but `performed` is entirely *absent*
    on the `not-done` fixture (confirmed via `ccc7deaf-...`, a `data-absent-reason: not-performed`
    `Procedure`), not just un-converted. The likely correct fix is the `.recorded()` extension
    accessor instead — but that's the exact fluent function this document's "External issues log"
    already flags as hitting a translator ambiguous-overload bug on `ProcedureNotDone` values (see
    CMS68's confirmed case). Applying it here risks reproducing that engine error rather than fixing
    the measure. Left alone pending a real fix for the translator issue, or a way to read the
    `recorded` extension without going through the ambiguous overload.
  - `input/cql/CMS135FHIRACEIorARBorARNIforHF.cql` — 3 of its 9 mismatches (`d18e37a6-...` and 2
    others) are a confirmed-allergy/intolerance-diagnosis check
    (`"Has Diagnosis of Allergy or Intolerance to ACEI or ARB"`), unrelated to the doNotPerform fix
    applied to this same file above. Not investigated further.
- **Status**: **VERIFIED** (`discrepancy_report-measure-fixes-20260823-0730.md`). All doNotPerform/
  reasonRefused/`.toInterval()` fixes landed with no regressions. Per-measure outcomes: CMS71 2
  mismatches left (exactly the documented-not-fixed TaskRejected union branch); CMS135's doNotPerform
  cases resolved (its 3 remaining mismatches are the documented allergy/intolerance issue, and its 3
  missing results are the separately-tabled Reference-extraction error); CMS144 fully passing;
  CMS645's DEXA case resolved (2 remaining mismatches are the documented unrelated IP-level failures);
  CMS22 down to 1 mismatch. Adjacent bug #2's CMS646 `.toInterval()` fix landed without crashing
  anything, but fixture `e648fa70-...` still mismatches (Denominator Exception E=1/A=0) — consistent
  with its own description self-flagging "negation issues"; treated as a separate open item, not a
  failed fix.

### 18. Fix #16's `.prevalenceInterval()` rollout caused total execution failure in 3 measures — CAUGHT, fixed, needs re-verification

- **File added**: a new fluent function overload in `input/cql/Status.cql`.
- **Why**: a fresh test run (`discrepancy_report-measure-fixes-20260821-1311.md`, triggered mid-session
  and shared by you) showed `CMS90FHIRFSAforHeartFailure`, `CMS133FHIRCataracts2040BCVA90Days`, and
  `CMS951FHIRKidneyHealthEval` had gone from partial mismatches to **100% "Missing Results"** (total
  execution failure, all test cases) immediately after fix #16 replaced their `.onset.toInterval()`
  calls with `.prevalenceInterval()` — a regression, not an improvement. Root-caused by diffing the
  measures where the substitution was safe (`CMS347FHIRStatinPreventionTxCVD`, `CMS129FHIRProstCaBoneScanUse`
  — both confirmed still just "mismatched", not "missing", in the same fresh run) against the ones that
  crashed: the safe ones call `.prevalenceInterval()` on a value from a **single concrete retrieve type**
  (e.g. `[ConditionProblemsHealthConcerns: "Diabetes"]` alone), while the crashing ones call it on a
  **`union` of two USQualityCore condition profile types**
  (`[ConditionProblemsHealthConcerns: "..."] union [ConditionEncounterDiagnosis: "..."]`), which the CQL
  translator infers as `Choice<ConditionProblemsHealthConcerns, ConditionEncounterDiagnosis>`.
  `hl7.fhir.uv.cql.FHIRCommon`'s `prevalenceInterval()` is only declared for concrete `FHIR.Condition` /
  `FHIR.AllergyIntolerance` parameters (confirmed by reading the vendored library at
  `~/.cql-language-server/npm-library-cache/4.11.0-SNAPSHOT/f117967a37910fd6/FHIRCommon-2.0.0.cql:395`);
  invoking it directly on a genuine Choice value has no matching overload and fails at evaluation for
  every test case in the library, not just the specific `define`. Same root class of issue as the
  already-logged "ambiguous overload resolution" translator gap in the External issues log, just
  triggered by a missing overload rather than an ambiguous one.
  `CMS142FHIRCommWithDrManagingDiab`, `CMS143FHIRPOAGOpticNerveEval`, and
  `CMS157FHIRPainIntensityQuantified` (also touched by fix #16, also using the same `union` pattern) had
  **not yet been re-run** at the time of the 13:11 report — their result `.txt` files predated fix #16's
  edit to those specific files — so their "still mismatched, not missing" status in that report is stale
  and does **not** confirm they're safe; treat them as equally at risk until the next fresh run.
- **Fix**: added a `prevalenceInterval(condition Choice<ConditionProblemsHealthConcerns,
  ConditionEncounterDiagnosis>)` overload to `Status.cql`, alongside the file's existing
  `verified(conditions List<Choice<ConditionProblemsHealthConcerns, ConditionEncounterDiagnosis>>)`
  function — the same established pattern already used there for handling this exact Choice type. The
  new overload casts to the concrete branch type first (`if condition is ConditionProblemsHealthConcerns
  then (condition as ConditionProblemsHealthConcerns).prevalenceInterval() else (condition as
  ConditionEncounterDiagnosis).prevalenceInterval()`), which routes to FHIRCommon's real implementation
  via the same concrete-type dispatch that's already proven to work for CMS347/CMS129's single-retrieve
  usages — no per-measure CQL changes needed, since `CMS90`/`CMS133`/`CMS142`/`CMS143`/`CMS951`/`CMS157`
  all already `include Status` and their existing `.prevalenceInterval()` call sites will now resolve to
  this overload instead of failing.
- **Status**: fix applied, **not yet verified** — needs a fresh test run across all 6 affected measures
  (especially CMS90/CMS133/CMS951, to confirm they're back to at-worst-mismatched, and CMS142/143/157, to
  confirm they don't newly crash). **Lesson for future large-fixture-pattern sweeps**: verify a
  `.prevalenceInterval()` (or any FHIRCommon-delegating fluent function) substitution against a retrieve
  that unions two profile types specifically, not just against retrieves of a single type — the risk
  profile is different even though the surface-level CQL text pattern looks identical.
- **CONFIRMED against the actual pre-migration source, 2026-08-21**: cloned
  `https://github.com/cqframework/dqm-content-qicore-2025` locally (see the `dqm-content-qicore-2025`
  reference memory) and checked `input/cql/QICoreCommon.cql:452` — it declares
  `prevalenceInterval(condition Choice<"ConditionEncounterDiagnosis", "ConditionProblemsHealthConcerns">)`
  with logic byte-for-byte identical to what this fix added to `Status.cql`. `QICoreCommon` was the
  library that got refactored into `FHIRCommon`/`USCoreCommon`/`USQualityCoreCommon` during the
  migration (per `USQualityCoreUpdateProcess.md`'s library table) — this specific Choice-typed overload
  was dropped in that refactor and never carried over, which is the actual root cause. This is a genuine
  migration-completeness gap, not a one-off authoring mistake in any of the 6 measures, and it validates
  the project's corrected framing (see "Why this work exists," updated 2026-08-21): fix the CQL to match
  the pre-migration QICore intent, not "whichever engine's behavior."
  `USQualityCoreUpdateProcess.md` also documents a cleaner alternative fix for *new* code going forward
  — since `ConditionEncounterDiagnosis`/`ConditionProblemsHealthConcerns` both derive from `Condition` in
  the derived USQualityCore model (unlike flat QICore), a `union` of the two specific retrieves can be
  simplified to a single `[FHIR.Condition: "..."]` retrieve, sidestepping the Choice type entirely
  (precedent already in `CMS125FHIRBreastCancerScreen.cql`, `Hospice.cql`, `PalliativeCare.cql`,
  `AdvancedIllnessandFrailty.cql`, all currently passing). Did not apply that simplification to the 6
  measures here — the `Status.cql` overload fix is lower-risk (one shared-library addition vs. six
  measure-level rewrites) and already unblocks them; flagging the simplification as a nice-to-have
  cleanup for whoever revisits these measures, not required.
- Swept `QICoreCommon.cql` for other function names with no match in `USQualityCoreCommon.cql`/
  `Status.cql` to look for more of the same class of dropped-during-migration function. Found one
  other genuinely QICore-only name, `isHealthConcern`, but confirmed it's NOT missing — it exists with
  a concrete-type signature (`isHealthConcern(condition FHIR.Condition)`) in the vendored
  `hl7.fhir.us.cql.USCoreCommon`/`USCoreElements` libraries (cache dir `29dd7bae49368534`, a different
  cache directory than `FHIRCommon`'s), and its one call site (`CMS69FHIRPCSBMIScreenAndFollowUp.cql:142`)
  is on a single concrete `[ConditionProblemsHealthConcerns: ...]` retrieve, not a `union`-produced
  Choice — so no crash risk there, unlike `prevalenceInterval`. The rest of the QICoreCommon-only names
  (`earliest`, `latest`, `hasStart`/`hasEnd`, `includesCode`, `isActive`, `isEncounterDiagnosis`,
  `isProblemListItem`, `references`, `toInterval`, etc.) were confirmed present in the vendored
  `FHIRCommon`/`USCoreCommon` libraries under the same names and are already in active, working use
  across many currently-passing measures (`CMS117`, `CMS124`, `CMS130`, `CMS165`, etc.) — not a live
  risk. No direct `.abatementInterval()` calls on a `union`-produced Choice were found anywhere in the
  repo's measure/shared CQL (the one other function besides `prevalenceInterval` sharing that exact
  concrete-type-only signature pattern), so no further instances of this specific bug class are known
  at this time.

### 19. Broader QICore diff sweep, round 1 — multiple confirmed fixes, one solved mystery, one left deliberately unfixed

Continuing the direct-source-diff approach from #18, dispatched parallel diffs of `seena-fork/input/cql/*`
against the cloned `dqm-content-qicore-2025` for all 13 shared libraries and 7 measures still showing
discrepancies (CMS996, CMS816, CMS2, CMS108, CMS190, CMS159, CMS155). Findings below; each fix applied
directly (not parked), given the corrected project framing (see "Why this work exists") — the only
bar is "does this match the pre-migration source."

- **CMS190FHIRVTEProphylaxisICU — KEPT (partial), solves the fix #12 mystery.** QICore used
  `NoMedicationAdm.recorded` (a `MedicationAdministrationNotDone`) and `DeviceNotApplied.recorded` (a
  `ProcedureNotDone`) for two `authoredOn`/timing tuples; the migration silently swapped both to
  `.effective`/`.performed` — the wrong field, not a missing `.toInterval()` call. This is *why* fix
  #12's seemingly-safe, CMS72-precedented `.toInterval()` fix regressed CMS190 (24→28 mismatches) for
  reasons that were never root-caused at the time: it was fixing the wrong field's *shape*, not the
  wrong field. **Fixed**: `NoMedicationAdm.recorded ( )` (line ~302) — safe, since
  `USQualityCoreCommon.cql` declares `recorded(medicationAdministrationNotDone
  MedicationAdministrationNotDone)` with no sibling base-type overload to conflict with. **NOT fixed,
  reverted back to `.performed`**: `DeviceNotApplied.recorded` (line ~370) — `DeviceNotApplied` is typed
  `ProcedureNotDone`, and `USQualityCoreCommon.cql` declares *both* `recorded(Procedure)` and
  `recorded(ProcedureNotDone)` as separate overloads. Per the already-logged External issue ("Ambiguous
  overload resolution — likely cql-to-elm gap"), calling `.recorded()` on a `ProcedureNotDone` value hits
  a confirmed translator bug where it can't resolve to the more-specific overload — confirmed live in
  `CMS68FHIRDocumentationCurrentMeds` (1 "Missing Results" test case, unfixed, exactly this call). I
  initially reverted this line to bare `.recorded` (copying QICore's pre-migration property syntax
  verbatim) without checking for this — caught it during review before it went further: bare `.recorded`
  is invalid syntax in the derived model regardless (extensions are fluent functions here, per
  `USQualityCoreUpdateProcess.md` Step 3), and adding the required `( )` would just reproduce CMS68's
  crash. Reverted to the pre-existing `.performed` (wrong field, but stable/non-crashing) rather than
  risk a full-library "Missing Results" regression I can't verify without engine access.
- **CMS159FHIRDepRemissionat12Months — KEPT, pending re-verification.** 4 more confirmed instances of
  the `.onset.toInterval()` vs `.prevalenceInterval()` bug (#10/#15/#16/#18 family), all reverted to
  `.prevalenceInterval()`: `"Depression Encounter"`'s `Depression` check, `"Has Mental Health Disorder
  Diagnoses"`'s `MentalHealthDisorderDiagnoses` check, and the locally-inlined `HospiceCareDiagnosis`/
  `PalliativeDiagnosis` checks in `"Has Hospice Services..."`/`"Has Palliative Care..."` (this measure
  inlines its own copies rather than calling the `Hospice.cql`/`PalliativeCare.cql` shared functions,
  which is why the shared-library fix didn't already cover it). All four are `union`s of
  `ConditionProblemsHealthConcerns`/`ConditionEncounterDiagnosis`, and this file already includes
  `Status.cql`, so they're covered by fix #18's `prevalenceInterval(Choice<...>)` overload — no further
  library change needed. Matches the failure signature exactly (6 of 8 mismatches are Initial
  Population/Denominator misses, 1 is the hospice/palliative Denominator-Exclusion↔Numerator swap).
- **CMS155FHIRWgtAssessCounseling — KEPT, pending re-verification.** Same bug, 1 instance:
  `"Pregnancy Diagnosis Which Overlaps Measurement Period"`'s `PregnancyDiag` check, reverted to
  `.prevalenceInterval ( )`. Matches all 6 mismatches (a pregnancy-exclusion case across 3 groups for one
  test case). Covered by the same `Status.cql` overload (file already includes it).
- **AHAOverall.cql — KEPT, pending re-verification, repo-wide shared-library impact (affects CMS144
  directly, likely others via `AHAOverall` inclusion).** QICore's `overlapsHeartFailureOutpatientEncounter`
  and `overlapsAfterHeartFailureOutpatientEncounter` both took a
  `Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>` parameter; the migration
  narrowed both to `ConditionEncounterDiagnosis` only, silently dropping `ConditionProblemsHealthConcerns`
  support. `CMS144FHIRHFBetaBlockerForLVSD.cql` calls both functions on genuine `union`-produced Choice
  values in 7 separate Denominator-Exception `define`s (`"Has Hypotension Diagnosis"`, `"...Cardiac
  Pacer..."`, `"...Allergy or Intolerance to Beta Blocker..."`, `"...Bradycardia..."`, `"...Arrhythmia..."`,
  `"...Asthma..."`, `"...Atrioventricular Block..."`). **Fix, deliberately NOT a Choice-typed overload
  this time**: added a sibling `ConditionProblemsHealthConcerns`-typed overload for each function,
  matching this exact file's own pre-existing convention (it already has 4 other type-specific overloads
  of `overlapsAfterHeartFailureOutpatientEncounter` for `Procedure`/`AllergyIntolerance`/
  `MedicationRequest`/`HeartRateObservation`). Deliberately avoided widening to a `Choice<...>` parameter
  the way fix #18 did for `prevalenceInterval`, because that would require `Condition.isVerified()` and
  `Condition.prevalenceInterval()` to resolve inside the function body against the Choice type too — and
  `AHAOverall.cql` doesn't include `Status.cql` (where the `prevalenceInterval` Choice overload lives),
  and can't safely be given a local Choice-typed `isVerified`/`prevalenceInterval` of its own either,
  because every caller observed so far (`CMS144`, and by extension anything else including both
  `AHAOverall` and `Status`/`USQualityCoreCommon`) would then see the *same signature* declared in two
  independently-included libraries — a duplicate-declaration conflict, the same risk class this whole
  entry is about avoiding. Two concrete sibling overloads sidesteps this entirely: each new overload's
  body only ever sees a concrete (non-Choice) `Condition`, so `.isVerified()`/`.prevalenceInterval()`
  resolve the same way they already do for every other concrete-type retrieve in this codebase — no new
  dependency, no ambiguous/duplicate declaration. Confidence: high that this compiles cleanly (no
  overlapping-inheritance ambiguity between true CQL siblings, unlike the `Procedure`/`ProcedureNotDone`
  case above); NOT verified that CMS144's specific mismatches actually resolve — that's a data question,
  not just a compile question, since the function was only ever invoked in `exists(...)` guards that
  short-circuit on empty retrieves.
- **CMS2FHIRPCSDepScreenAndFollowUp — KEPT, pending re-verification.** The "hardcoded `false`, TODO: no
  ObservationCancelled profile" note from fix #17 was investigating on a **false premise** —
  `ObservationCancelled` exists in `usqualitycore-modelinfo-0.1.0-cibuild.xml`
  (`us-quality-core-observationcancelled`), and the identical `[ObservationCancelled: ...]` +
  `.notDoneReason()` pattern is already proven working in `CMS143FHIRPOAGOpticNerveEval.cql`. QICore's
  real logic (referencing `"Medical or Patient Reason for Not Screening Adolescent/Adult for
  Depression"`, each retrieving `[ObservationCancelled: code ~ "..."]` and checking `.notDoneReason`
  against "Depression screening declined"/"Medical Reason") was present verbatim in this file but
  **commented out on both ends** — the `"Denominator Exceptions"` definition itself, and its two
  dependency `define`s. Uncommented both blocks and fixed one additional bug found in the process: the
  commented code called `.notDoneReason` as a bare property (`NoAdolescentScreen.notDoneReason ~ "..."`)
  — valid in QICore where it might have been a first-class element, but `USQualityCoreCommon.cql`
  declares `notDoneReason(observationCancelled ObservationCancelled)` as a fluent function requiring
  `( )`, per the same "extensions are now fluent functions" migration pattern documented in
  `USQualityCoreUpdateProcess.md` Step 3. Fixed both occurrences to `.notDoneReason ( )`. This was
  correctly a straightforward missed-conversion bug, not a design task as fix #17 assumed.
- **CMS996FHIRAptTxforSTEMI and CMS108FHIRVTEProphylaxis — CONFIRMED root cause, deliberately left
  UNFIXED.** Both hit the exact same `.recorded` ambiguous-overload issue as CMS190's `DeviceNotApplied`
  above: QICore's `PCINotDone.recorded`/`FibrinolyticNoMed.recorded` (CMS996) and `DeviceNotApplied.recorded`
  (CMS108) were silently swapped to `.performed`/`.effective` during migration — the wrong field, chosen
  specifically to dodge the same `Procedure`/`ProcedureNotDone` overload ambiguity that crashes CMS68.
  Reverting to the correct field would very likely reproduce that crash (unconfirmed, since I can't
  compile/run the engine from this environment) rather than fix the data. Per the External issues log's
  own policy ("Not reshaping correct CQL to route around it — file upstream if this gets prioritized"),
  did not attempt a speculative disambiguating cast (e.g. `(PCINotDone as Procedure).recorded ( )`)
  without a way to verify it actually compiles — a wrong guess here risks turning a partial mismatch into
  a 100%-crashed "Missing Results" measure, the same class of mistake fix #18 caught and fixed elsewhere
  this session. **Needs a human/engine-verified decision**: either (a) confirm via a real compile/stack
  trace whether an explicit cast resolves the ambiguity safely, or (b) escalate the underlying
  `Procedure`/`ProcedureNotDone` overload-ambiguity translator bug upstream, per the existing External
  issues log entry.
- **CMS816FHIRHHHypo — confirmed NOT a migration regression, out of scope for this framing.** Diffed
  fully against QICore: logic is byte-identical (only mechanical header/measurement-period changes). The
  12 current mismatches must trace to a fixture/data issue or a bug that would reproduce against the
  pre-migration QICore version too — not fixable through the "migration correctness" lens this project is
  now scoped to. Not pursued further under this framing; would need the original symptom-driven
  fixture/CQL debugging approach (checking the actual failing test case's data) if picked up again.
- **PCMaternal.cql — unconfirmed lead, not acted on.** `lastEstimatedDeliveryDate()` and
  `lastTimeOfDelivery()` changed their internal cast from `.value as DateTime` (QICore, `System.DateTime`)
  to `.value as FHIR.dateTime` (USQualityCore) — a real type change, not a rename. Callers in
  `CMS0334FHIRPCCesareanBirth.cql` and `CMS1028FHIRPCSevereOBComps.cql` do direct date arithmetic/interval
  construction against these functions' return values, which could behave differently against a raw
  `FHIR.dateTime` vs. `System.DateTime` depending on whether the engine auto-unwraps it in those operator
  positions. Not verified against a live engine or a specific failing fixture (both callers currently show
  only 1-2 mismatches each, consistent with either a narrow real effect or no effect at all) — flagged for
  whoever next investigates `CMS0334`/`CMS1028`'s remaining mismatches, not fixed.
- **Everything else checked clean**: `AdultOutpatientEncounters.cql`, `AlaraCommonFunctions.cql`,
  `Antibiotic.cql`, `CQMCommon.cql` (despite being implicated in prior fix #11/#14 fixture work — no
  library-level regression, confirming those really were fixture bugs), `AdvancedIllnessandFrailty.cql`
  (already correctly applies the `union`→single-`[Condition: ...]` simplification, with its own
  self-aware TODO comment), `Hospice.cql`, `PalliativeCare.cql`, `SupplementalDataElements.cql`,
  `TJCOverall.cql`, `VTE.cql` — all mechanical-only diffs, no genuine logic regressions found.
- **Status**: **VERIFIED** (`discrepancy_report-measure-fixes-20260823-0730.md`). CMS155 fully
  passing; CMS159 improved 8→2 mismatches (the prevalenceInterval family resolved; the remaining 2
  are a Denominator Exclusion↔Numerator swap, now cleanly diagnosable); CMS2 fully passing (the
  ObservationCancelled uncomment worked); AHAOverall revert confirmed safe — CMS144 fully passing.
  CMS190's recorded() ext bypass landed separately in #21 (see below).

### 20. CRITICAL: fix #18's `Status.cql` `prevalenceInterval` overload caused a repo-wide circular-reference compile failure — caught and fixed same session

- **File**: `input/cql/Status.cql`.
- **Severity**: a fresh test run (`scripts/comparison/discrepancy_report.md`, generated 2026-08-21
  14:54) showed the suite collapse from 91.85% passing to **7.43% passing** — 67 of 74 measures went to
  100% "Missing Results" (total execution failure), including measures with zero relationship to
  anything touched this session (`CMS50`, `CMS56`, `CMS74`, `CMS117`, `CMS122`, etc.). The common thread:
  every affected measure includes `Status.cql`; the handful that survived (`CMS108`, `CMS0334`, and
  `CMS2`, which is now fully passing) do not include it.
- **Root cause**: fix #18's `prevalenceInterval(condition Choice<ConditionProblemsHealthConcerns,
  ConditionEncounterDiagnosis>)` delegated via `(condition as ConditionProblemsHealthConcerns
  ).prevalenceInterval ( )`, expecting the cast to resolve the call to FHIRCommon's concrete-type
  overload. It does not: a value cast to a concrete member of a `Choice` type is still considered a
  member of that `Choice` for overload-resolution purposes, so the call resolved back to this *same*
  newly-declared function — an infinite self-reference. Confirmed directly from the engine's own error
  message in `input/tests/results/CMS50FHIRReceiptofSpecialistReport.txt`: `"Cannot resolve reference to
  expression or function prevalenceInterval_...ChoiceTypeSpecifier..._ because it results in a circular
  reference."` This is an important, generalizable lesson: **casting to a concrete branch of a Choice
  type does NOT disambiguate a call against an overload declared for that same Choice type** — it only
  disambiguates against overloads declared for *unrelated, non-overlapping* types (which is why the
  identical casting technique worked fine for `AHAOverall.cql`'s sibling-overload fix in #19, and would
  likely have worked for the `ProcedureNotDone`/`Procedure` ambiguity discussed in #19 — those are true
  supertype/subtype or disjoint-sibling situations, not "cast to a member of the very Choice type you're
  currently inside").
- **Fix**: split into two functions. `prevalenceInterval(condition Choice<ConditionProblemsHealthConcerns,
  ConditionEncounterDiagnosis>)` now dispatches (via `is`/`as`) to a *differently-named* helper,
  `toPrevalenceInterval`, which has two concrete-type-only overloads (one per branch). Since
  `toPrevalenceInterval` isn't declared anywhere else, there's nothing for the call to circle back to,
  and each concrete overload's own `.abatementInterval ( )`/`.onset.toInterval ( )`/`.clinicalStatus`
  calls are unambiguous (this file declares no competing `abatementInterval` overload at all). Both
  overloads' bodies are FHIRCommon's own `prevalenceInterval` algorithm, inlined verbatim (same
  algorithm as before, just relocated). Noted in passing: the pre-migration `QICoreCommon.cql` had
  something structurally similar for the same reason — a deprecated plain `function ToPrevalenceInterval`
  (capital T) that called separately-named `ToInterval`/`ToAbatementInterval` helpers rather than the
  fluent `prevalenceInterval` — suggesting this self-reference hazard may be a known, older reason
  QICore itself kept these as distinct names.
- **How this got missed initially**: fix #18 was written and reasoned about carefully, but never
  actually verified against a compile — the "cast disambiguates" assumption was extrapolated from the
  `ProcedureNotDone`/`Procedure` case without checking whether the *same* reasoning actually applies when
  the overload you're casting away from is your *own* Choice-typed declaration rather than someone
  else's unrelated one. This fresh test run is exactly the kind of verification loop this whole document
  keeps saying is required before trusting a fix — treat this as the concrete example of why, not just a
  now-fixed bug.
- **Not yet re-verified**: this fix has not been run through the engine either. Needs a fresh test run
  before any of #16/#18/#19's confidence claims can be trusted. Given how large the blast radius was
  here, treat every measure that includes `Status.cql` as unverified until that happens, not just the
  ones this session directly edited.
- **Update — the first fix attempt above was itself still broken, caught via a second fresh test run
  (same 7.43% pass rate, different error).** `toPrevalenceInterval`'s body was copied verbatim from
  FHIRCommon's `prevalenceInterval(Condition)`, including `condition.clinicalStatus ~ "active"` /
  `"recurrence"` / `"relapse"`. In CQL, double-quoted names are identifier references (to a `code`,
  `concept`, `valueset`, or other named declaration), not string literals — `"active"` only resolves
  *inside* `FHIRCommon.cql` itself, where `code "active": 'active' from "ConditionClinicalStatusCodes"`
  is actually declared. Copying the expression into `Status.cql` without qualifying it produced
  `"Could not resolve identifier active in the current library."` for every one of the ~67 affected
  measures — the exact same blast radius as the circular-reference bug, just a different root cause.
  This codebase's own established convention for referencing another library's `code` declaration is
  qualification (`FHIRCommon."active"` — already used this way in `CMS108`, `CMS347`,
  `NHSNGlycemicControlHypoglycemiaInitialPopulation`, etc.), which is what the QICore-derived body was
  missing. **Fixed**: qualified all three references as `FHIRCommon."active"` / `FHIRCommon."recurrence"`
  / `FHIRCommon."relapse"` in both `toPrevalenceInterval` overloads. Still **not verified** against a
  fresh test run as of this edit — this entry has now been wrong twice in a row without a real compile
  check, which is itself the strongest argument in this whole document for not trusting any Status.cql/
  shared-library change until an actual test run confirms it, no matter how small or "obviously correct"
  it looks.
- **Update — a THIRD fresh test run showed the `FHIRCommon.` qualification fix above still didn't work,
  a third distinct error, and the whole `Status.cql` shared-function approach was abandoned in favor of
  a per-call-site fix.** `discrepancy_report-measure-fixes-20260821-1552.md` (partial recovery, 87.78%
  vs. the 91.85% pre-regression baseline) showed CMS90/CMS133/CMS142/CMS143/CMS155/CMS159/CMS951 —
  exactly the measures fix #18/#19 were meant to help — still 100% "Missing Results". Actual engine
  error this time: `"Ambiguous call to operator 'toPrevalenceInterval(org.hl7.fhir.r4.model.Condition)'
  in library 'Status'."` Root cause: `ConditionProblemsHealthConcerns` and `ConditionEncounterDiagnosis`
  are distinct CQL/model types, but both compile down to the *identical* underlying Java runtime class
  (`org.hl7.fhir.r4.model.Condition`) — USQualityCore profiles are metadata on top of the same FHIR
  resource, not distinct implementation classes. So the two-overload split from #18/the correction above
  (`toPrevalenceInterval(ConditionProblemsHealthConcerns)` / `toPrevalenceInterval(ConditionEncounterDiagnosis)`)
  is ambiguous at the *engine* level even though it looks perfectly fine at the CQL/ELM level — the
  runtime literally cannot tell the two overloads apart. **This also retroactively explains why fix #19's
  `AHAOverall.cql` sibling-overload fix (`overlapsHeartFailureOutpatientEncounter`/
  `overlapsAfterHeartFailureOutpatientEncounter`, one overload per profile type) was ALSO broken** —
  `CMS144FHIRHFBetaBlockerForLVSD` showed 48/48 "Missing Results" in the same 1552 report, the identical
  ambiguous-runtime-type failure mode, just not yet traced to its cause at the time #19 was written.
  **Final fix, this time verified conceptually against the actual runtime-type constraint rather than
  guessed**: fully reverted both `Status.cql` and `AHAOverall.cql` to their pre-session state (`git
  checkout --`, confirmed clean via `git diff`) — no shared-library changes at all for this bug class
  anymore. Instead, fixed at each of the 13 individual call sites across `CMS90`, `CMS133`, `CMS142`,
  `CMS143`, `CMS951` (×2), `CMS157` (×2), `CMS155`, `CMS159` (×4): changed `X.prevalenceInterval ( )` to
  `(X as FHIR.Condition).prevalenceInterval ( )`. This works because casting *up* to the common ancestor
  `FHIR.Condition` (not to a sibling member of the original Choice, which is what caused #18's circular
  reference) leaves exactly one candidate declaration in scope — `FHIRCommon`'s single, already-existing
  `prevalenceInterval(condition Condition)` — with no sibling overload of any kind to be ambiguous
  against, since nothing (no shared library, no per-file declaration) declares a second
  `prevalenceInterval` for plain `FHIR.Condition`. No new function declared anywhere; every fix is a
  same-line edit to an existing `such that`/`where` clause. **CMS144's `AHAOverall.cql` gap (dropped
  `ConditionProblemsHealthConcerns` support, from #19) is deliberately left UNFIXED** — the same
  runtime-ambiguity constraint applies there too (an analogous per-call-site cast doesn't work, since
  `overlapsHeartFailureOutpatientEncounter`/`overlapsAfterHeartFailureOutpatientEncounter` don't have a
  generic-`Condition`-typed overload to cast up to, only the narrow `ConditionEncounterDiagnosis`-typed
  one) — given the string of failed attempts on this exact bug class this session, leaving CMS144 at its
  known, non-crashing, pre-session mismatch level (2-3/48) is the responsible choice over a fourth guess.
  Needs a genuinely different approach (e.g. widening `AHAOverall.cql`'s own function to accept
  `FHIR.Condition` directly, verified against a real compile before landing) from whoever picks this back
  up.
- **Status of this whole saga (entries #16, #18, #19's `prevalenceInterval`-Choice fixes, #20's
  corrections): applied, still not verified against a fresh test run.** Given the track record
  (circular reference → identifier-qualification bug → runtime-ambiguity bug, three distinct failures
  in a row on what looked like an increasingly "obviously correct" fix each time), do not treat this as
  resolved until an actual fresh test run confirms CMS90/CMS133/CMS142/CMS143/CMS155/CMS159/CMS951 land
  as non-crashing with the expected mismatch counts, and that nothing else regressed. The concrete lesson
  for this document going forward, stated plainly: **a fix to any file included by many other files
  needs to be either (a) verified against a real compile/test run before being trusted, or (b) done as
  a same-file, no-new-shared-declaration edit like this final version** — sibling/Choice-typed overloads
  on USQualityCore profile types are not a safe pattern in this codebase, full stop, regardless of how
  they look on paper, because of the shared-runtime-class issue above.
- **Update — a FOURTH fresh test run (`discrepancy_report-measures-fixes-20260821-1607.md`, 85.92%)
  showed the `(X as FHIR.Condition)` call-site fix above still didn't work, a fourth distinct error.**
  Actual engine error this time: `"Expression of type
  'choice<USQualityCore.ConditionEncounterDiagnosis,USQualityCore.ConditionProblemsHealthConcerns>'
  cannot be cast as a value of type 'Condition'."` The `as` operator in this translator does not support
  widening a `Choice` to a common ancestor type at all — it only supports narrowing to one of the
  Choice's own listed members. My assumption that "casting up escapes the Choice type" was simply wrong
  about what `as` is allowed to do here, regardless of the ambiguity reasoning that motivated it.
  **Final-final fix**: replaced every `(X as FHIR.Condition).prevalenceInterval ( )` with an inline
  `is`/`as` branch to a concrete Choice *member* (not the ancestor) — `if X is ConditionProblemsHealthConcerns
  then (X as ConditionProblemsHealthConcerns).prevalenceInterval ( ) else (X as
  ConditionEncounterDiagnosis).prevalenceInterval ( )` — written directly inline at each of the same 13
  call sites, with **no function of any kind declared anywhere** (not in a shared library, not locally,
  not even under a different name). This is different from fix #18's original circular-reference mistake
  in one critical way: #18 also cast down to a Choice member, but then called a function (`prevalenceInterval`)
  that *I had also declared for the Choice type itself*, which is what made the member type still "count"
  as a Choice match. Here, nothing named `prevalenceInterval` is declared for anything Choice-shaped or
  profile-specific anywhere in the repo (confirmed via `grep -rn "function prevalenceInterval"
  input/cql/*.cql` — zero results after the `Status.cql` revert) — so after the narrowing cast, the ONLY
  reachable declaration is `FHIRCommon`'s single `prevalenceInterval(condition Condition)`, unambiguous
  by construction, sidestepping both the circular-reference failure mode (no self-declared Choice overload
  to loop back to) and the ambiguous-runtime-type failure mode (no sibling profile-specific overload of
  my own for the two branches to collide against — the *only* two-branch decision happening at all is the
  `is`/`as` type-check itself, which is a normal, already-working CQL/profile-discrimination mechanism —
  confirmed already in production use elsewhere in this exact codebase, e.g. `AHAOverall.cql`'s
  `TimingBoundToInterval` dispatching on `is FHIR.Period`/`is FHIR.Range`/`is FHIR.Duration`).
- **Status, honestly**: this is the fourth attempt at this exact bug class in one session, and the first
  three were each wrong for a different, only-visible-at-actual-compile-time reason. I'm reasonably
  confident in the reasoning above, but given the track record, **do not treat this as resolved without
  an actual fresh test run confirming it.** If this ALSO fails, the pragmatic fallback — matching what was
  ultimately done for `CMS144`/`AHAOverall.cql` above — is to leave `CMS90`/`CMS133`/`CMS142`/`CMS143`/
  `CMS155`/`CMS157`/`CMS159`/`CMS951` at their pre-session `.onset.toInterval()` state (reverted, known
  mismatched-but-non-crashing) rather than continue guessing at shared-runtime-type workarounds blind.
- **VERIFIED — the fourth attempt worked.** `discrepancy_report-measures-fixes-20260821-1620.md`
  (generated after this fix, confirmed via file timestamps): suite-wide pass rate **93.99%** — above
  the 91.85% baseline from *before* this entire regression saga started. `CMS90`, `CMS133`, `CMS143`,
  `CMS951`, `CMS155` now show **zero discrepancies** (fully passing). `CMS142`/`CMS157`/`CMS159` dropped
  from 100% "Missing Results" to small counts of genuine logic mismatches (5/32, 19/126, 2/67
  respectively) — no more crashes, meaning the actual clinical-logic questions those measures still
  have are now visible and debuggable again, rather than hidden behind an execution failure.
  `CMS144FHIRHFBetaBlockerForLVSD` (the `AHAOverall.cql`-adjacent one, deliberately left unfixed above)
  is also back to zero discrepancies — consistent with it never having been broken by anything other
  than the reverted `Status.cql`/`AHAOverall.cql` shared-library attempts, now that those are cleanly
  reverted. Consulting the `/cql` and `/qicore` skills mid-saga helped confirm the reasoning before this
  attempt (`as` is narrowing-only per the CQL reference; QICore's own convention already treats the
  "problems/health concerns" condition as plain `Condition`, reinforcing that a retrieve-level
  `[FHIR.Condition: "..."]` simplification — not attempted here, but documented above as the next-level
  fallback — is the actual canonical fix this codebase already uses successfully elsewhere). **This
  whole saga (entries #16, #18, #19, #20) is now closed out as resolved and verified** — the net result
  across all of it: the `.onset.toInterval()`/`.prevalenceInterval()` bug is fixed correctly in `CMS90`,
  `CMS133`, `CMS142`, `CMS143`, `CMS155`, `CMS157`, `CMS159`, `CMS951`, `CMS129`, and `CMS347`'s two
  parked items from #10 — ten measures total, without regressing anything else. `CMS144`'s
  `AHAOverall.cql`-level gap (dropped `ConditionProblemsHealthConcerns` support for
  `overlapsHeartFailureOutpatientEncounter`/`overlapsAfterHeartFailureOutpatientEncounter`) remains
  genuinely unfixed and parked for whoever picks it up next, per the same reasoning as CMS996/CMS108's
  `.recorded()` situation — needs a real compile-verified attempt, not another guess.

### 21. `recorded()` ambiguous-overload bug — real fix found (via the `Negation-troubleshooting` branch), applied to CMS68/CMS996/CMS108/CMS190

- **Files**: `input/cql/CMS68FHIRDocumentationCurrentMeds.cql`, `input/cql/CMS996FHIRAptTxforSTEMI.cql`
  (two sites), `input/cql/CMS108FHIRVTEProphylaxis.cql`, `input/cql/CMS190FHIRVTEProphylaxisICU.cql`.
- **Why**: the `Negation-troubleshooting` branch (merged into `measure-fixes`, see git history) added
  a scratch `NegationTest.cql` specifically to isolate the "negation resources don't seem to work"
  hypothesis. Using it as a fast, disposable repro (single test case, ~2s per run, zero blast radius
  to real measures), tried three approaches in sequence against its `"Procedure Not Done for Medical
  Reasons"` define, which calls `.recorded()` on a `ProcedureNotDone` value:
  1. `(X as FHIR.Procedure).recorded ( )` → `"Could not resolve call to operator recorded with
     signature (FHIR.Procedure)"`. Root cause: `recorded(procedure Procedure)` is declared inside
     `USQualityCoreCommon.cql`, which `using USQualityCore` — so its `Procedure` means
     `USQualityCore.Procedure`, not `FHIR.Procedure`. Casting to the wrong ancestor just lands on a
     type nothing is declared for.
  2. `USQualityCoreCommon.recorded(X)` (explicit qualified, non-fluent invocation) →
     `"Operator recorded with signature (USQualityCore.ProcedureNotDone) is a fluent function and can
     only be invoked with fluent syntax."` Confirms this engine flatly disallows calling a fluent
     function via qualified static syntax, regardless of ambiguity — a dead end independent of the
     runtime-class issue.
  3. **Working fix**: `(X.ext('http://fhir.org/guides/astp/us-quality-core/StructureDefinition/us-quality-core-recorded'
     ).value as FHIR.dateTime)` — bypass `recorded()` entirely and read the underlying FHIR extension
     directly, exactly matching what `recorded()`'s own implementation does internally. `ext()` is
     declared generically in `FHIRCommon.cql` for `DomainResource`/`Element` — one declaration, no
     per-profile sibling to collide with — so it isn't subject to the same runtime-class-collision bug.
     Confirmed via a fresh, completely error-free evaluation of `NegationTest.cql` (previously: 100%
     "Ambiguous call" error) — `"Diabetic Retinopathy Encounter"`/`"Primary Open Angle Glaucoma
     Encounter"` also both returned correct, non-empty results in the same run, incidentally also
     re-confirming entry #20's `prevalenceInterval` fix end-to-end.
- **Applied to the real measures blocked by this exact bug**:
  - `CMS68FHIRDocumentationCurrentMeds.cql:65` — `MedicationsNotDocumented.recorded ( )` (a
    `ProcedureNotDone`) → the `ext()` bypass.
  - `CMS996FHIRAptTxforSTEMI.cql:270` — `PCINotDone.performed` (a `ProcedureNotDone`, wrongly swapped
    to the wrong field during the original migration per entry #19's diagnosis) → the `ext()` bypass,
    restoring the correct field via the correct mechanism.
  - `CMS996FHIRAptTxforSTEMI.cql:278` — `FibrinolyticNoMed.effective` (a
    `MedicationAdministrationNotDone`, **not** actually affected by this bug at all — confirmed only
    one `recorded(medicationAdministrationNotDone MedicationAdministrationNotDone)` overload exists,
    no colliding sibling) → simply reverted to plain `FibrinolyticNoMed.recorded ( )`, no bypass needed.
  - `CMS108FHIRVTEProphylaxis.cql:410` and `CMS190FHIRVTEProphylaxisICU.cql:370` —
    `DeviceNotApplied.performed` (both `ProcedureNotDone`, both previously left at the wrong-but-stable
    field — `CMS190`'s specifically reverted back to this in entry #19 after the earlier ambiguity was
    found and no fix was known yet) → the `ext()` bypass in both.
- **Status**: **VERIFIED on the feature branches** (`discrepancy_report-measure-fixes-20260823-0730.md`),
  with partial resolution as predicted: CMS68 → fully passing (0 missing results); CMS996 12→8
  mismatches; CMS108 21→14; CMS190 19→11. All four improved without crashes or new missing results.
  The residuals are distinct, smaller-batch issues (e.g. CMS108/CMS190's remaining gaps line up with
  the Claim.item linkage follow-up in #14).
- **⚠ BRANCH CORRECTION (2026-09-08)**: the `.ext()` fixes described above were applied and verified on
  the `measure-fixes` / `Negation-troubleshooting` feature branches, but they were **NOT merged** into the
  `defect-tracking` branch. A grep of the `defect-tracking` source (HEAD `177df9074`) on 2026-09-08 found
  none of the four files carry the `.ext(...us-quality-core-recorded)` bypass:
  - `CMS68FHIRDocumentationCurrentMeds.cql:64` — still `.recorded ( )` (live E-22 crash, confirmed in
    `TestCaseResult-f2e2e1c0-*.json`)
  - `CMS996FHIRAptTxforSTEMI.cql:277` — still `.performed`
  - `CMS108FHIRVTEProphylaxis.cql:409` — still `.performed` (fixed on this branch 2026-09-08, below)
  - `CMS190FHIRVTEProphylaxisICU.cql:369` — still `.performed`
  So this entry's "applied to CMS68/CMS996/CMS108/CMS190" claim is **stale for `defect-tracking`**; treat
  `engine-issues.md` **E-22** (dated later, "Workaround: none shipped, engine fix needed") as the authoritative
  current status for that branch. The N/A entries in `improvement-tracking.md` §5.3 that claim the bypass was
  "applied" to these four should be read against this correction.
- **CMS108 fixed on `defect-tracking` (2026-09-08)** — `CMS108FHIRVTEProphylaxis.cql:409`, the mechanical
  `ProcedureNotDone` branch of `"No Mechanical VTE Prophylaxis Performed Or Ordered"`, changed
  `authoredOn: DeviceNotApplied.performed` → the `.ext(...)` bypass (same E-03/E-22-safe pattern). Root cause
  for test case `068814f1-4270-4e10-b470-9a5433bceb3e`: `performed` is data-absent
  (`_performedDateTime` + `data-absent-reason: not-performed`) so it evaluated to null, breaking the
  `intersect` in `"Encounter With No VTE Prophylaxis Due To Medical Reason"` (line 293) → Numerator `1→0`.
  Trace at `input/tests/results/CMS108FHIRVTEProphylaxis/TestCaseResult-068814f1-*.json` shows the clean
  `authoredOn: dateTime (null)` before the fix. (This entry is a migration-regression fix M-04 that uses the
  E-03 engine-workaround mechanism because the naive restore `.recorded()` re-triggers the E-22 ambiguity.)
- **Broader implication**: this pattern (bypass an ambiguous fluent function by inlining its
  underlying extension access via `.ext(...)`) is likely applicable to *any* future
  `recorded()`/similar-fluent-function ambiguity between USQualityCore sibling profile types that
  share a runtime class — worth checking first before assuming something is a genuine unfixable
  engine limitation, per this and entries #19/#20's pattern of over-attributing bugs to "external, not
  fixable" before actually testing a workaround.

### 22. CMS128: vendored CMD library's `medicationDispensePeriod()` returns null for every fixture (`convert Duration to days` engine gap), plus one `.onset.toInterval()` parity miss — KEPT, pending re-verification

- **Files**: `input/cql/CMS128FHIRAntidepressantMgmt.cql` (only; no shared-library changes).
- **Symptom**: 56 of 58 test cases failing (96.55%, the worst mismatch rate in the suite) in
  `discrepancy_report-measure-fixes-20260822-0736.md`, nearly all at Initial Population /
  Denominator level with Numerator/Exclusion cascades. The 2 passing cases are consistent with
  no-dispense patients where empty→null→false is the correct outcome.
- **Diagnosis method**: built a disposable scratch repro (`input/cql/CMS128DiagTest.cql` +
  `input/tests/measure/CMS128DiagTest/925ef058-.../`, untracked) with 11 probe defines isolating
  each stage of `"IPSD"`'s pipeline against one copied test case. Results pinned the failure point
  exactly: retrieves/valueset/status filters all fine (Diag1–Diag3 = 2 dispenses each step), raw
  `whenHandedOver`/`daysSupply` populated (Diag10/Diag11 = `[dateTime, dateTime]`/`[Quantity]`),
  Intake Period correct (Diag9), but `medicationDispensePeriod()` returned `[null]` for both
  dispenses (Diag4) — everything downstream (where-clause survivors, tuple lists, IPSD itself)
  empty purely by cascade (Diag5–Diag8).
- **Root cause**: the vendored `CumulativeMedicationDuration-2.0.0-ballot` library (cache copy at
  `~/.cql-language-server/npm-library-cache/4.11.0-SNAPSHOT/29dd7bae49368534/`) computes days of
  supply in BOTH dispense functions as `daysSupply: (convert D.daysSupply to days).value`
  (`MedicationDispensePeriod`:443, fluent `medicationDispensePeriod`:497). This engine evaluates
  that `convert` to null — **documented by the library's own authors**: the MedicationRequest-side
  variants carry an inline TODO ("this isn't working as expected, convert results in null",
  lines ~279/~330) and were patched to read `(R.dispenseRequest.expectedSupplyDuration).value`
  directly instead. The dispense variants never got the same patch. CMS128's fixtures all carry
  `daysSupply` + `whenHandedOver` but **no** `dosageInstruction` and no `quantity` (swept all 58
  test-case folders programmatically: 0 dispenses missing daysSupply), so when `convert` yields
  null there is nothing for the quantity/dose fallback branch to compute →
  `totalDaysSupplied` = null → function returns null despite perfectly good data. With IPSD null,
  `"Initial Population"`, `"Denominator Exclusions"` (via its `"IPSD" is not null` guard), and
  both Numerators (via empty intersects of null periods) all fail uniformly.
- **Why not fix in place / why safe here**: the cache file is auto-generated ("changes are not
  persisted across cql-language-server upgrades"). Blast radius confirmed narrow:
  `medicationDispensePeriod` is called only by CMS128 (3 sites); the other 12 measures including
  CMD use only the already-patched request-side functions (which is why e.g. CMS136 passes).
  Fixtures always supplying daysSupply makes the omitted fallback branch dead code for this suite.
- **Fix part 1** — local workaround function in the measure file, replicating the vendored logic
  minus the broken `convert` (daysSupply read mirrors upstream's own patched request-side shape):
  `AntidepressantCoveragePeriod(Dispense MedicationDispense)` — unqualified parameter type
  resolving against USQualityCore (the first `using`), exactly matching the retrieve alias type;
  `totalDaysSupplied: D.daysSupply.value`; `startDate: Coalesce(date from whenHandedOver,
  date from whenPrepared)`; same null-guard + `Interval[startDate, startDate +
  Quantity(totalDaysSupplied - 1, 'day')]` return as the original. One deviation from a verbatim
  port: bare `Quantity(...)` inside the vendored body is that library's own helper function, so the
  local version calls it qualified as `CMD.Quantity(totalDaysSupplied - 1, 'day')` (public, CMD
  already included) rather than duplicating it. Unique name → none of entry #20's ambiguity hazards
  apply (no Choice-typed self-loop, no sibling-profile overload pair sharing a runtime class).
  All 4 call sites updated (2 in `"IPSD"`, 1 per numerator define).
- **Fix part 2** — while diffing against QICore ground truth
  (`dqm-content-qicore-2025/input/cql/CMS128FHIRAntidepressantMgmt.cql` line 44): the fork's
  `"Has IPSD and Major Depression Diagnosis"` used
  `MajorDepression.onset.toInterval ( ) within 60 days of "IPSD"` where QICore uses
  `MajorDepression.prevalenceInterval ( )` — the #10/#15/#16 anti-pattern on a chronic-diagnosis
  check (a depression dx dated years before IPSD can never be "within 60 days" of it as a
  zero-width onset interval, even though it's active/ongoing). This was masked while IPSD itself
  was null but would have surfaced immediately after part 1 landed. Fixed with entry #20-final's
  proven inline dispatch pattern (`if X is ConditionProblemsHealthConcerns then (X as ...).
  prevalenceInterval ( ) else (X as ConditionEncounterDiagnosis).prevalenceInterval ( )`) —
  structurally identical to the already-verified CMS90 site.
- **Status**: **VERIFIED** (`discrepancy_report-measure-fixes-20260823-0730.md`) — CMS128 collapsed
  from 56 mismatches to **fully passing**; no other measure moved. Scratch repro files deleted
  (housekeeping this session).
- **Broader implication**: `convert <Duration> to days` evaluating to null is a genuine
  engine/translator gap (UCUM unit conversion), upstream-fileable alongside the library authors'
  existing TODO — the TODO proves they hit it on one code path and the dispense paths are the same
  bug left unfixed. If any future measure needs dispense-period logic or the quantity-based days
  fallback (dispenses without daysSupply), extend this workaround rather than calling the vendored
  dispense functions.

### 23. Verification sweep of all pending entries against the 2026-08-23 report — everything confirmed; new baseline 94.65%

- **Report**: `scripts/comparison/discrepancy_report-measure-fixes-20260823-0730.md`.
  Suite-wide pass rate **94.65%** (fail count 1268), **39 measures fully passing**, zero regressions
  attributable to any fix from entries #13/#15–#22. Statuses above updated from "pending
  re-verification" to VERIFIED with per-measure evidence.
- **Current failure buckets** (for prioritization):
  - *Missing Results — 256 cases across 9 measures*: CMS145 (106) + CMS149 (33) = no CQL authored;
    CMS1173 (62) + CMS156 (45) + CMS871 (3, +CMS645 2 + CMS646 1) = the `DateTimeType`
    engine-error family (see #24); CMS135 (3) + CMS165 (1) = "Unable to extract codes from fhirType
    Reference" (tabled, see External issues log).
  - *Mismatched — 154 cases across 31 measures*, largest: CMS157 (19), CMS69 (18), CMS108 (14),
    CMS816 (12), CMS190 (11), CMS72 (9), CMS996 (8).
- **Housekeeping**: scratch repro files from #21/#22 deleted; stale statuses and priority list
  refreshed.

### 24. `Min()`/`DateTimeType` engine-gap workarounds — 2 measures fixed, 1 regression caught and reverted, scratch probes pending characterization

- **Context**: fresh discrepancy report
  (`discrepancy_report-measure-fixes-20260823-1455.md`, suite pass rate **95.66%**, fail count
  1030) verifies everything below against the prior 0730 baseline (94.65%). The engine gap being
  worked around is the long-standing External issue ("`Min()` over plain `DateTime` values throws
  'not comparable'/'not implemented'"), but this session established it is broader than `Min()`
  itself: raw `FHIR.dateTime` values and raw choice-typed fields (`X.effective`) fed directly into
  temporal operators (`before`/`after`/`on or before`) or returned/sorted as DateTime values also
  fail downstream, and the workaround is uniform — convert to `System.DateTime` first
  (`FHIRHelpers.ToDateTime(...)`), or for choice types convert to an interval first
  (`start of X.effective.toInterval()`).
- **CMS1173FHIRDiagnosticDelayVTE — KEPT, VERIFIED (62 missing results → 0; measure now fully
  passing).** `"Qualified VTE Encounters"` compared `AntiCoagulantOrdered.authoredOn` /
  `IndexPCP.period` temporally against raw `VTEStudy.effective` (a `dateTime | Period` choice).
  Changed all three comparisons to use `start of VTEStudy.effective.toInterval ( )`. This was the
  single largest "Missing Results" bucket in the 0730 report (62 of its 65 test cases); all now
  produce results and match expected populations.
- **CMS156FHIRHighRiskMedsElderly — KEPT, VERIFIED (partial).** The two Index Prescription Start
  Date defines returned raw `.authoredOn` (`FHIR.dateTime`) values that broke downstream temporal
  arithmetic — 45 test cases were "Missing Results" at 0730. Wrapping both returns in
  `FHIRHelpers.ToDateTime ( ... )` eliminated all 45. **3 residual mismatches remain** (test cases
  `4aa75d19-...`, `c409fbc9-...`, `07f11229-...`, each failing `Numerator` in Groups 1+3), and they
  line up exactly with the `.onset.toInterval ( ) overlaps Interval[start of "Measurement Period"
  - 1 year, <IPS>]` checks at `CMS156FHIRHighRiskMedsElderly.cql:169` and `:179` — the sites fix
  #16's repo-wide sweep flagged as plausible-but-unverified instances of the chronic-diagnosis
  zero-width-onset anti-pattern (#10/#15 family). Now confirmable against concrete failing
  fixtures; next fix candidate.
- **CMS645FHIRBoneDensityPCADTherapy — REGRESSION CAUGHT AND REVERTED.** Attempted two
  `ToDateTime` conversions in the ADT start-date defines: (a)
  `Min({ FHIRHelpers.ToDateTime(ADTOrder.authoredOn), ... })` — same shape as CMS156's proven
  change, likely fine on its own; (b) a nested-query rewrite inside `firstMedicationEvent`
  (`First((dosageTiming.event DoseEvent return FHIRHelpers.ToDateTime(DoseEvent) sort ascending))`).
  Result was **total library-load failure**: every one of the 51 test cases went to "Missing
  Results" with `Library ... loaded, but had errors: Syntax error at firstEvents, Syntax error at
  dosageTiming, null cannot be cast to non-null type org.hl7.elm.r1.Expression`. The syntax error
  points at the (b) rewrite's nested aliased query, not the `Min()` change — but since neither
  half could be compile-validated independently from this environment, **the whole file was
  reverted to HEAD** (per user decision), restoring the 0730-verified state (2 missing + 2
  documented IP-level mismatches). Re-attempt the `Min()` fix later behind a validated scratch
  repro, per the #18–#20 lesson about unverified edits.
- **MinSpike scratch probes — kept, pending one test run.** Untracked probe libraries
  `input/cql/MinSpikeA/B/C1/C4/C5.cql` with fixtures under `input/tests/measure/MinSpike*/`
  exercise literal and retrieved-value forms of `Min()`, `sort`, temporal-sugar operators, and day
  precision, with and without `FHIRHelpers.ToDateTime` conversion. They have **never been run**
  (no `input/tests/results/MinSpike*.txt`). Kept in place so the *next* full harness run
  characterizes exactly which forms this engine supports — that matrix directly informs the
  pending `Min()` fixes (CMS871's `hospitalDaysMax10`, CMS645 re-attempt, CMS646). Expect them to
  appear as noise rows in the comparison reports until deleted; ignore `MinSpike*` entries when
  computing deltas.
- **Status**: VERIFIED via the 1455 report for CMS1173/CMS156/CMS645-revert. New working baseline:
  **95.66%, 1030 failures, 34 measures with discrepancies**. Remaining buckets: Missing Results —
  CMS145 (106) + CMS149 (33, both still no CQL authored), CMS135 (3) + CMS165 (1, tabled
  Reference-extraction error), CMS871 (3) + CMS646 (1, `Min()`/DateTimeType family); Mismatched —
  top counts CMS157 (19), CMS69 (18), CMS108 (14), CMS816 (12), CMS190 (11), CMS72 (9), CMS996
  (8).


## Tried and reverted (did not resolve the issue)

### CMS135FHIRACEIorARBorARNIforHF — two independent CQL rewrite attempts, both reverted

- **Symptom**: `"Unable to extract codes from fhirType Reference"`, causing 3 test cases to be
  reported as "Missing Results" (no MeasureReport population values at all).
- **Root cause hypothesis (turned out incomplete)**: `"Has ACEI or ARB or ARNI Ordered"` /
  `"Is Currently Taking ACEI or ARB or ARNI"` used
  `[MedicationRequest: medication in "ACE Inhibitor or ARB or ARNI"]`.
  `MedicationRequest.medication` is a FHIR choice type (`CodeableConcept | Reference(Medication)`),
  and the 3 failing fixtures legitimately use `medicationReference` (pointing to a separate
  `Medication` resource whose `.code` holds the actual coding) rather than
  `medicationCodeableConcept` — a valid, spec-compliant representation that a naive
  terminology-filtered retrieve can't auto-resolve.
- **Attempt 1**: rewrote both `define`s to retrieve unfiltered `[MedicationRequest]` and branch
  explicitly with an inline `if medication is FHIR.CodeableConcept then ... else exists([FHIR.Medication] ... where (medication as FHIR.Reference).references(...))`,
  matching a working precedent already in the repo (`CQMCommon.cql`'s `GetMedicationCode`).
  **Result**: byte-identical error, same 3 test cases, no change whatsoever.
- **Attempt 2**: rewrote again with an explicit `is FHIR.Reference` guard (not just the complement
  of the CodeableConcept check) and moved the valueset filter onto the `Medication.code` retrieve
  bracket (`[FHIR.Medication: "..."]` — safe, since `Medication.code` is always plain
  `CodeableConcept`, never a choice type). **Result**: still byte-identical error, same 3 test
  cases.
- **Decisive evidence neither attempt could have worked**: in both re-runs, the error blocks in
  `input/tests/results/CMS135FHIRACEIorARBorARNIforHF.txt` were **completely empty** — no
  `Patient=`, no `Initial Population=`, nothing printed at all for these 3 test cases — meaning the
  failure happens before any CQL `define` evaluates, not inside the code being rewritten.
- **Investigated further using the actual engine/translator source**
  (`/Users/raleigh.thompson/projects/smile/vs-code-cql/_repo/clinical_quality_language` and
  `.../clinical-reasoning`): traced the exact throw site to `CodeExtractor.getCodesFromBase` in
  `cqf-fhir-cql`. Generated the real ELM for attempt 2 and cross-referenced it against
  `InValueSetEvaluator.kt`, `AsEvaluator.kt`/`IsEvaluator.kt`, and `FhirModelResolver.kt` — the
  generated logic is spec-compliant and null-safe at every step (`As` with `strict=false` returns
  `null` on a type mismatch rather than throwing; `FHIRHelpers.ToConcept` and
  `InValueSetEvaluator` both null-check before touching codes; `FhirModelResolver.toCqlValue()`
  tags values by their actual runtime Java class, so `Reference` vs. `CodeableConcept`
  discrimination should be correct). Nothing in the ELM explains the crash.
- **Also tried**: the CQL language server's VS Code output channel showed
  `ERROR RepositoryFhirModelInfoProvider Unable to locate model info content for USCore` — a real,
  separate gap (no local `uscore-modelinfo-*.xml` exists in `input/cql/`, unlike `USQualityCore`,
  which has one vendored). Found a `uscore-modelinfo-6.1.0-derived.xml` of **unconfirmed
  provenance** elsewhere on disk (it "just appeared" via other tooling; not sourced from a verified
  authoritative location) and copied it into `input/cql/` as a reversible experiment.
  **Result**: no change, identical error persisted. Removed the file afterward (never committed).
- **Final state**: both CQL rewrites reverted; `CMS135FHIRACEIorARBorARNIforHF.cql` confirmed
  byte-identical to the committed original via `git diff`. **Tabled** — needs an actual JVM stack
  trace (debugger breakpoint on the exception, or increased engine log verbosity) to make further
  progress; static analysis of CQL, ELM, and engine source is exhausted.

### CMS165FHIRControllingHighBP — investigated, no fix attempted, tabled

- **Symptom**: same error string as CMS135, `"Unable to extract codes from fhirType Reference"`.
- **Investigation**: bisected the one failing test case (`43efb820-9e6e-4180-9a4d-2d7459896e5f`)
  down to its full fixture bundle — just `Patient`, `Condition`, `Encounter`, `Observation`, no
  `MedicationRequest`/`ServiceRequest`/any other `Reference`-bearing resource at all. Confirmed
  this is **not** the same medication-choice-type cause as CMS135. Searched CMS165's own CQL and
  its shared includes (`AdultOutpatientEncounters`, `Hospice`, `PalliativeCare`,
  `AdvancedIllnessandFrailty`, `SupplementalDataElements`) for any other `Reference`-typed
  code-extraction site — found none.
- **Final state**: no CQL change attempted (no credible hypothesis to test). Tabled alongside
  CMS135, pending the same stack-trace diagnostic.

## External issues log (not fixable in this repo — track, don't chase with more CQL rewrites)

- **`Min()`/`DateTimeType` — likely cql-engine gap.** `Min({...})` over a homogeneous set of plain
  `DateTime` values throws `"... not comparable"` / `"... not implemented"`. Per the CQL spec,
  `DateTime` is an explicitly supported `Min`/`Max` operand type, so this looks like a genuine
  engine limitation, not a CQL mistake. Affects `CMS645FHIRBoneDensityPCADTherapy`,
  `CMS646FHIRIntravesicalBCGTherapy`, `CMS156FHIRHighRiskMedsElderly`, `CMS871FHIRHHHyper`,
  `CMS1173FHIRDiagnosticDelayVTE`. Not reshaping correct CQL to route around it — file upstream if
  this gets prioritized.
- **RESOLVED, 2026-08-21 — workaround found and applied (see entry #21 below).** ~~Ambiguous overload
  resolution — likely cql-to-elm gap.~~ `USQualityCoreCommon.cql` defines `recorded(Procedure)` and
  `recorded(ProcedureNotDone)` as separate overloads. The original theory (ProcedureNotDone derives
  from Procedure, so the translator can't resolve to the more specific one) was wrong: per the
  model info, `USQualityCore.Procedure` and `USQualityCore.ProcedureNotDone` are actually **siblings**
  (both `baseType="USCore.ProcedureProfile"`, not one deriving from the other), and both compile to
  the identical underlying `org.hl7.fhir.r4.model.Procedure` Java class — the same
  "sibling-profile-types-share-a-runtime-class" issue documented at length in entries #19/#20 for
  `ConditionProblemsHealthConcerns`/`ConditionEncounterDiagnosis`. Confirmed via a live, isolated
  repro (`NegationTest.cql`'s `"Procedure Not Done for Medical Reasons"` define) that a disambiguating
  cast doesn't help here (unlike the true-subtype `ProcedureNotDone`/`Procedure` case this was
  originally mis-diagnosed as) — `(X as FHIR.Procedure).recorded()` fails outright ("Could not resolve
  call to operator recorded with signature (FHIR.Procedure)", since `recorded` is declared for
  `USQualityCore.Procedure`, not `FHIR.Procedure`), and explicit qualified invocation
  (`USQualityCoreCommon.recorded(X)`) is rejected by the engine entirely regardless of ambiguity
  ("... is a fluent function and can only be invoked with fluent syntax"). **The actual fix**: bypass
  `recorded()` altogether and inline what it does internally — read the extension directly:
  `(X.ext('http://fhir.org/guides/astp/us-quality-core/StructureDefinition/us-quality-core-recorded'
  ).value as FHIR.dateTime)`. `ext()` is declared generically for `DomainResource`/`Element` in
  `FHIRCommon.cql` — no per-profile sibling overload exists for it, so it's not subject to the same
  collision. Confirmed working via a fresh, error-free evaluation of `NegationTest.cql`. Applied to
  the three real measures blocked by this bug (`CMS68`, `CMS996`, `CMS108`, plus `CMS190`'s
  previously-reverted-to-a-known-safe-but-wrong-field instance from entry #19) — see entry #21.
- **CMS135/CMS165's `"Unable to extract codes from fhirType Reference"`** — see "Tried and
  reverted" above. Leaning external but not confirmed; needs a stack trace before filing anything.

### 25. Unresolved `CumulativeMedicationDuration` include silently nulled every CMD call — vendored minimal CMD 6.0.000, CMS156 root cause fixed

**Status: round 1 regressed and was corrected same session; round 2 fix applied 2026-08-23,
awaiting harness verification.** Discovered while root-causing CMS156's three failing
average-daily-dose cases (`4aa75d19`, `c409fbc9`, `07f11229` — Numerator 1 + Numerator 3, both
E=1/A=0).

- **Diagnosis chain**: A hop-by-hop probe library (`CMS156AvgDailyDoseProbe.cql`, verbatim copies of
  CMS156's functions plus P1–P9 exposures) showed `medicationRequestPeriodInDays()` returns null
  (P1) even though `dispenseRequest.expectedSupplyDuration` = 5 days should have satisfied its
  leading `Coalesce`. Strength/mult/divide hops (P3–P7) all evaluate fine.
- **Round 1 diagnosis (partially wrong)**: believed CMD never resolved in this repo AND that
  2.0.0-ballot's unguarded `Quantity(value Decimal, unit String)` constructor threw on nulls
  (6.0.000 guards it: `if value is not null then System.Quantity {…} else null`). Vendored a
  *minimal* CMD 6.0.000 (`ToDaily` + `Quantity` only) and flipped CMS156's include to unqualified
  `'6.0.000'`.
- **Round 1 regression (caught by 2017 report, pass rate 96.47% → 93.51%)**: CMS156 went from 3
  mismatches to **177 missing results** — `Could not resolve call to operator
  medicationRequestPeriod with signature (FHIR.MedicationRequest)`. CMS156 also uses CMD's fluent
  `medicationRequestPeriod()` (lines 269–271, antipsychotic different-day logic), which my grep for
  qualified `CMD."…"` calls missed. This proved the original include WAS resolving all along
  (translator-provided), so layer 1 of the diagnosis was wrong; the unresolved-include-silently-nulls
  theory is retracted. All other measures unchanged (CMS645's 2 missing predate this work).
- **Round 2 fix**: replaced the minimal vendor with a FULL copy of dqm-content-qicore-2025's
  CumulativeMedicationDuration.cql 6.0.000 (all 18 functions), adapted only in model declarations
  (QICore → USQualityCore/USCore/FHIR stack per CQMCommon convention; FHIRHelpers pinned to
  engine-provided 4.0.1; `QICoreCommon."SNOMEDCT"` inlined as a SNOMEDCT code system declaration).
- **Round 2b model adaptation**: full vendor surfaced a second, different failure class —
  `Expression of type 'choice<FHIR.Duration,FHIR.Period,FHIR.Range>' cannot be cast as a value of
  type 'Interval of System.DateTime'`. Upstream's functions write `timing.repeat.bounds as
  Interval<DateTime>` (3 sites), which QICore modelinfo tolerates because its profiles narrow the
  choice; our FHIR-based stack does not. Adapted all 3 sites to `as FHIR.Period` — the exact pattern
  CMS156's own local `medicationRequestPeriodInDays()` already uses for the same expression. Both
  deviations are recorded in the vendor file's header comment.
- **Remaining open question**: the 2026-08-23 ~21:09 probe run (minimal-CMD era, translating clean,
  guarded Quantity resolving locally) still showed P1=null — so either a second runtime thrower
  exists inside the let-chain beyond `Quantity(null,null)` (suspects: `Count()` on the absent
  `timing.repeat.timeOfDay` list — spec says Count(empty)=0 — or `ToDaily`'s else-branch
  `Message(null, true, …, 'Warning', …)` firing on null period, since a case on a null comparand
  falls through to else), *or* the minimal-era include silently failed to resolve and the guard was
  never exercised. Not yet distinguishable. Probe extended with PA–PE micro-exposures (ToDaily
  sanity/null args, Count of absent timeOfDay, Message warning, exact Coalesce-chain replica); the
  21:09 round could not evaluate them (probe translation was transitively poisoned by the then-broken
  CMD include). Next harness run settles it.
- **Open question ANSWERED (2026-08-24 run)**: PA–PE eliminated Count/Message/ToDaily-as-suspects
  (`PC`=0, `PD`=null, `PA`=1.0, `PB`=null, `PE`=0) but P1/P8/P9 stayed null. Fixture inspection
  found the real thrower: both probe MedicationRequests carry **no `doseAndRate`** (and no timing
  frequency/period), so `singleton from dosage.doseAndRate` in
  `medicationRequestPeriodInDays`/`MedicationRequestPeriod` aborts at runtime — this engine throws
  on `singleton` of an *empty* list where spec says null — killing the call **before** the
  `Coalesce(daysSupply, …)` branch that would have short-circuited to the present
  `expectedSupplyDuration` = 5 days. Upstream never hits this because upstream fixtures always
  populate dosage. PE also exposed a latent upstream hazard: `Count(absent timeOfDay)` = 0 is
  non-null, so the chain's `1.0` fallback can never fire; harmless here only while
  `expectedSupplyDuration` exists.
- **Fix applied (fixture-side, CQL untouched)**: added `doseAndRate[0].doseQuantity` = 0.25 mg and
  `timing.repeat` frequency=1/period=1/periodUnit='d' to both probe MedicationRequests
  (`d84056f4`, `f72dde40`). Next run should show P1=5, P8=0.25 mg/d, P9=true per the expected end
  state below. Watch for the same sparse-dosage pattern in real CMS156 fixtures (`c409fbc9`,
  `07f11229`) when CMS156 proper is re-run.
- **Round 3 (2026-08-24, post-fixture-fix run)**: singleton abort gone; P1 computes via the
  *division* branch (= 20 d) because `Coalesce(daysSupply, …)` still fell through —
  `(convert expectedSupplyDuration to days).value` returns **null despite the fixture carrying
  `{value: 5, code: 'd'}`**. Downstream values are arithmetically correct for 20 days (P8 ≈ 0.0625,
  P9=false); sole remaining defect was the Duration→days conversion.
- **Round 3 ANSWERED + fixed**: probes PF–PK split it cleanly — raw element resolves (`PF`),
  `.value` accessible, **implicit** convert on FHIR.Duration = null (`PG`), **explicit**
  `FHIRHelpers.ToQuantity(Duration)` = 5 'day' (`PI`), System-level convert = 5 'd' (`PJ`),
  decimal bypass = exactly 0.0625 (`PK`). Engine gap: the implicit FHIR.Duration→Quantity
  insertion inside `convert … to days` returns null at runtime while both the explicit conversion
  and the quantifier are healthy.
- **Rounds 4–6 (2026-08-24): final root cause + fix**. First attempt (route through explicit
  ToQuantity) still null — rounds 4/5 isolated why: `ToQuantity` emits unit **'day'** (calendar
  spelling), and this engine's `convert … to days` / `ConvertQuantity(…, 'd')` only accept UCUM
  `'d'`: literal `5 'day'` → convert null (`PY`) while literal `5 'd'` works (`PJ`);
  `ConvertQuantity` also null on 'day' inputs (`QB`/`QC`). Case-based arithmetic on the raw
  FHIR.Duration fields verified exact (`QD`=5). **Fix**: vendored CMD gained a local
  `ToDays(FHIR.Duration)` helper (s/min/h/d/wk/mo/a spellings, 30-day months, 365-day years,
  Message-error on unknown) and all four period functions' `daysSupply` lets plus CMS156's local
  copy and the probe copy now call it (`CMD."ToDays"(…)` outside CMD). Recorded as deviation #3
  in the CMD header (supersedes the intermediate ToQuantity attempt). Expected probe run:
  P1=5, P8=0.25 mg/d, P9=true.
- **Rounds 7–8 (2026-08-24): second engine gap found + fixed**. With P1=5 green (`ToDays`
  verified), P8/P9 still failed: probes QE–QH proved quantity division across mass/time
  dimensions is broken on engine 4.9.0 — it UCUM-normalizes to base units (g/s) then rounds the
  VALUE to 8 decimal places, so `1.25 'mg' / 5 'd'` ≈ 2.9e-9 g/s collapses to exactly zero
  (`QF`=0E-8), and even clean synthetic literals compare false (`QH`). Every mg/day quotient
  dies in the rounding gap → `averageDailyDose() > 0.125/6 'mg/d'` could never fire → Numerator
  1 impossible for all patients. **Fix (deviation #4, CMS156 local function + probe copy)**:
  `averageDailyDose` now constructs `System.Quantity { value: quantity.value * strength.value /
  DaysSupplied, unit: 'mg/d' }` — pure decimal math (shape probe-verified exact by PK) with an
  explicit unit string, so the downstream same-unit comparisons never trigger conversion.
  Expected probe run: P8 = 0.25 'mg/d', P9 = true.
- **Expected end state**: P1 DaysSupplied = 5, P8 AvgDose = 0.25 mg/d, P9 Compare = true →
  **ACHIEVED in 2026-08-24 probe run** (P1=5, P8=0.25 'mg/d', P9=true). All three engine gaps now
  worked around and run-verified: sparse-fixture singleton abort (fixture-side enrichment),
  Duration→days conversion (`ToDays` helper), quantity-division rounding collapse (decimal-math
  quantity construction). Remaining: run CMS156 proper (fixtures `c409fbc9`/`07f11229` may need
  the same dose enrichment), confirm Numerator flips with no regressions, then delete the scratch
  probe library + Measure resource + test dir per its header directive.
  Numerator 1 goes true for `4aa75d19`/`c409fbc9`; `07f11229`'s prolonged-duration branch (Sum of
  antiinfective days-supplied > 90) un-nulls via the same function. Whatever engine-gap workaround
  proves necessary will be applied locally in CMS156 and documented here.
- **Dead includes**: only CMS156 actually calls CMD functions (`medicationRequestPeriod`,
  `Quantity`, `ToDaily`); 11 other libraries carry the include as dead weight (`CMS22`, `CMS347`,
  `CMS153`, `CMS128`, `CMS138`, `CMS136`, `CMS2`, `Antibiotic`, `AdvancedIllnessandFrailty`,
  `CMS1017`, `CMS137`). Their includes remain at qualified 2.0.0-ballot (unused, so inert);
  cleanup candidate once this verifies.
 - **MADiE reference scrub (same session, user directive)**: all MADiE mentions removed from
   MeasureParityNotes.md and from the `.cql` header comments of `CQMCommon`,
   `SupplementalDataElements`, `TJCOverall`, `VTE` (comment-only). Generated ELM `.xml` artifacts in
   `input/cql/` still carry `madie.cms.gov` paths inside compiled output — left alone as generated
   files, same as `temp/pages/`.
- **Local translation pre-checks (2026-08-24, tooling investigation — tabled)**: attempted to
  validate CMD/CMS156 edits locally with the pinned cql-to-elm CLI
  (`vs-code-cql/_repo/clinical_quality_language/Src/java`, `:cql-to-elm-cli:run`) instead of
  harness round-trips. Results:
  - Works for plain-FHIR libraries: sandbox copies of `CumulativeMedicationDuration.cql` (usings
    reduced to `FHIR '4.0.1'`) translate clean, so the `as FHIR.Period` bounds adaptations are
    type-valid. Technique reusable for any self-contained System/FHIR-only library.
  - Does NOT work for USQualityCore-model libraries: the CLI's directory-backed
    `DefaultModelInfoProvider` resolves sibling `uscore-modelinfo.xml` / `fhir-modelinfo.xml`
    fine but returns null for our `usqualitycore-modelinfo.xml` (versionless request hits the
    exact-filename fast path at `DefaultModelInfoProvider.kt:43-49`; file present; root cause not
    reached — suspected kotlinx-io 0.8.0 API churn on the CLI classpath). Separately, the CLI's
    `--model <file>` option is broken in this fork (`ClassCastException`: the registered lambda is
    not a `ModelInfoProvider`) and its provider does no identifier matching anyway, so it would
    cross-contaminate FHIR requests. The harness remains the arbiter for measure-level
    translation; don't burn more time on the local path unless the translator fork gets updated.
- **Incident (2026-08-24, fixed same day)**: the local-sandbox work leaked into the repo — a
  broad sweep stripped `version '0.1.0-cibuild'` / `version '6.1.0-derived'` from the
  USQualityCore/USCore using declarations of 91 tracked libraries, and the vendored
  `CumulativeMedicationDuration.cql` briefly carried only `using FHIR version '4.0.1'` (the
  FHIR-only reduction used for the local CLI check). Restored all using declarations to HEAD's
  exact form (verified: zero drift vs HEAD across tracked files); CMD now declares the full
  three-model stack matching its header comment and every sibling library.

- **Branch `astp-update` (2026-08-25): re-apply stale `onc`→`astp` fixture namespace fix
  (replaces lost work from decommissioned branch)**
  - **Why now**: UQC main (`origin/main` @ `0866d738`) never received the bulk fixture
    namespace fix from the old `qicore-to-usqualitycore-conversion` branch (entry #1). That
    branch is being decommissioned, so the fix is re-applied on a fresh `astp-update` branch off
    main.
  - **What changed**: Python script replaced
    `http://fhir.org/guides/onc/us-quality-core/` →
    `http://fhir.org/guides/astp/us-quality-core/`
    in 2,679 fixture JSON files under `input/tests/measure/` (2,877 total URL occurrences).
    Scope verified: zero stale `onc` refs remain repo-wide; zero mixed-state files; no CQL or
    modelinfo touched (already on `astp` on main).
  - **Measures affected** (12): CMS347 (1,129 files), CMS129 (459), CMS69 (316), CMS56 (235),
    CMS90 (232), CMS50 (185), CMS68 (40), CMS75 (38), CMS74 (37), CMS165 (5),
    NHSNGlycemicControl (2), CMS135 (1).
  - **Validation**: 0 `guides/onc` hits remaining, 24,808 files now carry `guides/astp`, 0
    mixed-state files, 0 JSON parse errors, `git diff --stat` = 2,680 files (2,679 replacements +
    1 pre-existing artifact deletion from working tree).
  - **Expected pass-rate impact**: 85.23% → ~87.5% (fail 3,504 → ~2,960) per entry #1's
    verified delta. Awaiting user-driven harness run to confirm.
  - **Notes home**: moved into the repo at `defect-tracking/conversion-notes.md`
    (alongside `engine-issues.md` / `change-classification.md`, co-located with the
    `defect-tracking/` paths it references).

### 26. CMS145 and CMS149 CQL ported from QC to UQC — KEPT, pending verification

- **Files**: `input/cql/CMS145FHIRCADBBlockerTPMIorLVSD.cql` (new), `input/cql/CMS149FHIRDementiaCognitiveAssess.cql` (new).
- **Why**: CMS145 (106 Missing Results) and CMS149 (33 Missing Results) had no CQL source in UQC —
  the Measure resources and test fixtures existed but the CQL was never ported from QC. Together
  these account for 139 of the suite's Missing Results (9.4% of UQC's 1,472 total failures at the
  time of porting).
- **Port approach**: copied CQL from `dqm-content-qicore-2025` QC repo and applied the standard
  QICore → UQC migration header changes:
  - `using QICore version '6.0.0'` → `using USQualityCore version '0.1.0-cibuild'` + `using USCore version '6.1.0-derived'` + `using FHIR version '4.0.1'`
  - `include FHIRHelpers version '4.4.000'` → `include hl7.fhir.uv.cql.FHIRHelpers version '4.0.1'` + `include hl7.fhir.uv.cql.FHIRCommon version '2.0.0'`
  - Added `include hl7.fhir.us.cql.USCoreCommon version '2.0.0-ballot'` + `include hl7.fhir.us.cql.USCoreElements version '2.0.0-ballot'`
  - `include SupplementalDataElements version '5.1.000'` → `include SupplementalDataElements version '6.1.000'`
  - `include QICoreCommon version '4.0.000'` → `include USQualityCoreCommon version '0.1.0-cibuild'` + `include Status version '2.1.000'`
  - `include AHAOverall version '4.1.000'` → `include AHAOverall version '5.1.000'` (CMS145 only)
- **Symbol mapping**:
  - `QICoreCommon."allergy-active"` → `USQualityCoreCommon."allergy-active"` (CMS145)
  - `QICoreCommon."allergy-confirmed"` → `USQualityCoreCommon."allergy-confirmed"` (CMS145)
  - `QICoreCommon."confirmed"/"unconfirmed"/"provisional"/"differential"` → `Status."confirmed"` etc. (CMS149)
  - `["USCoreHeartRateProfile"]` → `["Heart Rate Profile"]` (CMS145 — USCore modelinfo type name)
- **Key finding**: condition verification status codes (`confirmed`, `unconfirmed`, `provisional`,
  `differential`) are defined in the `Status` library (version 2.1.000), not in `USQualityCoreCommon`.
  `USQualityCoreCommon` only defines allergy-intolerance verification status codes. All 18 existing
  UQC measures with `isVerified` functions get these codes from `Status."..."`, confirmed via
  `scripts/comparison/discrepancy_report*.md` and source inspection.
- **Branch**: `cms145-cms149-port` (off `astp-update`).
- **Status**: CQL written, **not yet verified** against a fresh harness run. Expect compilation or
  evaluation issues if any model-profile type used in the CQL doesn't exist in
  `usqualitycore-modelinfo-0.1.0-cibuild.xml` (e.g. `MedicationNotRequested` — confirmed present;
  `ConditionProblemsHealthConcerns` / `ConditionEncounterDiagnosis` — confirmed present).
  `isVerified` for CMS145 comes from `FHIRCommon.cql` (which defines it for `FHIR.Condition`) via
  the AHAOverall include chain; CMS149 defines it locally using `Status."..."` codes.

### 27. Shared CQL libraries captured for static review — `defect-tracking/_reference/` + package provenance

Added `defect-tracking/_reference/{FHIRCommon,FHIRHelpers,USCoreCommon,USCoreElements}.cql`.

**Why they were invisible**: every measure includes these as externally-resolved packages
(`hl7.fhir.uv.cql.*` / `hl7.fhir.us.cql.*`) that are not stored in `input/cql`, so they could not
be statically reviewed from the repo alone.

**Provenance (how they were captured)**: the VSCode CQL extension / CQL engine / translator
downloads FHIR packages into `~/.fhir/packages/`. Each shared library ships as a FHIR `Library`
resource whose `content[0]` (`contentType=text/cql`) holds the CQL as a **base64** string; the
`_reference/*.cql` files are the base64-decoded plain text (faithful — verified byte-exact for
`USCoreCommon`, and whitespace-normalized-only difference for `FHIRCommon`).

| Reference file | Source package | Library resource |
|---|---|---|
| `FHIRCommon.cql` | `~/.fhir/packages/hl7.fhir.uv.cql#2.0.0` | `Library-FHIRCommon.json` |
| `FHIRHelpers.cql` | `~/.fhir/packages/hl7.fhir.uv.cql#2.0.0` | `Library-FHIRHelpers.json` |
| `USCoreCommon.cql` | `~/.fhir/packages/hl7.fhir.us.cql#2.0.0-ballot` | `Library-USCoreCommon.json` |
| `USCoreElements.cql` | `~/.fhir/packages/hl7.fhir.us.cql#2.0.0-ballot` | `Library-USCoreElements.json` |

Dependency chain: FHIRCommon→FHIRHelpers; USCoreCommon→{FHIRHelpers,FHIRCommon};
USCoreElements→{FHIRHelpers,FHIRCommon,USCoreCommon}.

**Key findings:**
- `FHIRCommon` v2.0.0 is where `prevalenceInterval`/`abatementInterval`/`verified`/`isVerified`/
  `toInterval`/`ext` live. Line 394 confirms **E-15**: only `prevalenceInterval(condition
  Condition)` exists — no `Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>`
  overload — so a union of the two sibling profiles fails to translate (`Could not resolve call to
  operator prevalenceInterval with signature (choice<...>)`).
- `FHIRCommon.verified(List<FHIR.Condition>)` (line 438) and `isVerified(FHIR.Condition)`
  (line 427) exist, so the E-15 replacement `[FHIR.Condition: VS].verified()` resolves cleanly
  (same as the working CMS125 pattern).
- Correction: the failing fluent functions are in `FHIRCommon.cql`, not `FHIRHelpers.cql`
  (FHIRHelpers is conversion helpers only).
- Cross-referenced into `defect-tracking/engine-issues.md` #E-15.

### 28. CMS133 E-13 fix: replace 55-pair sibling-profile condition union with base `FHIR.Condition` retrieves — KEPT, VERIFIED fully passing

- **Context**: CMS133 was 73/73 "Missing Results" in the `20260830-0630` measure-failure report,
  all with the E-13 signature — `Could not resolve call to operator prevalenceInterval with
  signature (choice<USQualityCore.ConditionEncounterDiagnosis,USQualityCore.ConditionProblemsHealthConcerns>)`.
  This is Stage 3 of the E-13 rollout (`defect-tracking/engine-issues.md`); engine-issue workaround,
  not a migration regression.
- **Files**: `input/cql/CMS133FHIRCataracts2040BCVA90Days.cql`.
- **Why**: `"Cataract Surgeries in Patients with Significant Ocular Conditions Impacting the Visual
  Outcome of Surgery"` (`Denominator Exclusions`) builds `ComorbidDiagnosis` from a 55-pair union of
  `[ConditionProblemsHealthConcerns: <VS>]` ∪ `[ConditionEncounterDiagnosis: <VS>]` (one pair per
  ocular-comorbidity valueset), then calls `ComorbidDiagnosis.prevalenceInterval ( )` /
  `.isVerified ( )` on the resulting `Choice<...>` — the exact E-13 translator failure. The
  `ComorbidDiagnosis` alias is a `union` chain only used by this single `with (...)` clause; the
  choice-typed value never feeds anything else.
- **Fix**: replaced all 55 union pairs with a single base `[FHIR.Condition: <VS>]` retrieve each
  (55 base retrieves kept in the same valueset order, still `union`-ed — all `List<FHIR.Condition>`
  now, so no `Choice` forms). `.prevalenceInterval ( )`/`.isVerified ( )` resolve against
  `FHIRCommon`'s base-`Condition` overloads (`defect-tracking/_reference/FHIRCommon.cql:394` / `:427`).
  The local Choice-typed `isVerified` fluent (bottom of file, now unused) is left in place, matching
  the CMS143 precedent. Single-comment `// [E-13] ...` + `// Original:` compact note per the
  established convention (fix is NOT a logic change; all 55 valuesets and the `such that` predicate
  preserved byte-for-byte).
- **Verified (static)**: 55/55 valuesets present exactly once in the same order; define-region parens
  `6`/`6` and brackets `58`/`58` balanced; primary source
  `"Cataract Surgery Between January and September of Measurement Period" CataractSurgeryPerformed`
  retained; `such that` predicate identical to HEAD. (A first scripted pass dropped the primary-source
  line — caught and restored before this entry was written.)
- **Status**: **VERIFIED** (`discrepancy_report-20260830-0733-fix-cms133.md`, 2026-08-30) — **CMS133
  fully passing: 0 MR / 0 mismatched**, and the only new entry in "Measures with No Discrepancies"
  (14 → 15). Suite-wide Pass 18544 → 18915 (78.17% → 79.74%), MR measures 14 → 13, MR cases 947 → 767.
  Per-measure 0630→0733 delta is exactly three rows — CMS133 −73 MR (pass), NHSN-Glycemic 80 MR → 1
  MM, NHSN-Acute 27 MR → 27 MM (the latter two are result-coverage changes from the harness re-run,
  not E-13 edits) — so no regressions. Remaining Stage 3 (4 after CMS128 + CMS56 + CMS131): CMS159 67 MR,
  CMS996 114 MR, CMS157 126 MR, CMS156 177 MR.

### 29. CMS128 E-13 fix: replace `ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis` union with base `FHIR.Condition` retrieve — KEPT, VERIFIED

- **Context**: CMS128 was 58/58 "Missing Results" in the `20260830-0733` discrepancy report — E-13
  Stage 3 (`defect-tracking/engine-issues.md`). Note: entry #22's E-07 `AntidepressantCoveragePeriod()`
  workaround earlier made CMS128 fully passing (2026-08-23), but the current tree's `"Has IPSD and
  Major Depression Diagnosis"` define uses the E-13 choice-typed union, so the measure did not load
  past it (engine-issue workaround, not a migration regression).
- **Files**: `input/cql/CMS128FHIRAntidepressantMgmt.cql`.
- **Why**: `"Has IPSD and Major Depression Diagnosis"` builds
  `[ConditionProblemsHealthConcerns: "Major Depression"]` ∪ `[ConditionEncounterDiagnosis: "Major
  Depression"]`, then calls `.verified ( )` on the union and `.prevalenceInterval ( )` on each
  resulting choice-typed `MajorDepression` — the exact E-13 translator failure.
- **Fix**: replaced the 2-profile union with a single base `[FHIR.Condition: "Major Depression"]`
  retrieve; the `.verified ( )` list filter and `.prevalenceInterval ( )` per-element calls now
  resolve against FHIRCommon's base-`Condition` overloads
  (`defect-tracking/_reference/FHIRCommon.cql:438` / `:394`). `// [E-13]` + `// Original:` comment
  kept (full original union in comments; fix is NOT a logic change — valueset and predicate
  byte-for-byte preserved).
- **Verified (static)**: define-region parens `4`/`4`, brackets `1`/`1` balanced; no profile-typed
  retrieves remain in code (only inside the `// Original:` comment); whole-file paren/bracket offset
  identical to HEAD (pre-existing ±2); only E-13 site in the file.
- **Status**: **VERIFIED** (`discrepancy_report-20260830-0851-fix-cms128.md`, 2026-08-30) — **CMS128
  58 MR → 16 mismatched (42/58 passing)**. The 16 are 8 unique cases × 2 groups, all `Denominator
  Exclusion 1→0`, every fixture Hospice-triggered (`170935008` Condition, `45755-6` Observation,
  `385763009` SR/Procedure, `428361000124107` Encounter) = the known class-B Hospice bucket (same as
  CMS117/136/153/155/138/75); E-13 mechanism closed — base retrieve behaviorally identical to the
  reference. Suite-wide Pass 18915 → 19131 (80.65%), Fail 4807 → 4591, MR 767 → 709, MM 957 → 973;
  per-measure delta is exactly CMS128 (no regressions).

### 30. CMS56 E-13 fix: replace 4 sibling-profile condition unions with base `FHIR.Condition` retrieves — KEPT, VERIFIED

- **Context**: CMS56 was 58/58 "Missing Results" in the `20260830-0851` discrepancy report — E-13
  Stage 3 (`defect-tracking/engine-issues.md`); engine-issue workaround, not a migration regression.
- **Files**: `input/cql/CMS56FHIRFuncStatHipReplacement.cql`.
- **Why**: four `Denominator Exclusions` defines chain 2-profile unions and then call
  `prevalenceInterval ( )` / `isVerified ( )` on the resulting `Choice<...>` — the E-13 translator
  failure:
  - `"Has Severe Cognitive Impairment"` (`ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis`)
  - `"Has Total Hip Arthroplasty with 1 or More Lower Body Fractures"`
  - `"Has Malignant Neoplasm of Lower and Unspecified Limbs"`
  - `"Has Mechanical Complication"`
- **Fix**: each union → a single base `[FHIR.Condition: <VS>]` retrieve; `.prevalenceInterval ( )` /
  `.isVerified ( )` now resolve against FHIRCommon's base-`Condition` overloads
  (`defect-tracking/_reference/FHIRCommon.cql:394` / `:427`). The local Choice-typed `isVerified`
  fluent (bottom of file, now unused) is left in place, matching the CMS143 precedent. `// [E-13]` +
  `// Original:` comments kept; fix is NOT a logic change (valuesets and predicates byte-for-byte
  preserved).
- **Verified (static)**: all 4 define regions balanced (parens 3/3, 5/5, 5/5, 5/5; brackets 1/1 each);
  no profile-typed retrieves remain in code (only inside `// Original:` comments and the unused local
  `isVerified` signature); all 4 predicate lines confirmed identical to HEAD.
- **Status**: **VERIFIED** (`discrepancy_report-20260830-1228.md`, 2026-08-30) — **CMS56 58 MR → 18
  mismatched (40/58 passing)**. The 18 are all `1→0`:
  - **8 `Denominator Exclusion` = class-B Hospice bucket** (same as CMS117/136/153/155/138/75):
    6 confirmed Hospice resources — `170935008` Condition ×2, `45755-6` Observation, `385763009`
    Procedure ×2, `428361000124107` Encounter — plus 2 Hospice-library encounter-code variants
    (`183452005`+`428371000124100`, `183921001`). **None flow through the 4 E-13-edited defines**
    (those all evaluate `false` correctly), so the E-13 mechanism is closed — the base retrieve is
    behaviorally identical to the reference.
  - **10 `Numerator` = new E-17 issue** (`ObservationScreeningAssessment` retrieve gap): every
    `Date {HOOS,HOOSJr,PROMIS10,VR12} Total Assessment Completed` returns `[]` across all 58 cases,
    including fixtures that fully satisfy the logic (e.g. c19b82ba: all 5 HOOS subscales on
    2024-11-01 and 2025-08-28, `valueInteger: 60`). Profiled as E-17 in
    `defect-tracking/engine-issues.md`.
  - Suite-wide Pass 19131 → 19345 (81.55%), Fail 4591 → 4377, MR 709 → 651, MM 973 → 991;
    per-measure delta is exactly CMS56 (no regressions).

### 31. New engine issue E-17: `ObservationScreeningAssessment` profile retrieve / `isAssessmentPerformed()` gap (CMS56 Numerator) — OPEN

- **Context**: found while verifying entry #30 against
  `discrepancy_report-20260830-1228.md`. CMS56's 10 `Numerator 1→0` mismatches do **not** flow
  through the E-13-edited DenExcl defines — they come from the five assessment-based Numerator
  defines.
- **Symptom**: `Date {HOOS,HOOSJr,PROMIS10,VR12 Oblique,VR12 Orthogonal} Total Assessment Completed`
  evaluate to `[]` in **all 58** CMS56 result traces, even where the fixture carries every required
  `ObservationScreeningAssessment` resource. Example c19b82ba: all 5 HOOS subscales (72093-8, 72094-6,
  72095-3, 72096-1, 72097-9) present on 2024-11-01 (initial) and 2025-08-28 (follow-up), each
  `status: final`, category `survey`, `valueInteger: 60`, with the
  `us-quality-core-observation-screening-assessment` profile — expected Numerator 1, actual 0.
- **Root-cause family**: the CQL chains `[ObservationScreeningAssessment: <code>].isAssessmentPerformed()`.
  This is the observation-side analogue of the known class-B profile-retrieve gaps
  (`[USCore.BMIProfile]` → `[]`, `[USCore.ObservationPregnancyStatusProfile]` → `[]`): either the
  screening-assessment profile retrieve misses the fixtures or the `isAssessmentPerformed()` status
  filter is over-aggressive on this engine. Attribution (engine vs CQL) still open.
- **CMS131 corroboration (2026-08-31, `discrepancy_report-20260831-0007.md`)**: 6 of CMS131's 24
  DenExcl mismatches route through the same screening/simple-observation retrieves — `45755-6`
  Hospice MDS (f77b9abc), `71007-9` FACIT-Pal ×2 (a6cd48c6, e9b9b388), `98181-1` Medical equipment
  (61dfb0bd), `71802-3` Housing status (f0b61b7a), `R26.89` Frailty Symptom `isSymptom` (f45a1cb0) —
  all expected-exclusion resources present but excludes don't fire. Widens E-17's confirmed footprint
  from CMS56-only to CMS56 + CMS131 (16 total cases).
- **Tracking**: `defect-tracking/engine-issues.md` E-17 (confirmed, no workaround shipped;
  optional `testE15`-style isolation probe recommended).
- **Outlook**: if resolved, CMS56 would move from 40/58 to 50/58 passing (remaining 8 = class-B
  Hospice DenExcl) and CMS131 from 39/63 to ~45/63 (remaining ~18 = class-B Hospice/Palliative/AIF).
  Not blocking the E-13 rollout.

### 32. CMS131 E-13 fix: replace 2 sibling-profile condition unions with base `FHIR.Condition` retrieves — KEPT, VERIFIED

- **Context**: CMS131 was 63/63 "Missing Results" — E-13 Stage 3 (`defect-tracking/engine-issues.md`).
- **Files**: `input/cql/CMS131FHIRDiabetesEyeExam.cql`.
- **Why / Fix**: two defines build a `ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis`
  union and then call `.verified ( )` / `.prevalenceInterval ( )` on the resulting `Choice<...>` — the
  E-13 translate failure:
  - `"Bilateral Absence of Eyes"` (Denominator Exclusions) — `Anophthalmos of bilateral eyes (disorder)`
  - `"Diabetic Retinopathy Overlapping Measurement Period"` (Numerator) — `Diabetic Retinopathy`
  Each union → a single base `[FHIR.Condition: <VS>]` retrieve; `.verified ( )` / `.prevalenceInterval ( )`
  now resolve against FHIRCommon's base-`Condition` overloads
  (`defect-tracking/_reference/FHIRCommon.cql:394` / `:427` / `:438`). `// [E-13]` + `// Original:`
  comments kept; fix is NOT a logic change (valuesets and all predicates byte-for-byte preserved).
  The single-profile `[ConditionEncounterDiagnosis: "Diabetes"]` retrieve in "Initial Population"
  (no union, no Choice) is left untouched.
- **Verified (static)**: live-code (non-comment) parens balanced in both define regions
  (`Bilateral Absence of Eyes` 5/5; `Diabetic Retinopathy Overlapping Measurement Period` 4/4),
  brackets 1/1 each; no profile-typed condition unions remain in live code (only inside
  `// Original:` comments); the two `.verified ( )` / `.prevalenceInterval ( )` call sites now
  resolve over `List<FHIR.Condition>`.
- **Status**: **VERIFIED** (`discrepancy_report-20260831-0007.md`, 2026-08-31) — **CMS131 63 MR → 0
  MR (39/63 passing, 24 mismatched)**. The 24 are all `Denominator Exclusion 1→0`, and **none flow
  through the E-13-edited defines** — the base-retrieve mechanism is closed:
  - `"Bilateral Absence of Eyes"` evaluates correctly on both Anophthalmos fixtures
    (5432b9e7-1fee…, b70ba99a-4ed9… → DenExcl 1, matching expected); `"Diabetic Retinopathy
    Overlapping Measurement Period"` is Numerators-side and every Numerator row matches.
  - **Bucket breakdown (same class-B families as CMS56)**:
    - ~8 **Hospice**: `170935008` Condition ×2 (c1340d6e, ea0e556f), `183921001` Encounter
      (97935b1b), `183452005` Encounter ×2 (106633c6, c36eddf7), `385763009` Procedure (3624228c)
      + ServiceRequest (728333bf), `45755-6` Observation (f77b9abc).
    - ~4 **PalliativeCare**: `305824005` Encounter (d46ab51c), `395694002` Procedure (36222907),
      `71007-9` FACIT-Pal Observation ×2 (a6cd48c6, e9b9b388).
    - ~11 **Advanced Illness & Frailty / LTCF / nursing home**: dementia MedReq +
      `217083007`/`441874000`/`100721000119109` Conditions, Frailty Device order, `98181-1` Medical
      equipment (61dfb0bd), `71802-3` Housing status (f0b61b7a), `R26.89` Frailty Symptom (f45a1cb0).
    - **1 expected-anomaly**: `65c895d1` asserts DenExcl 1 in its own fixture MeasureReport but
      carries **no** exclusion-triggering resource (only 2 encounters + type-1-diabetes condition) —
      fixture/expected-data issue, not engine; logged pending trace.
  - The 6 of 24 flowing through `ObservationScreeningAssessment`/`SimpleObservation` retrievals
    (`45755-6`, `71007-9` ×2, `98181-1`, `71802-3`, `R26.89` `isSymptom`) extend **E-17** (CMS131
    corroboration; see #31 and `defect-tracking/engine-issues.md` E-17).
  - Suite-wide 1228→0007: Pass 19345 → 19573 (82.51%), Fail 4377 → 4149, MR 651 → 588, MM 991 →
    1015; per-measure delta is exactly CMS131 (no regressions).

### 33. CMS157 E-13 fix: replace 2 sibling-profile condition unions with base `FHIR.Condition` retrieves — KEPT, VERIFIED (base-retrieve mechanism sound; 19 residual MM = fixture/terminology mismatch, not E-13)

- **Context**: CMS157 was 126/126 "Missing Results" — E-13 Stage 3 (`defect-tracking/engine-issues.md`).
- **Files**: `input/cql/CMS157FHIRPainIntensityQuantified.cql`.
- **Why / Fix**: two Numerator defines built a `ConditionProblemsHealthConcerns` ∪
  `ConditionEncounterDiagnosis` union and called `.prevalenceInterval()` on the `Choice<...>` — the
  E-13 translate failure. Each union → a single base `[FHIR.Condition: ...]` retrieve
  (lines 61-63 face-to-face oncology, 81-83 radiation + cancer-dx):
  - `"Face to Face or Telehealth Encounter with Ongoing Chemotherapy"`
  - `"Radiation Treatment Management During Measurement Period with Cancer Diagnosis"`
  `// [E-13]` + `// Original:` comments kept; fix is NOT a logic change (valuesets and all predicates
  preserved byte-for-byte).
- **Verified (static)**: live-code parens balanced in both define regions; no profile-typed condition
  unions remain in live code (only inside `// Original:` comments); the oncology/radiation defines now
  resolve over `List<FHIR.Condition>`.
- **Status**: **VERIFIED** (`discrepancy_report-20260831-0853-add-cms149.md`, 2026-08-31) — **CMS157
  126 MR → 107 passing / 19 mismatched** (all `Initial Population 1→0`).
- **KEY — the 19 residual MM are a fixture/terminology mismatch, NOT an E-13 regression**.
  Every failing fixture codes its Cancer Condition with **ICD-10-CM** (`C00.0`, `C00.3`, `C18.6`,
  `C22.0`, `C40.00`, `C34.01`, …), but the measure's `"Cancer"` valueset
  (`2.16.840.1.113883.3.526.3.1010`) is **SNOMED-CT only** (`http://snomed.info/sct` + an
  `urn:ietf:rfc:3986` include). Verified programmatically: none of the failing codes are members of
  the valueset (`inCancerVS=False` for all 15 resolvable failing cases). The base
  `[Condition: "Cancer"]` retrieve is **behaviorally identical** to the original union for valueset
  membership (both require `code in "Cancer"`), so these fixtures fail identically whether the
  retrieve is profile-based or base-based. They only appear as mismatches now because E-13 previously
  crashed the whole library into Missing Results; they were never evaluated as IP. **E-13 is
  exonerated.** The residual is a fixture/code-system question (NCQA test cases carry ICD-10-CM; the
  measure valueset is SNOMED) — see `improvement-tracking.md` "Remaining work".

### 34. CMS996 E-13 fix: replace 3 sibling-profile condition unions with base `FHIR.Condition` retrieves — KEPT, VERIFIED (STEMI retrieve sound; 7 residual MM all non-E-13)

- **Context**: CMS996 was 114/114 "Missing Results" — E-13 Stage 3.
- **Files**: `input/cql/CMS996FHIRAptTxforSTEMI.cql`.
- **Why / Fix**: three defines built `ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis`
  unions and fed them to `.prevalenceInterval()` — the E-13 translate failure. Each → a single base
  `[FHIR.Condition: ...]` retrieve:
  - `"Active Long Term use of Anticoagulants"` (Denominator Exclusion)
  - `"ED Encounter with STEMI Diagnosis"` (Initial Population)
  - `"Received tPA in Another Facility within 24 hours Prior to Admission"`
  `// [E-13]` + `// Original:` comments kept; not a logic change.
- **Status**: **VERIFIED** (`discrepancy_report-20260831-0853-add-cms149.md`) — **CMS996 114 MR → 107
  passing / 7 mismatched**. The E-13 STEMI retrieve **works**: STEMI (`I21.21`) is found in every
  case (`Initial Population = 1` correct throughout). **All 7 residual MM are non-E-13**:
  - 4× Denominator **Exception** `1→0` via `ProcedureNotDone` ("Reason for No PCI",
    ccc7deaf/7edab122) and `MedicationAdministrationNotDone` ("Reason for Not Administering
    Fibrinolytic", 60823d79/8bb7c40b) — class-B `*NotDone` profile-retrieve gap.
  - 2× Denominator **Exclusion** `0→1` via completed `Procedure` "Major Surgical Procedure"
    (10259007/10683007, 88d99809/f71b56bb) — class-B procedure retrieve gap.
  - 1× Denominator **Exclusion** `0→1` genuine CQL temporal-logic mismatch (f6c7dbc1):
    `AllergyIntolerance` (anistreplase) `onsetPeriod` 2021→2026 ends **before** the ED; the exclude
    clause `ThrombolyticAllergy.onset before end of ED` still fires — class-c logic bug (see
    improvement-tracking "Remaining work").

### 35. CMS156 E-13 fix + new E-18 engine issue (`FHIR.dateTime` in `sort`/mixed-`Interval`) — KEPT, VERIFIED (45→41 MR; 41 residual MM all non-E-13/E-18)

- **Context**: CMS156 was 177/177 "Missing Results" — E-13 Stage 3.
- **Files**: `input/cql/CMS156FHIRHighRiskMedsElderly.cql`.
- **E-13 fix**: two Denominator-Numerator-adjacent defines built a
  `ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis` union → base
  `[FHIR.Condition: ...]` retrieve: `"Schizophrenia Diagnosis"` and `"Seizure Disorder Diagnosis"`.
  `// [E-13]` + `// Original:` comments kept; not a logic change.
- **E-18 (new engine issue, `defect-tracking/engine-issues.md` E-18)**: after E-13, the residual
  Missing Results surfaced a raw-`FHIR.dateTime` failure family — `"Antipsychotic Index Prescription
  Start Date"` / `"Benzodiazepine Index Prescription Start Date"` (lines 230-243) `return
  …Medication.authoredOn` (raw `FHIR.dateTime`). Two consumers threw
  `"Values FHIR.dateTime and FHIR.dateTime are not comparable"` at runtime, aborting the library:
  1. the `sort asc` over the 2-medication source list, and 2. Numerator 2's mixed-type interval
  `Interval[start of "Measurement Period" - 1 year, <Index Prescription Start Date>]` (lines 168/178).
  **Fix (applied 2026-08-31)**: `FHIRHelpers.ToDateTime(...)` in both Index defines — keeps full
  timestamp precision and makes `sort`/interval endpoints homogeneous `System.DateTime`. This is the
  post-E-13 reappearance of the E-01/E-02 `FHIR.dateTime` family. Repro/unit-test:
  `input/cql/testE18DateTimeCompare.cql` + registration + fixture (see "Unit-test (repro)" convention).
- **Status**: **VERIFIED** (`discrepancy_report-20260831-0853-add-cms149.md`) — **CMS156 177 MR → 136
  passing / 41 mismatched** (mismatches were 45 pre-E-18, now 41; E-18 pending full harness re-run).
  **None of the 41 residual MM flow through the E-13-edited Schizophrenia/Seizure defines nor the
  E-18 `ToDateTime` fix** — the E-13/E-18 mechanisms are closed:
  - 39 rows (13 cases × 3 population groups) = Denominator **Exclusion** `1→0` via Hospice/Palliative
    profile retrieves (`183921001` Encounter, `305824005` Encounter, `170935008`/`441874000`
    Conditions, `45755-6`/`98181-1`/`71007-9` Observations, ServiceRequest/Procedure hospice,
    Encounter `dischargeDisposition` 428371000124100) — class-B profile-retrieve gaps.
  - 2 rows (case 4aa75d19, Numerators 1 & 3) = `1→0` via `[MedicationRequest: "Digoxin Medications"]`
    + `moreThanOneOrder()`/`averageDailyDose()` fluent dose logic (2 digoxin 0.25mg orders,
    authoredOn 2026-01-01 & 01-04) — class-B MedicationRequest/dose-logic gap, data present/correct.

### 36. CMS149 port closed — fully passing (moved out of content-gap list; `#26` KEPT, VERIFIED)

- **Context**: CMS149 (Dementia Cognitive Assessment) had no CQL authored in UQC (#26 port target,
  listed in improvement-tracking and change-classification as a content gap).
- **Why / Fix**: CMS149's CQL was ported QC→UQC (see #26). Now full CQL present.
- **Status**: **VERIFIED** (`discrepancy_report-20260831-0853-add-cms149.md`) — **CMS149 33 MR → fully
  passing (0 MR / 0 MM)**. Appears in the report's "Measures with No Discrepancies (16)" list
  (`CMS149FHIRDementiaCognitiveAssess`). Removed from the "no CQL authored" content-gap bucket in
  improvement-tracking.md / change-classification.md.

### 37. Automated patient-reference fix via `validate_test_fixtures.py` — 229 fixtures across 10 measures — KEPT, verified

- **Files**: 229 fixture JSON files across 10 measures (`CMS72FHIRSTKAntithromboticDay2` ×100,
  `CMS104FHIRSTKDCAntithrombotic` ×73, `CMS347FHIRStatinPreventionTxCVD` ×44,
  `CMS108FHIRVTEProphylaxis` ×3, `CMS871FHIRHHHyper` ×3, `CMS71FHIRSTKAnticoagAFFlutter` ×2,
  `CMS190FHIRVTEProphylaxisICU` ×2, `NHSNGlycemicControlHypoglycemiaInitialPopulation` ×1,
  `CMS1264FHIRECATREHQR` ×1, `CMS1028FHIRPCSevereOBComps` ×1) + the introducing script
  (`scripts/validate_test_fixtures.py`) + its test (`scripts/tests/test_validate_test_fixtures.py`).
- **Why**: same class of data-authoring bug as entries #7 and #11 — a resource's patient-identity
  reference (`subject`, `patient`, or `beneficiary`) pointed at a different patient GUID than the
  one whose test-case folder it lives in. Entries #7 and #11 fixed these manually (40 + 188 files);
  this commit fixed the remaining ~229 files via the `validate_test_fixtures.py --fix --apply`
  automated fix, which rewrites only the CORE patient-identity fields (`subject`, `patient`,
  `beneficiary`). The script also resolved two structural anomalies: a `null-null.json` misnamed
  Patient file in CMS871 (renamed to `Patient-{guid}.json` with `id` injected) and a
  `Claim-ClaimBehavior-NotSubsAbuse.json` oddly-named Claim in CMS1264 whose `patient.reference`
  was a placeholder GUID.
- **Fix**: automated — `validate_test_fixtures.py --fix --apply` (commit `fe41c7bcc` 2026-09-01
  14:16). Every `subject.reference` / `patient.reference` / `beneficiary.reference` pointing at a
  non-matching Patient GUID was rewritten to `Patient/<its own folder's GUID>`. The script was
  introduced in the same commit as a regression guard for this class of bug.
- **Verified** (`discrepancy_report-20260901-1428-after-resources-update.md`, 2026-09-01): pass
  82.51% → **93.58%** (+11.07 pts suite-wide); Missing Results 588 → 9; measures with no
  discrepancies 16 → 19. Key per-measure deltas: CMS72 98 → 13, CMS104 69 → 15, CMS871,
  NHSNAcuteCareHospitalMonthlyInitialPopulation1, NHSNGlycemicControl now fully green. CMS347
  134+4MR → 177MM (MR→MM conversion — resource fix exposed previously-invisible mismatches that
  were hidden under Missing Results).
- **Commit**: `fe41c7bcc` "fix resource references" (defect-tracking branch).

### 38. CMS130 DenExcl triage — corroborated class-B/E-17, then RESOLVED by the 2026-09-03 re-run

- **Context**: CMS130 (`CMS130FHIRColorectalCancerScrn`) showed 17 `Denominator Exclusion | 1 → 0`
  mismatches in the 2026-09-03 11:40 report (all pass on QICore except `f9ef1fd1`). Prior tracking
  mislabeled this "§5 — Numerator mismatch"; the failures were never triaged.
- **Triage (2026-09-04)**: every one of the 17 flowed through the shared, unedited
  `Hospice` / `PalliativeCare` / `AdvancedIllnessandFrailty` libraries — the same
  **class-B** systemic profile-retrieve family (`[USQualityCore.Encounter]` /
  `[USQualityCore.Procedure]` / `[USQualityCore.ServiceRequest]` / `[USQualityCore.DeviceRequest]` /
  `[ObservationScreeningAssessment]`) already catalogued for CMS128/56/131/156. Bucket breakdown:
  - **3 E-17** (`ObservationScreeningAssessment` profile-retrieve gap): `007ec5f1` (FACIT-Pal
    `71007-9` → Palliative), `0f930f59` (`45755-6` MDS → Hospice), `59128a5c` (`71802-3` Housing
    status → AIFrailLTCF).
  - **13 class-B** (Hospice/Palliative/AIFrailLTCF profile-retrieves): Hospice Condition `170935008`
    (`fede210f`, `02488708`), Procedure/ServiceRequest `385763009` (`46635c8a`, `b70f2fc0`),
    Encounter discharge-disposition `428361000124107` (`6dbaf3b3`) / `428371000124100` (`d0c9e870`),
    Encounter.type `305336008` (`6f6cdf8c`); PalliativeCare Condition `305686008` (`a989a58f`,
    `5fd0d61d`), Procedure `103735009` (`4e1abf20`), Encounter.type `305284002` (`7ee1a25c`);
    AIFrailLTCF frailty DeviceRequest `183240000` (`dcaccac3`, `df62e712`).
  - **1 content/anomaly (NOT class-B): `f9ef1fd1`** — sole trigger is rivastigmine `MedicationRequest`
    (RxNorm `312836`); QICore fails it too (see below).
- **RESOLUTION DISCOVERY**: while documenting, I detected that the discrepancy report on-disk had been
  regenerated (16:56) and the engine results re-run (09-03T20:56Z) under commit `5dc822c70` ("add global
  measurement period parameter" — a **global `Measurement Period`** added to `input/tests/config.json`).
  In that run **all 17 CMS130 DenExcl cases now evaluate `Denominator Exclusions = true`
  (expected 1)**, and CMS130 reports **`256 / 0` (no CMS discrepancies)**. The 13 class-B + 3 E-17 rows
  are therefore **resolved for CMS130** on the current engine/config. The exact clearing mechanism
  (global MP config vs terminology re-expansion vs re-run artifacts) is not yet isolated and should be
  confirmed before treating it as a general class-B/E-17 workaround — CMS56/CMS131/CMS128/CMS156 still
  exhibit the catalogue.
- **Only residual CMS130 row: `f9ef1fd1`** — **QICore-side** (QC actual 0 vs expected 1; CMS actual 1 =
  expected, so CMS passes fully). Hinges on whether RxNorm `312836` ∈ "Dementia Medications" VS
  (`2.16.840.1.113883.3.464.1003.196.12.1510`). Verify via CTS and re-check the QICore expectation
  before filing against either engine.
- **Status**: **DOCUMENTED** — engine-issues.md E-17 + Class B catalog updated to record the
  corroboration followed by resolution; improvement-tracking.md CMS130 row/markers updated to `RESOLVED`
  with the residual `f9ef1fd1` flagged QICore-side. No CQL or fixture changes were made.

### 39. Task resources missing the `for` patient reference — 26 fixtures across 6 measures injected, KEPT, VERIFIED

- **Context**: #37 fixed `subject.reference` / `patient.reference` / `beneficiary.reference` wrong-
  patient refs but never audited the separate **`for`** field that Task resources use to point at
  their patient. A scan found **26 of 47 Task resources** (all `us-quality-core-taskrejected`) with
  no `for` property at all — CMS104 ×2, CMS108 ×8, CMS138 ×1, CMS190 ×7, CMS71 ×6, CMS72 ×2.
- **Mechanism**: the engine's `BaseRetrieveProvider.filterByContext` (clinical-reasoning) filters
  every retrieve against the model's patient `contextPath` (`for` for Task) via FHIRPath; a missing
  `for` evaluates empty, so the resource is **silently dropped** from `[Task...]` retrieves. The old
  wrong-reference detector could never catch this — it only flag rewrote non-matching refs, and
  there were zero refs pointing at the wrong patient.
- **Fix (2026-09-10)**: extended `scripts/validate_test_fixtures.py` with
  `REQUIRED_PATIENT_FIELDS = {"Task": ("for",)}`, a `MISSING-REQUIRED-FIELD` finding category in
  `validate()`, `collect_task_for_findings()`, `apply_task_for_fix()` (injects `for` after
  `focus`/`code`), and the `--fix-task-for [--apply]` CLI flag. Applied → `for` injected into **26 of
  26**; re-run → 0 findings / 0 anomalies. Regression tests added in
  `scripts/tests/test_validate_test_fixtures.py` (`RequiredPatientFieldTest`, 9 cases); full suite
  298 passed, `check_generated.py` exit 0. Changes unstaged (no commit per policy).
- **Verified** (`discrepancy_report-20260910-0039.md`, 2026-09-10T00:39): pass **3598 → 3622**
  (90.77% → **91.37%**), fail 366 → 342. All +24 recovered cases trace to the Task measures:
  CMS108 −8, CMS190 −8, CMS71 −4, CMS104 −2, CMS72 −2 (CMS138's 1 Task did not flip its case).
  Case-level diff confirmed the exact recovered GUIDs; the 38 residuals in those measures are all
  pre-catalogued (E-17/E-21 engine profile-retrieve, B-01 baseline, C-14 content) — tracked next.
- **Status**: **KEPT, VERIFIED**.

## Not yet started
- The remaining ~30 measures with measure-specific mismatches, one at a time, top-down by fail
  count from the latest discrepancy report (`discrepancy_report-measure-fixes-20260823-1455.md`).
  Current order: `CMS157FHIRPainIntensityQuantified` (19 mismatches) →
  `CMS69FHIRPCSBMIScreenAndFollowUp` (18) → `CMS108FHIRVTEProphylaxis` (14) → `CMS816FHIRHHHypo`
  (12) → `CMS190FHIRVTEProphylaxisICU` (11) → `CMS72FHIRSTKAntithromboticDay2` (9) →
  `CMS996FHIRAptTxforSTEMI` (8) → `CMS104FHIRSTKDCAntithrombotic` (7). Before the CQL pass,
  finish entry #14's fixture follow-up first: the 21 remaining `Claim.item` linkage gaps target
  exactly the residual buckets of CMS108 (12), CMS190 (7), CMS104 (1), CMS1017 (1).
  CMS347's parked issues (see #10) are down to 2 mismatches — pick back up if convenient.
