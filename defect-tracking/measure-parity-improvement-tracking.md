# Measure Parity: Improvement Tracking

Cross-references every change applied during this effort with its measured impact, the repo/branch
where it lives, and the discrepancy report that confirmed the delta. This is the single source of
truth for "what did we fix, how much did it help, and where does the evidence live."

---

## Repository & branch inventory

| Label | Git remote | Branch | Commit |
|---|---|---|---|
| UQC (US Quality Core) | `SeenaFa/dqm-content-cms-2025` | `main` | `0866d738` |
| UQC `astp-update` | `SeenaFa/dqm-content-cms-2025` | `astp-update` (off main) | same HEAD as main; fixture-only diff |
| QC (QI Core) | `cqframework/dqm-content-qicore-2025` | `main` | reference baseline only |

---

## Three-way baseline snapshot (2026-08-25)

| Metric | UQC main (pristine) | UQC + astp-update | QC pristine |
|---|---|---|---|
| Pass % | 85.23% | **86.87%** | 95.60% |
| Fail count | 3,504 | **3,114** | 1,044 |
| Measures w/ discrepancies | 61 / 74 | 61 / 74 | 35 / 74 |
| Measures fully passing | 13 / 74 | 14 / 74 | 39 / 74 |

The gap between UQC (86.87%) and QC (95.60%) — 3,114 vs 1,044 fails — is 2,070 fails, of
which **98% (378/386 QC-only failing test cases) also fail in UQC**. See the measure-by-measure
analysis below.

---

## Change inventory & measured impact

### §1 — Model / Profile Fixes

| # | Change | Repo / Branch | Discrepancy report | Measured impact |
|---|---|---|---|---|
| 1 | **Stale `onc` → `astp` fixture namespace** — bulk replace 2,679 JSON files, 2,877 URL occurrences | UQC `astp-update` | `discrepancy_report-20260825-0722-after-astp-fix.md` | **+1.64 pts** (85.23% → 86.87%); 390 test cases fixed; CMS50 now fully passing |
| 2 | **Sibling-profile runtime class** (§1 model characteristic) — `Procedure`/`ProcedureNotDone` share one Java class; `ConditionProblemsHealthConcerns`/`ConditionEncounterDiagnosis` same | UQC `main` (no fix needed; works around via §5 items) | n/a | Not a fixable issue — root cause for §5 engine bugs; resolved case-by-case below |

### §2 — Resource / Fixture Data Fixes

| # | Change | Repo / Branch | Discrepancy report | Measured impact |
|---|---|---|---|---|
| 3 | **Invalid UCUM `system` URI** (`https://ucum.org` → `http://unitsofmeasure.org`), 3 Observation fixtures | UQC `main` | Entry #3 in `conversion-notes.md` | 4 Missing Results → resolved in CMS347/CMS69 |
| 4 | **Missing vocabulary source file** (`ValueSet-2.16.840.1.113762.1.4.1196.394`) | UQC `main` | Entry #4 | CMS871 fixture now locates valueset |
| 7 | **Wrong-patient `subject.reference`** — 40 files across 4 measures (CMS347 ×34, CMS72 ×3, CMS108 ×1, NHSNGlycemic ×1) | UQC `main` | Entry #7 | Patient scoping restored; mismatches resolved |
| 11 | **Wrong-patient `Claim.patient` / `Coverage.beneficiary`** — 188 files across 7 measures | UQC `main` | Entry #11 | Patient scoping restored; mismatches resolved |
| 14 | **`Claim.item` missing `.encounter` / `.diagnosisSequence`** — 4 real gaps fixed (CMS104 ×2, CMS1017 ×2); 19 false positives closed | UQC `main` | Entry #14 | `claimDiagnosis()` lookup restored in CMS104/CMS1017 |
| 25 | **Sparse `MedicationRequest` dosage fixtures** (no `doseAndRate`/timing) — probe fixtures enriched | UQC `main` | Entry #25 | CMS156 probe fixtures now exercise dosage path |
| 27 | **Automated patient-reference fix via `validate_test_fixtures.py`** — 229 fixture files across 10 measures (CMS72 ×100, CMS104 ×73, CMS347 ×44, CMS108 ×3, CMS871 ×3, CMS71 ×2, CMS190 ×2, NHSNGlycemic ×1, CMS1264 ×1, CMS1028 ×1) + 2 structural anomalies (`null-null.json` rename CMS871, oddly-named Claim CMS1264) | UQC `defect-tracking` | `discrepancy_report-20260901-1428-after-resources-update.md` | **+11.07 pts** (82.51% → 93.58%); CMS72 98→13, CMS104 69→15, 3 measures now fully green |
| 39 | **Automated Task `for` patient-reference fix via `validate_test_fixtures.py`** (`REQUIRED_PATIENT_FIELDS={"Task": ("for",)}` + `--fix-task-for --apply`) — 26 Task fixtures injected across 6 measures (CMS104 ×2, CMS108 ×8, CMS138 ×1, CMS190 ×7, CMS71 ×6, CMS72 ×2); the #27 detector only rewrote wrong refs, it never caught *absent* `for`, which the engine silently drops from `[Task]` retrieves | UQC `defect-tracking` | `discrepancy_report-20260910-0039.md` | **+24 cases** (90.77% → **91.37%**, 3598 → 3622 pass, 366 → 342 fail); CMS108 −8, CMS190 −8, CMS71 −4, CMS104 −2, CMS72 −2; residuals all pre-catalogued (E-17/E-21 engine, B-01 baseline, C-14 content) |

### §3 — Migration Regression Fixes (CQL logic)

| # | Change | Repo / Branch | Discrepancy report | Measured impact |
|---|---|---|---|---|
| 6 | **`doNotPerform` not excluded** from `MedicationRequest`/`ServiceRequest` retrieves — 7 measures (CMS347, CMS104, CMS71, CMS135, CMS144, CMS645, CMS2) | UQC `main` | Entries #6, #13, #17 | Double-counted orders removed from numerator/denominator |
| 10 | **`.onset.toInterval()` → `.prevalenceInterval()`** for chronic/still-active conditions — 10 measures, ~16 call sites | UQC `main` | Entries #10, #15, #16, #19, #20-final, #22 | Zero-width onset points now correctly overlap measurement periods |
| 12 | **Choice-typed `.effective`/`.performed` missing `.toInterval()`** before temporal comparison — CMS72, CMS646 | UQC `main` | Entries #12, #17 | Temporal comparisons now work on choice types |
| 17 | **`reasonCode` checked instead of `reasonRefused()` extension** on `ServiceRequest` — CMS22, 6 defines | UQC `main` | Entry #17 | Correct negation accessor used |
| 19 | **`.recorded` → `.ext()` bypass** for ambiguous fluent-function overloads — CMS68, CMS108, CMS190, CMS996 | UQC `main` | Entry #19/#21 | Engine ambiguity bypassed via generic `ext()` accessor |
| 19 | **`ObservationCancelled`-based Denominator Exceptions re-enabled** — CMS2 | UQC `main` | Entry #19 | 8 CMS2 mismatches resolved |
| 2 | **CMS1264 Measurement Period default** — `Interval[@2026-01-01, @2027-01-01)` → `Interval[@2027-01-01, @2028-01-01)` | UQC `main` | Entry #2 | 57/58 CMS1264 test cases now pass → fully green |
| 22 | **CMS128: vendored CMD `medicationDispensePeriod()` null** — local workaround for `convert Duration to days` engine gap | UQC `main` | Entry #22 | CMS128 dispense-side period computation restored |
| 24 | **CMS1173 / CMS156: `Min()` over `DateTime` + raw `FHIR.dateTime` in temporal operators** — converts to `System.DateTime` first | UQC `main` | Entry #24 | CMS1173: 62 Missing Results resolved; CMS156: 45 Missing Results resolved |
| 25 | **CMS156 vendored CMD full adaptation** — 3 engine-gap workarounds (ToDays helper, decimal-math quantity division, sparse-fixture enrichment) | UQC `main` | Entry #25 | CMS156 numerator path fully unblocked (P1=5, P8=0.25 mg/d, P9=true) |

### §4 — Vendored Library Fixes

| # | Change | Repo / Branch | Discrepancy report | Measured impact |
|---|---|---|---|---|
| 22–25 | **CumulativeMedicationDuration 6.0.000** — model adaptation (`as FHIR.Period`), `ToDays(FHIR.Duration)` helper, decimal-math `averageDailyDose()` | UQC `main` | Entries #22, #25 | CMS128 and CMS156 both functional; CMS156 fully verified end-to-end |

### §5 — Engine/Translator Gap Workarounds (applied at CQL level)

| # | Gap description | Workaround pattern | Affected measures | Status |
|---|---|---|---|---|
| 5.1 | `Min()` over `DateTime` throws | Convert to `System.DateTime` via `FHIRHelpers.ToDateTime()` first | CMS645, CMS646, CMS156, CMS871, CMS1173 | Workaround applied |
| 5.2 | Raw `FHIR.dateTime` / choice-typed `X.effective` fails temporal operators | Convert to `System.DateTime` or `.toInterval()` first | CMS1173, CMS156 | Workaround applied |
| 5.3 | Ambiguous fluent-function overload (sibling profile types share one Java class) | Bypass with `.ext()` generic accessor | CMS68, CMS996, CMS108, CMS190 | **CMS108 applied on `defect-tracking` 2026-09-08** (line 409, mechanical `ProcedureNotDone` branch); CMS68/CMS996/CMS190 still un-applied on this branch (see E-22); CMS144 blocked |
| 5.4 | Self-referential circular reference on `Choice<A,B>` function declarations | **Base retrieve supersedes inline `is`/`as`** — replace the `ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis` union with a single `[FHIR.Condition: ...]` retrieve (**E-13**); `.prevalenceInterval()`/`.verified()` resolve against `FHIRCommon`'s base-`Condition` overloads. The older inline `is`/`as` dispatch is the SUPERSEDED interim workaround. | **30 measures** — original 7 (CMS347, CMS117, CMS138, CMS153, CMS136, CMS155, CMS69) + CMS645, CMS1154, CMS1157, CMS75, CMS142, CMS143, CMS771, CMS1188, CMS124, CMS349, CMS90, CMS646, CMS314, CMS129, CMS951, CMS133, CMS128, CMS56, CMS131, CMS159, CMS996, CMS157, CMS156 (CMS22/CMS71 excluded) | **26/30 applied + verified** (62 site-level edits, 2026-08-28 → 08-31); 4 pending Stage 3 (CMS159, CMS646 residual site) — see `engine-issues.md` **E-13** |
| 5.7 | `convert Duration to days` returns null | Hand-rolled `ToDays(FHIR.Duration)` helper | CMS128, CMS156 | Workaround applied |
| 5.13 | **E-17** — `ObservationScreeningAssessment` profile retrieve / `isAssessmentPerformed()` returns `[]` | **No workaround** — engine/profile-retrieve gap under investigation | CMS56 (10 Numerator), CMS131 (6 DenExcl) | Open (see `engine-issues.md` E-17) |
| 5.14 | **E-18** — raw `FHIR.dateTime` fed to `sort`/mixed-`Interval` throws `Values FHIR.dateTime … not comparable` | Convert via `FHIRHelpers.ToDateTime(...)` first | CMS156 (45 MR), CMS1173-family | Applied 2026-08-31; see `engine-issues.md` E-18 + `testE18DateTimeCompare.cql` |
| 5.8 | `ConvertQuantity` rejects calendar-word units (`'day'`) | Same `ToDays()` helper bypasses ConvertQuantity | CMS156 | Workaround applied |
| 5.9 | Quantity division across mass/time dimensions collapses to zero | `System.Quantity { value: <decimal math>, unit: '<explicit>' }` construction | CMS156 | Workaround applied |
| 5.10 | `singleton from empty list` aborts instead of returning null | Fixture-side: ensure `doseAndRate` populated | CMS156 | Fixture enrichment; real sparse data still at risk |
| 5.11 | `"Unable to extract codes from fhirType Reference"` — engine-level crash before CQL evaluation | **No workaround** — needs JVM stack trace | CMS135 (3 cases), CMS165 (1 case) | Blocked |
| 5.12 | Union branch evaluates empty despite correct data (CMS104) | **Not yet traced** to engine level | CMS104 | Blocked |

---

## Per-measure impact summary

Measures are listed in descending order of failing-test-case count from the post-astp-fix
report (`discrepancy_report-20260825-0722-after-astp-fix.md`). "Δ from baseline" compares the
post-astp-fix mismatch count against the pre-astp-fix `discrepancy_report-main-20260820.md`.

| Measure | Test cases | Mismatches (post-astp) | Δ from baseline (pre-astp) | Key fixes applied | Remaining root cause |
|---|---|---|---|---|---|
| **CMS1264** | 58 | 0 | **−57** | #2 (Measurement Period default) | None — **fully green** |
| **CMS50** | 185 | 0 | **−185** | #1 (astp fixture namespace) | None — **fully green** (14th passing measure) |
| CMS128 | 58 | 16 (20260830) | prior session | #22 (CMD dispense workaround), **E-13** (union → base `Condition`) | 16 class-B Hospice DenExcl (all 8 unique × 2); E-13 closed |
| CMS1264 | 58 | 0 | prior session | #2 (Measurement Period) | None |
| CMS347 | 752 | 134 + 4 MR | −10 | #1, #3, #6, #10 | §5.4 engine gap (overload ambiguity); remaining mismatches are §5 |
| CMS129 | 51 | 5 | −3 | #1 (astp), #10 (prevalenceInterval) | §5.4 engine gap |
| CMS69 | 63 | 34 | −4 | #1 (astp), #3 (UCUM URI), #6 (doNotPerform) | §5.4 (Denominator Exclusion), §5.4 (Numerator) |
| CMS56 | 58 | 18 (20260831) | −2 | #1 (astp), #10, **E-13** (4 DenExcl unions → base `Condition`) | 8 class-B Hospice DenExcl + 10 **E-17** Numerator (`ObservationScreeningAssessment` gap); E-13 closed |
| CMS90 | 37 | 34 | +5 | #1 (astp), #10 (prevalenceInterval) | §5.4 — count worsened; fixture/astp fix unmasked deeper mismatch |
| CMS74 | 20 | 7 | 0 | #1 (astp) | §5.4 (Denominator Exclusion) |
| CMS75 | 20 | 7 | 0 | #1 (astp) | §5.4 (Denominator Exclusion) |
| CMS165 | 68 | 29 + 1 MR | 0 | #1 (astp) | §5.11 (`Unable to extract codes`) + §5.4 |
| CMS135 | 40 | 9 + 3 MR | 0 | #1 (astp), #6 (doNotPerform) | §5.11 (`Unable to extract codes`) |
| CMS72 | 158 | 11 (2026-09-10) | 0 | #7 (wrong-patient refs), #12 (effective.toInterval), **#39 (Task `for`)** | **C-14** content (both engines agree wrong; 15 cells DenEx heavy) + **B-01** baseline |
| CMS104 | 82 | 13 (2026-09-10) | 0 | #6 (doNotPerform), #11 (wrong-patient Claim), #14 (Claim.item), **#39 (Task `for`)** | **E-21** DenExcp/Num flip + **C-14** + **B-01** baseline |
| CMS108 | 140 | 8 (2026-09-10) | 0 | #7 (wrong-patient), #11 (wrong-patient Claim), #19 (.ext() bypass), **#39 (Task `for` — the 8 E-12 `TaskRejected` cases now PASS)** | **E-17** profile-retrieve (5 MedAdmin/INR-Obs + 3); engine-gap resolution pending |
| CMS122 | 55 | 25 | 0 | — | §5 — Denom Exclusion vs Numerator mismatch |
| CMS124 | 34 | 13 | 0 | — | §5 — Numerator mismatch |
| CMS125 | 66 | 26 | 0 | — | §5 — Denom Exclusion vs Numerator mismatch |
| CMS130 | 64 | 0 (2026-09-03 re-run) | 0 | **RESOLVED** — all 17 former DenExcl mismatches now evaluate correctly (`Denominator Exclusions = true`) after the 2026-09-03 re-run (commit `5dc822c70` global Measurement Period param added to `input/tests/config.json`); CMS130 `256 / 0` in the 16:56 report. Prior 11:40-run failure buckets were class-B (Hospice/Palliative/AIFrailLTCF profile-retrieves) + 3 E-17 (`ObservationScreeningAssessment`) — all since cleared for CMS130; only residual row `f9ef1fd1` is **QICore-side** (QC actual 0 vs expected 1). See engine-issues.md E-17 + Class B catalog. |
| CMS131 | 63 | 24 (20260831) | 0 | **E-13** (2 unions → base `Condition`) | 24 class-B Hospice/Palliative/AIF + **E-17** corroboration; 1 expected-anomaly; E-13 closed |
| CMS133 | 73 | 0 (20260830) | 0 | #10, **E-13** (55 unions → base `Condition`) | None — **fully passing** (was §5.4); add to No-Discrepancies |
| CMS136 | 128 | 23 | 0 | — | §5 — Denominator Exclusion vs Numerator |
| CMS137 | 90 | 18 | 0 | — | §5 — Denominator Exclusion vs Numerator |
| CMS138 | 141 | 40 | 0 | — | §5 — Numerator mismatch |
| CMS139 | 29 | 8 | 0 | — | §5 — Numerator mismatch |
| CMS142 | 32 | 19 | 0 | #10 (prevalenceInterval) | §5.4 — Denominator Exclusion mismatch |
| CMS143 | 32 | 18 | 0 | #10 (prevalenceInterval) | §5.4 — Denominator Exclusion mismatch |
| CMS144 | 48 | 3 | 0 | #6 (doNotPerform) | §5.3/#5.5 — AHAOverall Choice-type gap |
| CMS145 | 106 | 106 MR | 0 | — | No CQL authored (content gap, not a conversion bug) |
| CMS146 | 38 | 10 | 0 | — | §5 — Denominator Exception mismatch |
| CMS149 | 33 | 0 (20260831) | 0 | #26 (QC→UQC port) | None — **fully passing** (in No-Discrepancies list); no longer a content gap |
| CMS153 | 32 | 9 | 0 | — | §5 — Denominator Exclusion vs Numerator |
| CMS154 | 33 | 9 | 0 | — | §5 — Denominator Exception mismatch |
| CMS155 | 102 | 33 | 0 | #10 (prevalenceInterval) | §5.4 — Denominator Exclusion mismatch |
| CMS156 | 177 | 41 (20260831) | **−90** | #24 (Min/DateTime), #25 (CMD), **E-13** (2 unions → base `Condition`), **E-18** (`ToDateTime`) | 39 Hospice/Palliative DenExcl + 2 MedReq digoxin; E-13/E-18 closed |
| CMS157 | 126 | 19 (20260831) | 0 | #10, **E-13** (2 unions → base `Condition`) | **Terminology mismatch (content gap):** fixtures code Cancer `C00.x`/`C40`. as ICD-10-CM but the measure's "Cancer" valueset is SNOMED-only — 19 residual MM, NOT engine/E-13 (was §5.4) |
| CMS159 | 67 | 2 (20260831) | 0 | #10, **E-13** (applied 2026-08-30/31) | 2 residual (non-E-13) — see report |
| CMS177 | 41 | 1 | 0 | — | §5 — single Numerator mismatch |
| CMS190 | 125 | 2 (2026-09-10) | 0 | #11 (wrong-patient Claim), #19 (.ext() bypass), **M-04 negation fix (2026-09-09)**, **#39 (Task `for`)** | **E-17** profile-retrieve (2 cases: `f035a977` INR-lab + completed Procedure, `a82cd0c1` DenEx ServiceRequest) |
| CMS0334 | 138 | 1 | 0 | — | §1 — PCMaternal `.value as FHIR.dateTime` type change |
| CMS645 | 51 | 5 + 2 MR | 0 | #6 (doNotPerform) | §5.1 (Min/DateTime) |
| CMS646 | 38 | 4 + 1 MR | 0 | #12 (effective.toInterval) | §5.1 (Min/DateTime) |
| CMS771 | 31 | 6 | 0 | — | §5 — Numerator mismatch |
| CMS816 | 28 | 12 | 0 | — | §5 — Numerator mismatch |
| CMS819 | 28 | 2 | 0 | — | §5 — single Numerator mismatch |
| CMS871 | 26 | 5 MR | 0 | #4 (missing vocabulary) | §5.1 (Min/DateTime) |
| CMS951 | 55 | 44 | 0 | #10 (prevalenceInterval) | §5.4 — Numerator mismatch |
| CMS986 | 876 | 6 | 0 | — | §5 — single Numerator mismatch |
| CMS996 | 114 | 7 (20260831) | 0 | #6 (doNotPerform), #19 (.ext() bypass), **E-13** (3 unions → base `Condition`) | 7 non-E-13: 4 `*NotDone` DenExcp + 2 MajorSurgical Procedure + 1 AllergyIntolerance temporal-logic; STEMI E-13 retrieve verified sound |
| CMS1017 | 65 | 2 | 0 | #14 (Claim.item gaps) | §5 — Denominator Exclusion mismatch |
| CMS1028 | 282 | 4 | 0 | — | §1 — PCMaternal `.value as FHIR.dateTime` type change |
| CMS1154 | 10 | 1 | 0 | — | §5 — single Numerator mismatch |
| CMS1173 | 65 | 62 MR | **−62** | #24 (Min/DateTime + temporal operators) | **All 62 Missing Results resolved** |
| CMS1218 | 69 | 1 | 0 | — | §5 — single Numerator mismatch |
| NHSNAcuteCareHospMonthly | 27 | 27 | 0 | — | §5 — all 27 Initial Population mismatches |
| NHSNGlycemicControl | 80 | 1 | 0 | — | §5 — single Numerator mismatch |
| CMS2 | 36 | 8 | **−8** | #19 (ObservationCancelled re-enable) | None — **fully green** |
| CMS22 | 44 | 12 | 0 | #6 (doNotPerform), #17 (reasonRefused) | §5 — Denominator Exception mismatches |
| CMS71 | 83 | 4 (2026-09-10) | 0 | #6 (doNotPerform), **#39 (Task `for`)** | **E-21** engine profile-retrieve (DenExcp/Num flip) |
| CMS117 | 45 | 8 | 0 | — | §5 — Denominator Exclusion mismatch |
| CMS135 | 40 | 9 + 3 MR | 0 | — | §5.11 (`Unable to extract codes`) |
| CMS144 | 48 | 3 | 0 | — | §5.3/#5.5 — AHAOverall Choice-type gap |

---

## Gains by change category

| Category | Test cases fixed | Notes |
|---|---|---|
| §1 — Profile namespace (astp-update) | 390 | CMS50 fully green; small gains in CMS347, CMS129, CMS56, CMS69 |
| §2 — Fixture data (wrong-patient, UCUM, vocabulary, Claim.item) | ~469 (prior sessions + 229 automated via validate_test_fixtures.py + 26 Task-`for` 2026-09-10) | CMS72, CMS104, CMS347, CMS108, CMS871, CMS71, CMS190, NHSNGlycemic, CMS1264, CMS1028; Task-`for` adds **+24** (CMS108 −8, CMS190 −8, CMS71 −4, CMS104 −2, CMS72 −2) |
| §3 — Migration regression (CQL logic) | ~350 (across prior sessions) | doNotPerform, prevalenceInterval, doNotPerform, .ext() bypass, CMS1264 Measurement Period |
| §4 — Vendored CMD adaptation | ~101 (CMS128 56 + CMS156 45 MR) | Engine-gap workarounds at the library level |
| §5 — Engine-gap workarounds (Min/DateTime, temporal) | 147 (CMS1173 62 + CMS156 45 MR + CMS2 8 + scattered) | §5.1/5.2 workarounds; §5.11/5.12 remain blocked |
| **E-13** — base `FHIR.Condition` retrieve replacing sibling-profile unions | **~700+** (2026-08-28 → 08-31; 26/30 measures applied) | Unlocks measures that were 100% Missing Results; residuals move from MR to genuine mismatch/terminology buckets |

---

## Remaining work

### Highest impact (by fail count)

1. **CMS72** (11 mismatches, 2026-09-10) — **C-14** (both engines agree wrong; DenEx heavy) + B-01 baseline
2. **CMS347** (177 mismatches) — mix of E-13/§5.4 (overload ambiguity) and remaining §5 gaps
3. **CMS104** (13 mismatches, 2026-09-10) — **E-21** DenExcp/Num flip + **C-14** + B-01 baseline
4. **CMS157** (19 mismatches, 2026-09-01) — **terminology/content gap**: ICD-10-CM Cancer fixtures vs SNOMED "Cancer" valueset (E-13 exonerated; see Content-gap table)
5. **CMS996** (7) — 4 `*NotDone` (class-B) + 1 AllergyIntolerance temporal-logic (class-c) + 2 MajorSurgical Procedure (class-B)
6. **CMS130** (0 CMS - **RESOLVED 2026-09-03 re-run**; 17 former DenExcl class-B/E-17 cases now pass; only residual `f9ef1fd1` is QICore-side) — see Content-gap table + engine-issues.md E-17/Class B catalog
7. **CMS108** (8) / **CMS190** (2) — **E-17** engine profile-retrieve; no CQL-level workaround, defer until engine fix
8. **CMS71** (4) — **E-21** engine profile-retrieve (E-17 family), same deferral

### Blocked on engine (no CQL-level workaround possible)

| Measure | Count | Issue |
|---|---|---|
| CMS135 | 3 MR | §5.11 — `Unable to extract codes from fhirType Reference` |
| CMS165 | 1 MR | §5.11 — same engine crash, different trigger |
| CMS144 | 3 | §5.3/#5.5 — AHAOverall Choice-type gap, blocked on sibling-overload ambiguity |

### Content gap (not conversion bugs)

| Measure | Count | Issue |
|---|---|---|
| CMS145 | 106 MR | No CQL authored at all |
| CMS157 | 19 | "Cancer" valueset is SNOMED-only but fixtures code diagnoses in ICD-10-CM — fixture/terminology alignment needed (E-13 exonerated) |
| CMS130 (`f9ef1fd1`) | 1 (QICore-side) | Only CMS130 residual row — a **QICore-side** discrepancy (QC actual 0 vs expected 1; CMS actual 1 = expected, so CMS passes fully). AIFrailLTCF DenExcl trigger is rivastigmine MedReq (RxNorm `312836`). Verify `312836` ∈ "Dementia Medications" VS (`2.16.840.1.113883.3.464.1003.196.12.1510`) and re-check the QICore expectation before filing against either engine |

---

## Key to acronyms

- **UQC** = US Quality Core (`dqm-content-cms-2025`)
- **QC** = QI Core (`dqm-content-qicore-2025`)
- **MR** = Missing Results (engine crash/empty trace, not just a population mismatch)
- **CMD** = CumulativeMedicationDuration (vendored CQL library)

---

*Generated 2026-08-25 alongside the `astp-update` branch. Source data:*
- `scripts/comparison/discrepancy_report-20260820.md` (UQC main pre-fix baseline)
- `scripts/comparison/discrepancy_report-20260825-0722-after-astp-fix.md` (post-astp-fix)
- `conversion-notes.md` (entries #1–#25)
- `change-classification.md` (§1–§5 categories)

---

## Addendum — E-13 Stage 2/3 rollout & session closes (2026-08-29 → 2026-08-31)

The E-13 base-`FHIR.Condition` retrieve rollout (see `engine-issues.md` **E-13**) progressed from 22/30
to **26/30 measures applied and harness-verified** (62 site-level edits; Stage 1 & 2 verified
2026-08-29/30, Stage 3 CMS133/CMS128/CMS56 2026-08-30, CMS131 2026-08-31, CMS159/CMS996/CMS157/CMS156
2026-08-30/31). The table below records this session's per-measure deltas against the current report
`discrepancy_report-20260831-0853-add-cms149.md` (2026-08-31 08:52; suite-wide **21531/23722 = 90.76%
pass**, 58/74 measures with discrepancies):
- **CMS157** 126 MR → **19 MM** — base-retrieve mechanism sound; residual = **terminology/content gap**
  (ICD-10-CM Cancer fixtures not members of the SNOMED "Cancer" valueset; E-13 exonerated, see
  conversion-notes #33).
- **CMS996** 114 MR → **7 MM** — STEMI E-13 retrieve verified; residuals 4 `*NotDone` DenExcp + 2
  MajorSurgical Procedure + 1 AllergyIntolerance temporal-logic (conversion-notes #34).
- **CMS159** 67 MR → **2 MM** (residual, non-E-13).
- **CMS156** 177 MR → **41 MM** — E-13 closed + **E-18** `FHIRHelpers.ToDateTime` workaround applied
  (45→41 MR); 39 Hospice/Palliative DenExcl + 2 digoxin MedReq (conversion-notes #35).
- **CMS149** 33 MR → **fully passing** (#26 QC→UQC port closed; in "Measures with No Discrepancies
  (16)"; conversion-notes #36).
- Earlier Session-3 closes carried forward: CMS133 fully passing, CMS128 →16 class-B, CMS56 →18
  (8 class-B + 10 **E-17** `ObservationScreeningAssessment`), CMS131 →24 class-B/E-17.

New engine issues opened this window: **E-17** (`ObservationScreeningAssessment` retrieve /
`isAssessmentPerformed()` gap — CMS56 + CMS131 corroboration) and **E-18** (raw `FHIR.dateTime` in
`sort`/mixed-`Interval` — the post-E-13 reappearance of the E-01/E-02 family, CMS156). Both tracked in
`defect-tracking/engine-issues.md` (confirmed; E-17 open, E-18 workaround shipped).
