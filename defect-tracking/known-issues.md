# Known Issues

Every issue the test suite has surfaced so far, with its root-cause class and
current status. **This file is maintained by hand — edit it freely.** Nothing
generates it and no script reads it, so a bad edit cannot break the reporting.

The file is in two parts. **Open issues** is the actionable list. **Resolved —
reference patterns** keeps the closed issues around for a different reason: a
resolved issue is a catalogued *failure fingerprint*, and several of the open
ones were first misdiagnosed as something already sitting in that list.

If you fix something, move its row into **Resolved — reference patterns** under
the matching family, reduce it to a one-line fingerprint plus the fix, and cite
the `I-` id in your `change-log.md` entry. If you find something new, add a row
to **Open issues** with the next free `I-` id.

Issue ids also appear in CQL comments next to the affected logic, e.g.
`// [I-28] base FHIR.Condition retrieve (defect-tracking/known-issues.md)`.

## Root-cause classes

| Class | Means |
|---|---|
| `engine` | the CQL runtime engine misbehaves at evaluation time |
| `translator` | `cql-to-elm` misbehaves at compile time |
| `content` | measure authoring gap — no CQL written, wrong valueset |
| `migration` | a bug introduced converting QI-Core → US Quality Core |
| `fixture` | bad or incomplete test data |
| `vendored` | a bug in an upstream-authored CQL library vendored here |
| `implementation` | the measure steward's test data or content misuses FHIR/CQL semantics; present in both QI-Core and CMS, not a parity defect, fixed by the steward |
| `harness` | a limitation of the comparison harness, not of the content |
| `baseline` | the QI-Core baseline `qicore-2025-actual-results.csv` disagrees with the fixture MeasureReports while the CMS engine matches |

## Statuses

| Status | Means |
|---|---|
| `Open — confirmed` | reproduced, root cause understood |
| `Open — suspected` | reproduced, root cause still a hypothesis |
| `Open — needs re-confirmation` | previously confirmed, but the evidence no longer matches any failing cell |
| `Worked around` | mitigated in CQL; the underlying defect is still live |
| `Fix applied — unverified` | fix is in the repo but no engine re-run has confirmed the cells recover |
| `Fixed` | fix applied **and** verified against engine output |
| `Retired` | withdrawn, superseded, or never assigned |

Do not promote anything to `Fixed` without confirming the fix is actually
present in the repo and that the engine agrees — I-44 sat at `Fixed` for weeks
while the valueset it needed had never been committed.

## Open issues (44)

| ID | Issue | Class | Status | Measures |
|---|---|---|---|---|
| I-01 | QI-Core baseline disagrees with fixture MeasureReports on 670 cases / 26 measures; CMS engine matches, baseline is fresh | `baseline` | Open — confirmed | 26 measures |
| I-04 | CMS986 malnutrition Measure-Observation component rows | `content` | Open — confirmed | CMS986 |
| I-05 | CMS1017 fall-prevention HHFI Denominator/Numerator/Measure-Observation rows | `content` | Open — confirmed | CMS1017 |
| I-08 | CMS871 HH Hyperglycemia fixture MR/Denominator authoring mismatch. **Needs re-confirmation (2026-09-18):** after the I-44 fix CMS871 has 10 failing cells, all `MISSING` and all attributable to I-63; no currently-failing cell matches this authoring-mismatch symptom | `content` | Open — needs re-confirmation | CMS871 |
| I-10 | CMS819 HH Opioid-Related Adverse Events fixture MR authoring mismatch | `content` | Open — confirmed | CMS819 |
| I-11 | CMS159 Depression Remission fixture MR authoring mismatch | `content` | Open — confirmed | CMS159 |
| I-12 | CMS0334 Cesarean Birth fixture MR authoring mismatch | `content` | Open — confirmed | CMS0334 |
| I-13 | CMS1218 HH Respiratory Failure fixture MR authoring mismatch | `content` | Open — confirmed | CMS1218 |
| I-14 | CMSFHIR844 fixture MR hand-authors Initial Population; both engines compute 0/1 against an expected 1/2 | `content` | Open — confirmed | CMSFHIR844 |
| I-15 | CMS72 / CMS104 / CMS646 / CMS71 fixture MRs author Denominator-Exception / Denominator / Numerator / IP counts that neither engine can compute from the resources present | `content` | Open — confirmed | CMS1028, CMS104, CMS646 +3 more |
| I-16 | `Min()` over DateTime throws | `engine` | Worked around | CMS1173, CMS871 |
| I-17 | Raw `FHIR.dateTime` / choice-typed `X.effective` in temporal operators fails | `engine` | Worked around | CMS1173 |
| I-18 | Fluent overload ambiguity (sibling profiles, same Java class); upstream [cqframework/clinical_quality_language#1855](https://github.com/cqframework/clinical_quality_language/issues/1855) | `translator` | Worked around | CMS104, CMS108, CMS144 +3 more |
| I-19 | Choice-type self-reference circular dispatch | `engine` | Worked around | CMS133, CMS142, CMS143 +5 more |
| I-20 | Sibling overloads ambiguous at runtime (same Java class) | `engine` | Worked around | CMS133, CMS142, CMS143 +6 more |
| I-21 | `as` cannot widen Choice to ancestor type | `engine` | Worked around | — |
| I-22 | `convert Duration to days` returns null | `engine` | Worked around | CMS128, CMS156 |
| I-23 | `ConvertQuantity` rejects calendar-word units from `ToQuantity` | `engine` | Worked around | CMS156 |
| I-24 | Quantity division across dimensions rounds to zero (`mg / d` normalizes to g/s and rounds to `0E-8`). Workaround applied to CMS156 `averageDailyDose()` 2026-10-02 (see `change-log.md`); upstream [cqframework/clinical_quality_language#1690](https://github.com/cqframework/clinical_quality_language/issues/1690) (engine returns UCUM canonical base units; spec question HL7 Jira FHIR-58137) | `engine` | Worked around | CMS156 |
| I-25 | `singleton from empty list` throws instead of returning null | `engine` | Worked around | CMS156 |
| I-26 | `Unable to extract codes from fhirType Reference` | `engine` | Open — confirmed | CMS135, CMS165 |
| I-27 | Union branch evaluates empty despite correct data | `engine` | Open — confirmed | CMS104, CMS108, CMS190 |
| I-28 | Sibling-profile Condition union fed to `prevalenceInterval(Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>)` will not resolve — `FHIRCommon.cql` declares only the base `FHIR.Condition` overload and the translator cannot widen a Choice | `engine` | Worked around | CMS1154, CMS1157, CMS117 +29 more |
| I-29 | `PCMaternal.cql` cast type change (`.value as DateTime` → `.value as FHIR.dateTime`) | `engine` | Open — suspected | CMS0334, CMS1028 |
| I-31 | Singleton-source query skips `return` when its source is null, so `FHIRCommon.prevalenceInterval()` returns null for a non-active or no-status Condition with no abatement end | `engine` | Worked around (CMS108, CMS71) | CMS108, CMS1154, CMS71 |
| I-32 | `us-quality-core-*` profile retrieves return empty (broader than ObservationScreeningAssessment alone) | `engine` | Open — confirmed | CMS108, CMS131, CMS190, CMS56 |
| I-33 | Raw `FHIR.dateTime` returned from a define breaks `sort` and a mixed-type `Interval` endpoint — `"Values FHIR.dateTime and FHIR.dateTime are not comparable"` (CMS156 Index Prescription Start Date; the post-I-28 reappearance of the I-16/I-17 family) | `engine` | Worked around | CMS156 |
| I-34 | `doNotPerform` negative-indication `MedicationRequest`s counted as positive orders by CMS347's statin logic | `engine` | Open — confirmed | CMS347 |
| I-36 | `us-quality-core-*` profile retrieves return empty across the screening-assessment, observation, medication and procedure profile families (I-32 extended; corroborated on CMS135 ACEI/ARB HF, CMS144 HFrEF beta-blocker, CMS771 urinary-symptom, CMS645 CAD-bone-density) | `engine` | Open — confirmed | CMS135, CMS144, CMS145 +2 more |
| I-37 | Ambiguous `recorded(...)` overload in `USQualityCoreCommon` throws, aborting CMS68 test case `f2e2e1c0` across all 4 populations (Missing Results). Overload removed and call sites bypassed 2026-10-02 (see `change-log.md`); upstream [cqframework/clinical_quality_language#1855](https://github.com/cqframework/clinical_quality_language/issues/1855) | `translator` | Worked around | CMS68, CMS190 |
| I-38 | QI-Core engine-side regressions surfaced by 2026-09-05 fresh re-run | `engine` | Open — confirmed | CMS1028, CMS108, CMS129 +10 more |
| I-39 | `[CommunicationNotDone: category in ...]` retrieve returns no resources when the category-bearing resource has `subject: null` | `engine` | Open — suspected | CMS142 |
| I-45 | Sparse `MedicationRequest` dosage fixtures trip `singleton from empty list` | `fixture` | Worked around | CMS156 |
| I-52 | `doNotPerform` not excluded from MedicationRequest/ServiceRequest retrieves | `migration` | Worked around | CMS104, CMS135, CMS144 +3 more |
| I-55 | Field swapped `.recorded` → `.effective`/`.performed` to dodge a translator ambiguity | `migration` | Worked around | CMS108, CMS190, CMS68, CMS996, CMS646 |
| I-56 | `AHAOverall.cql` Choice narrowing dropped `ConditionProblemsHealthConcerns` support (CMS144) | `migration` | Open — confirmed | CMS144 |
| I-57 | Vendored `CMD.cql` `convert…to days` null / calendar-unit bug (medication dispense side) | `vendored` | Worked around | CMS128 |
| I-58 | Vendored `CumulativeMedicationDuration` 6.0.000 model adaptation (CMS156) | `vendored` | Worked around | CMS156 |
| I-61 | Translator's ChoiceType compatibility check bypasses a registered FHIRHelpers conversion when a union produces divergent tuple-element types, leaving raw `FHIR.instant` `INRLabTest.issued` unconverted (CMS108/CMS190 INR Low Risk Indicator); corroborated on CMS986's bare-value `union` of raw `authoredOn` with an already-converted `System.DateTime` branch (Hospice/Dietitian Referral defines) | `translator` | Worked around | CMS108, CMS190, CMS986 |
| I-62 | Comparison harness cannot invoke per-member `cqfm-aggregateMethod` measure-observations for ratio / continuous-variable measures, so those cells are excluded from automated scoring | `harness` | Worked around | CMS1017, CMS871, CMS986 |
| I-63 | `Invalid Interval - the ending boundary (0) must be greater than or equal to the starting boundary (1).` aborts CMS871 cases `98533ccd` and `fd579f44`, 10 cells (both, all 5 populations MISSING). An integer `[1, 0]` interval, suggesting a range built over an empty list; root cause not yet traced and the error appears nowhere else in `input/tests/results/`. `fd579f44` only surfaced once I-44 was fixed — the ValueSet error had been masking it | `engine` | Open — confirmed | CMS871 |
| I-67 | CMS347 `1ba7b147`: the steward's test case uses a start-only `abatementPeriod` to mean "resolved 2025-12-31". FHIRCommon correctly reads the missing `end` as open-ended, so `prevalenceInterval()` runs to end of time and `"Has ESRD Diagnosis"` fires (`Group_4:Denominator Exception` 0→1). QI-Core returns 0 only by accident (I-69, plus I-68). Malformed resource, confirmed with SME 2026-10-08: **not a parity defect**. The steward should use `abatementDateTime` or add an `end`. No CQL change | `implementation` | Open — confirmed | CMS347 |
| I-68 | Interval selector returns `null` when both bounds evaluate to null (`Interval[null, null)`), instead of an interval with unknown bounds (`IntervalEvaluator.kt`, acknowledged by a TODO); upstream [cqframework/clinical_quality_language#1850](https://github.com/cqframework/clinical_quality_language/issues/1850) (open). No result difference for `overlaps`. A start-sensitive consumer (`starts before`) loses rows the spec would keep. No affected case found yet | `engine` | Open — confirmed | CMS347 |
| I-69 | `is Interval<T>` returns `false` when the interval has a null boundary (engine v5 `IsEvaluator`), so QICoreCommon's `abatementInterval()` / `toInterval()` return `null` for open-ended Periods. The same check nulls an open-ended interval passed to a `Choice`-typed function parameter (the call-site `As` delegates to `IsEvaluator`), which is why QICoreCommon's `toInterval()` sees `choice` = null. QI-Core side only in practice; FHIRCommon tests `is FHIR.Period` and isn't affected (one latent CMS exposure, NHSN `"Normalize Interval"`, no fixture hits). Fixed upstream in [cqframework/clinical_quality_language#1857](https://github.com/cqframework/clinical_quality_language/pull/1857) (`fb8055df`, merged 2026-10-08, not yet released); details in the outer repo's `defect-tracking/tickets/T-is-interval-null-bound.md` | `engine` | Open — confirmed | CMS347, CMS646 |
| I-70 | CMS646 `ab48e0c0`: the test expects Numerator 0 because "BCG [was] given before staging results back", but the CQL measures from the start of the staging Procedure, and the BCG dose and staging Procedure both start 2026-04-02T08:00, so `"First BCG Administered"` qualifies (`Group_1:Numerator` expected 0, CMS 1). QI-Core returns 0 only by accident (I-69: the start-only `onsetPeriod` nulls `onset.toInterval()`, so `"Bladder Cancer Diagnosis"` is empty); QI-Core on engine 4.9.0 also returns 1. Test expectation the logic cannot produce: **not a parity defect**. The steward should move the BCG before the staging start or expect Numerator 1. No CQL change | `implementation` | Open — confirmed | CMS646 |
| I-71 | CMS996 `f6c7dbc1`: the test expects Denominator Exclusion 0 because the thrombolytic allergy "starts before and ends before ED", but it marks the end with `onsetPeriod.end` on an allergy that is still `active` with no `allergyintolerance-abatement`. `onset[x]` only says when the allergy began, so FHIRCommon `prevalenceInterval()` correctly runs to end of time and overlaps the ED encounter (`Group_1:Denominator Exclusion` expected 0, CMS 1). QI-Core returns 0 because its CQL still reads `onset.toInterval()`; CMS uses `prevalenceInterval()` by deliberate upstream change `6cccf5ce`. Test data that doesn't express its intent: **not a parity defect**. The steward should mark the allergy resolved with an abatement, or expect Denominator Exclusion 1. No CQL change | `implementation` | Open — confirmed | CMS996 |
| I-72 | CMS145 `1f70822b`: the test expects no `Group_2:Denominator Exception` because the patient-reason beta-blocker not-ordered falls on the first visit, but `"Has Medical or Patient Reason for Not Ordering Beta Blocker Therapy"` accepts the reason at any `"Qualifying CAD Encounter and Prior MI"`, and both visits qualify (expected 0, CMS 1). Twin case `b19af44d`, with the reason at the last visit, expects 1. QI-Core returns 0 only by accident (I-38: it scores the patient out of every population); the CQL is the same in both versions. Test case and logic disagree: **not a parity defect**. The steward should change the expectation, or the CQL in both versions. No CQL change | `implementation` | Open — confirmed | CMS145 |

## Resolved — reference patterns

Kept for pattern-matching, not for tracking: if an undiagnosed failure looks
like one of these, start there. Each bullet names the concrete field, URI or
operator involved, because that — not the measure name — is what transfers.
The fix itself is recorded in `change-log.md`.

### Profile URI mismatch empties a retrieve

- **I-40** `fixture` — 2,679 fixtures kept `meta.profile` and extension URLs on
  `.../guides/onc/us-quality-core/...` after the IG renamed the namespace to
  `astp`, so profile-typed retrieves matched nothing. Fixed by a bulk
  `onc` → `astp` replacement.
- **I-47** `fixture` — 16 CMS69 BMI Observations (LOINC `39156-5`) and one
  pregnancy-status Observation (`82810-3` / SCT `77386006`) were profiled
  `us-quality-core-observation-screening-assessment`, so `[USCore.BMIProfile]`
  and `[USCore.ObservationPregnancyStatusProfile]` retrieved nothing. Fixed by
  re-attributing `meta.profile` to `us-core-bmi` /
  `us-core-observation-pregnancystatus`.
  - **Latent, not yet fixed:** 4 CMS69 Observations still carry a malformed
    `us-quality-core-observationcancelled` profile (missing hyphen).
- **I-48** `fixture` — same shape on CMS165/CMS135: blood-pressure panels
  (LOINC `85354-9` / `8480-6` / `8462-4`) and an `82810-3` pregnancy
  Observation carried `...observation-screening-assessment`, emptying
  `[USCore.BloodPressureProfile]`. Fixed by re-attributing to
  `us-core-blood-pressure` / `us-core-observation-pregnancystatus`.

### Broken or typo'd patient reference

A reference that does not resolve produces a silent zero, never an error — the
resource simply never attaches to the patient.

- **I-41** `fixture` — 40 `Condition` / `Encounter` / `Observation` /
  `MedicationRequest` / `Procedure` / `ServiceRequest` files had a typo'd
  Patient GUID in `subject.reference` (stray character, embedded double space).
- **I-42** `fixture` — 188 files' `Claim.patient` / `Coverage.beneficiary` /
  `AllergyIntolerance.patient` pointed at two fixed placeholder GUIDs left
  behind by a generation template; no such test cases ever existed.
- **I-46** `fixture` — 229 files' `subject` / `patient` / `beneficiary` pointed
  at the wrong Patient GUID, plus a `null-null.json` (CMS871) and a
  misnamed CMS1264 Claim. Repaired by `scripts/validate_test_fixtures.py`.
- **I-07** `fixture` — CMS816's 12 failing Initial Population/Denominator cases
  (2 also Numerator) were originally filed as a `content` "fixture MR hand-
  authors expected values that don't reproduce" mismatch. The real cause was
  narrower and mechanical: 17 of CMS816's 28 fixture `Encounter` resources
  (19 files) had **no `subject` element at all**, so `context Patient` scoping
  silently dropped them from `["Encounter": "Encounter Inpatient"]` regardless
  of how correctly type/status/period were authored — confirmed by direct CQL
  execution (`["Encounter": "Encounter Inpatient"]` returned empty *before* any
  `where` filter applied) and by a control check (every failing case's
  Encounter lacked `subject`; every passing case's had it). 5 of the 17
  patients hadn't produced a visible mismatch (expected value was `0`
  regardless), so the defect was wider than the 12 originally filed. Fixed by
  extending `scripts/validate_test_fixtures.py`'s required-field presence
  check from `Task.for` only to also cover `Encounter` / `MedicationAdministration`
  / `Observation` `subject` (`--fix-required-fields --apply`), which injected
  `subject: {"reference": "Patient/<folder guid>"}` into all 19 files.
  **Verified 2026-09-18**: CMS816FHIRHHHypo now passes all 28 test cases
  (84/84 population cells). See `change-log.md`.
- **I-51** `fixture` — CMS347 `1d3021bb` carried
  `Patient/1d3021bb-b593-4efc-af5b-3  20243bbe9b7` (two stray spaces splitting
  the GUID) on Encounter `9d311cdd` (CPT `99385`) and FACIT-Pal Observation
  `ea2c69a6` (LOINC `71007-9`), zeroing IP / Denominator / Denominator
  Exception.

### Non-canonical UCUM `system`

`https://ucum.org` instead of `http://unitsofmeasure.org`. Note the two very
different failure modes — a silent mismatch versus a hard throw that takes out
the whole library.

- **I-43** `fixture` — 3 Observations (CMS347 ×1, CMS69 ×2) used the wrong
  system URI; failed silently.
- **I-50** `fixture` — CMS347 `6da189af`'s LDL Observation `dce97708` (LOINC
  `13457-7`) threw
  `FHIRHelpers.ToQuantity.InvalidFHIRQuantity: Invalid FHIR Quantity code: mg/dL (https://ucum.org|mg/dL)`,
  killing the library and producing Missing Results across 20 cells.

### Terminology content missing or truncated

- **I-03** `content` — the committed valueset files held only the first page of
  a paged `$expand`, so `[Condition: "Cancer"]`
  (`2.16.840.1.113883.3.526.3.1010`) silently missed `C00.0` / `80914001`.
  42 of 45 expansions were repaired from the IG Publisher `txcache`.
  **Detection signature: `expansion.contains` shorter than
  `expansion.total`.** Note what is *not* the signature — the
  `expansion.parameter` `count: 1000 / offset: 0` block is ordinary VSAC
  `$expand` response metadata and appears on healthy files too.
  **Before "completing" any expansion, check for CPT codes:** a valueset
  containing `http://www.ama-assn.org/go/cpt` codes must never be committed
  with more than 1000 entries, an AMA licensing restriction. This is why I-03's
  repair was scoped to non-CPT valuesets.
- **I-06** `content` — **a misdiagnosis worth remembering.** CMS157's
  Initial Population of 0 was first attributed to a fixture `Encounter.type`
  mismatch against `"Office Visit"` / `"Audio Visual Telehealth Encounter"`.
  That conclusion was retracted: the real cause was I-03's empty `"Cancer"`
  expansion. A population that collapses to 0 looks like an attribution bug and
  is often a terminology bug.
- **I-44** `fixture` — **the "Fixed" status was a phantom.**
  `ValueSet-2.16.840.1.113762.1.4.1196.394` ("Hypoglycemics Treatment
  Medications") was recorded as fixed by "committing the external valueset
  source file", but the file had never been committed. The engine threw
  `Unable to locate ValueSet …1196.394` on every run and
  `Encounter with Hypoglycemic Medication` was `[]` for every patient.
  Committed from the IG Publisher `txcache` on 2026-09-18 (238 RxNorm codes,
  complete expansion, no CPT). **Verified:** CMS871 went from 20 failing cells
  to 10, cases `7507debb` and `35719b1a` now pass, and no other measure moved.
  Two lessons: confirm a `Fixed` status is actually present in the repo, and
  note that a hard terminology error **masks** everything downstream of it —
  fixing this one exposed I-63 on a third case that had looked like a pure
  I-44 failure.

### CQL authored against the wrong element or overload

- **I-53** `migration` — choice-typed `.effective` / `.performed` fed straight
  into a temporal operator with no `.toInterval()` conversion. Fixed on
  CMS72/CMS646.
- **I-54** `migration` — `.onset.toInterval()` used where
  `.prevalenceInterval()` was meant. `.onset.toInterval()` yields a zero-width
  point at onset, which can never `overlaps` a measurement period years later,
  so chronic / still-active checks never fire. Fixed at 16 sites by moving to a
  base `[FHIR.Condition: ...]` retrieve plus `prevalenceInterval()` (see I-28).
- **I-59** `migration` — CMS22's 6 `ServiceNotRequested` negation defines
  filtered base FHIR `.reasonCode in "Patient Declined"`, but US Quality Core
  carries the decline reason in the `us-quality-core-doNotPerformReason`
  extension, so the Denominator Exception under-fired. Fixed by switching to
  the `.reasonRefused()` accessor.
  - The 2 remaining CMS22 cases (`f9417a57`, `c41f9946`, filed under I-52) were
    a positive-retrieve over-fire. Fixed 2026-10-07 by qualifying the
    unqualified `[ServiceRequest: …]` retrieves as
    `[USQualityCore.ServiceRequest: …]`; no `doNotPerform` filter was needed.
- **I-60** `content` — CMS2's `"Denominator Exceptions"` was hardcoded to
  `false`, with its two `ObservationCancelled` negation defines commented out.
  Fixed by restoring them and reading the reason with the `notDoneReason()`
  fluent function instead of the `.notDoneReason` element. All 8 cases
  (`Group_1:Denominator Exception` 1→0) now pass and match QI-Core.

### Measurement-period / date-window mismatch

- **I-66** `fixture` — CMS177 `85e6225c` ("psychotherapy encounter before MP")
  had its Encounter dated 2026-02-11, inside the 2026 measurement period, and no
  `meta.profile`. The MADiE source has 2025-02-11 and `qicore-encounter`. CMS
  over-included it (Initial Population 0→1). QI-Core returned 0 only because its
  Encounter retrieve requires the profile. Fixed by restoring the 2025 date and
  setting `us-quality-core-encounter`. Not a CQL, translator or engine issue;
  the earlier I-36 attribution was wrong.
- **I-49** `fixture` — every CMS1264 fixture date sat in 2027/2028 while the
  measurement-period override was `@2026-01-01 .. @2027-01-01`, so 57 of 58
  cases evaluated 0. Note case `9bac5045` inverted instead (`exp=0, act=1`) —
  a uniform date offset can push a case *into* a population as easily as out.
  Fixed by re-keying 653 date tokens back one year and setting the CQL default
  measurement period from `@2027` to `@2026`.

### No CQL authored

- **I-02** `content` — CMS145 and CMS149 had no CQL at all; not a conversion or
  engine defect. Fixed by porting the libraries from QI-Core.

### Retired or superseded ids

- **I-09** `content`, retired 2026-09-10 — **another misdiagnosis.** CMS142's 5
  cases with `Group_1:Denominator Exception` = 0 were filed as fixture MR
  authoring. Expected values and fixture data were both correct; the
  category-bearing `CommunicationNotDone` has `subject: null`, so
  `[CommunicationNotDone: category in ...]` returns `[]`. All 5 cases moved to
  I-39.
- **I-30** `engine`, retired 2026-08-29 — duplicate of I-28 (same missing
  `FHIRCommon` Choice overload for `prevalenceInterval`). Rolled into I-28 and
  the `[I-30]` CQL comments renamed.
- **I-35** — never assigned. The number was skipped, and the slot is kept as a
  permanent gap rather than renumbered into.
