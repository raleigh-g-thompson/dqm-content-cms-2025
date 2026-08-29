# Engine / Translator Issues Tracker

Running list of confirmed, suspected, and unverified engine/translator issues surfaced by the
QICore → USQualityCore migration test suite. These are issues in `clinical_quality_language`
(cql-engine, cql-to-elm) and/or `clinical-reasoning` (cqf-fhir-cql), **not** bugs in this
repo's CQL or test fixtures. Every confirmed issue has a reproducible symptom and a
currently-applied CQL-level workaround (where one exists); the workaround is a mitigation, not
proof the underlying behavior is correct.

Cross-referenced to `conversion-notes.md` entries (#N) and `change-classification.md` (§5).

---

## Summary

| ID | Issue | Status | Workaround | Affected measures |
|----|-------|--------|------------|-------------------|
| E-01 | `Min()` over DateTime throws | **Confirmed** | `FHIRHelpers.ToDateTime()` | CMS1173, CMS871 (post-E-13 survivors; CMS645, CMS646, CMS156 re-attributed to E-13) |
| E-02 | Raw `FHIR.dateTime` / choice-typed `X.effective` in temporal operators fails | **Confirmed** | `ToDateTime()` or `.toInterval()` | CMS1173 (post-E-13 survivor; CMS156 re-attributed to E-13) |
| E-03 | Fluent overload ambiguity (sibling profiles, same Java class) | **Confirmed** | `.ext()` bypass | CMS68, CMS996, CMS108, CMS190, CMS144 |
| E-04 | Choice-type self-reference circular dispatch | **Confirmed** | Inline `is`/`as` per call site | CMS90, CMS133, CMS142, CMS143, CMS155, CMS157, CMS159, CMS951 |
| E-05 | Sibling overloads ambiguous at runtime (same Java class) | **Confirmed** | Same as E-04 | CMS90, CMS133, CMS142, CMS143, CMS155, CMS157, CMS159, CMS951, CMS144 |
| E-06 | `as` cannot widen Choice to ancestor type | **Confirmed** | Same as E-04 | (same as E-04/E-05) |
| E-07 | `convert Duration to days` returns null | **Confirmed** | Hand-rolled `ToDays()` helper | CMS128, CMS156 |
| E-08 | `ConvertQuantity` rejects calendar-word units from `ToQuantity` | **Confirmed** | Same `ToDays()` helper | CMS156 |
| E-09 | Quantity division across dimensions rounds to zero | **Confirmed** | `System.Quantity` construction | CMS156 |
| E-10 | `singleton from empty list` throws instead of returning null | **Confirmed** | Fixture-side enrichment | CMS156 |
| E-11 | `Unable to extract codes from fhirType Reference` | **Confirmed** | **None — blocked** | CMS135, CMS165 |
| E-12 | Union branch evaluates empty despite correct data | **Confirmed** | **None — not traced** | CMS104 |
| E-13 | Union of `ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis` → `Choice<...>` fed to `prevalenceInterval()` mis-resolves: missing FHIRCommon Choice overload + translator cannot resolve the call (the Choice should coerce to base `Condition` — engine/translator issue) | **Confirmed / Applied** | Single `FHIR.Condition` retrieve replacing the union (38 site-level edits; 13 measures applied of 30; 17 pending Stages 2–3); inline `is`/`as` interim superseded | 30 measures total: original 7 (CMS347, CMS117, CMS138, CMS153, CMS136, CMS155, CMS69) + CMS645, CMS1154, CMS1157, CMS75, CMS142, CMS143, CMS771, CMS1188, CMS124, CMS349, CMS90, CMS646, CMS314, CMS129, CMS951, CMS128, CMS56, CMS131, CMS159, CMS133, CMS996, CMS157, CMS156 (CMS22/CMS71 excluded — non-mixed retrieves) |
| E-14 | `PCMaternal.cql` cast type change (`.value as DateTime` → `.value as FHIR.dateTime`) | **Suspected** | None — unverified | CMS0334, CMS1028 |
| ~~E-15~~ | ~~Union of `ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis` → `Choice<...>` fed to `prevalenceInterval()` mis-resolves on the new engine~~ | **RETIRED 2026-08-29 — all E-15 issues rolled into E-13** (see E-13; CQL comments updated from `[E-15]` to `[E-13]`) | — | — |
| E-16 | `overlaps` on a half-open null-high interval (`[start, null)`) evaluates false — `FHIRCommon.prevalenceInterval()` inactive branch | **Confirmed** | **None — engine runtime** (see E-16; deferred) | CMS1154 |

---

## Detailed Entries

### E-01: `Min()` over DateTime throws `"... not comparable"` / `"... not implemented"`

- **Symptom**: `Min({...})` over a homogeneous set of plain `DateTime` values throws at runtime.
- **Expected**: Per the CQL spec, `DateTime` is an explicitly supported `Min`/`Max` operand type.
- **Confirmed affected (current, after 2026-08-29 re-scope)**: CMS871, CMS1173. **CMS645, CMS646, and
  CMS156 were originally grouped here, but their Missing Results are the E-13 (union→`Choice`→
  `prevalenceInterval()`) translate failure** (see E-13); they are tracked under E-13 and re-checked
  after the Condition-replace fix is applied.
- **Workaround**: Convert operands to `System.DateTime` via `FHIRHelpers.ToDateTime(...)` before
  calling `Min()`. **Not yet applied to CMS1173 in the current tree** — earlier wording claiming
  "CMS1173 62 → 0 fully passing" was stale/incorrect; CMS1173 still shows **62 Missing Results** in
  the 2026-08-29 reports (`The Minimum operator is not implemented for type {http://hl7.org/fhir}dateTime`,
  no literal `Min()` in the measure — the operand is a raw `FHIR.dateTime`; see E-02 family). The
  "CMS645 attempt regressed" and "CMS156 45 → 0" notes are likewise superseded (CMS156 never loaded
  past E-13; CMS645's post-E-13 numerator 0→1 mismatches — d07cf359, 8c41481d, c5bfac21 — are the
  current E-01/`Min()` candidate under investigation).
- **Note**: This is a broader family than just `Min()` — see E-02.
- **References**: External issues log (original); #24.

### E-02: Raw `FHIR.dateTime` / choice-typed `X.effective` in temporal operators fails

- **Symptom**: Raw `FHIR.dateTime` values and raw choice-typed fields (`X.effective`) fed directly
  into temporal operators (`before`/`after`/`on or before`) or returned/sorted as DateTime values
  fail at runtime with the same `DateTimeType` error family as E-01.
- **Expected**: Temporal operators should accept FHIR dateTime values per the CQL spec.
- **Confirmed affected**: CMS1173 (raw `FHIR.dateTime` fed into temporal operators). CMS156 was
  originally grouped here, but its Missing Results are the E-13 (union→`prevalenceInterval()`)
  translate failure (re-attributed 2026-08-29); re-checked after the E-13 fix.
- **Workaround**: Convert to `System.DateTime` (`FHIRHelpers.ToDateTime(...)`) or, for choice types,
  convert to an interval first (`start of X.effective.toInterval()`).
- **References**: #24.

### E-03: Ambiguous fluent-function overload resolution between sibling profile types sharing one Java class

- **Symptom**: When two overloads of the same fluent function are declared for sibling USQualityCore
  profile types that compile to the identical underlying Java class (e.g.
  `recorded(Procedure)` vs. `recorded(ProcedureNotDone)`, both `org.hl7.fhir.r4.model.Procedure`),
  the engine cannot resolve between them at runtime.
- **Confirmed failure modes**:
  - Casting to the wrong ancestor (`FHIR.Procedure`) → `"Could not resolve call to operator
    recorded with signature (FHIR.Procedure)"`.
  - Explicit qualified static invocation (`Library.recorded(X)`) → `"... is a fluent function and
    can only be invoked with fluent syntax"`.
  - `is`/`as` narrowing cast to concrete sibling → still ambiguous (runtime-identical classes).
- **Expected**: The translator should resolve to the more-specific overload based on the declared
  CQL type, not the runtime Java class.
- **Confirmed affected**: CMS68, CMS996, CMS108, CMS190 (`.recorded()` on `ProcedureNotDone`);
  CMS144 (`AHAOverall.cql` — `overlapsHeartFailureOutpatientEncounter` /
  `overlapsAfterHeartFailureOutpatientEncounter`, `ConditionProblemsHealthConcerns` support
  dropped during migration).
- **Workaround**: Bypass the fluent function entirely; read the underlying FHIR extension directly:
  `(X.ext('http://fhir.org/guides/astp/us-quality-core/StructureDefinition/us-quality-core-recorded').value as FHIR.dateTime)`.
  `ext()` is declared generically for `DomainResource`/`Element` — no per-profile sibling to collide
  with. Applied to CMS68, CMS996 (one site), CMS108, CMS190. CMS144 is blocked (no `ext()`-style
  bypass exists for the `AHAOverall.cql` functions).
- **References**: #19, #21; §5 item 3.

### E-04: Choice-type self-reference circular dispatch

- **Symptom**: A function declared for a `Choice<A, B>` type, whose body casts to one branch and
  calls itself (or a function with the same name), resolves back to the Choice-typed declaration —
  `"Cannot resolve reference to expression or function prevalenceInterval_..._ because it results
  in a circular reference."`
- **Root cause**: Casting to a concrete member of a Choice type does **not** disambiguate a function
  declared for that same Choice type. It *does* disambiguate against functions declared for
  unrelated, non-overlapping types.
- **Confirmed affected**: Caused repo-wide collapse (91.85% → 7.43%) when first introduced via
  `Status.cql`.
- **Workaround**: Avoid any shared-library function typed on the Choice. Instead, inline `is`/`as`
  at each call site, dispatching to a concrete Choice member and relying on the single
  pre-existing, non-Choice-typed base function (`FHIRCommon.prevalenceInterval(Condition)`).
- **References**: #20 (entry 18); §5 item 4.

### E-05: Sibling overloads ambiguous at runtime (same Java class)

- **Symptom**: Two overloads of the same function, one per branch of a Choice type backed by
  profiles sharing a runtime class, are distinct at the CQL/ELM level but ambiguous at the
  engine/runtime level — `"Ambiguous call to operator 'toPrevalenceInterval(
  org.hl7.fhir.r4.model.Condition)' in library 'Status'."`
- **Root cause**: `ConditionProblemsHealthConcerns` and `ConditionEncounterDiagnosis` are distinct
  CQL types but compile to `org.hl7.fhir.r4.model.Condition`. The runtime literally cannot select
  between overloads by argument type.
- **Same underlying model characteristic as E-3**, exposed via declaration instead of invocation.
- **Confirmed affected**: Same measures as E-04. Retroactively explains why `AHAOverall.cql`'s
  sibling-overload fix for CMS144 also failed (48/48 "Missing Results").
- **Workaround**: Same as E-04 (inline dispatch, no shared declaration).
- **References**: #20; §5 item 5.

### E-06: `as` cannot widen a Choice to a common ancestor type

- **Symptom**: `(X as FHIR.Condition)` where `X: Choice<ConditionProblemsHealthConcerns,
  ConditionEncounterDiagnosis>` throws `"Expression of type 'choice<...>' cannot be cast as a
  value of type 'Condition'."`
- **Root cause**: `as` only supports narrowing to one of the Choice's listed member types, not
  widening to an ancestor.
- **Confirmed affected**: Entry #20's fourth fix attempt.
- **Workaround**: Same as E-04 — narrow to a concrete member, not up to an ancestor.
- **References**: #20; §5 item 6.

### E-07: `convert <Duration> to days` returns null at runtime

- **Symptom**: `convert D.daysSupply to days` evaluates to `null` for otherwise valid
  `FHIR.Duration` values. The implicit FHIR.Duration→Quantity insertion inside `convert … to days`
  returns null at runtime.
- **Expected**: A `FHIR.Duration` with a valid value and unit should convert successfully.
- **Confirmed affected**: CMS128 (`medicationDispensePeriod()`), CMS156
  (`medicationRequestPeriodInDays()`). Also confirmed via literal probes (CMS156 probe `PG`).
- **Workaround**: Hand-rolled `ToDays(FHIR.Duration)` helper doing s/min/h/d/wk/mo/a unit
  arithmetic directly on the Duration's numeric value/unit fields, bypassing `convert` entirely.
  Applied to CMS128 and CMS156.
- **Note**: The vendored CMD library's own authors patched the MedicationRequest-side functions
  with an inline TODO ("this isn't working as expected, convert results in null") but left the
  MedicationDispense-side functions unpatched — same bug, different code path.
- **References**: #22, #25 (rounds 3-6); §5 items 7-8.

### E-08: `ConvertQuantity` / `convert … to days` rejects calendar-word units from `FHIRHelpers.ToQuantity`

- **Symptom**: `FHIRHelpers.ToQuantity(Duration)` emits unit `'day'` (calendar spelling), but the
  engine's `convert … to days` / `ConvertQuantity(…, 'd')` only accept UCUM-spelled unit `'d'`.
  A quantity of `5 'day'` → convert null; literal `5 'd'` → works.
- **Expected**: `ToQuantity` (engine-supplied) and `convert` (engine operator) should be internally
  consistent about unit spelling.
- **Confirmed affected**: CMS156.
- **Workaround**: Same `ToDays()` helper as E-07 — bypasses both `convert` and `ConvertQuantity`.
- **References**: #25 (rounds 4-6); §5 item 8.

### E-09: Quantity division across differing dimensions silently rounds to zero

- **Symptom**: `1.25 mg / 5 d` normalizes to SI base units (g/s) → ~2.9×10⁻⁹ g/s → rounds to
  8 decimal places → exactly `0E-8`. Any downstream `> threshold` comparison can never be true.
- **Expected**: Unit-aware division should preserve sufficient precision for meaningful comparison.
- **Confirmed affected**: CMS156 (`averageDailyDose() > 0.125/6 'mg/d'` — the Numerator 1
  comparison could never fire).
- **Workaround**: Construct the result via raw decimal math:
  `System.Quantity { value: <decimal arithmetic>, unit: 'mg/d' }` — bypasses UCUM normalization
  and rounding entirely.
- **References**: #25 (rounds 7-8); §5 item 9.

### E-10: `singleton from <empty list>` throws instead of returning null

- **Symptom**: `singleton from dosage.doseAndRate` aborts at runtime when the underlying
  `MedicationRequest` has no `doseAndRate` element, killing the calling function before a later
  `Coalesce(...)` fallback branch can short-circuit.
- **Expected**: Per the CQL spec, `singleton from` of an empty list should return null.
- **Confirmed affected**: CMS156 (both probe fixtures and real fixtures `c409fbc9`/`07f11229`).
- **Workaround**: Fixture-side — ensure `doseAndRate` and timing are populated. Genuinely sparse
  real-world data would still hit this.
- **References**: #25 ("Open question ANSWERED"); §5 item 10.

### E-11: `Unable to extract codes from fhirType Reference` — engine-level crash before CQL evaluation

- **Symptom**: `"Unable to extract codes from fhirType Reference"` thrown before any CQL `define`
  evaluates. Trace output for affected test cases is completely empty (no `Patient=`, no population
  values). Root-caused to `CodeExtractor.getCodesFromBase` in `cqf-fhir-cql`.
- **Confirmed affected**: CMS135 (3 cases — `MedicationRequest.medication` as
  `Reference(Medication)` choice type); CMS165 (1 case — **no** `MedicationRequest` or
  `Reference`-bearing resource in the fixture at all, confirmed a different trigger).
- **Workaround**: **None.** Two independent CQL rewrites (explicit Reference branch + valueset
  filter on `Medication.code`) both failed identically. Needs a live JVM stack trace (debugger
  breakpoint or increased engine log verbosity) — static analysis is exhausted.
- **References**: Tried-and-reverted section; §5 item 11.

### E-12: Union branch evaluates empty despite correct data

- **Symptom**: CMS104's `"Reason For Not Giving Antithrombotic At Discharge"` second union branch
  (`MedicationRequest`-with-`TaskRejected` pattern) evaluates empty even when every referenced field
  (`Task` status, `MedicationRequest`, valueset membership, `reasonCode`) is individually confirmed
  correct.
- **Confirmed affected**: CMS104 (fixture `5adc911a-...`, description "task rejected-patient
  refusal"). Trace dump shows `Reason For Not Giving Antithrombotic At Discharge=[]`.
- **Workaround**: **None.** Needs a translator/ELM-level trace to diagnose further. Distinct from
  (not yet confirmed to be an instance of) any other issue on this list.
- **References**: #17; §5 item 12.

### E-13: Sibling-profile condition union → `prevalenceInterval(Choice<...>)` mis-resolution — missing FHIRCommon Choice overload + translator cannot resolve the call (E-15 RETIRED, rolled into E-13)

**E-15 retired 2026-08-29**: E-15 was the same defect confirmed on the new engine across the full
30-measure sweep; all E-15 issues have been rolled into E-13. CQL comments previously marking the
fix as `[E-15]` now read `[E-13]`; any remaining "E-15" mention in this file or in
`change-classification.md` / `conversion-notes.md` refers to this merged defect.

- **Symptom**: `prevalenceInterval()` is declared in FHIRCommon for plain `FHIR.Condition` only
  (line 394). When CQL unions two USQualityCore condition profile retrieves
  (`[ConditionProblemsHealthConcerns: ...] union [ConditionEncounterDiagnosis: ...]`), the result is
  typed as `Choice<...>` and the subsequent `.prevalenceInterval()` / `.isVerified()` /
  `.verified()` calls fail to resolve: the cql-to-elm translator errors the whole library and every
  test case reports Missing Results with `Could not resolve call to operator prevalenceInterval with
  signature (choice<USQualityCore.ConditionEncounterDiagnosis, USQualityCore.ConditionProblemsHealthConcerns>)`.
- **Root cause — stacked defects**:
  1. **Library gap (content)**: the pre-migration `QICoreCommon.cql` declared this exact overload at
     line 452 — `prevalenceInterval(condition Choice<"ConditionEncounterDiagnosis",
     "ConditionProblemsHealthConcerns">)` — and it was **dropped during the QICoreCommon →
     FHIRCommon/USCoreCommon/USQualityCoreCommon refactor** and never carried over. Confirmed by
     direct diff against `dqm-content-qicore-2025/input/cql/QICoreCommon.cql:452`.
  2. **Engine/translator issue (the Choice SHOULD work)**: even without that overload, a Choice whose
     members (`ConditionEncounterDiagnosis`, `ConditionProblemsHealthConcerns`) both derive from
     `FHIR.Condition` should coerce against the base `Condition` overload under CQL type semantics —
     the reference engine and QICoreCommon handled it. Instead the translator hard-fails with
     `Could not resolve call ... signature (choice<...>)` and cannot widen the Choice to its
     ancestor (consistent with E-06, "`as` cannot widen a Choice to a common ancestor"). **The
     natural Choice-based code is correct and should work; the resolution failure is itself the
     engine/translator defect to fix. The base-retrieve workaround below is a deflection, not the
     ideal.**
- **Why re-adding the overload to a shared library doesn't work**: Three consecutive attempts failed
  for different engine-level reasons:
  1. Adding to `Status.cql` → circular reference (E-04)
  2. Splitting into two named overloads → ambiguous at runtime (E-05)
  3. Widening the parameter to `FHIR.Condition` → `as` widen unsupported (E-06)
- **Confirmed affected (mixed-type union → `prevalenceInterval()`) — 30 measures total**: original
  7 — CMS347 (`StatinPreventionTxCVD`:97–101), CMS117 (`ChildImmunStatus`, e.g. 229–236), CMS138
  (`TobaccoScrnCessation`:127–132), CMS153 (`ChlamydiaScreening`:96–105), CMS136
  (`ChildADHDMedFollowUp`, Narcolepsy Exclusion:136–140), CMS155 (`WgtAssessCounseling`, Pregnancy
  Diagnosis:63–66), CMS69 (`PCSBMIScreenAndFollowUp`:102–106) — plus a trace sweep of every remaining
  failing measure (2026-08-28, `measure-failure-report-20260828-2131-condition-union-fix-applied.md`)
  that found the **identical `prevalenceInterval(choice<...>)` signature in 23 further measures**:
  CMS645, CMS1154, CMS1157, CMS75, CMS142, CMS143, CMS771, CMS1188, CMS124, CMS349, CMS90, CMS646,
  CMS314, CMS129, CMS951, CMS128, CMS56, CMS131, CMS159, CMS133, CMS996, CMS157, CMS156 = **1,267
  Missing-Results test cases across the same single root cause**. Exclusion corrections: the claim
  that "CMS157, CMS645 use separate/non-mixed retrieves and are not affected" was **wrong** — both
  crash with the E-13 signature; only **CMS22 and CMS71** use genuinely non-mixed retrieves and stay
  excluded; CMS156/CMS646 were mis-labelled E-01/E-02 — their Missing Results are this E-13 translate
  failure (they cannot load past the union; see E-01/E-02 re-scope).
- **Workaround (preferred and APPLIED)**: Replace the union of the two sibling profiles with a single
  base `[FHIR.Condition: "..."]` retrieve — both profiles derive from `Condition`, so the base
  retrieve captures every instance without ever forming the Choice. `.prevalenceInterval()` /
  `.isVerified()` / `.verified()` then resolve against FHIRCommon's base-`Condition` overloads
  (`defect-tracking/_reference/FHIRCommon.cql` lines 394 / 427 / 438). **38 site-level edits, 13
  measures applied** (2026-08-28: 7 measures / 25 sites; 2026-08-29 Stage 1: 6 measures / 13 branches);
  **17 measures pending** Stages 2–3. **Inline `is`/`as` is the SUPERSEDED interim workaround**: the
  earlier host of attempts on CMS90, CMS129, CMS133, CMS142, CMS143, CMS155, CMS157, CMS159, CMS347,
  CMS951 used per-call-site `is`/`as` dispatch before the base-retrieve approach was proven; any of
  those sites still present (CMS90, CMS129, CMS951 — Stage 2; CMS133, CMS157, CMS159 — Stage 3;
  leftovers on CMS142/143/155/347) are converted to base retrieves as each measure passes. The
  `testE15*` / `defectHelper.cql` isolation artifacts retain their `E15` filenames (not renamed; only
  comment text was updated).
- **Status**: **Verified / Applied**. Original 7: 0 errors / 0 MR, class A EMPTY (2026-08-28);
  Stage 1 (2026-08-29): CMS1157 & CMS143 fully passing, CMS645 0 MR + 3, CMS75 0 MR + 7, CMS142 0 MR
  + 5 (residuals = class B); **CMS1154 0 MR + 1 residual = E-16** (0125). Full operational record
  (per-measure site counts, syntax-regression post-mortem, class-A reconciliation) in the block
  below.
- **References**: E-06; #10, #15, #16, #18, #19, #20 (the full saga); §5 items 4–6;
  change-classification.md §3 / §5; conversion-notes.md #27.

### E-14: `PCMaternal.cql` cast type change — suspected, unverified

- **Symptom**: `lastEstimatedDeliveryDate()` and `lastTimeOfDelivery()` changed their internal cast
  from `.value as DateTime` (QICore, `System.DateTime`) to `.value as FHIR.dateTime`
  (USQualityCore) during the model migration.
- **Why suspected**: Callers in `CMS0334FHIRPCCesareanBirth.cql` and
  `CMS1028FHIRPCSevereOBComps.cql` do direct date arithmetic / interval construction against
  these functions' return values, which could behave differently against raw `FHIR.dateTime` vs.
  `System.DateTime` depending on whether the engine auto-unwraps it.
- **Confirmed affected**: Not verified against a live engine or a specific failing fixture. Both
  callers show only 1-2 mismatches each, consistent with either a narrow real effect or no effect.
- **Workaround**: None — unconfirmed lead, not acted on.
- **References**: #19; §1; change-classification.md §3.

### E-13 applied workaround and confirmatory evidence (E-15, RETIRED 2026-08-29 — all E-15 issues rolled into E-13)

This block is the operational record of the merged E-13 defect above, originally filed as E-15.
CQL comments marking the fix now read `[E-13]` (renamed from `[E-15]` 2026-08-29).

- **Symptom**: When CQL forms a union of the two sibling USQualityCore condition profile
  retrieves — `[ConditionProblemsHealthConcerns: "..."]` union `[ConditionEncounterDiagnosis:
  "..."]` (including unioning the *same* code across both types, e.g. `"Pregnancy"` on both) — the
  result is typed as `Choice<ConditionProblemsHealthConcerns, ConditionEncounterDiagnosis>`. When
  that Choice is then passed to the `prevalenceInterval()` fluent function, **the cql-to-elm
  translator fails to resolve the call**, so the whole library errors and every test case reports
  Missing Results. The engine traces (`input/tests/results/*.txt`) for all affected measures record
  the identical error: `Could not resolve call to operator prevalenceInterval with signature
  (choice<USQualityCore.ConditionEncounterDiagnosis, USQualityCore.ConditionProblemsHealthConcerns>)`.
  This is a translate-time failure, not a runtime mis-behavior (earlier wording claiming it
  "resolves but picks the wrong behavior" was superseded by the trace evidence).
- **Root cause**: The shared `FHIRCommon` library (`defect-tracking/_reference/FHIRCommon.cql`,
  v2.0.0, line 394) declares only `prevalenceInterval(condition Condition)` — a base `FHIR.Condition`
  overload. It has **no** `Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>`
  overload, and the translator cannot auto-widen a union-built Choice to the base `Condition`
  overload (consistent with E-06, "`as` cannot widen a Choice to a common ancestor"). The old-engine
  E-13 missing-overload finding is thus confirmed on the new engine via the concrete translator error.
- **Confirmed affected (mixed-type union → `prevalenceInterval()`) — 30 measures total**: the
  original 7 — CMS347 (`StatinPreventionTxCVD` lines 97–101, its Exclusion Diagnosis union), CMS117
  (`ChildImmunStatus`, e.g. lines 229–236 DTaP), CMS138 (`TobaccoScrnCessation`, e.g. lines
  127–132), CMS153 (`ChlamydiaScreening`, "Has Diagnoses Identifying Sexual Activity", lines
  96–105), CMS136 (`ChildADHDMedFollowUp`, Narcolepsy Exclusion, lines 136–140), CMS155
  (`WgtAssessCounseling`, Pregnancy Diagnosis, lines 63–66), CMS69 (`PCSBMIScreenAndFollowUp`,
  Overweight/Obese, lines 102–106) — plus a trace sweep of every remaining failing measure
  (2026-08-28, `measure-failure-report-20260828-2131-condition-union-fix-applied.md`) that found
  the **identical `prevalenceInterval(choice<...>)` signature in 23 further measures**: CMS645,
  CMS1154, CMS1157, CMS75, CMS142, CMS143, CMS771, CMS1188, CMS124, CMS349, CMS90, CMS646, CMS314,
  CMS129, CMS951, CMS128, CMS56, CMS131, CMS159, CMS133, CMS996, CMS157, CMS156 — 1,267 Missing-
  Results test cases across the same single root cause. **Earlier exclusion note corrected**: the
  claim that "CMS157, CMS645 ... use separate/non-mixed retrieves and are not in the confirmed
  list" was WRONG — both crash with the E-13 signature; only **CMS22 and CMS71** use genuinely
  non-mixed retrieves and remain excluded. **CMS156 and CMS646 were previously mis-labelled as
  E-01/E-02 crashes** — their Missing Results are the E-13 translate failure (they cannot load past
  the union); they are re-checked after the fix (see E-01/E-02 re-scope).
- **Workaround (proposed and now APPLIED)**: Replace the union of the two sibling profiles with a
  single base `FHIR.Condition` retrieve that carries the code(s), e.g.
  `( [FHIR.Condition: "Pregnancy"] ) where ... `. Because `ConditionProblemsHealthConcerns` and
  `ConditionEncounterDiagnosis` both derive from `FHIR.Condition`, a base-type retrieve captures
  every instance without ever forming the problem Choice type. The shared `FHIRCommon.cql`
  (`defect-tracking/_reference/FHIRCommon.cql`, lines 427/438) also declares
  `isVerified(FHIR.Condition)` / `verified(List<FHIR.Condition>)`, so the `.verified()` calls the
  affected measures make after the union resolve cleanly against the base-type retrieve too (this
  is the working `CMS125` pattern). This is the same approach E-13 flagged as a "cleaner future
  alternative"; the new engine makes it the necessary fix rather than a cleanup.
- **Applied 2026-08-28 (25 sites, 7 measures, originals commented out)**: every `prevalenceInterval()`
  error site now uses the base `[FHIR.Condition: ...]` retrieve, preserving the existing
  `.prevalenceInterval()` / `.isVerified()` (FHIRCommon) and `verified()` call shapes and writes.
  Per-measure site totals: CMS347 1 (`"Denominator Exclusions"`), CMS69 3 (`"High BMI Interventions
  Ordered"`, `"Low BMI Interventions Ordered"`, `"Is Pregnant During Measurement Period"`), CMS136 1
  (`"Narcolepsy Exclusion"`), CMS138 1 (`"Tobacco Cessation Counseling Given"` union branch),
  CMS155 1 (`"Pregnancy Diagnosis Which Overlaps Measurement Period"`), CMS153 1 (`"Has Diagnoses
  Identifying Sexual Activity"`, 3 codes), CMS117 17 (exclusions SCID / Immunodeficiency / HIV /
  Lymphoreticular / Intussusception + numerator inclusion conditions DTaP, Polio, Mumps, Rubella,
  MMR, Hib, HepB, VZV, PCV, HepA, Rotavirus, Influenza). Each site retains `// [E-13] base
  FHIR.Condition retrieve (defect-tracking/engine-issues.md E-13)` + `// Original:` comments with
  the replaced union lines, per methodology (fix is NOT a logic change). Paren/bracket balance is
  unchanged vs. HEAD for all seven files (verified) and the resulting retrieve count matches the
  minus-2-per-site union reduction with a 1-per-code conditional-line increase.
- **Applied 2026-08-29 — Stage 1 (additional 6 measures; 10 defines, 13 union branches, originals
  commented out)**: same base-`[FHIR.Condition: ...]` fix, same `// [E-13]` + `// Original:`
  convention (fix is NOT a logic change). CMS645 (`BoneDensityPCADTherapy`, `"Bone Density Testing
  in PCD"`), CMS1154 (`ScreeningPrediabetes`, 4 defines / 7 union branches: Advanced Illness +
  Limited Life Expectancy × 2 profiles, Pregnancy, Prediabetes, Diabetes), CMS75 (`PCS`, `"Has
  Diagnosis of Diabetes or Prediabetes"`), CMS1157 (`DiabetesSUB`, 2 sites), CMS142 (`Postpartum`,
  `"Has Maternal Depression or Anxiety"`), CMS143 (`Postpartum`, `"Has Postpartum Depression"`).
- **CMS1154 syntax regression and re-fix — RESOLVED (harness-verified 2026-08-29 01:25, `discrepancy_report-20260829-0125.md`)**: the initial Stage-1 edit introduced an unmatched leading
  `(` before each of the 4 edited defines (a stray opener layered on the canonical
  `( ( ... ).verified ( ) )` double-wrap), producing 4× `Syntax error at define` and 10 MR on the
  2026-08-29 0058 run (the other 5 files used `with (` / `exists (` forms that closed the opener,
  so they compiled). Restructured 2026-08-29 to the proven no-dangling-opener canonical shape
  (comments on their own lines before the expression; balanced
  `( ( [FHIR.Condition: ...] ... ).verified ( ) ) Alias`). Paren balance re-verified; the 0125
  re-run confirms **0 MR (10 → 0)** with zero `Error=` lines. The single remaining mismatch
  (`bc9c82ca`, DenExcl 1→0) adjudicates as the new **E-16** runtime defect (see E-16), not an E-13
  regression — corroborated by `input/cql/original_CMS1154.cql` (identical exclusion logic) and the
  active-vs-inactive natural experiment.
- **Status (harness-verified 2026-08-28 21:31, `discrepancy_report-20260828-2131-condition-union-fix-applied.md`)**:
  all 7 measures re-ran with **zero translate/eval errors and zero Missing Results** — the
  `prevalenceInterval(choice<...>)` resolver errors are gone (verified by grep on all seven
  `input/tests/results/<Name>.txt`; traces are clean). Report totals: pass 48.20% → 70.13%
  (11,432 → 16,637), Missing Results 2634 → 1372 (−1262, exactly the 7 measures' former crash
  count). The 7 measures now expose 330 mismatched output rows that were previously invisible under
  the crash. **Class A reconciliation is COMPLETE with an empty result: after root-cause triage of
  every mismatch fixture, ZERO discrepancies flow through an E-13-edited define.** Each exclusion-loss
  row traces to the shared, unedited semantic libraries or profile retrieves failing systemically on
  the new engine (class B): Hospice (Condition `170935008`, ServiceRequest/Procedure `385763009`,
  Observation `45755-6`, Encounter `hospitalization.dischargeDisposition` `428361000124107` /
  `428371000124100`, Encounter.type `183921001` / `305336008`), PalliativeCare (`441874000`,
  `103735009`, `170936009`, `71007-9`, Encounter.type `305284002`), USCore profile retrieves
  (`[USCore.BMIProfile]` → `BMI During Measurement Period=[]`, `[USCore.ObservationPregnancyStatusProfile]`
  → e.g. LOINC `82810-3` case), and the medical-reason ServiceNotRequested / ObservationCancelled
  pattern. This is the same "Denominator Exclusion 1→0" signature that recurs 214× across non-E-13
  measures such as CMS22/CMS71/CMS139. Because on this engine `[PHC]` ∪ `[CED]` ≡ `[FHIR.Condition]`
  (E-03 shared runtime class), and because the triaged define-level traces show the base-retrieve
  predicates agreeing with the reference everywhere they matter, the workaround is now behaviourally
  verified as well as non-regressive: the E-13 edits themselves are confirmed clean, and no
  disposition (CQL or fixture) is required for them. Detailed per-measure triage is in the
  "Class A reconciliation (2026-08-28)" bullet below; class B is deferred to the systemic
  engine-behavior investigation (see "Class B catalog (deferred)" bullet).
- **Stage-1 results (harness-verified 2026-08-29, `measure-failure-report-20260829-0058-additional-condition-union-fixes.md`)**: report totals failing test cases **2233 → 2086** (−147: MR 1372 → 1210 −162, mismatched 861 → 876 +15 — the +15 newly-visible rows are exactly CMS645 3 + CMS75 7 + CMS142 5). **CMS1157 (27 MR) and CMS143 (32 MR) are now fully passing (0 errors / 0 MR)** — the base retrieve is behaviourally identical to the reference. CMS645 51 MR → 0 MR + 3 mismatches (Denominator Exception = Patient-Refusal negative-indication class B; Numerator 0→1 = E-01/`Min()` candidate under investigation), CMS75 20 MR → 0 MR + 7 (all `Denominator Exclusion` = Hospice class B), CMS142 32 MR → 0 MR + 5 (all "Medical or Patient Reason for Not Communicating..." negation class B). The exposed rows are exactly the class-B catalogue, not the E-13 edits. **CMS1154 not fixed on this run**
  (my syntax regression on the 4 defines, see note above). Remaining E-13-crashing: 17 measures
  (Stages 2–3: CMS771, CMS1188, CMS124, CMS349, CMS90, CMS646, CMS314, CMS129, CMS951 = 359 MR, then
  CMS128, CMS56, CMS131, CMS159, CMS133, CMS996, CMS157, CMS156 = 736 MR), each through the same
  fix/verify/doc-update loop.
- **Stage-1 follow-up (harness-verified 2026-08-29 01:25, `measure-failure-report-20260829-0125.md`)**: after the
  CMS1154 re-fix the report totals moved failing test cases **2086 → 2077** (MR 1210 → 1200 −10,
  mismatched 876 → 877 +1). **CMS1154 is now 0 MR** with zero errors: its 10 crash cases resolved to
  9 passing + 1 mismatch (`bc9c82ca`, DenExcl 1→0), which adjudicates as the new **E-16** runtime
  defect — the base retrieve is behaviourally identical to the reference (the E-13 mechanism is
  closed for CMS1154). The remaining 17 measures above are still E-13-crashing.
- **Class A reconciliation (2026-08-28): triage of all 330 exposed mismatch rows — result: EMPTY
  (zero class A, no CQL or fixture disposition required)**. For every measure the exclusive,
  E-13-edited define(s) were confirmed independent of the mismatched populations, and each mismatch
  fixture was swept for the trigger resource(s) of the shared Hospice / PalliativeCare libraries and
  the relevant profiles:
  - CMS117 (8 cases): all 8 `Denominator Exclusion 1→0` — every case is Hospice-triggered
    (Condition `170935008`, ServiceRequest/Procedure `385763009`, Observation `45755-6`, Encounter
    dd `428361000124107`/`428371000124100`, Encounter.type `305336008`); none flows through the
    E-13-edited SCID/Immunodeficiency/HIV/Lymphoreticular/Intussusception/numerator-inclusion
    defines (which agreed with the reference everywhere — no value-set hit or miss through them).
  - CMS136 (15 cases): all `Denominator Exclusion 1→0` — Hospice-triggered (same code family incl.
    dd `428371000124100`); zero through the E-13-edited `Narcolepsy Exclusion`.
  - CMS155 (9 cases): all `Denominator Exclusion 1→0` — Hospice-triggered (Condition `170935008`,
    Observation `45755-6`, dd `428361000124107`, SR/Procedure `385763009`, Encounter.type
    `183921001`); zero through the E-13-edited `Pregnancy Diagnosis Which Overlaps Measurement
    Period`.
  - CMS153 (8 cases): all `Denominator Exclusion 1→0` — 7 Hospice-triggered (Condition
    `170935008`, dd dispositions, `45755-6`, SR/Procedure `385763009`), 1 (5e5374d9) expected
    exclusion unsupported by any resource (drift-style, see catalog below); the HIV Condition
    (`111880001`) in each fixture feeds the E-13 `Has Diagnoses Identifying Sexual Activity` only
    through a negation and matched expected.
  - CMS138 (16 cases): all `Denominator Exclusion 1→0` (`170935008`, `385763009`, `45755-6`, dd,
    Encounter.type `183921001`); co-occurring `Numerator 0→1` rows are downstream artifacts of the
    failed exclusion, not the E-13-edited `Tobacco Cessation Counseling Given` (which only adds
    numerator value when no exclusion fires).
  - CMS69 (46 cases): `Denominator Exclusion 1→0` bucket all Hospice (`170935008`, `385763009`,
    dd) or PalliativeCare (`170936009`, `103735009`, `71007-9`, Encounter.type `305284002`); one
    exclusion loss (e25fc2f1) via the `[USCore.ObservationPregnancyStatusProfile]` observation
    branch (LOINC `82810-3`) — a profile-retrieve gap, not the E-13 condition branch;
    `Numerator 1→0` bucket all downstream of `[USCore.BMIProfile]` returning `[]`
    (`BMI During Measurement Period=[]` in traces) — the E-13 Overweight/Obese/Underweight branches
    are never the deciding factor; `Denominator Exception 1→0` bucket all medical-reason
    ServiceNotRequested/ObservationCancelled pattern (unedited).
  - CMS347 (178 rows): dominated by systemic Initial Population/Denominator/etc. `1→0` (referenced
    criteria evaluate to false/empty on the new engine); the 8 "exception-only" single-row cases flow
    through the unedited `Denominator Exceptions` (statin allergy/Hospice/Palliative/hepatitis/
    ESRD/SAMS/medical reason); the E-13-edited `Denominator Exclusions` (Breastfeeding/Rhabdomyolysis)
    produced zero pure exclusion-only losses — its one-row co-occurrences ride the same systemic
    IP/Den `1→0`.
  Net: the base-`[FHIR.Condition]` replacement is behaviorally identical to the reference wherever
  the 7 measures' edited defines are reached; there is nothing to fix for class A.
- **Class B catalog (deferred to the systemic engine-behavior investigation)**. Same fixture-coded
  evidence as above: (1) **Hospice** shared library fails on the new engine across ALL trigger types
  — the failing branches are its profile-based retrieves `[USQualityCore.Encounter: ...]` /
  `[USQualityCore.ObservationScreeningAssessment]` / `[USQualityCore.ServiceRequest: ...]` /
  `[USQualityCore.Procedure: ...]` (the base `[FHIR.Condition: "Hospice Diagnosis"]` branch is the
  only one that could still fire); (2) **PalliativeCare** shared library fails similarly; (3) **USCore
  profile retrieves** `[USCore.BMIProfile]` and `[USCore.ObservationPregnancyStatusProfile]` return
  `[]` where fixtures carry valid LOINC `39156-5` / `82810-3` observations (trace: `BMI During
  Measurement Period=[]`); (4) **negative/medical-reason indication pattern** (ServiceNotRequested /
  MedicationNotRequested / ObservationCancelled) fails; (5) **CMS347-class systemic flip to
  false/empty** for Initial Population / Denominator criteria (possibly the same profile-retrieve root
  cause via `[ConditionProblemsHealthConcerns: ...]` unions that are not Choice-typed). These account
  for the bulk of the 29.87% fail rate and the recurring `"Denominator Exclusion | 1 | 0"` 214×
  signature measure-wide; they should be tracked as new engine issues (E-16+).
- **Expected-value drift candidates (subset of class B, fixture-level)**: CMS153 5e5374d9 (expected
  DenExcl=1 with no qualifying Hospice/Pregnancy-Test resource in fixture); CMS69 6092a810 and CMS117
  239d5e6f were re-checked and resolved to PalliativeCare (`305284002`) / Hospice (`305336008`)
  respectively — NOT drift. 5e5374d9 remains the single candidate needing expected-JSON review when
  the class B phase is opened.
- **Unit test / repro**: `input/cql/testE15ConditionUnionPrevalence.cql` + fixture
  `input/tests/measure/testE15ConditionUnionPrevalence/8c3fcda1-9ed7-4725-8a66-2cdcd5f959c4/`
  (Patient with both a `ConditionProblemsHealthConcerns` and a `ConditionEncounterDiagnosis`
  for SNOMED `72892002` "Pregnancy"), + registration resources
  `input/resources/library/testE15ConditionUnionPrevalence.json` (id/version `0.0.000`) and
  `input/resources/measure/testE15ConditionUnionPrevalence.json`. These resources are required
  for the engine to resolve the test library (without them the library loaded with
  `id: null, version null` — `Error=Library testE15ConditionUnionPrevalence was included with
  version null, but id: null and version null of the library was found`). The
  `FAILING Plain Union` / `FAILING Verified Union` defines reproduce the translator error; the
  `PASSING Plain` / `PASSING Verified` / `PASSING MultiCode Union` defines are the working
  `[FHIR.Condition: ...]` replacement.
- **Isolation library for the PASSING fix**: `input/cql/testE15Passing.cql` (+ resources
  `input/resources/library/testE15Passing.json`, `input/resources/measure/testE15Passing.json`,
  fixture `input/tests/measure/testE15Passing/124b25e9-4d11-4d0f-8077-da4df6ab9786/`) contains
  ONLY the 3 PASSING defines (base `FHIR.Condition` retrieve + `.verified()` + `.prevalenceInterval()`),
  so a clean run confirms the fix translates/evaluates in isolation. (Its results file was not
  produced in the first re-run — the harness run covered the main library. A dedicated run is TBD.)
- **Status (harness-verified, main library)**: on re-run, `testE15ConditionUnionPrevalence` reports
  ONLY the two expected resolver errors — `prevalenceInterval(choice<...>)` and
  `verified(list<choice<...>>)` — and **all three PASSING defines translate with ZERO errors**.
  This confirms (a) the E-13 root cause is exactly the union→Choice resolution failure, and
  (b) the `[FHIR.Condition: ...]` fix (with the CMS125 double-paren form) translates cleanly.
  Note: `verified(List<Choice<...>>)` also fails to resolve — widening fails for the list-of-choice
  argument too, so both `.prevalenceInterval()` and `.verified()` must be replaced by the fix.
- **Second finding — fluent results need wrapping parens before a source alias (a real syntax
  rule, not cascade)**: with unique aliases (`PregnancyDx1..5`) it became clearly attributable:
  `PASSING Plain` (`[FHIR.Condition: ...] Alias` — direct retrieve alias) translates CLEANLY, and
  both FAILING defines fail only on the expected Choice resolver errors. But the original
  `PASSING Verified` / `PASSING MultiCode Union` wrote `( retrieve ).verified() Alias` (a fluent
  result directly taking a source alias) → `Syntax error at <alias>` + `),` + `Internal translator
  error`. The proven CMS125 pattern wraps it: `( ( retrieve ).verified() ) Alias`. Both the main
  and isolation libraries now use the CMS125 form.
- **Choice-overload experiment — ANSWERED, NEGATIVE**: a library-level
  `prevalenceInterval(choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>)`
  fluent overload was added to see if we could make the failing union pass WITHOUT a base-type
  retrieve (i.e. fix shared `FHIRCommon` instead of every measure). Translator 5.2.0 threw
  `Internal translator error` / `could not determine result type` on a Choice-typed fluent
  parameter, and even the overload body re-failed — `Could not resolve call to operator
  abatementInterval with signature (choice<...>)` — because `abatementInterval()` is itself a
  Condition-typed fluent that hits the *same* widening limitation recursively. The errors also
  contaminated the whole library (spurious `Syntax error at PregnancyDx`, and a `verified`
  `(list<choice<...>>)` resolution failure), so the overload was removed from the unit test to
  keep the FAILING/PASSING attribution clean. **Conclusion**: fixing `FHIRCommon` via a Choice
  overload is NOT viable; the base `[FHIR.Condition: ...]` retrieve (PASSING defines) is the fix,
  so the workaround must be applied per-measure. Standalone hand-off artifact for the
  engine/translator team.
- **Choice-overload experiment re-attempted via `defectHelper.cql` — NEGATIVE in practice**:
  a standalone library `input/cql/defectHelper.cql` (v `1.0.000`, registered in
  `input/resources/library/defectHelper.json`) was added with the same 4 Choice-typed definitions
  (plain `abatementInterval(Choice<...>)`, fluent `prevalenceInterval(Choice<...>)`, fluent
  `isVerified(Choice<...>)`, fluent `verified(List<Choice<...>>)`) so measures could include it once.
  It could not be made to work — the user reported "can't get the defectHelper to work" and removed
  the choice tests from the unit test. Decision: **keep `defectHelper.cql` in the repo for later
  use, with all function bodies commented out** (`// [E-13] DISABLED` marker; doc blocks retained),
  and proceed with the per-measure base-retrieve fix. All 7 measures now implement the fix directly
  (25 sites applied, see Workaround bullet above); `defectHelper.cql` is not referenced by any
  measure.
- **Fix status**: the per-measure CQL fix is APPLIED for all 7 E-13 measures (25 sites, originals
  commented out). The Choice-overload library experiment is closed/negative. Harness re-run
  `2026-08-28 21:31` confirms the `prevalenceInterval(choice<...>)` errors are gone: 7/7 measures
  0 errors / 0 Missing Results; pass 48.20% → 70.13%, MR −1262. Class A reconciliation (complete
  `2026-08-28`) is EMPTY — zero mismatches flow through an E-13-edited define, so no CQL or fixture
  disposition is required; the workaround is behaviorally verified, not just non-regressive. The
  exposed mismatches are catalogued as class B (Hospice / PalliativeCare / USCore-profile-retrieve /
  medical-reason systemic gaps, plus one expected-value drift candidate CMS153 5e5374d9) and are
  deferred to the systemic engine-behavior investigation (see "Class B catalog (deferred)" bullet).
- **References**: E-13 (same family, missing-overload variant); E-06; #19/#20 (prevalenceInterval
  saga); change-classification.md §3 / §5; conversion-notes.md #27 (shared-library provenance:
  `defect-tracking/_reference/FHIRCommon.cql`, sourced from `~/.fhir/packages/hl7.fhir.uv.cql#2.0.0`).

---

### E-16: `overlaps` on a half-open null-high interval (`[start, null)`) evaluates false — `FHIRCommon.prevalenceInterval()` inactive branch (new engine)

- **Symptom**: a condition whose `clinicalStatus` is not active/recurrence/relapse (e.g. `inactive`)
  with a populated `onset` and no `abatement` hits `FHIRCommon.prevalenceInterval()`'s inactive
  branch (`defect-tracking/_reference/FHIRCommon.cql` lines 394–405): `abatementDate` is null, so it
  returns `Interval[start of onset.toInterval(), null)` (half-open, **null high bound**). On the new
  engine the subsequent `... overlaps [Interval]` predicate evaluates to null/false and the condition
  is dropped; on the reference engine the null high bound does not prevent the overlap, so the
  condition matches. Translation is clean — this is a runtime interval-semantics discrepancy in the
  E-01/E-02 family, surfaced only because the E-13 union fix let CMS1154 evaluate far enough to
  reach it.
- **Evidence — natural experiment (CMS1154, 2026-08-29)**: two fixtures with byte-identical
  prediabetes conditions (code `714628002`, `onsetDateTime` 2025-12-31, no abatement, no
  verificationStatus) differing ONLY in `clinicalStatus`:

  | fixture | `clinicalStatus` | `prevalenceInterval()` | `overlaps` "Look Back Period" | result |
  | --- | --- | --- | --- | --- |
  | `b4eff700` | `active` | `[2025-12-31, null]` (closed, null high) | TRUE | DenExcl 1 — matches reference |
  | `bc9c82ca` | `inactive` | `[2025-12-31, null)` (half-open, null high) | FALSE | DenExcl 0 vs expected 1 — mismatch |

  The expected value is sound: reproducible by measure intent and by the reference on the original
  `input/cql/original_CMS1154.cql`, whose exclusion logic is identical. This is NOT expected-value
  drift (earlier hypothesis retracted) and NOT an E-13 regression — the E-13 union→Choice fix is
  verified (CMS1154 10 MR → 0).
- **Confirmed affected**: CMS1154 (residual 1 mismatch). Other measures reaching `prevalenceInterval()`
  with inactive/absent clinicalStatus and no abatement over a bounded window are candidates to sweep
  during Stage 2/3.
- **Workaround**: none shipped. Candidates: construct the interval without a null high bound (e.g.
  coalesce `abatement` to a closed bound) at measure or FHIRCommon level, or fix `overlaps` in the
  engine to treat unbounded/null interval bounds per the CQL spec. Deferred pending the engine-runtime
  investigation (class B / E-01–E-02 family).
- **Status**: **Confirmed** (2026-08-29, active/inactive natural experiment). Optional next step:
  `testE15`-style isolation probe locking `[start, null) overlaps` behavior for the engine team. Not
  yet remediated.

---

## Cross-Cutting Lessons

These are generalizable patterns discovered during the engine-issue investigation. They apply
beyond the specific measures listed above and should inform any future CQL edits that touch
USQualityCore profile types.

### Sibling-profile runtime-class collision (E-03, E-05)

USQualityCore profile types that derive from the same base FHIR resource compile to the identical
underlying Java class. The engine cannot distinguish between them at runtime by argument type.

**Known affected pairs**:
- `USQualityCore.Procedure` / `USQualityCore.ProcedureNotDone` → `org.hl7.fhir.r4.model.Procedure`
- `USQualityCore.ConditionProblemsHealthConcerns` / `USQualityCore.ConditionEncounterDiagnosis` →
  `org.hl7.fhir.r4.model.Condition`

**Impact**: Any fluent function with overloads for both members of a pair is ambiguous at runtime,
regardless of how the CQL/ELM looks on paper. This affects both invocation (calling the function)
and declaration (defining a new function for one branch of a Choice built from the pair).

**Safe bypass for invocation**: Use `.ext()` to read the underlying extension directly, bypassing
the fluent function entirely. `ext()` is generic (`DomainResource`/`Element`), no per-profile
sibling to collide with. (E-03's workaround.)

**No safe pattern for shared declaration**: Do not declare a new function typed on a Choice built
from a sibling-profile pair. Inline the dispatch at each call site instead. (E-04/E-05/E-06's
combined lesson.)

### Choice type dispatch rules (E-04, E-05, E-06)

| What you're doing | What happens | Example |
|---|---|---|
| `is`/`as` narrowing to a concrete Choice member, then calling a function declared for an **unrelated** type | **Works** — disambiguates against unrelated overloads | `AHAOverall.cql`'s `TimingBoundToInterval` dispatching on `is FHIR.Period`/`is FHIR.Range` |
| `is`/`as` narrowing to a concrete Choice member, then calling a function declared for **that same Choice type** | **Circular reference** — the narrowed value still matches the Choice-typed declaration | E-04 (Status.cql `prevalenceInterval` attempt) |
| Two overloads for two concrete members of a Choice backed by sibling profiles | **Ambiguous at runtime** — same Java class, engine can't select | E-05 (`toPrevalenceInterval` split) |
| `as` widening from a Choice to a common ancestor type | **Not supported** — throws type error | E-06 (`as FHIR.Condition` attempt) |
| `is`/`as` narrowing + calling a function declared only for the **ancestor** type (no Choice-typed sibling) | **Works** — after narrowing, only one declaration is reachable | E-13's interim fix (inline `is`/`as` → `FHIRCommon.prevalenceInterval(Condition)`); superseded for the mixed-union case by the base retrieve |

### Safe pattern: inline `is`/`as` at each call site

When a Choice-typed value needs a function that only exists for a concrete type:

1. `if X is ConcreteTypeA then (X as ConcreteTypeA).function() else (X as ConcreteTypeB).function()`
2. **No new function declared anywhere** — not in a shared library, not locally, not under a
   different name.
3. After narrowing, the only reachable declaration is the single, pre-existing, non-Choice-typed
   base function. No self-loop, no sibling collision.

This is the pattern that finally worked across 10 measures (#20-final) after four consecutive
attempts at shared-library solutions each failed for a different reason.

**Superseded 2026-08-29 for the mixed-condition-union case by E-13's base retrieve**: when the
Choice members both derive from a common base resource, the cleaner fix is to replace the
`union` with a single base-type `[FHIR.Condition: "..."]` retrieve so the Choice is never formed
(E-13; see E-13/choice-should-work note). The inline `is`/`as` pattern remains valid for genuine
per-member Choice dispatch where no common-base retrieve is appropriate.

---

## Affected Measures Cross-Reference

Measures blocked entirely on engine issues (cannot be fixed at the CQL level):

| Measure | Issue | Blocked? | Notes |
|---|---|---|---|
| CMS135 | E-11 | **Yes** | 3 Missing Results — needs JVM stack trace |
| CMS165 | E-11 | **Yes** | 1 Missing Result — needs JVM stack trace |
| CMS144 | E-03 | **Yes** | 3 mismatches — `AHAOverall.cql` Choice-type gap, no ext()-style bypass |
| CMS145 | — | **Yes** | 106 Missing Results — no CQL authored (content gap, now ported on `cms145-cms149-port` branch, pending verification) |
| CMS149 | — | **Yes** | 33 Missing Results — no CQL authored (content gap, now ported on `cms145-cms149-port` branch, pending verification) |

Measures with engine-issue workarounds applied (residual mismatches are non-engine):

| Measure | Issues | Workaround applied | Residual mismatches |
|---|---|---|---|
| CMS68 | E-03 | `.ext()` bypass for `.recorded()` | 0 — fully passing |
| CMS996 | E-03, E-02 | `.ext()` bypass for `.recorded()` | 8 — distinct issues |
| CMS108 | E-03 | `.ext()` bypass for `.recorded()` | 14 — distinct issues |
| CMS190 | E-03 | `.ext()` bypass for `.recorded()` | 11 — distinct issues |
| CMS1173 | E-01, E-02 | **Not applied in current tree** (see E-01) | **62 MR** — `The Minimum operator is not implemented for type {http://hl7.org/fhir}dateTime` |
| CMS156 | **E-13** (was E-15; re-attributed 2026-08-29; was mis-labelled E-01/E-02) | E-13 fix pending (Stage 3) | **177 MR** baseline — cannot load past the condition union |
| CMS128 | E-07 | Local `AntidepressantCoveragePeriod()` | 0 — fully passing |
| CMS871 | E-01 | (pending `Min()` fix) | **5 MR** — `Unable to locate ValueSet ... 1196.394` + `Invalid Interval` |
| CMS645 | **E-13** (was E-15; re-attributed; baseline MR was the union translate failure), E-03 | Base `FHIR.Condition` replace (E-13) + `.ext()` | **0 MR** + 3 mismatches: DenException = Patient-Refusal negation (class B); 2 Numerator 0→1 = E-01/`Min()` candidate |
| CMS646 | **E-13** (was E-15; re-attributed 2026-08-29) | E-13 fix pending (Stage 2) | **38 MR** baseline — cannot load past the condition union |
| CMS90, CMS133, CMS142, CMS143, CMS155, CMS157, CMS159, CMS951, CMS347, CMS129 | E-13 (was E-15) | Base `FHIR.Condition` retrieve (inline `is`/`as` interim superseded) | Varies — genuine logic mismatches now visible |
| original 7 (CMS347, CMS117, CMS138, CMS153, CMS136, CMS155, CMS69) + CMS645, CMS1154, CMS1157, CMS75, CMS142, CMS143, CMS771, CMS1188, CMS124, CMS349, CMS90, CMS646, CMS314, CMS129, CMS951, CMS128, CMS56, CMS131, CMS159, CMS133, CMS996, CMS157, CMS156 | E-13 (was E-15; 30 measures confirmed; CMS22/CMS71 excluded) | Base `FHIR.Condition` retrieve replacing sibling-profile union (38 site-level edits / 13 measures applied; 17 pending Stages 2–3) | **Verified** — original 7: 0 errors / 0 MR, class A EMPTY (2026-08-28); Stage 1 (2026-08-29): CMS1157 & CMS143 fully passing, CMS645 0 MR + 3, CMS75 0 MR + 7, CMS142 0 MR + 5 (residuals = class B), CMS1154 verified 0 MR + 1 mismatch = E-16 (0125) |
| CMS1154 | E-13 (was E-15), **E-16** (2026-08-29) | Base `FHIR.Condition` replace (E-13) | **0 MR** — 9/10 passing; 1 residual mismatch (`bc9c82ca` DenExcl 1→0) = E-16 `overlaps` null-high runtime defect (not class B / not drift) |
| CMS104 | E-12 | None | 7 — union branch empty |
| CMS0334, CMS1028 | E-14 | None | 1-2 each — unconfirmed |
