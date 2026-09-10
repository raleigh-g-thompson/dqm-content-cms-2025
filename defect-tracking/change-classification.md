# QI-Core → US Quality Core: Change Classification

This document classifies every change captured in `conversion-notes.md` (the detailed,
chronological changelog) by **root-cause category**, so it's easy to answer "was this a
model/profile issue, a bad fixture, a bug we introduced converting the CQL, a bug in a vendored
library, or a real engine/translator gap?" without re-reading the full narrative log.

Each item below cites the `conversion-notes.md` entry number (`#N`) it came from, so you can jump
back to the full reasoning, fixture GUIDs, and verification evidence.

Category 5 (Suspected Engine/Translator Issues) is the one to track most closely per your ask —
it's the running list to hand to whoever digs into `clinical_quality_language` /
`clinical-reasoning`. Every item there has a confirmed repro and a stated workaround; none should
be assumed fixed at the engine level just because a CQL workaround exists.

---

## 1. Model / Profile Issues

Problems rooted in the USQualityCore model itself — namespace changes, type changes, or
structural characteristics of the derived model (as opposed to a mistake in someone's CQL or
fixture data).

| Issue | Details | Status |
|---|---|---|
| Stale `onc` → `astp` profile namespace | IG renamed its profile namespace (`.../guides/onc/us-quality-core/...` → `.../guides/astp/us-quality-core/...`). All `.cql` and the modelinfo XML had moved, but 2,679 fixture files across 12 measures still had `meta.profile`/extension URLs on the old `onc` path, so profile-typed retrieves silently matched nothing. | **Fixed** — bulk find/replace (original #1; re-applied on `astp-update` branch off UQC main 0866d738, 2026-08-25). +2.28 pts suite-wide expected. |
| `USQualityCore.Procedure` / `USQualityCore.ProcedureNotDone` are siblings sharing one runtime class | Both declared `baseType="USCore.ProcedureProfile"` (not one deriving from the other), and both compile to the identical `org.hl7.fhir.r4.model.Procedure` Java class. Same pattern for `ConditionProblemsHealthConcerns`/`ConditionEncounterDiagnosis` → `org.hl7.fhir.r4.model.Condition`. This is a property of the model, not a bug by itself — but it's the root enabler of most of the Section 5 engine bugs below (ambiguous overloads, circular references). | Confirmed model characteristic (#19/#20/#21) — not itself "fixable," just needs to be designed around. |
| `PCMaternal.cql`: `.value as DateTime` (QICore) → `.value as FHIR.dateTime` (USQualityCore) | A real type change on the modelinfo side for `lastEstimatedDeliveryDate()`/`lastTimeOfDelivery()`'s internal cast, not a rename. Callers in `CMS0334FHIRPCCesareanBirth` / `CMS1028FHIRPCSevereOBComps` do date arithmetic against the return value. | **Unconfirmed lead, not fixed** (#19) — flagged for whoever picks up CMS0334/CMS1028's remaining mismatches. |

---

## 2. Resource / Fixture Data Issues

Bad or incomplete test data — typos, wrong references, missing vocabulary — as opposed to a CQL
logic bug. These would fail regardless of which engine ran them.

| Issue | Scope | Status |
|---|---|---|
| Invalid UCUM `system` URI (`https://ucum.org` instead of `http://unitsofmeasure.org`) | 3 `Observation` fixtures (CMS347 ×1, CMS69 ×2) | **Fixed** (#3) |
| Missing vocabulary source file | `ValueSet-2.16.840.1.113762.1.4.1196.394` ("Hypoglycemics Treatment Medications") existed only in the IG Publisher's terminology cache, never committed to `input/vocabulary/valueset/external/` (CMS871) | **Fixed** (#4) |
| Wrong-patient `subject.reference` | 40 files: `Condition`/`Encounter`/`Observation`/`MedicationRequest`/`Procedure`/`ServiceRequest` across CMS347 (34), CMS72 (3), CMS108 (1), NHSNGlycemicControl (1) — typo'd GUIDs (stray character, embedded double-space) | **Fixed** (#7) |
| Wrong-patient `Claim.patient` / `Coverage.beneficiary` / `AllergyIntolerance.patient` | 188 files across 7 measures (CMS72 97, CMS104 70, CMS108/CMS190 2 each, CMS1028/CMS1264/CMS71 1 each, plus 14 `Coverage` + 1 `AllergyIntolerance`) — two fixed placeholder GUIDs from a generation template, never confirmed to exist as real test cases | **Fixed** (#11) |
| `Claim.item` missing `.encounter` / `.diagnosisSequence` links | Required by `claimDiagnosis()`'s principal-diagnosis lookup. 4 real gaps fixed (CMS104 ×2, CMS1017 ×2, incl. one dangling-sequence repair); 19 of 21 flagged cases were false positives (procedure-type claims, correctly have no `diagnosis[]`) | **Fixed (4), 19 false positives closed** (#14) |
| Sparse `MedicationRequest` dosage fixtures (no `doseAndRate`/timing) | Trips an engine bug (see §5, `singleton from empty list`) rather than falling through to a valid fallback. Found via CMS156 probe fixtures; real CMS156 fixtures `c409fbc9`/`07f11229` flagged as likely needing the same enrichment. | **Fixed in probes; watch real CMS156 fixtures** (#25) |
| Automated CORE-field fix via `validate_test_fixtures.py` | 229 files across 10 measures (`subject`/`patient`/`beneficiary` pointing at wrong Patient GUID) + 2 structural anomalies (`null-null.json` rename CMS871, oddly-named Claim CMS1264). Same class as #7/#11; applied via script. | **Fixed** (#27, commit `fe41c7bcc` 2026-09-01) |

---

## 3. Migration Regressions in Measure / Shared CQL Logic

CQL that changed meaning during the QI-Core → USQualityCore conversion — confirmed by diffing
against `dqm-content-qicore-2025` ground truth. These are bugs *we* introduced converting the
logic, not engine or fixture problems.

| Issue | Measures affected | Status |
|---|---|---|
| `doNotPerform` not excluded from `MedicationRequest`/`ServiceRequest` retrieves — a "do NOT perform" record double-counted as an actual order | CMS347, CMS104, CMS71, CMS135, CMS144, CMS645, CMS22 (3 defines) | **Fixed** (#6, #13, #17) |
| `reasonCode` checked instead of the `reasonRefused()` extension accessor on `ServiceNotRequested`-typed retrieves (`ServiceRequest` has no native `reasonCode`) | CMS22 (6 defines) | **Fixed** (#17) |
| Choice-typed `.effective`/`.performed` compared directly against a temporal operator without `.toInterval()` first | CMS72 (fixed, #12); CMS646 (fixed, #17); CMS190 (attempted, **reverted** — wrong diagnosis, see below) | **Fixed (CMS72, CMS646)** |
| `.onset.toInterval()` used for "chronic/still-active condition" checks instead of `.prevalenceInterval()` — a zero-width onset point can never `overlaps` a measurement period years later | CMS347 (×2), CMS90, CMS133, CMS142, CMS143, CMS951 (×2), CMS157 (×2), CMS129, CMS159 (×4), CMS155, CMS128 — 10 measures, ~16 call sites | **Fixed** (#10, #15, #16, #19, #20-final, #22) — required 4 attempts to get the shared-library-vs-per-call-site approach right; see §5 for the engine constraints discovered along the way. Final fix evolved from same-file inline `is`/`as` dispatch into the E-13 base-retrieve generalization (see §5 item 6). |
| `QICoreCommon.prevalenceInterval(Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>)` overload dropped entirely during the `QICoreCommon` → `FHIRCommon`/`USCoreCommon`/`USQualityCoreCommon` library refactor | Root cause of the whole prevalenceInterval saga above | **Fixed** (per-call-site dispatch, then superseded by the E-13 base `[FHIR.Condition: ...]` retrieve — applied 26/30 measures; restoring the Choice overload as a shared declaration is unsafe, see §5) |
| Field swapped from `.recorded` to `.effective`/`.performed` during migration, specifically to dodge a translator ambiguity (wrong field, chosen to avoid a crash) | CMS190 (`NoMedicationAdm.recorded`, fixed), CMS996 (`PCINotDone.performed`, fixed), CMS108 (`DeviceNotApplied.performed`, fixed), CMS68 (`.recorded()`, fixed) | **Fixed via `.ext()` bypass** (#19, #21) — see §5, item 3, for why the direct fix wasn't safe |
| `AHAOverall.cql`'s `overlapsHeartFailureOutpatientEncounter`/`overlapsAfterHeartFailureOutpatientEncounter` narrowed from `Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>` to `ConditionEncounterDiagnosis` only | CMS144 (7 Denominator-Exception defines lose `ConditionProblemsHealthConcerns` support) | **Not fixed** (#19/#20) — needs a genuinely new approach (e.g., widen the function to accept `FHIR.Condition` directly, engine-verified before landing), not another sibling-overload guess |
| `ObservationCancelled`-based Denominator Exceptions commented out on both definition and dependencies, with a stale "no such profile" TODO (profile now exists) — plus a bare-property `.notDoneReason` never converted to fluent-function `( )` syntax | CMS2 (8 mismatches) | **Fixed** (#19) |
| CMS1264's `"Measurement Period"` default a year behind its own fixtures | CMS1264 (57/58 failing) | **Fixed** (#2) |

---

## 4. Third-Party / Vendored CQL Library Issues

Bugs living in vendored, upstream-authored CQL — not this repo's own measure logic, and not (as
far as diagnosed) an engine bug either. These need an upstream fix or a locally-scoped,
documented workaround.

| Issue | Details | Status |
|---|---|---|
| `CumulativeMedicationDuration` (`CMD.cql`) `MedicationDispensePeriod`/`medicationDispensePeriod()` compute `daysSupply` via `(convert D.daysSupply to days).value`, which the engine returns null for (see §5 item 6) — and unlike the sibling `MedicationRequest`-side functions, the dispense-side functions were never patched with the same fallback the library's own authors used elsewhere (their own inline TODO: *"this isn't working as expected, convert results in null"*) | CMS128 (56/58 failing) | **Fixed locally** — CMS128-local workaround function replicating the vendored logic minus the broken `convert`, `CMD.Quantity()` reused for the shared helper (#22). Not patched upstream. |
| Full vendored `CumulativeMedicationDuration` 6.0.000 needed model adaptation beyond a drop-in copy: `timing.repeat.bounds as Interval<DateTime>` (valid under QICore's narrower profiles) had to become `as FHIR.Period` for the FHIR-based USQualityCore stack (3 sites) | CMS156 (only real caller of the vendored `medicationRequestPeriod()`) | **Fixed** (#25, "Round 2b") |
| Same vendored library needed a net-new `ToDays(FHIR.Duration)` helper because the engine's own `convert … to days` / `ConvertQuantity` reject the calendar-word unit spelling (`'day'`) that `FHIRHelpers.ToQuantity()` produces (see §5 item 7) | CMS156 | **Fixed locally** (deviation #3 in the vendor file header) (#25) |
| Same vendored library's `averageDailyDose()` needed to bypass unit-aware `Quantity` division entirely and construct the result via raw decimal math, because unit-aware division across mass/time dimensions collapses to zero on this engine (see §5 item 8) | CMS156 | **Fixed locally** (deviation #4 in the vendor file header) (#25) |
| 11 other libraries carry the `CumulativeMedicationDuration` include as dead weight (never call its functions) | CMS22, CMS347, CMS153, CMS128, CMS138, CMS136, CMS2, Antibiotic, AdvancedIllnessandFrailty, CMS1017, CMS137 | Not a bug — noted cleanup candidate (#25) |

---

## 5. Suspected CQL Engine / Translator Issues — running list

**These are the items to escalate to `clinical_quality_language` (cql-engine, cql-to-elm) and/or
`clinical-reasoning` (cqf-fhir-cql) investigation.** Every entry has a confirmed, reproducible
symptom and a currently-applied CQL-level workaround; the workaround is a mitigation, not proof
the underlying engine/translator behavior is correct. None of these have been fixed upstream.

1. **`Min()` over a homogeneous set of plain `DateTime` values throws `"... not comparable"` /
   `"... not implemented"`.** Per the CQL spec, `DateTime` is an explicitly supported `Min`/`Max`
   operand type. Affects `CMS645FHIRBoneDensityPCADTherapy`, `CMS646FHIRIntravesicalBCGTherapy`,
   `CMS156FHIRHighRiskMedsElderly`, `CMS871FHIRHHHyper`, `CMS1173FHIRDiagnosticDelayVTE`.
   **Workaround**: convert operands to `System.DateTime` via `FHIRHelpers.ToDateTime(...)` before
   calling `Min()`. *(Original entry, External issues log; broadened below.)*

2. **Raw `FHIR.dateTime` values and raw choice-typed fields (`X.effective`) fed directly into
   temporal operators (`before`/`after`/`on or before`) fail the same way as `Min()` above** — this
   turned out to be a broader family than just `Min()`. **Workaround**: convert to
   `System.DateTime` (`FHIRHelpers.ToDateTime(...)`), or for choice types convert to an interval
   first (`start of X.effective.toInterval()`). Applied to `CMS1173` (fully resolved 62 missing
   results) and `CMS156` (resolved 45 missing results). (#24)

   **Post-E-13 reappearance tracked as E-18**: once the E-13 base-`Condition` fix was applied to
   CMS156, its 45 remaining Missing Results reappeared as this exact family — raw `FHIR.dateTime`
   (`authoredOn`) returned from `CMS156FHIRHighRiskMedsElderly.cql:230-243` (the two Index Prescription
   Start Date defines) and fed to a `sort asc` plus a mixed-type `Interval` endpoint, throwing
   `"Values FHIR.dateTime and FHIR.dateTime are not comparable"`. Fixed with the same
   `FHIRHelpers.ToDateTime(...)` conversion (applied 2026-08-31); repro in
   `input/cql/testE18DateTimeCompare.cql`. See `engine-issues.md` **E-18**.

3. **Ambiguous fluent-function overload resolution between sibling USQualityCore profile types
   that share an identical runtime Java class** (e.g. `recorded(Procedure)` vs.
   `recorded(ProcedureNotDone)`, both really `org.hl7.fhir.r4.model.Procedure`). Confirmed
   failure modes, in order attempted:
   - Casting to the *wrong* ancestor (`FHIR.Procedure` instead of `USQualityCore.Procedure`) →
     `"Could not resolve call to operator recorded with signature (FHIR.Procedure)"`.
   - Explicit qualified static invocation (`Library.recorded(X)`) → flatly rejected:
     `"... is a fluent function and can only be invoked with fluent syntax"`, independent of the
     ambiguity itself.
   - A disambiguating `is`/`as` narrowing cast to the concrete sibling type does **not** help
     here (unlike true supertype/subtype situations) — the two profile types are runtime-identical,
     so the engine still can't tell them apart.
   **Working bypass** (not a translator fix): skip the fluent function and read the underlying
   FHIR extension directly, e.g.
   `(X.ext('http://fhir.org/guides/astp/us-quality-core/StructureDefinition/us-quality-core-recorded').value as FHIR.dateTime)`.
   `ext()` is declared once, generically, for `DomainResource`/`Element`, so it has no sibling
   overload to collide against. Confirmed via an isolated repro (`NegationTest.cql`) and applied to
   `CMS68`, `CMS996` (one of two sites), `CMS108`, `CMS190`. (#19, #21) Still unresolved/left
   unfixed at the CQL level for `CMS144`'s `AHAOverall.cql` gap and part of `CMS996`/`CMS108`
   pending an engine-verified disambiguation approach (#19).

4. **A function newly declared for a `Choice<A, B>` type, whose body casts down to one branch (`A`
   or `B`) and calls itself (directly or via the same name), resolves back to the very same
   Choice-typed declaration — a self-referential "circular reference" error** —
   `"Cannot resolve reference to expression or function prevalenceInterval_..._ because it results
   in a circular reference."` I.e., **casting to a concrete member of a Choice type does not
   disambiguate an overload declared for that same Choice type** (it *does* disambiguate against
   overloads declared for unrelated, non-overlapping types — that part works fine, see item 3's
   `AHAOverall.cql` sibling-overload case). Discovered via `Status.cql`'s first
   `prevalenceInterval(Choice<...>)` overload attempt; caused a repo-wide collapse (91.85% → 7.43%
   pass rate, 67 measures to 100% "Missing Results") before being caught. (#20)

5. **Two sibling concrete-type overloads of the *same new function name*, one per branch of a
   Choice type backed by profiles that share a runtime class, are still ambiguous at the
   engine/runtime level even though they are distinct, valid declarations at the CQL/ELM level** —
   `"Ambiguous call to operator 'toPrevalenceInterval(org.hl7.fhir.r4.model.Condition)' in library
   'Status'."` Root cause: `ConditionProblemsHealthConcerns` and `ConditionEncounterDiagnosis` are
   distinct CQL/model types but compile to the identical Java class
   `org.hl7.fhir.r4.model.Condition`, so the runtime literally cannot select between the two
   overloads by argument type. This is the same underlying model characteristic as item 3, exposed
   via declaration instead of invocation. Retroactively explains why `AHAOverall.cql`'s
   sibling-overload fix (item 3-style) for `CMS144` also failed the same way (48/48 "Missing
   Results"). **No safe workaround found that keeps a shared declaration** — the eventual fix
   avoided the pattern entirely (see item 6). (#20)

6. **`as` does not support widening a `Choice<...>` value to a common ancestor type** — e.g.
   `(X as FHIR.Condition)` where `X: Choice<ConditionProblemsHealthConcerns,
   ConditionEncounterDiagnosis>` throws `"Expression of type 'choice<...>' cannot be cast as a
   value of type 'Condition'."` Only narrowing to one of the Choice's own listed member types is
   supported. This directly contradicts an initial (wrong) assumption that "casting up escapes the
   Choice type." **Working fix (current, supersedes the inline `is`/`as` interim — E-13)**: skip any
   shared/ancestor-typed function declaration *and* any union of sibling profile retrieves entirely;
   replace the `ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis` union with a single
   base `[FHIR.Condition: ...]` retrieve. Since both profiles derive from `Condition`, the base
   retrieve captures every instance without ever forming the `Choice<...>`, and
   `.prevalenceInterval()`/`.isVerified()`/`.verified()` then resolve unambiguously against
   `FHIRCommon`'s base-`Condition` overloads. This is fix #20-final's pattern generalized: the base
   retrieve is behaviorally identical to the union (both assert the same `code in <VS>` valueset and
   predicates) and never hits the Choice/overload failure. **Applied across 26/30 E-13 measures**
   (62 site-level edits, 2026-08-28 → 08-31; supersedes the per-call-site `is`/`as` interim that #20
   used on CMS90/CMS129/CMS133/CMS142/CMS143/CMS155/CMS157/CMS159/CMS347/CMS951). See
   `engine-issues.md` **E-13**. Residuals after the base retrieve are genuine logic/fixture buckets,
   not this engine issue (e.g. CMS157's 19 MM are a terminology mismatch, not an E-13 regression).

7. **`convert <Duration> to days` (implicit unit conversion inside the `convert ... to` operator)
   evaluates to `null` at runtime for otherwise valid `FHIR.Duration` values.** Confirmed
   independently in two vendored-library code paths (`MedicationRequestPeriod` had already been
   patched around it by the library's own authors with an inline TODO; the sibling
   `MedicationDispensePeriod` was not). Also reproduced via direct literal-quantity probes (`PG` in
   the CMS156 probe series). **Workaround**: convert explicitly via
   `FHIRHelpers.ToQuantity(Duration)` first — *but see item 8*, that's not sufficient by itself. A
   from-scratch `ToDays(FHIR.Duration)` helper doing manual unit arithmetic was ultimately needed.
   (#22, #25 rounds 3–6)

8. **The engine's `convert … to days` / `ConvertQuantity(...)` operators only accept UCUM-spelled
   unit codes (e.g. `'d'`), but `FHIRHelpers.ToQuantity()` — the engine's own supplied helper for
   converting a `FHIR.Duration` — emits the calendar-word spelling (`'day'`).** A quantity of `5
   'day'` fails to convert (`null`) while the literal `5 'd'` converts fine. This is an
   internal inconsistency between two engine-provided pieces (a helper function and a conversion
   operator) rather than a CQL-authoring mistake. Confirmed via paired literal probes (`PJ` works,
   `PY`/`QB`/`QC` don't). **Workaround**: hand-rolled `ToDays()` helper doing s/min/h/d/wk/mo/a unit
   arithmetic directly on the `FHIR.Duration`'s numeric value/unit fields, bypassing both
   `convert` and `ConvertQuantity` entirely. (#25 rounds 4–6)

9. **Quantity division/multiplication across differing dimensions (e.g. `mg / d`) silently
   collapses small results to exactly zero.** The engine UCUM-normalizes both operands to SI base
   units (g, s) *before* dividing, then rounds the resulting VALUE to 8 decimal places — a quotient
   like `1.25 mg / 5 d` normalizes to ~2.9×10⁻⁹ g/s, which rounds to `0E-8`, i.e. exactly zero, so
   any downstream `> threshold` comparison can never be true. Reproduced with clean synthetic
   literal quantities (probe `QH`), not just derived measure values, ruling out a data problem.
   **Workaround**: construct the result as `System.Quantity { value: <plain decimal arithmetic>,
   unit: '<explicit unit string>' }` instead of using quantity-aware `/`/`*`, so the downstream
   comparison never triggers unit conversion/rounding. (#25 rounds 7–8)

10. **`singleton from <empty list>` aborts/throws at runtime instead of returning `null`, contrary
    to spec.** Confirmed via `singleton from dosage.doseAndRate` where the underlying
    `MedicationRequest` fixture had no `doseAndRate` at all — this killed the calling function
    before a later `Coalesce(...)` fallback branch (which would have short-circuited correctly) ever
    ran. **Workaround (fixture-side, not a CQL change)**: ensure fixtures exercising this code path
    populate `doseAndRate`/timing rather than relying on the empty-list-to-null spec behavior;
    genuinely sparse real-world data would still hit this. (#25, "Open question ANSWERED")

11. **`"Unable to extract codes from fhirType Reference"` thrown before any CQL `define`
    evaluates at all** (trace output for the affected test cases is completely empty — no
    `Patient=`, no population values, nothing). Root-caused as far as static analysis allows to
    `CodeExtractor.getCodesFromBase` in `cqf-fhir-cql` (`clinical-reasoning`), but the generated ELM
    for two independent rewrite attempts was confirmed spec-compliant and null-safe at every
    relevant step (`As` with `strict=false`, `FHIRHelpers.ToConcept`, `InValueSetEvaluator`,
    `FhirModelResolver.toCqlValue()` — none of them explain the crash). Affects `CMS135` (3 cases,
    `MedicationRequest.medication` as a `Reference(Medication)` choice) and `CMS165` (1 case, **no**
    `MedicationRequest`/`Reference`-bearing resource in the fixture at all — confirmed a different
    trigger than CMS135's). **No workaround found; two independent CQL rewrites both failed
    identically.** Needs a live JVM stack trace (debugger breakpoint or increased engine log
    verbosity) — static analysis is exhausted. (Tried-and-reverted section, External issues log)

12. **A `union` branch pattern (`MedicationRequest` + `Task`-rejection lookup) evaluates to an
    empty result even when every referenced field (Task status, MedicationRequest, valueset
    membership, reasonCode) is individually confirmed correct.** `CMS104`'s
    `"Reason For Not Giving Antithrombotic At Discharge"` second union branch, fixture
    `5adc911a-...`. Not yet traced to ELM/engine level — flagged as needing a translator-level trace,
    distinct from (not yet confirmed to be an instance of) any of the above. **Not investigated
    beyond CQL-level confirmation that the data is correct.** (#17)

### Cross-cutting lesson (applies to items 3–6 especially)

A fix to any function declared in a *shared* library, when that function is typed against (or
casts to a member of) a Choice built from USQualityCore sibling profile types, cannot be trusted
without an actual compile/test-run cycle — four consecutive "obviously correct" attempts at the
`prevalenceInterval` fix each failed for a different, only-visible-at-runtime reason (items 4 → 5
→ 6 → working fix). The safe pattern that emerged: **no new shared-library declaration typed on or
dispatching through a sibling-profile Choice at all** — prefer replacing the sibling-profile union at
the retrieval with a single base `[FHIR.Condition: ...]` retrieve (E-13, applied 26/30 measures),
whose `.prevalenceInterval()`/`.verified()` resolve against the single reachable base-`Condition`
overload. Where a base retrieve isn't an option, inline the `is`/`as` branch directly at each call
site down to a function that only ever takes a single concrete, non-profile-specific type (e.g. plain
`FHIR.Condition`) with exactly one reachable declaration. (#20, E-13)

---

## Not fixed / open, cutting across categories

- `CMS144` / `AHAOverall.cql` — Choice-type support gap (§3), blocked on the same engine
  constraint as §5 item 5.
- `CMS996` / `CMS108` — residual `.recorded()`/procedure-retrieve mis-lock: CMS996's 2 `Major
  Surgical Procedure` DenExcl + 1 `AllergyIntolerance` temporal-logic mismatch remain open beyond the
  E-13 STEMI fix (§5 item 3; conversion-notes #34).
- `CMS104` — `Reason For Not Giving Antithrombotic At Discharge` union branch (§5 item 12).
- `CMS72` — 5 `Denominator Exception` mismatches suspected date-window/`calendarDayOfOrDayAfter()`
  logic issue, not yet root-caused to any category above.
- `CMS2` — hardcoded-exception TODO already resolved (§3), but originally also suspected a model
  gap that turned out not to exist; no further open item.
- `CMS135` / `CMS165` — §5 item 11, needs a stack trace.
- `PCMaternal.cql` cast-type change (§1) — unconfirmed impact on `CMS0334`/`CMS1028`.
- **E-17** — `ObservationScreeningAssessment` profile retrieve / `isAssessmentPerformed()` returns
  `[]` (CMS56 10 Numerator + CMS131 6 DenExcl corroboration); no workaround shipped, under
  investigation (`engine-issues.md` E-17).
- **E-18** — raw `FHIR.dateTime` in `sort`/mixed-`Interval` (post-E-13 reappearance of §5 item 2's
  family, CMS156 Index Prescription Start Date); workaround shipped (`FHIRHelpers.ToDateTime`,
  2026-08-31), engine issue still open upstream (`engine-issues.md` E-18).
- `CMS145` — no CQL authored at all; a content-authoring gap, not a conversion bug (out of scope for
  this classification). (CMS149 was in this bucket but its CQL was ported QC→UQC — now **fully
  passing**; see conversion-notes #36.)
- `CMS157` — **terminology/content gap, not engine**: fixtures code the "Cancer" diagnosis in
  ICD-10-CM (`C00.0`, `C40.00`, …) but the measure's "Cancer" valueset
  (`2.16.840.1.113883.3.526.3.1010`) is SNOMED-only, so the (E-13-sound) base `[Condition: "Cancer"]`
  retrieve correctly matches none of them → 19 residual MM. Fixture/valueset alignment needed
  (conversion-notes #33).
