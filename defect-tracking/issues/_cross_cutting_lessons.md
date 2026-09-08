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
| CMS108 | E-03, **E-12** | `.ext()` bypass for `.recorded()` (E-03) | 14 — distinct issues (8 of which are E-12 `TaskRejected`-join; reclassified 2026-09-08) |
| CMS190 | E-03 | `.ext()` bypass for `.recorded()` | 11 — distinct issues |
| CMS1173 | E-01, E-02 | **Not applied in current tree** (see E-01) | **62 MR** — `The Minimum operator is not implemented for type {http://hl7.org/fhir}dateTime` |
| CMS156 | **E-13** (was E-15; re-attributed 2026-08-29; was mis-labelled E-01/E-02) | E-13 fix pending (Stage 3) | **177 MR** baseline — cannot load past the condition union |
| CMS128 | E-07; **E-13** (`"Has IPSD and Major Depression Diagnosis"` union, applied 2026-08-30) | Local `AntidepressantCoveragePeriod()` (E-07) + base `FHIR.Condition` retrieve (E-13) | **58 MR → 16 mismatched** (verified 0851): all 8 unique cases × 2 groups `Denominator Exclusion 1→0` = class-B Hospice; E-13 mechanism closed |
| CMS133 | **E-13** | Base `FHIR.Condition` retrieve applied 2026-08-30 | **0 — fully passing** (73 MR baseline → 0 MR / 0 mismatched; No Discrepancies) |
| CMS871 | E-01 | (pending `Min()` fix) | **5 MR** — `Unable to locate ValueSet ... 1196.394` + `Invalid Interval` |
| CMS645 | **E-13** (was E-15; re-attributed; baseline MR was the union translate failure), E-03 | Base `FHIR.Condition` replace (E-13) + `.ext()` | **0 MR** + 3 mismatches: DenException = Patient-Refusal negation (class B); 2 Numerator 0→1 = E-01/`Min()` candidate |
| CMS646 | **E-13** (was E-15; re-attributed 2026-08-29) | E-13 base `FHIR.Condition` fix applied 2026-08-29 (pending harness verification) | **38 MR** baseline — cannot load past the condition union |
| CMS56 | **E-13**, **E-17** | Base `FHIR.Condition` retrieves applied 2026-08-30 (4 DenExcl defines) | **58 MR → 18 mismatched** (verified 1228): 8 `Denominator Exclusion 1→0` = class-B Hospice (NOT the 4 E-13-edited excludes); 10 `Numerator 1→0` = **E-17** `ObservationScreeningAssessment` retrieve gap; E-13 mechanism closed |
| CMS90, CMS124, CMS129, CMS314, CMS349, CMS771, CMS951, CMS1188 (Stage 2 applied + verified 2026-08-30); CMS133, CMS128, CMS56 (Stage 3 applied + verified 2026-08-30); CMS131 (Stage 3 applied 2026-08-30, verified 2026-08-31); CMS157, CMS159, CMS996, CMS156 (Stage 3 pending); CMS142, CMS143, CMS155, CMS347 (Stage 1 applied) | E-13 (was E-15) | Base `FHIR.Condition` retrieve (inline `is`/`as` interim superseded) | Varies — genuine logic mismatches now visible |
| original 7 (CMS347, CMS117, CMS138, CMS153, CMS136, CMS155, CMS69) + CMS645, CMS1154, CMS1157, CMS75, CMS142, CMS143, CMS771, CMS1188, CMS124, CMS349, CMS90, CMS646, CMS314, CMS129, CMS951, CMS133, CMS128, CMS56, CMS131, CMS159, CMS996, CMS157, CMS156 | E-13 (was E-15; 30 measures confirmed; CMS22/CMS71 excluded) | Base `FHIR.Condition` retrieve replacing sibling-profile union (62 site-level edits / 26 measures applied; 4 pending Stage 3) | **Verified** — original 7: 0 errors / 0 MR, class A EMPTY (2026-08-28); Stage 1 (2026-08-29): CMS1157 & CMS143 fully passing, CMS645 0 MR + 3, CMS75 0 MR + 7, CMS142 0 MR + 5 (residuals = class B), CMS1154 verified 0 MR + 1 mismatch = E-16 (0125); **Stage 2 (2026-08-29/30): 9 measures harness-verified via 0733 report** (CMS314/349/1188 fully passing; CMS646 1 residual MR); **Stage 3 (2026-08-30/31): CMS133 applied + verified fully passing (0733 report); CMS128 verified 58 MR → 16 class-B mismatches (0851 report); CMS56 verified 58 MR → 18 (8 class-B Hospice DenExcl + 10 E-17 Numerator, 1228 report); CMS131 applied + verified 2026-08-31 (63 MR → 0 MR, 24 class-B/E-17 DenExcl mismatches + 1 expected-anomaly, 0007 report)** |
| CMS1154 | E-13 (was E-15), **E-16** (2026-08-29) | Base `FHIR.Condition` replace (E-13) | **0 MR** — 9/10 passing; 1 residual mismatch (`bc9c82ca` DenExcl 1→0) = E-16 `overlaps` null-high runtime defect (not class B / not drift) |
| CMS104 | E-12 | None | 7 — union branch empty |
| CMS0334, CMS1028 | E-14 | None | 1-2 each — unconfirmed |