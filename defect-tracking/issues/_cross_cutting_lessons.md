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

This is the pattern that finally worked across 10 measures (E-13's interim fix — see
E-04/E-05/E-06) after four consecutive
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
| CMS145 | — | **No** (content gap resolved) | CQL ported + merged; 0 MR / 6 mismatched (2026-09-08 live) |
| CMS149 | — | **No** (content gap resolved) | CQL ported + merged; 0 MR / 0 mismatched (2026-09-08 live; QI-Core divergences = B-01) |

Measures with engine-issue workarounds applied (residual mismatches are non-engine). Residuals are
current as of the **2026-09-08 live run** unless dated:

| Measure | Issues | Workaround applied | Residual (2026-09-08) |
|---|---|---|---|
| CMS68 | E-22; E-03 | `.ext()` bypass **NOT merged** — feature-branch only (only CMS108 has `.ext()` on `defect-tracking`; see E-03) | **1 Missing Result** (`f2e2e1c0`, all 4 populations) — E-22 `recorded()` ambiguity |
| CMS996 | E-21; E-03 | `.ext()` bypass **NOT merged** (feature-branch only) | 0 MR; **5 mismatched cells, all E-21** |
| CMS108 | E-12; E-17; E-03 | `.ext()` bypass applied 2026-09-08 (per E-03) | 0 MR; **16 of 140** — E-17 profile-retrieve gaps (8 E-12 `TaskRejected`-join cases reclassified 2026-09-08; see E-17) |
| CMS190 | E-17; E-03 | `.ext()` bypass **NOT merged** (feature-branch only) | 0 MR; **23 of 125** — E-17 profile-retrieve gaps |
| CMS1173 | E-01/E-02 | `start of <choice>.toInterval()` applied (E-02; folded from `engine-fixes.md`) | **0 MR / 0 mismatched** (CMS side passes; QI-Core 2 = B-01) |
| CMS156 | E-13; E-18 (both applied) | base `FHIR.Condition` retrieve (08-31) + `FHIRHelpers.ToDateTime` (08-31, per E-18) | 0 MR; **1** mismatched — C-14 |
| CMS128 | E-07; E-13 | `AntidepressantCoveragePeriod()` (E-07) + base retrieve (E-13, 08-30) | **fully passing** (0 MR / 0) |
| CMS133 | E-13 | base retrieve (08-30, verified) | **fully passing** (0 MR / 0) |
| CMS871 | E-01 (open `Min()` question); C-07 | `Min()` fix not applied; residual attributed C-07 | **4 MR + 12 mismatched** — C-07 fixture authoring (16 cells) |
| CMS645 | E-13; E-03; E-21 | base retrieve (E-13) + `.ext()` (E-03) | 0 MR; **3** mismatched (E-21); the earlier "2 Numerator = E-01/`Min()` candidate" is superseded (now E-21 / B-01) |
| CMS646 | E-13; E-21; C-14 | base retrieve applied 2026-08-29 + harness-verified (Stage 2) | **1 MR** + 3 mismatched; the "38 MR baseline" was pre-fix |
| CMS56 | E-13; (E-17 closed for CMS56) | base retrieves (08-30) | **fully passing** (58 / 0) — the 10 E-17-attributed Numerator cells no longer mismatch as of 09-08 |
| CMS131 | E-13; (E-17 closed for CMS131) | base retrieve (08-30/31, verified) | **fully passing** (63 / 0) — the 6 E-17-attributed DenExcl cells no longer mismatch as of 09-08 |
| CMS157 | E-13 (applied 08-31) | base retrieve | **fully passing** (63 / 0) |
| CMS159 | E-13 (applied 08-31); C-10 | base retrieve | 0 MR; **2** mismatched — C-10 |

The remaining four E-13 "pending" measures were all applied **2026-08-31** (E-13 Stage 3
completion): CMS157, CMS159, CMS996, CMS156. Stage/group provenance and the per-measure
verification log follow:

| Group | Issue | Workaround applied | Verified |
|---|---|---|---|
| CMS90, CMS124, CMS129, CMS314, CMS349, CMS771, CMS951, CMS1188 (Stage 2 applied + verified 2026-08-30); CMS133, CMS128, CMS56 (Stage 3 applied + verified 2026-08-30); CMS131 (Stage 3 applied 2026-08-30, verified 2026-08-31); **CMS157, CMS159, CMS996, CMS156 (Stage 3 applied 2026-08-31 — E-13 completion)**; CMS142, CMS143, CMS155, CMS347 (Stage 1 applied 2026-08-29) | E-13 (was E-15) | Base `FHIR.Condition` retrieve (inline `is`/`as` interim superseded) | Varies — genuine logic mismatches now visible; all 30 libraries load with 0 Missing Results in the 2026-09-08 live run |
| original 7 (CMS347, CMS117, CMS138, CMS153, CMS136, CMS155, CMS69) + CMS645, CMS1154, CMS1157, CMS75, CMS142, CMS143, CMS771, CMS1188, CMS124, CMS349, CMS90, CMS646, CMS314, CMS129, CMS951, CMS133, CMS128, CMS56, CMS131, CMS159, CMS996, CMS157, CMS156 | E-13 (was E-15; 30 measures confirmed; CMS22/CMS71 excluded) | Base `FHIR.Condition` retrieve replacing sibling-profile union (62 site-level edits; **all 30 applied, completed 2026-08-31**) | **Verified** — original 7: 0 errors / 0 MR, class A EMPTY (2026-08-28); Stage 1 (2026-08-29): CMS1157 & CMS143 fully passing, CMS645 0 MR + 3, CMS75 0 MR + 7, CMS142 0 MR + 5 (residuals = class B), CMS1154 verified 0 MR + 1 mismatch = E-16 (0125); **Stage 2 (2026-08-29/30): 9 measures harness-verified via the 0733 report** (CMS314/349/1188 fully passing; CMS646 1 residual MR); **Stage 3 (2026-08-30/31): CMS133 applied + verified fully passing (0733 report); CMS128 verified 58 MR → 16 class-B mismatches (0851 report); CMS56 verified 58 MR → 18 (8 class-B Hospice DenExcl + 10 E-17 Numerator, 1228 report); CMS131 applied + verified 2026-08-31 (63 MR → 0 MR, 24 class-B/E-17 DenExcl mismatches + 1 expected-anomaly, 0007 report)**; **Stage 3 completion (2026-08-31): CMS157/CMS159/CMS996/CMS156 applied**. Note: the class-B/E-17 counts in this log are per-verified-report snapshots (0851/1228/0007) and are **superseded for current state** by the 2026-09-08 live run — e.g. CMS56, CMS128, CMS131 are now fully passing and the final four measures show only their non-engine residuals (CMS156 1, CMS159 2, CMS996 5, CMS157 0). |
| CMS1154 | E-13 (was E-15), **E-16** (2026-08-29) | Base `FHIR.Condition` replace (E-13) | **0 MR** — 9/10 passing; 1 residual mismatch (`bc9c82ca` DenExcl 1→0) = E-16 `overlaps` null-high runtime defect (not class B / not drift) |
| CMS104 | E-12 | None | 7 — union branch empty |
| CMS0334, CMS1028 | E-14 | None | 1-2 each — unconfirmed |