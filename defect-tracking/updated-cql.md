<!-- GENERATED from defect-tracking/cql-changes.jsonl — do not edit by hand -->

# Updated CQL

- Generated: 2026-09-10T08:57:06
- Source: `defect-tracking/cql-changes.jsonl` (append-only, one JSON line per CQL change)
- Entries recorded: 3

_This file is regenerated from the CQL-change log; do not hand-edit. Append new changes to `cql-changes.jsonl` (or use `cql_changes.record()`), then re-run `scripts/run_reports.py`._

| Ref | Date | Measure | Kind | Issues | Engine | Verified |
|-----|------|---------|------|--------|--------|----------|
| CQL-003 | 2026-09-09 | CMS190FHIRVTEProphylaxisICU | fix | M-04, E-17 | 5.3 | passing 3585→3598; failing 379→366; 0 unattributed; 13/13 CMS190 Numerator flip (0→1). 13 cases reclassified from E-17 to M-04; M-04 marked resolved, its cases.csv rows removed as no longer failing. |
| CQL-002 | 2026-09-08 | CMS108FHIRVTEProphylaxis | fix | M-04, E-03 | — | 8 CMS108 negation cases removed from E-17 as resolved; run state 3585 passing / 379 failing / 683 stale attributions |
| CQL-001 | historical | (all measures) | migration-origin | M-04 | — | — |

### CQL-003 — 2026-09-09 — CMS190FHIRVTEProphylaxisICU (fix)

**CMS190 port of the CMS108 negation fix: recorded()/us-quality-core-recorded ext + revert end-of-Period workaround (M-04)**

- **Library**: `input/cql/CMS190FHIRVTEProphylaxisICU.cql`
- **Summary**: Ported CQL-002 to CMS190. Medication arm ~line 301 now compares `recorded()`; device arm ~line 373 reads the `us-quality-core-recorded` extension; the temporary `start of ... authoredOn` end-of-Period workaround at ~line 385 was reverted to a plain `authoredOn` comparison, which is now safe with dateTime inputs. All 13 CMS190 not-done fixtures carry the `us-quality-core-recorded` extension. Verified under LS v5.3: all 13 previously-failing Numerator cases flip 0→1.

**1. line ~301 "No VTE Prophylaxis Medication Administered Or Ordered"**

```cql
authoredOn: NoMedicationAdm.effective
```
→
```cql
authoredOn: NoMedicationAdm.recorded()
```

**2. line ~373 device arm (ProcedureNotDone); removed `let DeviceNotDoneTiming: DeviceNotApplied.performed`**

```cql
let DeviceNotDoneTiming: DeviceNotApplied.performed ... authoredOn: DeviceNotDoneTiming
```
→
```cql
authoredOn: DeviceNotApplied.ext('http://fhir.org/guides/astp/us-quality-core/StructureDefinition/us-quality-core-recorded').value as FHIR.dateTime
```

**3. line ~385 "Encounter With VTE Prophylaxis Received Day Of Or Day After..." comparison**

```cql
start of NoVTEMedication.authoredOn during day of ( end of AnesthesiaProcedure.performed.toInterval ( ) ).calendarDayOfOrDayAfter ( )
```
→
```cql
NoVTEMedication.authoredOn during day of ( end of AnesthesiaProcedure.performed.toInterval ( ) ).calendarDayOfOrDayAfter ( )
```

**Issues**: M-04, E-17 · **Engine**: 5.3 · **Verified**: passing 3585→3598; failing 379→366; 0 unattributed; 13/13 CMS190 Numerator flip (0→1). 13 cases reclassified from E-17 to M-04; M-04 marked resolved, its cases.csv rows removed as no longer failing. · **Propagation**: input/resources/measure/CMS190FHIRVTEProphylaxisICU.json embedded CQL resynced by hand (2026-09-09); no IG Publisher refresh · **Commit**: pending

### CQL-002 — 2026-09-08 — CMS108FHIRVTEProphylaxis (fix)

**Negation authoredOn uses recorded()/us-quality-core-recorded ext (M-04/E-03 bypass)**

- **Library**: `input/cql/CMS108FHIRVTEProphylaxis.cql`
- **Summary**: Restored the source-level `recorded()` in the medication negation arm and switched the device arm to the `us-quality-core-recorded` extension value via `.ext()`, so `authoredOn` is a `FHIR.dateTime` instead of a start-only Period from the not-done fixture. Resolved 8 CMS108 negation cases previously mis-attributed to E-17; they now pass.

**1. line ~327 "No VTE Prophylaxis Medication Administered Or Ordered" (medication negation arm)**

```cql
authoredOn: NoMedicationAdm.effective
```
→
```cql
authoredOn: NoMedicationAdm.recorded()
```

**2. ~lines 409-412 device arm (ProcedureNotDone)**

```cql
let DeviceNotDoneTiming: DeviceNotApplied.performed ... authoredOn: DeviceNotDoneTiming
```
→
```cql
authoredOn: DeviceNotApplied.ext('http://fhir.org/guides/astp/us-quality-core/StructureDefinition/us-quality-core-recorded').value as FHIR.dateTime
```

**Issues**: M-04, E-03 · **Verified**: 8 CMS108 negation cases removed from E-17 as resolved; run state 3585 passing / 379 failing / 683 stale attributions · **Propagation**: input/resources/measure/CMS108FHIRVTEProphylaxis.json resynced (commit 177df9074 "resource update after IG Publisher refresh") · **Commit**: 0cdde575b ("bypass fix for CMS108")

### CQL-001 — historical — (all measures) (migration-origin)

**Negation fields swapped `.recorded` → `.effective`/`.performed` during US QualityCore migration**

- **Library**: `input/cql/*.cql`
- **Summary**: During the QICore→USQualityCore migration the not-done negation resources' `authoredOn` was changed from the source-level `recorded()` (a FHIR `dateTime`) to the comparison-site `.effective`/`.performed` (a `Period`, start-only on not-done fixtures) to dodge an upstream translator ambiguity (E-03). That left `X during day of Y` null on open-ended Periods and silently dropped cases from Numerator/exception logic. This is the historical origin of M-04; CMS108/CMS190 later reverted the swap at the source level (CQL-002/CQL-003).

**1. negation `authoredOn` fields across measure CQL (exemplar: "No VTE Prophylaxis Medication Administered Or Ordered", medication arm)**

```cql
authoredOn: NoMedicationAdm.recorded()
```
→
```cql
authoredOn: NoMedicationAdm.effective
```

**2. negation device arms (exemplar: "No Mechanical VTE Prophylaxis Performed Or Ordered")**

```cql
authoredOn: DeviceNotApplied.recorded()
```
→
```cql
authoredOn: DeviceNotApplied.performed
```

**Issues**: M-04 · **Propagation**: n/a (feature-branch migration; reverted per measure in CQL-002/CQL-003) · **Commit**: unknown — see feature-branch migration history

