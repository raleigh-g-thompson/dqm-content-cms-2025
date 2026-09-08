# Discrepancy Report
| Details | Value |
| --- | --- |
| Generated | 2026-09-08 15:14:27.439267 |
| Total Measures | 74 |
| Total Test Cases | 3964 |
| Measures with Discrepancies | 30 |
| Known Issues (resolution pending) | 38 issues / 1004 test cases |
| Passing Test Cases | 3585 (90.44%) |
| Failing Test Cases | 379 (9.56%) |
| &nbsp;&nbsp;— attributed to an open known issue | 379 |
| &nbsp;&nbsp;— **UNATTRIBUTED** (regression / untriaged) | **0** |
| QICore Passing Test Cases | 3045 (76.82%) |
| QICore Failing Test Cases | 919 (23.18%) |
| QICore Measures with Discrepancies | 35 |



## Attribution Health

_Every percentage in the summary uses the full 3,964-case denominator. **Unattributed** is the trust metric: 0 means every failing case has a written, case-linked cause._

### Stale attributions (683)

Cases an open issue still claims that now pass. These inflate any known-issues-excluded score; each is a candidate for closing the issue or trimming its case list.

| Issue | Cases now passing |
| --- | --- |
| B-01 | 613 |
| C-12 | 1 |
| C-14 | 2 |
| E-17 | 9 |
| E-23 | 58 |

### Repro-scaffold references (2)

Issues citing a `testE*` repro library. These are excluded from scoring by design, so they never appear as pass or fail -- listed for completeness, not as a problem to fix.

| Issue | Measure |
| --- | --- |
| E-11 | testE11MedicationReference |
| E-18 | testE18DateTimeCompare |


## Known Issues (resolution-pending)

| ID | Issue | Category | Status | Affected measures | Tracked test cases |
|---|------|---------|-------|-----|------|
| B-01 | QI-Core baseline `qicore-2025-actual-results.csv` (post 2026-09-05 fresh re-run) tracks 670 test cases across 26 measures where the fresh QI-Core engine output disagrees with the fixture MeasureReports' expectations (CMS engine matches; baseline is fresh, not stale, but the disagreements remain) | baseline | **Confirmed (refreshed 2026-09-08)** | CMS0334FHIRPCCesareanBirth, CMS1028FHIRPCSevereOBComps, CMS104FHIRSTKDCAntithrombotic, CMS108FHIRVTEProphylaxis, CMS1173FHIRDiagnosticDelayVTE, CMS1188FHIRHIVSTITesting, CMS1264FHIRECATREHQR, CMS129FHIRProstCaBoneScanUse, CMS135FHIRACEIorARBorARNIforHF, CMS144FHIRHFBetaBlockerForLVSD, CMS145FHIRCADBBlockerTPMIorLVSD, CMS149FHIRDementiaCognitiveAssess, CMS190FHIRVTEProphylaxisICU, CMS22FHIRPCSBPScreeningFollowUp, CMS347FHIRStatinPreventionTxCVD, CMS506FHIRSafeUseofOpioids, CMS645FHIRBoneDensityPCADTherapy, CMS646FHIRIntravesicalBCGTherapy, CMS71FHIRSTKAnticoagAFFlutter, CMS72FHIRSTKAntithromboticDay2, CMS771FHIRUrinarySymptomScoreBPH, CMS819FHIRHHORAE, CMS871FHIRHHHyper, CMS996FHIRAptTxforSTEMI, NHSNAcuteCareHospitalMonthlyInitialPopulation1, NHSNGlycemicControlHypoglycemiaInitialPopulation | 670 |
| C-03 | CMS986 malnutrition Measure-Observation component rows authored in fixture MeasureReports do not match what the measure resource / CQL emits (CQL has function defines for `Measure Observation 1/2/3/4` score components but the measure resource population criteria only wire the count `Measure Observation`, not the score values; both engines return obs count = 0 so the authored score rows from MR are unreproducible) | content | **Confirmed** | CMS986FHIRMalnutritionScore | 120 |
| C-04 | CMS1017 fall-prevention HHFI Denominator/Numerator/Measure-Observation rows authored in fixture MeasureReports do not match what the fixture's resources + CQL emits (fixtures carry no BMI Observations / no AdverseEvent entries, yet expected Denom Observation = 2/4/6 etc.) | content | **Confirmed** | CMS1017FHIRHHFI | 55 |
| C-06 | CMS816 HH Hypoglycemia fixture MR/Denominator authoring mismatch (shared %) | content | **Confirmed** | CMS816FHIRHHHypo | 12 |
| C-07 | CMS871 HH Hyperglycemia fixture MR/Denominator authoring mismatch (shared %) | content | **Confirmed** | CMS871FHIRHHHyper | 16 |
| C-08 | CMS142 Diabetes Communication Hand-Off fixture MR authoring mismatch (shared %) | content | **Confirmed** | CMS142FHIRCommWithDrManagingDiab | 5 |
| C-09 | CMS819 HH Opioid-Related Adverse Events fixture MR authoring mismatch (shared %) | content | **Confirmed** | CMS819FHIRHHORAE | 2 |
| C-10 | CMS159 Depression Remission fixture MR authoring mismatch (shared %) | content | **Confirmed** | CMS159FHIRDepRemissionat12Months | 2 |
| C-11 | CMS0334 Cesarean Birth fixture MR authoring mismatch (shared %) | content | **Confirmed** | CMS0334FHIRPCCesareanBirth | 1 |
| C-12 | CMS1218 HH Respiratory Failure fixture MR authoring mismatch (shared %) | content | **Confirmed** | CMS1218FHIRHHRF | 1 |
| C-13 | CMSFHIR844 Hybrid Hospital-Wide Mortality fixture MR Initial Population authoring mismatch (shared %) - both engines 0/1 vs exp 1/2 | content | **Confirmed** | CMSFHIR844HybridHospitalWideMortality | 2 |
| C-14 | CMS72 / CMS104 / CMS646 / CMS71 residual fixture MR authoring gaps where both engines agree (Denominator-Exception / Denominator / Numerator / IP populations hand-authored in fixture MR but uncomputable from present resources) | content | **Confirmed** | CMS72FHIRSTKAntithromboticDay2, CMS104FHIRSTKDCAntithrombotic, CMS646FHIRIntravesicalBCGTherapy, CMS71FHIRSTKAnticoagAFFlutter, CMS145FHIRCADBBlockerTPMIorLVSD, CMS156FHIRHighRiskMedsElderly, CMS1028FHIRPCSevereOBComps, CMS996FHIRAptTxforSTEMI | 28 |
| E-01 | `Min()` over DateTime throws | engine | **Confirmed** | CMS1173, CMS871, CMS645, CMS646, CMS156 | 0 |
| E-02 | Raw `FHIR.dateTime` / choice-typed `X.effective` in temporal operators fails | engine | **Confirmed** | CMS1173, CMS156 | 0 |
| E-03 | Fluent overload ambiguity (sibling profiles, same Java class) | engine | **Confirmed** | CMS68, CMS996, CMS108, CMS190, CMS144 | 0 |
| E-04 | Choice-type self-reference circular dispatch | engine | **Confirmed** | CMS90, CMS133, CMS142, CMS143, CMS155, CMS157, CMS159, CMS951 | 0 |
| E-05 | Sibling overloads ambiguous at runtime (same Java class) | engine | **Confirmed** | CMS90, CMS133, CMS142, CMS143, CMS155, CMS157, CMS159, CMS951, CMS144 | 0 |
| E-06 | `as` cannot widen Choice to ancestor type | engine | **Confirmed** |  | 0 |
| E-07 | `convert Duration to days` returns null | engine | **Confirmed** | CMS128, CMS156 | 0 |
| E-08 | `ConvertQuantity` rejects calendar-word units from `ToQuantity` | engine | **Confirmed** | CMS156 | 0 |
| E-09 | Quantity division across dimensions rounds to zero | engine | **Confirmed** | CMS156 | 0 |
| E-10 | `singleton from empty list` throws instead of returning null | engine | **Confirmed** | CMS156 | 0 |
| E-11 | `Unable to extract codes from fhirType Reference` | engine | **Confirmed** | CMS135, CMS165 | 5 |
| E-12 | Union branch evaluates empty despite correct data | engine | **Confirmed** | CMS104, CMS108FHIRVTEProphylaxis | 8 |
| E-13 | Union of `ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis` → `Choice<...>` fed to `prevalenceInterval()` mis-resolves: missing FHIRCommon Choice overload + translator cannot resolve the call (the Choice should coerce to base `Condition` — engine/translator issue) | engine | **Confirmed / Applied** | CMS347, CMS117, CMS138, CMS153, CMS136, CMS155, CMS69, CMS645, CMS1154, CMS1157, CMS75, CMS142, CMS143, CMS771, CMS1188, CMS124, CMS349, CMS90, CMS646, CMS314, CMS129, CMS951, CMS128, CMS56, CMS131, CMS159, CMS133, CMS996, CMS157, CMS156, CMS22, CMS71 | 0 |
| E-14 | `PCMaternal.cql` cast type change (`.value as DateTime` → `.value as FHIR.dateTime`) | engine | **Suspected** | CMS0334, CMS1028 | 0 |
| E-16 | `overlaps` on a half-open null-high interval (`[start, null)`) evaluates false — `FHIRCommon.prevalenceInterval()` inactive branch | engine | **Confirmed** | CMS1154, CMS347FHIRStatinPreventionTxCVD, CMS1154ScreeningPrediabetesFHIR | 2 |
| E-17 | `us-quality-core-*` profile retrieves return empty (broader than ObservationScreeningAssessment — corroborated by VTE CMS108/CMS190 medicationadministration / procedure / medicationrequest / servicerequest / condition profile-retrieve gaps) | engine | **Confirmed** | CMS56FHIRFunctionalStatus, CMS131FHIRDiabetesEyeExam, CMS108FHIRVTEProphylaxis, CMS190FHIRVTEProphylaxisICU | 40 |
| E-18 | Raw `FHIR.dateTime` returned from a define feeding `sort` and a mixed-type `Interval` endpoint throws `"Values FHIR.dateTime and FHIR.dateTime are not comparable"` (CMS156 Index Prescription Start Date — the post-E-13 reappearance of the E-01/E-02 family) | engine | **Confirmed** | CMS156 | 1 |
| E-19 | `doNotPerform` negative-indication `MedicationRequest`s counted as positive orders by CMS347's `[MedicationRequest: "..."]` retrieve (Numerator double-count) | engine | **Confirmed** | CMS347FHIRStatinPreventionTxCVD | 23 |
| E-21 | `us-quality-core-*` profile retrieves return empty for screening-assessment plus service/medication/procedure profile families (E-17 extended): CMS22 blood-pressure-screen + CMS135 ACEI/ARB HF + CMS144 HFrEF beta-blocker + CMS771 urinary-symptom + CMS177 MDD-screening + CMS645 CAD-bone-density + CMS71 anticoagulation-FLutter | engine | **Confirmed** | CMS22FHIRPCSBPScreeningFollowUp, CMS135FHIRACEIorARBorARNIforHF, CMS144FHIRHFBetaBlockerForLVSD, CMS771FHIRUrinarySymptomScoreBPH, CMS177FHIRChildMDDSuicideAssmt, CMS645FHIRBoneDensityPCADTherapy, CMS71FHIRSTKAnticoagAFFlutter, CMS2FHIRPCSDepScreenAndFollowUp, CMS996FHIRAptTxforSTEMI, CMS646FHIRIntravesicalBCGTherapy, CMS145FHIRCADBBlockerTPMIorLVSD, CMS104FHIRSTKDCAntithrombotic | 69 |
| E-22 | `recorded(...)` operator ambiguous call in `USQualityCoreCommon` library throws (CMS68 test-case `f2e2e1c0` produces Missing Results across all 4 populations - engine error) | engine | **Confirmed** | CMS68FHIRDocumentationCurrentMeds | 1 |
| E-23 | QI-Core engine-side regressions surfaced by 2026-09-05 fresh re-run — QI-Core 4.11.0 / translator 5.2.0 returns 0 for InitialPopulation / Denominator / Numerator populations where CMS engine + fixture MeasureReports agree on 1 (consistent with a profile-retrieve failure on QI-Core's stricter US Quality Core / QI-Core 6.0.0 profile handling) | engine | **Confirmed (refreshed 2026-09-05)** | CMS347FHIRStatinPreventionTxCVD, CMS145FHIRCADBBlockerTPMIorLVSD, CMS144FHIRHFBetaBlockerForLVSD, CMS645FHIRBoneDensityPCADTherapy, CMS135FHIRACEIorARBorARNIforHF, CMS129FHIRProstCaBoneScanUse, CMS771FHIRUrinarySymptomScoreBPH, CMS149FHIRDementiaCognitiveAssess, CMS190FHIRVTEProphylaxisICU, CMS108FHIRVTEProphylaxis, CMS1028FHIRPCSevereOBComps, CMS996FHIRAptTxforSTEMI, CMS506FHIRSafeUseofOpioids | 64 |
| F-06 | Sparse `MedicationRequest` dosage fixtures trip `singleton from empty list` | fixture | Fixed in probes | CMS156 | 0 |
| M-04 | Field swapped `.recorded` → `.effective`/`.performed` to dodge a translator ambiguity | migration | Fixed (.ext() bypass) | CMS190, CMS996, CMS108, CMS68 | 0 |
| M-05 | `AHAOverall.cql` Choice narrowing dropped `ConditionProblemsHealthConcerns` support (CMS144) | migration | Not fixed | CMS144 | 0 |
| V-01 | Vendored `CMD.cql` `convert…to days` null / calendar-unit bug (medication dispense side) | vendored | Fixed locally | CMS128 | 0 |
| V-02 | Vendored `CumulativeMedicationDuration` 6.0.000 model adaptation (CMS156) | vendored | Fixed locally | CMS156 | 0 |


| Discrepancy Summary | Measure Count | Test Case Count |
|---|:---:|:---:|
| Missing Results | 5 | 10 |
| Missing Populations | 1 | 4 |
| Mismatched Test Cases | 28 | 365 |



_Note: Measures can have multiple discrepancies, so the Measures with Discrepancies count may not match the summary counts._
## CMS vs QICore Comparison

| Measure | CMS Passing / Failing Test Cases | QICore Passing / Failing Test Cases | Notes |
|---|:---:|:---:|---|
| CMS2FHIRPCSDepScreenAndFollowUp | 28 / 8 | 36 / 0 | CMS has discrepancies, QICore passes |
| CMS22FHIRPCSBPScreeningFollowUp | 32 / 12 | 43 / 1 | Both have discrepancies |
| CMS50FHIRReceiptofSpecialistReport | 33 / 0 | 33 / 0 | Match — both pass |
| CMS56FHIRFuncStatHipReplacement | 58 / 0 | 58 / 0 | Match — both pass |
| CMS68FHIRDocumentationCurrentMeds | 18 / 1 | 19 / 0 | CMS has discrepancies, QICore passes |
| CMS69FHIRPCSBMIScreenAndFollowUp | 63 / 0 | 63 / 0 | Match — both pass |
| CMS71FHIRSTKAnticoagAFFlutter | 75 / 8 | 80 / 3 | Both have discrepancies |
| CMS72FHIRSTKAntithromboticDay2 | 145 / 13 | 51 / 107 | Both have discrepancies |
| CMS74FHIRDentalCariesPrevention | 20 / 0 | 20 / 0 | Match — both pass |
| CMS75FHIRChildrenDentalDecay | 20 / 0 | 20 / 0 | Match — both pass |
| CMS90FHIRFSAforHeartFailure | 37 / 0 | 37 / 0 | Match — both pass |
| CMS104FHIRSTKDCAntithrombotic | 67 / 15 | 13 / 69 | Both have discrepancies |
| CMS108FHIRVTEProphylaxis | 124 / 16 | 114 / 26 | Both have discrepancies |
| CMS117FHIRChildImmunStatus | 45 / 0 | 45 / 0 | Match — both pass |
| CMS122FHIRDiabetesAssessGT9Pct | 55 / 0 | 55 / 0 | Match — both pass |
| CMS124FHIRCervicalCancerScreen | 34 / 0 | 34 / 0 | Match — both pass |
| CMS125FHIRBreastCancerScreen | 66 / 0 | 66 / 0 | Match — both pass |
| CMS128FHIRAntidepressantMgmt | 29 / 0 | 29 / 0 | Match — both pass |
| CMS129FHIRProstCaBoneScanUse | 51 / 0 | 4 / 47 | CMS passes, QICore has discrepancies |
| CMS130FHIRColorectalCancerScrn | 64 / 0 | 64 / 0 | Match — both pass |
| CMS131FHIRDiabetesEyeExam | 63 / 0 | 63 / 0 | Match — both pass |
| CMS133FHIRCataracts2040BCVA90Days | 73 / 0 | 73 / 0 | Match — both pass |
| CMS135FHIRACEIorARBorARNIforHF | 29 / 11 | 10 / 30 | Both have discrepancies |
| CMS136FHIRChildADHDMedFollowUp | 64 / 0 | 64 / 0 | Match — both pass |
| CMS137FHIRSUDTxInitEngagement | 45 / 0 | 45 / 0 | Match — both pass |
| CMS138FHIRTobaccoScrnCessation | 47 / 0 | 47 / 0 | Match — both pass |
| CMS139FHIRFallRiskScreening | 29 / 0 | 29 / 0 | Match — both pass |
| CMS142FHIRCommWithDrManagingDiab | 27 / 5 | 27 / 5 | Both have discrepancies |
| CMS143FHIRPOAGOpticNerveEval | 32 / 0 | 32 / 0 | Match — both pass |
| CMS144FHIRHFBetaBlockerForLVSD | 45 / 3 | 4 / 44 | Both have discrepancies |
| CMS145FHIRCADBBlockerTPMIorLVSD | 47 / 6 | 4 / 49 | Both have discrepancies |
| CMS146FHIRApproTestPharyngitis | 38 / 0 | 38 / 0 | Match — both pass |
| CMS149FHIRDementiaCognitiveAssess | 33 / 0 | 24 / 9 | CMS passes, QICore has discrepancies |
| CMS153FHIRChlamydiaScreening | 32 / 0 | 32 / 0 | Match — both pass |
| CMS154FHIRAppropriateTxforURI | 33 / 0 | 33 / 0 | Match — both pass |
| CMS155FHIRWgtAssessCounseling | 34 / 0 | 34 / 0 | Match — both pass |
| CMS156FHIRHighRiskMedsElderly | 58 / 1 | 58 / 1 | Both have discrepancies |
| CMS157FHIRPainIntensityQuantified | 63 / 0 | 63 / 0 | Match — both pass |
| CMS159FHIRDepRemissionat12Months | 65 / 2 | 65 / 2 | Both have discrepancies |
| CMS165FHIRControllingHighBP | 67 / 1 | 67 / 1 | Both have discrepancies |
| CMS177FHIRChildMDDSuicideAssmt | 40 / 1 | 41 / 0 | CMS has discrepancies, QICore passes |
| CMS190FHIRVTEProphylaxisICU | 102 / 23 | 99 / 26 | Both have discrepancies |
| CMS314FHIRHIVViralSuppression | 43 / 0 | 43 / 0 | Match — both pass |
| CMS0334FHIRPCCesareanBirth | 137 / 1 | 136 / 2 | Both have discrepancies |
| CMS347FHIRStatinPreventionTxCVD | 164 / 24 | 76 / 112 | Both have discrepancies |
| CMS349FHIRHIVScreening | 36 / 0 | 36 / 0 | Match — both pass |
| CMS506FHIRSafeUseofOpioids | 51 / 0 | 46 / 5 | CMS passes, QICore has discrepancies |
| CMSFHIR529HybridHospitalWideReadmission | 1 / 0 | 1 / 0 | Match — both pass |
| CMS645FHIRBoneDensityPCADTherapy | 48 / 3 | 15 / 36 | Both have discrepancies |
| CMS646FHIRIntravesicalBCGTherapy | 34 / 4 | 28 / 10 | Both have discrepancies |
| CMS771FHIRUrinarySymptomScoreBPH | 24 / 7 | 9 / 22 | Both have discrepancies |
| CMS816FHIRHHHypo | 16 / 12 | 16 / 12 | Both have discrepancies |
| CMS819FHIRHHORAE | 26 / 2 | 24 / 4 | Both have discrepancies |
| CMS826FHIRHHPI | 9 / 0 | 9 / 0 | Match — both pass |
| CMS832FHIRHHAKI | 37 / 0 | 37 / 0 | Match — both pass |
| CMSFHIR844HybridHospitalWideMortality | 8 / 2 | 8 / 2 | Both have discrepancies |
| CMS871FHIRHHHyper | 10 / 16 | 10 / 16 | Both have discrepancies |
| CMS951FHIRKidneyHealthEval | 55 / 0 | 55 / 0 | Match — both pass |
| CMS986FHIRMalnutritionScore | 26 / 120 | 27 / 119 | Both have discrepancies |
| CMS996FHIRAptTxforSTEMI | 109 / 5 | 108 / 6 | Both have discrepancies |
| CMS1017FHIRHHFI | 10 / 55 | 10 / 55 | Both have discrepancies |
| CMS1028FHIRPCSevereOBComps | 140 / 1 | 136 / 5 | Both have discrepancies |
| CMS1056FHIRCTClinical | 10 / 0 | 10 / 0 | Match — both pass |
| CMS1074FHIRCTIQR | 10 / 0 | 10 / 0 | Match — both pass |
| CMS1154ScreeningPrediabetesFHIR | 9 / 1 | 9 / 1 | Both have discrepancies |
| CMS1157FHIRHIVRetention | 27 / 0 | 27 / 0 | Match — both pass |
| CMS1173FHIRDiagnosticDelayVTE | 65 / 0 | 63 / 2 | CMS passes, QICore has discrepancies |
| CMS1188FHIRHIVSTITesting | 34 / 0 | 32 / 2 | CMS passes, QICore has discrepancies |
| CMS1206FHIRCTOQR | 10 / 0 | 10 / 0 | Match — both pass |
| CMS1218FHIRHHRF | 69 / 0 | 69 / 0 | Match — both pass |
| CMS1244FHIRECATHOQR | 72 / 0 | 72 / 0 | Match — both pass |
| CMS1264FHIRECATREHQR | 58 / 0 | 1 / 57 | CMS passes, QICore has discrepancies |
| NHSNAcuteCareHospitalMonthlyInitialPopulation1 | 27 / 0 | 0 / 27 | CMS passes, QICore has discrepancies |
| NHSNGlycemicControlHypoglycemiaInitialPopulation | 80 / 0 | 76 / 4 | CMS passes, QICore has discrepancies |


## Measures with No Discrepancies

### CMS Measures (44)
- CMS50FHIRReceiptofSpecialistReport [ [cql] ](../../input/cql/CMS50FHIRReceiptofSpecialistReport.cql) [ [test results] ](../../input/tests/results/CMS50FHIRReceiptofSpecialistReport.txt) — matches QICore
- CMS56FHIRFuncStatHipReplacement [ [cql] ](../../input/cql/CMS56FHIRFuncStatHipReplacement.cql) [ [test results] ](../../input/tests/results/CMS56FHIRFuncStatHipReplacement.txt) — matches QICore
- CMS69FHIRPCSBMIScreenAndFollowUp [ [cql] ](../../input/cql/CMS69FHIRPCSBMIScreenAndFollowUp.cql) [ [test results] ](../../input/tests/results/CMS69FHIRPCSBMIScreenAndFollowUp.txt) — matches QICore
- CMS74FHIRDentalCariesPrevention [ [cql] ](../../input/cql/CMS74FHIRDentalCariesPrevention.cql) [ [test results] ](../../input/tests/results/CMS74FHIRDentalCariesPrevention.txt) — matches QICore
- CMS75FHIRChildrenDentalDecay [ [cql] ](../../input/cql/CMS75FHIRChildrenDentalDecay.cql) [ [test results] ](../../input/tests/results/CMS75FHIRChildrenDentalDecay.txt) — matches QICore
- CMS90FHIRFSAforHeartFailure [ [cql] ](../../input/cql/CMS90FHIRFSAforHeartFailure.cql) [ [test results] ](../../input/tests/results/CMS90FHIRFSAforHeartFailure.txt) — matches QICore
- CMS117FHIRChildImmunStatus [ [cql] ](../../input/cql/CMS117FHIRChildImmunStatus.cql) [ [test results] ](../../input/tests/results/CMS117FHIRChildImmunStatus.txt) — matches QICore
- CMS122FHIRDiabetesAssessGT9Pct [ [cql] ](../../input/cql/CMS122FHIRDiabetesAssessGT9Pct.cql) [ [test results] ](../../input/tests/results/CMS122FHIRDiabetesAssessGT9Pct.txt) — matches QICore
- CMS124FHIRCervicalCancerScreen [ [cql] ](../../input/cql/CMS124FHIRCervicalCancerScreen.cql) [ [test results] ](../../input/tests/results/CMS124FHIRCervicalCancerScreen.txt) — matches QICore
- CMS125FHIRBreastCancerScreen [ [cql] ](../../input/cql/CMS125FHIRBreastCancerScreen.cql) [ [test results] ](../../input/tests/results/CMS125FHIRBreastCancerScreen.txt) — matches QICore
- CMS128FHIRAntidepressantMgmt [ [cql] ](../../input/cql/CMS128FHIRAntidepressantMgmt.cql) [ [test results] ](../../input/tests/results/CMS128FHIRAntidepressantMgmt.txt) — matches QICore
- CMS129FHIRProstCaBoneScanUse [ [cql] ](../../input/cql/CMS129FHIRProstCaBoneScanUse.cql) [ [test results] ](../../input/tests/results/CMS129FHIRProstCaBoneScanUse.txt) — QICore has discrepancies
- CMS130FHIRColorectalCancerScrn [ [cql] ](../../input/cql/CMS130FHIRColorectalCancerScrn.cql) [ [test results] ](../../input/tests/results/CMS130FHIRColorectalCancerScrn.txt) — matches QICore
- CMS131FHIRDiabetesEyeExam [ [cql] ](../../input/cql/CMS131FHIRDiabetesEyeExam.cql) [ [test results] ](../../input/tests/results/CMS131FHIRDiabetesEyeExam.txt) — matches QICore
- CMS133FHIRCataracts2040BCVA90Days [ [cql] ](../../input/cql/CMS133FHIRCataracts2040BCVA90Days.cql) [ [test results] ](../../input/tests/results/CMS133FHIRCataracts2040BCVA90Days.txt) — matches QICore
- CMS136FHIRChildADHDMedFollowUp [ [cql] ](../../input/cql/CMS136FHIRChildADHDMedFollowUp.cql) [ [test results] ](../../input/tests/results/CMS136FHIRChildADHDMedFollowUp.txt) — matches QICore
- CMS137FHIRSUDTxInitEngagement [ [cql] ](../../input/cql/CMS137FHIRSUDTxInitEngagement.cql) [ [test results] ](../../input/tests/results/CMS137FHIRSUDTxInitEngagement.txt) — matches QICore
- CMS138FHIRTobaccoScrnCessation [ [cql] ](../../input/cql/CMS138FHIRTobaccoScrnCessation.cql) [ [test results] ](../../input/tests/results/CMS138FHIRTobaccoScrnCessation.txt) — matches QICore
- CMS139FHIRFallRiskScreening [ [cql] ](../../input/cql/CMS139FHIRFallRiskScreening.cql) [ [test results] ](../../input/tests/results/CMS139FHIRFallRiskScreening.txt) — matches QICore
- CMS143FHIRPOAGOpticNerveEval [ [cql] ](../../input/cql/CMS143FHIRPOAGOpticNerveEval.cql) [ [test results] ](../../input/tests/results/CMS143FHIRPOAGOpticNerveEval.txt) — matches QICore
- CMS146FHIRApproTestPharyngitis [ [cql] ](../../input/cql/CMS146FHIRApproTestPharyngitis.cql) [ [test results] ](../../input/tests/results/CMS146FHIRApproTestPharyngitis.txt) — matches QICore
- CMS149FHIRDementiaCognitiveAssess [ [cql] ](../../input/cql/CMS149FHIRDementiaCognitiveAssess.cql) [ [test results] ](../../input/tests/results/CMS149FHIRDementiaCognitiveAssess.txt) — QICore has discrepancies
- CMS153FHIRChlamydiaScreening [ [cql] ](../../input/cql/CMS153FHIRChlamydiaScreening.cql) [ [test results] ](../../input/tests/results/CMS153FHIRChlamydiaScreening.txt) — matches QICore
- CMS154FHIRAppropriateTxforURI [ [cql] ](../../input/cql/CMS154FHIRAppropriateTxforURI.cql) [ [test results] ](../../input/tests/results/CMS154FHIRAppropriateTxforURI.txt) — matches QICore
- CMS155FHIRWgtAssessCounseling [ [cql] ](../../input/cql/CMS155FHIRWgtAssessCounseling.cql) [ [test results] ](../../input/tests/results/CMS155FHIRWgtAssessCounseling.txt) — matches QICore
- CMS157FHIRPainIntensityQuantified [ [cql] ](../../input/cql/CMS157FHIRPainIntensityQuantified.cql) [ [test results] ](../../input/tests/results/CMS157FHIRPainIntensityQuantified.txt) — matches QICore
- CMS314FHIRHIVViralSuppression [ [cql] ](../../input/cql/CMS314FHIRHIVViralSuppression.cql) [ [test results] ](../../input/tests/results/CMS314FHIRHIVViralSuppression.txt) — matches QICore
- CMS349FHIRHIVScreening [ [cql] ](../../input/cql/CMS349FHIRHIVScreening.cql) [ [test results] ](../../input/tests/results/CMS349FHIRHIVScreening.txt) — matches QICore
- CMS506FHIRSafeUseofOpioids [ [cql] ](../../input/cql/CMS506FHIRSafeUseofOpioids.cql) [ [test results] ](../../input/tests/results/CMS506FHIRSafeUseofOpioids.txt) — QICore has discrepancies
- CMSFHIR529HybridHospitalWideReadmission [ [cql] ](../../input/cql/CMSFHIR529HybridHospitalWideReadmission.cql) [ [test results] ](../../input/tests/results/CMSFHIR529HybridHospitalWideReadmission.txt) — matches QICore
- CMS826FHIRHHPI [ [cql] ](../../input/cql/CMS826FHIRHHPI.cql) [ [test results] ](../../input/tests/results/CMS826FHIRHHPI.txt) — matches QICore
- CMS832FHIRHHAKI [ [cql] ](../../input/cql/CMS832FHIRHHAKI.cql) [ [test results] ](../../input/tests/results/CMS832FHIRHHAKI.txt) — matches QICore
- CMS951FHIRKidneyHealthEval [ [cql] ](../../input/cql/CMS951FHIRKidneyHealthEval.cql) [ [test results] ](../../input/tests/results/CMS951FHIRKidneyHealthEval.txt) — matches QICore
- CMS1056FHIRCTClinical [ [cql] ](../../input/cql/CMS1056FHIRCTClinical.cql) [ [test results] ](../../input/tests/results/CMS1056FHIRCTClinical.txt) — matches QICore
- CMS1074FHIRCTIQR [ [cql] ](../../input/cql/CMS1074FHIRCTIQR.cql) [ [test results] ](../../input/tests/results/CMS1074FHIRCTIQR.txt) — matches QICore
- CMS1157FHIRHIVRetention [ [cql] ](../../input/cql/CMS1157FHIRHIVRetention.cql) [ [test results] ](../../input/tests/results/CMS1157FHIRHIVRetention.txt) — matches QICore
- CMS1173FHIRDiagnosticDelayVTE [ [cql] ](../../input/cql/CMS1173FHIRDiagnosticDelayVTE.cql) [ [test results] ](../../input/tests/results/CMS1173FHIRDiagnosticDelayVTE.txt) — QICore has discrepancies
- CMS1188FHIRHIVSTITesting [ [cql] ](../../input/cql/CMS1188FHIRHIVSTITesting.cql) [ [test results] ](../../input/tests/results/CMS1188FHIRHIVSTITesting.txt) — QICore has discrepancies
- CMS1206FHIRCTOQR [ [cql] ](../../input/cql/CMS1206FHIRCTOQR.cql) [ [test results] ](../../input/tests/results/CMS1206FHIRCTOQR.txt) — matches QICore
- CMS1218FHIRHHRF [ [cql] ](../../input/cql/CMS1218FHIRHHRF.cql) [ [test results] ](../../input/tests/results/CMS1218FHIRHHRF.txt) — matches QICore
- CMS1244FHIRECATHOQR [ [cql] ](../../input/cql/CMS1244FHIRECATHOQR.cql) [ [test results] ](../../input/tests/results/CMS1244FHIRECATHOQR.txt) — matches QICore
- CMS1264FHIRECATREHQR [ [cql] ](../../input/cql/CMS1264FHIRECATREHQR.cql) [ [test results] ](../../input/tests/results/CMS1264FHIRECATREHQR.txt) — QICore has discrepancies
- NHSNAcuteCareHospitalMonthlyInitialPopulation1 [ [cql] ](../../input/cql/NHSNAcuteCareHospitalMonthlyInitialPopulation1.cql) [ [test results] ](../../input/tests/results/NHSNAcuteCareHospitalMonthlyInitialPopulation1.txt) — QICore has discrepancies
- NHSNGlycemicControlHypoglycemiaInitialPopulation [ [cql] ](../../input/cql/NHSNGlycemicControlHypoglycemiaInitialPopulation.cql) [ [test results] ](../../input/tests/results/NHSNGlycemicControlHypoglycemiaInitialPopulation.txt) — QICore has discrepancies

### QICore Measures (39)
- CMS2FHIRPCSDepScreenAndFollowUp [ [cql] ](../../input/cql/CMS2FHIRPCSDepScreenAndFollowUp.cql) [ [test results] ](../../input/tests/results/CMS2FHIRPCSDepScreenAndFollowUp.txt) — CMS has discrepancies
- CMS50FHIRReceiptofSpecialistReport [ [cql] ](../../input/cql/CMS50FHIRReceiptofSpecialistReport.cql) [ [test results] ](../../input/tests/results/CMS50FHIRReceiptofSpecialistReport.txt) — also passes in CMS
- CMS56FHIRFuncStatHipReplacement [ [cql] ](../../input/cql/CMS56FHIRFuncStatHipReplacement.cql) [ [test results] ](../../input/tests/results/CMS56FHIRFuncStatHipReplacement.txt) — also passes in CMS
- CMS68FHIRDocumentationCurrentMeds [ [cql] ](../../input/cql/CMS68FHIRDocumentationCurrentMeds.cql) [ [test results] ](../../input/tests/results/CMS68FHIRDocumentationCurrentMeds.txt) — CMS has discrepancies
- CMS69FHIRPCSBMIScreenAndFollowUp [ [cql] ](../../input/cql/CMS69FHIRPCSBMIScreenAndFollowUp.cql) [ [test results] ](../../input/tests/results/CMS69FHIRPCSBMIScreenAndFollowUp.txt) — also passes in CMS
- CMS74FHIRDentalCariesPrevention [ [cql] ](../../input/cql/CMS74FHIRDentalCariesPrevention.cql) [ [test results] ](../../input/tests/results/CMS74FHIRDentalCariesPrevention.txt) — also passes in CMS
- CMS75FHIRChildrenDentalDecay [ [cql] ](../../input/cql/CMS75FHIRChildrenDentalDecay.cql) [ [test results] ](../../input/tests/results/CMS75FHIRChildrenDentalDecay.txt) — also passes in CMS
- CMS90FHIRFSAforHeartFailure [ [cql] ](../../input/cql/CMS90FHIRFSAforHeartFailure.cql) [ [test results] ](../../input/tests/results/CMS90FHIRFSAforHeartFailure.txt) — also passes in CMS
- CMS117FHIRChildImmunStatus [ [cql] ](../../input/cql/CMS117FHIRChildImmunStatus.cql) [ [test results] ](../../input/tests/results/CMS117FHIRChildImmunStatus.txt) — also passes in CMS
- CMS122FHIRDiabetesAssessGT9Pct [ [cql] ](../../input/cql/CMS122FHIRDiabetesAssessGT9Pct.cql) [ [test results] ](../../input/tests/results/CMS122FHIRDiabetesAssessGT9Pct.txt) — also passes in CMS
- CMS124FHIRCervicalCancerScreen [ [cql] ](../../input/cql/CMS124FHIRCervicalCancerScreen.cql) [ [test results] ](../../input/tests/results/CMS124FHIRCervicalCancerScreen.txt) — also passes in CMS
- CMS125FHIRBreastCancerScreen [ [cql] ](../../input/cql/CMS125FHIRBreastCancerScreen.cql) [ [test results] ](../../input/tests/results/CMS125FHIRBreastCancerScreen.txt) — also passes in CMS
- CMS128FHIRAntidepressantMgmt [ [cql] ](../../input/cql/CMS128FHIRAntidepressantMgmt.cql) [ [test results] ](../../input/tests/results/CMS128FHIRAntidepressantMgmt.txt) — also passes in CMS
- CMS130FHIRColorectalCancerScrn [ [cql] ](../../input/cql/CMS130FHIRColorectalCancerScrn.cql) [ [test results] ](../../input/tests/results/CMS130FHIRColorectalCancerScrn.txt) — also passes in CMS
- CMS131FHIRDiabetesEyeExam [ [cql] ](../../input/cql/CMS131FHIRDiabetesEyeExam.cql) [ [test results] ](../../input/tests/results/CMS131FHIRDiabetesEyeExam.txt) — also passes in CMS
- CMS133FHIRCataracts2040BCVA90Days [ [cql] ](../../input/cql/CMS133FHIRCataracts2040BCVA90Days.cql) [ [test results] ](../../input/tests/results/CMS133FHIRCataracts2040BCVA90Days.txt) — also passes in CMS
- CMS136FHIRChildADHDMedFollowUp [ [cql] ](../../input/cql/CMS136FHIRChildADHDMedFollowUp.cql) [ [test results] ](../../input/tests/results/CMS136FHIRChildADHDMedFollowUp.txt) — also passes in CMS
- CMS137FHIRSUDTxInitEngagement [ [cql] ](../../input/cql/CMS137FHIRSUDTxInitEngagement.cql) [ [test results] ](../../input/tests/results/CMS137FHIRSUDTxInitEngagement.txt) — also passes in CMS
- CMS138FHIRTobaccoScrnCessation [ [cql] ](../../input/cql/CMS138FHIRTobaccoScrnCessation.cql) [ [test results] ](../../input/tests/results/CMS138FHIRTobaccoScrnCessation.txt) — also passes in CMS
- CMS139FHIRFallRiskScreening [ [cql] ](../../input/cql/CMS139FHIRFallRiskScreening.cql) [ [test results] ](../../input/tests/results/CMS139FHIRFallRiskScreening.txt) — also passes in CMS
- CMS143FHIRPOAGOpticNerveEval [ [cql] ](../../input/cql/CMS143FHIRPOAGOpticNerveEval.cql) [ [test results] ](../../input/tests/results/CMS143FHIRPOAGOpticNerveEval.txt) — also passes in CMS
- CMS146FHIRApproTestPharyngitis [ [cql] ](../../input/cql/CMS146FHIRApproTestPharyngitis.cql) [ [test results] ](../../input/tests/results/CMS146FHIRApproTestPharyngitis.txt) — also passes in CMS
- CMS153FHIRChlamydiaScreening [ [cql] ](../../input/cql/CMS153FHIRChlamydiaScreening.cql) [ [test results] ](../../input/tests/results/CMS153FHIRChlamydiaScreening.txt) — also passes in CMS
- CMS154FHIRAppropriateTxforURI [ [cql] ](../../input/cql/CMS154FHIRAppropriateTxforURI.cql) [ [test results] ](../../input/tests/results/CMS154FHIRAppropriateTxforURI.txt) — also passes in CMS
- CMS155FHIRWgtAssessCounseling [ [cql] ](../../input/cql/CMS155FHIRWgtAssessCounseling.cql) [ [test results] ](../../input/tests/results/CMS155FHIRWgtAssessCounseling.txt) — also passes in CMS
- CMS157FHIRPainIntensityQuantified [ [cql] ](../../input/cql/CMS157FHIRPainIntensityQuantified.cql) [ [test results] ](../../input/tests/results/CMS157FHIRPainIntensityQuantified.txt) — also passes in CMS
- CMS177FHIRChildMDDSuicideAssmt [ [cql] ](../../input/cql/CMS177FHIRChildMDDSuicideAssmt.cql) [ [test results] ](../../input/tests/results/CMS177FHIRChildMDDSuicideAssmt.txt) — CMS has discrepancies
- CMS314FHIRHIVViralSuppression [ [cql] ](../../input/cql/CMS314FHIRHIVViralSuppression.cql) [ [test results] ](../../input/tests/results/CMS314FHIRHIVViralSuppression.txt) — also passes in CMS
- CMS349FHIRHIVScreening [ [cql] ](../../input/cql/CMS349FHIRHIVScreening.cql) [ [test results] ](../../input/tests/results/CMS349FHIRHIVScreening.txt) — also passes in CMS
- CMSFHIR529HybridHospitalWideReadmission [ [cql] ](../../input/cql/CMSFHIR529HybridHospitalWideReadmission.cql) [ [test results] ](../../input/tests/results/CMSFHIR529HybridHospitalWideReadmission.txt) — also passes in CMS
- CMS826FHIRHHPI [ [cql] ](../../input/cql/CMS826FHIRHHPI.cql) [ [test results] ](../../input/tests/results/CMS826FHIRHHPI.txt) — also passes in CMS
- CMS832FHIRHHAKI [ [cql] ](../../input/cql/CMS832FHIRHHAKI.cql) [ [test results] ](../../input/tests/results/CMS832FHIRHHAKI.txt) — also passes in CMS
- CMS951FHIRKidneyHealthEval [ [cql] ](../../input/cql/CMS951FHIRKidneyHealthEval.cql) [ [test results] ](../../input/tests/results/CMS951FHIRKidneyHealthEval.txt) — also passes in CMS
- CMS1056FHIRCTClinical [ [cql] ](../../input/cql/CMS1056FHIRCTClinical.cql) [ [test results] ](../../input/tests/results/CMS1056FHIRCTClinical.txt) — also passes in CMS
- CMS1074FHIRCTIQR [ [cql] ](../../input/cql/CMS1074FHIRCTIQR.cql) [ [test results] ](../../input/tests/results/CMS1074FHIRCTIQR.txt) — also passes in CMS
- CMS1157FHIRHIVRetention [ [cql] ](../../input/cql/CMS1157FHIRHIVRetention.cql) [ [test results] ](../../input/tests/results/CMS1157FHIRHIVRetention.txt) — also passes in CMS
- CMS1206FHIRCTOQR [ [cql] ](../../input/cql/CMS1206FHIRCTOQR.cql) [ [test results] ](../../input/tests/results/CMS1206FHIRCTOQR.txt) — also passes in CMS
- CMS1218FHIRHHRF [ [cql] ](../../input/cql/CMS1218FHIRHHRF.cql) [ [test results] ](../../input/tests/results/CMS1218FHIRHHRF.txt) — also passes in CMS
- CMS1244FHIRECATHOQR [ [cql] ](../../input/cql/CMS1244FHIRECATHOQR.cql) [ [test results] ](../../input/tests/results/CMS1244FHIRECATHOQR.txt) — also passes in CMS
## Measures with Discrepancies (30)
| Measure | Total Test Cases | Missing Results | Missing Populations | Mismatched Test Cases | QICore Passing / Failing Test Cases | QICore Status |
|---|:---:|:---:|:---:|:---:|:---:|---|
| [CMS2FHIRPCSDepScreenAndFollowUp](#cms2fhirpcsdepscreenandfollowup) | 36 | 0 | 0 | 22.22%   (8) | 36 / 0 | passes |
| [CMS22FHIRPCSBPScreeningFollowUp](#cms22fhirpcsbpscreeningfollowup) | 44 | 0 | 0 | 27.27%   (12) | 43 / 1 | has discrepancies (1) |
| [CMS68FHIRDocumentationCurrentMeds](#cms68fhirdocumentationcurrentmeds) | 19 | 1 | 0 | 0.00%   (0) | 19 / 0 | passes |
| [CMS71FHIRSTKAnticoagAFFlutter](#cms71fhirstkanticoagafflutter) | 83 | 0 | 0 | 9.64%   (8) | 80 / 3 | has discrepancies (3) |
| [CMS72FHIRSTKAntithromboticDay2](#cms72fhirstkantithromboticday2) | 158 | 0 | 0 | 8.23%   (13) | 51 / 107 | has discrepancies (107) |
| [CMS104FHIRSTKDCAntithrombotic](#cms104fhirstkdcantithrombotic) | 82 | 0 | 0 | 18.29%   (15) | 13 / 69 | has discrepancies (69) |
| [CMS108FHIRVTEProphylaxis](#cms108fhirvteprophylaxis) | 140 | 0 | 0 | 11.43%   (16) | 114 / 26 | has discrepancies (26) |
| [CMS135FHIRACEIorARBorARNIforHF](#cms135fhiraceiorarborarniforhf) | 40 | 3 | 0 | 20.00%   (8) | 10 / 30 | has discrepancies (27) |
| [CMS142FHIRCommWithDrManagingDiab](#cms142fhircommwithdrmanagingdiab) | 32 | 0 | 0 | 15.62%   (5) | 27 / 5 | has discrepancies (5) |
| [CMS144FHIRHFBetaBlockerForLVSD](#cms144fhirhfbetablockerforlvsd) | 48 | 0 | 0 | 6.25%   (3) | 4 / 44 | has discrepancies (44) |
| [CMS145FHIRCADBBlockerTPMIorLVSD](#cms145fhircadbblockertpmiorlvsd) | 53 | 0 | 0 | 11.32%   (6) | 4 / 49 | has discrepancies (49) |
| [CMS156FHIRHighRiskMedsElderly](#cms156fhirhighriskmedselderly) | 59 | 0 | 0 | 1.69%   (1) | 58 / 1 | has discrepancies (1) |
| [CMS159FHIRDepRemissionat12Months](#cms159fhirdepremissionat12months) | 67 | 0 | 0 | 2.99%   (2) | 65 / 2 | has discrepancies (2) |
| [CMS165FHIRControllingHighBP](#cms165fhircontrollinghighbp) | 68 | 1 | 0 | 0.00%   (0) | 67 / 1 | has discrepancies (0) |
| [CMS177FHIRChildMDDSuicideAssmt](#cms177fhirchildmddsuicideassmt) | 41 | 0 | 0 | 2.44%   (1) | 41 / 0 | passes |
| [CMS190FHIRVTEProphylaxisICU](#cms190fhirvteprophylaxisicu) | 125 | 0 | 0 | 18.40%   (23) | 99 / 26 | has discrepancies (26) |
| [CMS0334FHIRPCCesareanBirth](#cms0334fhirpccesareanbirth) | 138 | 0 | 0 | 0.72%   (1) | 136 / 2 | has discrepancies (2) |
| [CMS347FHIRStatinPreventionTxCVD](#cms347fhirstatinpreventiontxcvd) | 188 | 0 | 0 | 12.77%   (24) | 76 / 112 | has discrepancies (111) |
| [CMS645FHIRBoneDensityPCADTherapy](#cms645fhirbonedensitypcadtherapy) | 51 | 0 | 0 | 5.88%   (3) | 15 / 36 | has discrepancies (36) |
| [CMS646FHIRIntravesicalBCGTherapy](#cms646fhirintravesicalbcgtherapy) | 38 | 1 | 0 | 7.89%   (3) | 28 / 10 | has discrepancies (10) |
| [CMS771FHIRUrinarySymptomScoreBPH](#cms771fhirurinarysymptomscorebph) | 31 | 0 | 0 | 22.58%   (7) | 9 / 22 | has discrepancies (22) |
| [CMS816FHIRHHHypo](#cms816fhirhhhypo) | 28 | 0 | 0 | 42.86%   (12) | 16 / 12 | has discrepancies (12) |
| [CMS819FHIRHHORAE](#cms819fhirhhorae) | 28 | 0 | 0 | 7.14%   (2) | 24 / 4 | has discrepancies (4) |
| [CMSFHIR844HybridHospitalWideMortality](#cmsfhir844hybridhospitalwidemortality) | 10 | 0 | 0 | 20.00%   (2) | 8 / 2 | has discrepancies (2) |
| [CMS871FHIRHHHyper](#cms871fhirhhhyper) | 26 | 4 | 0 | 46.15%   (12) | 10 / 16 | has discrepancies (12) |
| [CMS986FHIRMalnutritionScore](#cms986fhirmalnutritionscore) | 146 | 0 | 0 | 82.19%   (120) | 27 / 119 | has discrepancies (119) |
| [CMS996FHIRAptTxforSTEMI](#cms996fhirapttxforstemi) | 114 | 0 | 0 | 4.39%   (5) | 108 / 6 | has discrepancies (6) |
| [CMS1017FHIRHHFI](#cms1017fhirhhfi) | 65 | 0 | 4 | 78.46%   (51) | 10 / 55 | has discrepancies (51) |
| [CMS1028FHIRPCSevereOBComps](#cms1028fhirpcsevereobcomps) | 141 | 0 | 0 | 0.71%   (1) | 136 / 5 | has discrepancies (5) |
| [CMS1154ScreeningPrediabetesFHIR](#cms1154screeningprediabetesfhir) | 10 | 0 | 0 | 10.00%   (1) | 9 / 1 | has discrepancies (1) |



#### CMS2FHIRPCSDepScreenAndFollowUp
[ [cql] ](../../input/cql/CMS2FHIRPCSDepScreenAndFollowUp.cql) [ [test results] ](../../input/tests/results/CMS2FHIRPCSDepScreenAndFollowUp.txt)

QICore: 36 / 0 — passes

Mismatched Test Cases (8 of 36 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 0e463fc3-d1bf-4e19-882b-fad6342aa668 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/0e463fc3-d1bf-4e19-882b-fad6342aa668/MeasureReport-38443362-8261-414c-80b3-1f719f4ba56e.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ 12786a64-c20e-4542-a4c0-bf3129d6a9e0 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/12786a64-c20e-4542-a4c0-bf3129d6a9e0/MeasureReport-d404e2d0-2ded-4329-b254-482be8b54a7c.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ 41df0dbe-ae84-4496-b355-320ff8707a85 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/41df0dbe-ae84-4496-b355-320ff8707a85/MeasureReport-922ffb7d-2d13-47b8-ad5d-4f42ff55f77d.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ 6078e73e-3265-4022-ae63-216c096b6246 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/6078e73e-3265-4022-ae63-216c096b6246/MeasureReport-dfcfbb31-9da9-4947-8444-53a25c8b8121.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ 6aaff09e-4a7b-4efa-93f8-13033e95c230 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/6aaff09e-4a7b-4efa-93f8-13033e95c230/MeasureReport-5981d1e2-7d0b-4887-aed2-884d0e7df4fe.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ 86ca7528-efcb-44ed-9203-6f21f37f4332 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/86ca7528-efcb-44ed-9203-6f21f37f4332/MeasureReport-51f60250-c8a8-49d8-81c1-56b58ad0125f.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ d0ba1182-26fa-4cfa-9f91-960503b7fe53 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/d0ba1182-26fa-4cfa-9f91-960503b7fe53/MeasureReport-277359bb-b41c-4dd4-b1af-b3afdb6ee15d.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ f29e2786-fade-4dca-b14d-7037a34ef498 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/f29e2786-fade-4dca-b14d-7037a34ef498/MeasureReport-32baa107-7be1-4a64-a10d-1f25307962e6.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |


#### CMS22FHIRPCSBPScreeningFollowUp
[ [cql] ](../../input/cql/CMS22FHIRPCSBPScreeningFollowUp.cql) [ [test results] ](../../input/tests/results/CMS22FHIRPCSBPScreeningFollowUp.txt)

QICore: 43 / 1 — has discrepancies (1 mismatched, 0 missing)

Mismatched Test Cases (12 of 44 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 0278fdf0-f067-46e8-aeb1-fb96dff3c947 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/0278fdf0-f067-46e8-aeb1-fb96dff3c947/MeasureReport-064f5dc2-d804-4a03-a0c8-d0c25ae3b8fb.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ 1f16120b-56c9-4d72-8dd4-01d8a0175d77 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/1f16120b-56c9-4d72-8dd4-01d8a0175d77/MeasureReport-b5acac31-18e7-4172-802f-041d29ba3da1.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ 695cee04-cf12-411e-a258-99e430093a4e ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/695cee04-cf12-411e-a258-99e430093a4e/MeasureReport-e887022a-7961-4768-9cf3-e48ecfced710.json) | Group_1 | Denominator Exception | 2 | 0 | E-21 — resolution pending | PASS |
| [ 86618b52-e0cc-4e90-b48c-cd64bbae8973 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/86618b52-e0cc-4e90-b48c-cd64bbae8973/MeasureReport-ad10338d-d04c-44de-badb-b69f01b20de5.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ 9ed1ecf5-2d93-4bde-a293-5d5fbf209475 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/9ed1ecf5-2d93-4bde-a293-5d5fbf209475/MeasureReport-bd56dca9-e498-4ec5-bf78-c6322930e980.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ a55c6265-a05c-4fad-beb4-c5338420d1b1 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/a55c6265-a05c-4fad-beb4-c5338420d1b1/MeasureReport-a08e2374-4dea-4a09-8163-296239dcd454.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ ad737f80-c9ea-41fd-a142-78d9c80a9c7c ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/ad737f80-c9ea-41fd-a142-78d9c80a9c7c/MeasureReport-29212fe6-6c26-4e87-9711-8b5694567caa.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ afdeaa75-d332-40f2-9b30-0b6ddf7e7c14 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/afdeaa75-d332-40f2-9b30-0b6ddf7e7c14/MeasureReport-fcac6417-0a19-457d-a23b-b55bfb352064.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ c41f9946-cb0f-4489-8367-581a5b876165 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/c41f9946-cb0f-4489-8367-581a5b876165/MeasureReport-f183c739-a20c-4dcd-b12c-5c2cef29eaf5.json) | Group_1 | Denominator Exception<br>Numerator | 2<br>0 | 1<br>1 | E-21 — resolution pending | PASS<br>PASS |
| [ dda022c0-3234-4ad7-ad6e-d696b0b57440 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/dda022c0-3234-4ad7-ad6e-d696b0b57440/MeasureReport-2b4791bc-bde7-4af7-9665-df0a21abc7b0.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ ef9a58ac-e252-480a-bed8-2309c503587d ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/ef9a58ac-e252-480a-bed8-2309c503587d/MeasureReport-292f318b-0b76-4666-9e3e-4b0d8c6924b2.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ f9417a57-54e8-4a0b-a516-ab62b8d4aae0 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/f9417a57-54e8-4a0b-a516-ab62b8d4aae0/MeasureReport-e90efb05-4493-4006-a537-3896b6bf37ba.json) | Group_1 | Denominator Exception<br>Numerator | 2<br>0 | 0<br>1 | E-21 — resolution pending | PASS<br>PASS |


#### CMS68FHIRDocumentationCurrentMeds
[ [cql] ](../../input/cql/CMS68FHIRDocumentationCurrentMeds.cql) [ [test results] ](../../input/tests/results/CMS68FHIRDocumentationCurrentMeds.txt)

QICore: 19 / 0 — passes

Missing Results (1 of 19 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ f2e2e1c0-9e35-4592-9579-72a236cb2f56 ](../.././input/tests/measure/CMS68FHIRDocumentationCurrentMeds/f2e2e1c0-9e35-4592-9579-72a236cb2f56/MeasureReport-7384d607-6a08-487a-9129-d90036bae37e.json) | Group_1 | E-22 — resolution pending |


#### CMS71FHIRSTKAnticoagAFFlutter
[ [cql] ](../../input/cql/CMS71FHIRSTKAnticoagAFFlutter.cql) [ [test results] ](../../input/tests/results/CMS71FHIRSTKAnticoagAFFlutter.txt)

QICore: 80 / 3 — has discrepancies (3 mismatched, 0 missing)

Mismatched Test Cases (8 of 83 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 017a2267-f463-47a6-8b8b-dc91465e0869 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/017a2267-f463-47a6-8b8b-dc91465e0869/MeasureReport-3a870421-64af-44eb-8c7a-533079bc2259.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | C-14 — resolution pending | FAIL<br>FAIL |
| [ 0587a75d-0dcc-4c6b-bfc0-f5727342ec1f ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/0587a75d-0dcc-4c6b-bfc0-f5727342ec1f/MeasureReport-c8a99645-6e7a-467b-87aa-456cdc7cafb9.json) | Group_1 | Denominator<br>Numerator | 1<br>1 | 0<br>0 | E-21 — resolution pending | PASS<br>PASS |
| [ 56ae006d-ab1b-428d-8614-2ccd5d962650 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/56ae006d-ab1b-428d-8614-2ccd5d962650/MeasureReport-71b26a14-7533-4479-82e3-7bc54d9ce0db.json) | Group_1 | Denominator<br>Numerator | 1<br>1 | 0<br>0 | E-21 — resolution pending | PASS<br>PASS |
| [ 595ebfd1-fe6a-4b4b-96a1-23a72f6a70da ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/595ebfd1-fe6a-4b4b-96a1-23a72f6a70da/MeasureReport-793a4c67-2bc9-4601-9521-999a2628ffdd.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-21 — resolution pending | PASS<br>PASS |
| [ 9a72ea26-595f-4442-8b00-fc52ed228aa6 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/9a72ea26-595f-4442-8b00-fc52ed228aa6/MeasureReport-47b2254f-ca43-470b-9229-eeb4071ba6e0.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | C-14 — resolution pending | FAIL<br>FAIL |
| [ b29204ac-96ce-4be0-90ad-ae8ecfa4f245 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/b29204ac-96ce-4be0-90ad-ae8ecfa4f245/MeasureReport-e5339c1c-c4cd-497b-97a1-ed9fb1a1bc2e.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-21 — resolution pending | PASS<br>PASS |
| [ c640ff8f-5b2a-448e-85a2-e739af7a8dc4 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/c640ff8f-5b2a-448e-85a2-e739af7a8dc4/MeasureReport-8b1280e5-8c6d-48b1-ac5a-e4c07e338f56.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-21 — resolution pending | PASS<br>PASS |
| [ e20b4e76-8523-43ab-abc2-a4f4137a84bb ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/e20b4e76-8523-43ab-abc2-a4f4137a84bb/MeasureReport-ce8fcdb9-f3ff-4f3f-a6cc-114d96185bcb.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-21 — resolution pending | PASS<br>PASS |


#### CMS72FHIRSTKAntithromboticDay2
[ [cql] ](../../input/cql/CMS72FHIRSTKAntithromboticDay2.cql) [ [test results] ](../../input/tests/results/CMS72FHIRSTKAntithromboticDay2.txt)

QICore: 51 / 107 — has discrepancies (107 mismatched, 0 missing)

Mismatched Test Cases (13 of 158 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 2f7681fa-66b0-4395-aa35-7622e37709ae ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2f7681fa-66b0-4395-aa35-7622e37709ae/MeasureReport-97f5ba10-36d6-4246-b935-fcfc8f4b1061.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL |
| [ 3432dedb-7130-4614-9283-6c1569fab90f ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3432dedb-7130-4614-9283-6c1569fab90f/MeasureReport-acfc5ee1-09d4-4012-b12a-8487396b9856.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL |
| [ 5a329008-fcc1-4168-ab9c-89cb5dd6ff32 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5a329008-fcc1-4168-ab9c-89cb5dd6ff32/MeasureReport-dda268cb-4395-4776-acd8-0fee046d392a.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 1<br>1<br>0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ 7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8/MeasureReport-bad7b4ba-e916-41e2-a314-11854e1021ff.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL |
| [ 82399522-ba6c-4997-afc9-23f55bb7da89 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/82399522-ba6c-4997-afc9-23f55bb7da89/MeasureReport-fe335f74-59a9-4afc-ba4c-7a9e003733d6.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL |
| [ a1a37483-1a67-4dd9-a8ca-b4d49a28a19d ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a1a37483-1a67-4dd9-a8ca-b4d49a28a19d/MeasureReport-e3bfac2a-251a-49fe-9694-6c60803d9ded.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL |
| [ be5c4068-2639-4b0c-bea3-5b7c80a6fe3b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/be5c4068-2639-4b0c-bea3-5b7c80a6fe3b/MeasureReport-ad329961-b67b-413b-a186-d6b269572c42.json) | Group_1 | Denominator Exception | 1 | 0 | C-14 — resolution pending | FAIL |
| [ cb7c95fc-6d6b-4e07-81e8-a79385142b94 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cb7c95fc-6d6b-4e07-81e8-a79385142b94/MeasureReport-6844e7ed-08a4-43d5-be1c-720dc795b3cf.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 3<br>3<br>2 | 1<br>1<br>0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ d496f08e-c55b-44b1-97a7-f86cf9ead1e2 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d496f08e-c55b-44b1-97a7-f86cf9ead1e2/MeasureReport-81e3066d-7dba-46fa-bb3f-2abc24625551.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL |
| [ dc187313-245c-4ed6-b6bb-fcb94c117fec ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/dc187313-245c-4ed6-b6bb-fcb94c117fec/MeasureReport-d0cc2adb-8b9f-442d-82e2-5ef90a9c30d3.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL |
| [ e126cdec-dbc8-4ee8-964f-e88e46c04f88 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e126cdec-dbc8-4ee8-964f-e88e46c04f88/MeasureReport-58249af5-0abc-464b-9e0a-456f7c31b4cf.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion | 1<br>1<br>1 | 0<br>0<br>0 | C-14 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ ed638412-155e-4349-8461-4550fd4fae3b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ed638412-155e-4349-8461-4550fd4fae3b/MeasureReport-cf1aeb73-d464-4dd9-9f46-38afe84f76ec.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL |
| [ febd4b3e-99bc-4c55-bba9-3b2136c2160b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/febd4b3e-99bc-4c55-bba9-3b2136c2160b/MeasureReport-4f80f98a-71ab-45d6-bdda-d0875ec02ec9.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 4<br>4<br>2<br>2 | 1<br>1<br>0<br>1 | C-14 — resolution pending | FAIL<br>FAIL<br>FAIL<br>FAIL |


#### CMS104FHIRSTKDCAntithrombotic
[ [cql] ](../../input/cql/CMS104FHIRSTKDCAntithrombotic.cql) [ [test results] ](../../input/tests/results/CMS104FHIRSTKDCAntithrombotic.txt)

QICore: 13 / 69 — has discrepancies (69 mismatched, 0 missing)

Mismatched Test Cases (15 of 82 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 0b1aa8ee-e8bf-49f5-b968-48c5a9702843 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0b1aa8ee-e8bf-49f5-b968-48c5a9702843/MeasureReport-38f44642-a505-41c0-b367-013e4bb44d58.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion | 1<br>1<br>1 | 0<br>0<br>0 | C-14 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ 146a6714-8663-4f45-826a-01110ff34490 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/146a6714-8663-4f45-826a-01110ff34490/MeasureReport-e1b111ec-80f6-4548-b462-dc44dd07fd1e.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ 2d54a94c-edf1-4f92-baf8-3813a8ef452d ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2d54a94c-edf1-4f92-baf8-3813a8ef452d/MeasureReport-023784a8-b40e-491b-850f-0c87cb2e5e03.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ 348471db-5aaa-4bf3-a280-75222f20d599 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/348471db-5aaa-4bf3-a280-75222f20d599/MeasureReport-bf54d81d-f635-45ff-b69b-1580a144d3fb.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 3<br>3<br>1<br>1 | 1<br>1<br>0<br>0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL<br>FAIL<br>FAIL<br>FAIL |
| [ 451b6853-3734-4c1c-b37e-5904629e0350 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/451b6853-3734-4c1c-b37e-5904629e0350/MeasureReport-4eefe8af-efb3-47eb-91df-e2ea877a39e7.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 3<br>3<br>2<br>1 | 1<br>1<br>1<br>0 | C-14 — resolution pending | FAIL<br>FAIL<br>FAIL<br>FAIL |
| [ 48952352-d74c-491c-9420-6e999e60f52a ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/48952352-d74c-491c-9420-6e999e60f52a/MeasureReport-5eeb7443-d897-40c5-8815-c5dead56e05e.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ 591c23ea-1ddd-4800-9203-4b6946979818 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/591c23ea-1ddd-4800-9203-4b6946979818/MeasureReport-a871588f-5c88-44ce-890e-ccac41059f64.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ 593382e8-4ad5-4300-b0ad-26c8954281c6 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/593382e8-4ad5-4300-b0ad-26c8954281c6/MeasureReport-bb6002b4-0bd0-43fa-a7a0-748bd0444688.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ 5adc911a-c2a1-475c-a347-9da4ee98c6df ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/5adc911a-c2a1-475c-a347-9da4ee98c6df/MeasureReport-fbd77dd4-8f40-4bf2-bee9-e1e5ce62d7aa.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ 7b1ac1a8-b7be-41ec-a77f-db545af22263 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7b1ac1a8-b7be-41ec-a77f-db545af22263/MeasureReport-373169e3-3ba1-4ace-bf0c-5c212910cccf.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ a2b8327c-eaf4-4552-863e-851426e729d4 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a2b8327c-eaf4-4552-863e-851426e729d4/MeasureReport-0ced6c1b-75a5-4ee3-a7a0-017818c03e9a.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>2 | 1<br>1<br>1 | C-14 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ ac56c496-c5d6-4c23-be20-130ee8327fd2 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ac56c496-c5d6-4c23-be20-130ee8327fd2/MeasureReport-34148ef9-fbdd-48ca-ab5d-6a11fd288074.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ c15bee15-84c1-494a-ac82-2159b06da175 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/c15bee15-84c1-494a-ac82-2159b06da175/MeasureReport-bbe28035-6557-410d-964f-21cf38904d0f.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 3<br>3<br>2 | 1<br>1<br>0 | B-01 — resolution pending<br>C-14 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ e081bee5-67f8-464f-9356-9b287e32a35a ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e081bee5-67f8-464f-9356-9b287e32a35a/MeasureReport-560b8ee7-5246-423f-8065-7f02c28eb91f.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ e84c89f7-3c9e-4ee9-b71a-5025aadb5990 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e84c89f7-3c9e-4ee9-b71a-5025aadb5990/MeasureReport-51e29a50-abca-429e-95eb-8364998be573.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exception | 1<br>1<br>1 | 0<br>0<br>0 | C-14 — resolution pending | FAIL<br>FAIL<br>FAIL |


#### CMS108FHIRVTEProphylaxis
[ [cql] ](../../input/cql/CMS108FHIRVTEProphylaxis.cql) [ [test results] ](../../input/tests/results/CMS108FHIRVTEProphylaxis.txt)

QICore: 114 / 26 — has discrepancies (26 mismatched, 0 missing)

Mismatched Test Cases (16 of 140 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 182103c1-0a38-4d85-819c-148e4e105716 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/182103c1-0a38-4d85-819c-148e4e105716/MeasureReport-ccb6ece2-ea74-4377-b826-2118740d1eee.json) | Group_1 | Numerator | 1 | 0 | E-12 — resolution pending | FAIL |
| [ 2eff6dbd-f3a2-43ee-9ad3-aab4d3b84812 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/2eff6dbd-f3a2-43ee-9ad3-aab4d3b84812/MeasureReport-735dcbb8-d535-493a-a79c-ff4a9f72ee50.json) | Group_1 | Numerator | 1 | 0 | E-12 — resolution pending | FAIL |
| [ 33d162ce-3bc7-4b0a-8c04-fec0a42a6263 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/33d162ce-3bc7-4b0a-8c04-fec0a42a6263/MeasureReport-da823951-b92e-4ee9-904f-839f7e8db8df.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ 3c854f27-5103-4367-bdef-97c3cde1edb8 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/3c854f27-5103-4367-bdef-97c3cde1edb8/MeasureReport-1c32114e-5b9f-4f01-b021-0b3dd5bd8adf.json) | Group_1 | Numerator | 1 | 0 | E-12 — resolution pending | FAIL |
| [ 3db5c5a1-2eec-4e01-8e59-ac389a0a2179 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/3db5c5a1-2eec-4e01-8e59-ac389a0a2179/MeasureReport-384a4771-57ba-472a-9ffd-17eeba8f39d7.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ 41f2785f-4c4f-4497-a46b-e17fd8b5ee3f ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/41f2785f-4c4f-4497-a46b-e17fd8b5ee3f/MeasureReport-ff4c0b9f-8014-4119-ab3f-78a8e7e8f935.json) | Group_1 | Denominator Exclusion | 0 | 1 | E-17 — resolution pending | PASS |
| [ 525e73f2-77be-49b1-920f-6fc31ef38d22 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/525e73f2-77be-49b1-920f-6fc31ef38d22/MeasureReport-9cb7f213-6011-4f8b-be16-010172559897.json) | Group_1 | Numerator | 1 | 0 | E-12 — resolution pending | FAIL |
| [ 5741c41a-04ec-4967-83b2-b0d746bd0ed5 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/5741c41a-04ec-4967-83b2-b0d746bd0ed5/MeasureReport-10dddf5e-f066-457d-b056-01329b17c73e.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ 5f739500-ee12-4662-8980-ef95d8fa74c8 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/5f739500-ee12-4662-8980-ef95d8fa74c8/MeasureReport-5dd7eca4-05b6-49c4-87b7-a7313b46d684.json) | Group_1 | Numerator | 1 | 0 | E-12 — resolution pending | FAIL |
| [ 8bb999a1-696a-497b-a5f4-aa55e146a16e ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/8bb999a1-696a-497b-a5f4-aa55e146a16e/MeasureReport-f1938984-85bf-4eff-b9b8-e89a556b2f35.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ 91ff5f1a-cfdb-472d-b8c3-144f499d1ccc ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/91ff5f1a-cfdb-472d-b8c3-144f499d1ccc/MeasureReport-cee9ae71-29f6-41ee-a479-0fc2d8b338c5.json) | Group_1 | Numerator | 1 | 0 | E-12 — resolution pending | FAIL |
| [ d205878e-b861-43a8-92e8-47f680987e4d ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/d205878e-b861-43a8-92e8-47f680987e4d/MeasureReport-e96f2279-a61f-40e2-9e19-9137ee4b12e6.json) | Group_1 | Numerator | 1 | 0 | E-12 — resolution pending | FAIL |
| [ d9b7ffa9-ed78-484c-8880-b4cbf2b4b6a1 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/d9b7ffa9-ed78-484c-8880-b4cbf2b4b6a1/MeasureReport-43331d8f-cf2d-4a0c-a3a2-e4b8e060a7eb.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ dc0dcb01-87f0-4e65-9c36-8cf6174abef1 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/dc0dcb01-87f0-4e65-9c36-8cf6174abef1/MeasureReport-7bc64137-ecc6-421a-bb2f-0177667a25b7.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ dd5a1e46-1b99-45a3-b4d3-1fde205d8a11 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/dd5a1e46-1b99-45a3-b4d3-1fde205d8a11/MeasureReport-bc945d90-f897-463b-bbc2-f9b922117784.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ ff814452-be6d-4e4b-905b-c1ae2a551645 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/ff814452-be6d-4e4b-905b-c1ae2a551645/MeasureReport-8f09729a-45b0-45dc-bfdd-047cf0d896ef.json) | Group_1 | Numerator | 1 | 0 | E-12 — resolution pending | FAIL |


#### CMS135FHIRACEIorARBorARNIforHF
[ [cql] ](../../input/cql/CMS135FHIRACEIorARBorARNIforHF.cql) [ [test results] ](../../input/tests/results/CMS135FHIRACEIorARBorARNIforHF.txt)

QICore: 10 / 30 — has discrepancies (27 mismatched, 3 missing)

Missing Results (3 of 40 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ c095195c-8893-4bf1-aa7d-ad2bfd9bafa5 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/c095195c-8893-4bf1-aa7d-ad2bfd9bafa5/MeasureReport-f2d033da-6f32-46dc-86bc-69fdf82b1cfd.json) | Group_1 | E-11 — resolution pending |
| [ cba5a449-1c45-4e11-ae0b-ba3974b410f7 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/cba5a449-1c45-4e11-ae0b-ba3974b410f7/MeasureReport-ae8c4b99-af76-4577-b66d-b1230ac09aa3.json) | Group_1 | E-11 — resolution pending |
| [ ec508dbb-76f6-4878-b8a2-114ea8e82297 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/ec508dbb-76f6-4878-b8a2-114ea8e82297/MeasureReport-d1b704c8-7e95-4cd9-89e7-a8b90f925ce2.json) | Group_1 | E-11 — resolution pending |


Mismatched Test Cases (8 of 40 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 149c3a7c-2b80-47f8-b50d-5c1d233eedb7 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/149c3a7c-2b80-47f8-b50d-5c1d233eedb7/MeasureReport-d8d9ace4-d191-4aff-a0e4-6de581275357.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending<br>E-23 — resolution pending | FAIL<br>PASS |
| [ 1f64a697-a90b-4aaf-a315-fa84168ac2b4 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/1f64a697-a90b-4aaf-a315-fa84168ac2b4/MeasureReport-cf4fe385-8e6f-4642-b1e5-ca08159c0b53.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-21 — resolution pending | PASS<br>PASS |
| [ 298d5342-fa0a-4386-bf48-b9c977a1c367 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/298d5342-fa0a-4386-bf48-b9c977a1c367/MeasureReport-090aa645-1e2b-44df-b6c0-2419bea96186.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending<br>E-23 — resolution pending | FAIL<br>PASS |
| [ 4bc4883f-0770-4a68-824a-5fa4dba72638 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/4bc4883f-0770-4a68-824a-5fa4dba72638/MeasureReport-d4dc5571-57c9-4b1b-95d9-a09ac4c6e34d.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ 5b7e720f-e2fc-4779-9b1c-3f34a0241482 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/5b7e720f-e2fc-4779-9b1c-3f34a0241482/MeasureReport-01fb5443-0f43-487e-ac44-f7cc6e163ca0.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |
| [ 64e76766-9760-4385-a977-cbe8136ce425 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/64e76766-9760-4385-a977-cbe8136ce425/MeasureReport-0488a022-da7e-4dcf-a9af-7e2fbf5e9423.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-21 — resolution pending | PASS<br>PASS |
| [ d18e37a6-7b66-4e7c-b305-692872c13f8d ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/d18e37a6-7b66-4e7c-b305-692872c13f8d/MeasureReport-ecbb5067-dcb1-48ce-8e78-6dfd556ac43d.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |
| [ d297e68e-3f02-42a8-a59f-a5a4cecbd47d ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/d297e68e-3f02-42a8-a59f-a5a4cecbd47d/MeasureReport-cc3a4e83-9689-4bb7-83e1-55cb47dc9848.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-21 — resolution pending | PASS<br>PASS |


#### CMS142FHIRCommWithDrManagingDiab
[ [cql] ](../../input/cql/CMS142FHIRCommWithDrManagingDiab.cql) [ [test results] ](../../input/tests/results/CMS142FHIRCommWithDrManagingDiab.txt)

QICore: 27 / 5 — has discrepancies (5 mismatched, 0 missing)

Mismatched Test Cases (5 of 32 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 05f1e2a6-b317-42bb-827f-993ca3995f5b ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/05f1e2a6-b317-42bb-827f-993ca3995f5b/MeasureReport-84bcf708-71bb-4169-8067-18fd354f3c37.json) | Group_1 | Denominator Exception | 1 | 0 | C-08 — resolution pending | FAIL |
| [ 41ae0086-ac99-4a31-9546-21b054bbf7d8 ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/41ae0086-ac99-4a31-9546-21b054bbf7d8/MeasureReport-b77a6309-214c-4fc2-a9bc-18d81c740da6.json) | Group_1 | Denominator Exception | 1 | 0 | C-08 — resolution pending | FAIL |
| [ 6aef5a18-59bd-4a47-80bc-2bd44636e41f ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/6aef5a18-59bd-4a47-80bc-2bd44636e41f/MeasureReport-e5735d61-0444-4958-8f47-165a59e91dc0.json) | Group_1 | Denominator Exception | 1 | 0 | C-08 — resolution pending | FAIL |
| [ b85440e4-b902-49cd-b3d6-363ba7a99bce ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/b85440e4-b902-49cd-b3d6-363ba7a99bce/MeasureReport-9d61df39-18a0-451f-a795-988388d58778.json) | Group_1 | Denominator Exception | 1 | 0 | C-08 — resolution pending | FAIL |
| [ d9840e8c-3359-42c2-b354-4b236c3c1b15 ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/d9840e8c-3359-42c2-b354-4b236c3c1b15/MeasureReport-1fbf56ab-6e60-4ce6-a1d5-b520382164bd.json) | Group_1 | Denominator Exception | 1 | 0 | C-08 — resolution pending | FAIL |


#### CMS144FHIRHFBetaBlockerForLVSD
[ [cql] ](../../input/cql/CMS144FHIRHFBetaBlockerForLVSD.cql) [ [test results] ](../../input/tests/results/CMS144FHIRHFBetaBlockerForLVSD.txt)

QICore: 4 / 44 — has discrepancies (44 mismatched, 0 missing)

Mismatched Test Cases (3 of 48 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 07efd4bb-b45d-4bfd-aeb2-08de49742d91 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/07efd4bb-b45d-4bfd-aeb2-08de49742d91/MeasureReport-ad01867d-c2c7-4317-9925-deb909d156e6.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending<br>E-23 — resolution pending | FAIL<br>PASS |
| [ 67779bc6-07ee-42cf-8ca7-e71302915dba ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/67779bc6-07ee-42cf-8ca7-e71302915dba/MeasureReport-5b182aca-ad2a-4651-ba6b-df02e001ec36.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ 7b8885c5-ad14-4361-9755-c76a6e3b8530 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/7b8885c5-ad14-4361-9755-c76a6e3b8530/MeasureReport-7e421d2a-1ee4-4c56-a454-815983c21106.json) | Group_1 | Numerator | 0 | 1 | B-01 — resolution pending<br>E-21 — resolution pending | PASS |


#### CMS145FHIRCADBBlockerTPMIorLVSD
[ [cql] ](../../input/cql/CMS145FHIRCADBBlockerTPMIorLVSD.cql) [ [test results] ](../../input/tests/results/CMS145FHIRCADBBlockerTPMIorLVSD.txt)

QICore: 4 / 49 — has discrepancies (49 mismatched, 0 missing)

Mismatched Test Cases (6 of 53 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 1f70822b-c513-4c3a-8162-49f0bb9c914b ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/1f70822b-c513-4c3a-8162-49f0bb9c914b/MeasureReport-9b3577fa-355c-409d-8d3f-21e9720fb889.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>C-14 — resolution pending | PASS |
| [ 4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e/MeasureReport-e77c61ff-cc3a-402c-9752-7a97a6727a39.json) | Group_2 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |
| [ 5fd0d626-e9c5-4e6c-a10d-1a1183fa7702 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/5fd0d626-e9c5-4e6c-a10d-1a1183fa7702/MeasureReport-ce1b8712-b9dd-48e2-adf4-554ed641bee5.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |
| [ 61306767-0e74-44b8-ac06-1339c3783355 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/61306767-0e74-44b8-ac06-1339c3783355/MeasureReport-6ea40199-5a45-4c8d-8a2b-c08bf93ebd8a.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |
| [ b65680a0-9768-4ce4-b08d-972fcd84e28e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/b65680a0-9768-4ce4-b08d-972fcd84e28e/MeasureReport-b5ebd0a9-a2de-4b31-b0d9-588888e95872.json) | Group_2 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |
| [ fd5fb311-a466-4c59-966d-48fa7aa88931 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/fd5fb311-a466-4c59-966d-48fa7aa88931/MeasureReport-05ffed3e-5604-40eb-bcf8-99cacecc26c0.json) | Group_1 | Denominator Exception | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |


#### CMS156FHIRHighRiskMedsElderly
[ [cql] ](../../input/cql/CMS156FHIRHighRiskMedsElderly.cql) [ [test results] ](../../input/tests/results/CMS156FHIRHighRiskMedsElderly.txt)

QICore: 58 / 1 — has discrepancies (1 mismatched, 0 missing)

Mismatched Test Cases (1 of 59 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 4aa75d19-ac8b-49b0-a686-429fbc033d77 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/MeasureReport-139cc56e-5ffb-46ca-89ce-accd0bb642ab.json) | Group_1 | Numerator | 1 | 0 | C-14 — resolution pending | FAIL |
| [ 4aa75d19-ac8b-49b0-a686-429fbc033d77 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/MeasureReport-139cc56e-5ffb-46ca-89ce-accd0bb642ab.json) | Group_3 | Numerator | 1 | 0 | C-14 — resolution pending | FAIL |


#### CMS159FHIRDepRemissionat12Months
[ [cql] ](../../input/cql/CMS159FHIRDepRemissionat12Months.cql) [ [test results] ](../../input/tests/results/CMS159FHIRDepRemissionat12Months.txt)

QICore: 65 / 2 — has discrepancies (2 mismatched, 0 missing)

Mismatched Test Cases (2 of 67 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 491f554e-e897-40c5-ad2b-0983923df4e8 ](../.././input/tests/measure/CMS159FHIRDepRemissionat12Months/491f554e-e897-40c5-ad2b-0983923df4e8/MeasureReport-580087e1-b59e-43eb-b110-692c35a82dca.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | C-10 — resolution pending | FAIL<br>FAIL |
| [ 96b6579c-1cee-423f-9433-a72db6fb8a0a ](../.././input/tests/measure/CMS159FHIRDepRemissionat12Months/96b6579c-1cee-423f-9433-a72db6fb8a0a/MeasureReport-e3ec1311-05ed-4a6f-b13f-a4d290865bb3.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | C-10 — resolution pending | FAIL<br>FAIL |


#### CMS165FHIRControllingHighBP
[ [cql] ](../../input/cql/CMS165FHIRControllingHighBP.cql) [ [test results] ](../../input/tests/results/CMS165FHIRControllingHighBP.txt)

QICore: 67 / 1 — has discrepancies (0 mismatched, 1 missing)

Missing Results (1 of 68 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 45e01fed-56bb-483d-a860-af3d566bda11 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/45e01fed-56bb-483d-a860-af3d566bda11/MeasureReport-02991ca7-859d-422d-8849-655760f8e10a.json) | Group_1 | E-11 — resolution pending |


#### CMS177FHIRChildMDDSuicideAssmt
[ [cql] ](../../input/cql/CMS177FHIRChildMDDSuicideAssmt.cql) [ [test results] ](../../input/tests/results/CMS177FHIRChildMDDSuicideAssmt.txt)

QICore: 41 / 0 — passes

Mismatched Test Cases (1 of 41 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 85e6225c-a9bb-4338-a228-297564e38c4d ](../.././input/tests/measure/CMS177FHIRChildMDDSuicideAssmt/85e6225c-a9bb-4338-a228-297564e38c4d/MeasureReport-89005c1a-09a3-421d-aa89-d44837ae5904.json) | Group_1 | Initial Population<br>Denominator | 0<br>0 | 1<br>1 | E-21 — resolution pending | PASS<br>PASS |


#### CMS190FHIRVTEProphylaxisICU
[ [cql] ](../../input/cql/CMS190FHIRVTEProphylaxisICU.cql) [ [test results] ](../../input/tests/results/CMS190FHIRVTEProphylaxisICU.txt)

QICore: 99 / 26 — has discrepancies (26 mismatched, 0 missing)

Mismatched Test Cases (23 of 125 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 208cb0f9-a6e9-4207-b6a4-3325fb463099 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/208cb0f9-a6e9-4207-b6a4-3325fb463099/MeasureReport-3cb6a3ba-7c97-47c9-9ac7-cd39959ecc39.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ 282ae3a0-a4fd-4fed-8ce9-bff3840c7ca9 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/282ae3a0-a4fd-4fed-8ce9-bff3840c7ca9/MeasureReport-bb0ca899-9892-4d53-a171-fa41dc45d404.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | FAIL |
| [ 2bcbe960-db7d-4088-a574-d771baf0f9c7 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/2bcbe960-db7d-4088-a574-d771baf0f9c7/MeasureReport-cfb7bc83-85fe-45b7-b133-a2b1429e1e31.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | FAIL |
| [ 4724cb2f-b5bd-4c50-85cc-4a5ba25f04ca ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/4724cb2f-b5bd-4c50-85cc-4a5ba25f04ca/MeasureReport-4ca4bed8-36fa-40a9-a273-ce3f8e9f377e.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | FAIL |
| [ 4c32b73b-abba-431b-a352-f0f454e7c9dd ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/4c32b73b-abba-431b-a352-f0f454e7c9dd/MeasureReport-e9ac894c-9f4c-47d8-8325-7750b25036e0.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ 4fc421c7-e490-4d4e-a326-53d08635efb9 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/4fc421c7-e490-4d4e-a326-53d08635efb9/MeasureReport-c206bcec-44ba-493e-8114-8ae57bf6b7e6.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ 632831b0-1ebf-47b5-b439-3a124cd77c37 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/632831b0-1ebf-47b5-b439-3a124cd77c37/MeasureReport-dff9d9bd-b0cc-400f-815b-9255b426e828.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ 7e7f4563-a628-40ab-990b-ca0837313759 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/7e7f4563-a628-40ab-990b-ca0837313759/MeasureReport-6b131b52-199b-46ac-b099-fad21dbda4ad.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ 8ec9cf6a-2dcd-4c2e-9e2e-1ba237b66808 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/8ec9cf6a-2dcd-4c2e-9e2e-1ba237b66808/MeasureReport-53445771-3d55-46d3-8091-a92e9f7a0915.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | FAIL |
| [ 95a54d01-197e-48ef-bb48-d3d398aecbe8 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/95a54d01-197e-48ef-bb48-d3d398aecbe8/MeasureReport-89a6d854-e283-4df7-bd78-60dfa86483cf.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ 98d6da30-f55a-411d-94b4-359b204bcb5a ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/98d6da30-f55a-411d-94b4-359b204bcb5a/MeasureReport-6e63dc69-1e82-44f5-bccb-e417baa090e5.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | FAIL |
| [ 9ddea16c-55d3-4dda-a1d8-a256fbff0b64 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/9ddea16c-55d3-4dda-a1d8-a256fbff0b64/MeasureReport-90c1518e-8e3a-4f2a-b266-9210baffdcbf.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ a30e5588-0e2a-487c-b4d3-15d9e0006741 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/a30e5588-0e2a-487c-b4d3-15d9e0006741/MeasureReport-bdba93da-ab6a-4f3b-b72e-86f0168f9b43.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | FAIL |
| [ a82cd0c1-900e-4ab3-a498-840ac1608486 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/a82cd0c1-900e-4ab3-a498-840ac1608486/MeasureReport-94a26fc6-de93-43a2-9be0-2ca52b24d988.json) | Group_1 | Denominator Exclusion | 0 | 1 | E-17 — resolution pending | PASS |
| [ a9c75661-be1c-41b2-aa15-222cc7d2ca81 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/a9c75661-be1c-41b2-aa15-222cc7d2ca81/MeasureReport-21816bad-859d-416f-883b-24246a1db64c.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ c0481b47-738b-4a09-8901-915ece2beb7e ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/c0481b47-738b-4a09-8901-915ece2beb7e/MeasureReport-a28ce7c4-934f-4fac-a002-aee0c87b7cb9.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ dbfc823e-0e2f-409d-a409-2d9399db1118 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/dbfc823e-0e2f-409d-a409-2d9399db1118/MeasureReport-e7db6f05-3243-4d94-bf90-1b5c6cff7c10.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | FAIL |
| [ e8931859-4ad8-49c8-9cdd-8697293456a2 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/e8931859-4ad8-49c8-9cdd-8697293456a2/MeasureReport-cfc06289-ff74-4caa-ba81-3647f98e3646.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ f00f3778-6ad1-466d-a3bd-bcbc63d62b55 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f00f3778-6ad1-466d-a3bd-bcbc63d62b55/MeasureReport-d3f2a4f2-6c34-484a-b29b-b2d34f1d8334.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ f035a977-30d0-487c-b542-a596e718420c ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f035a977-30d0-487c-b542-a596e718420c/MeasureReport-2318030c-b923-45ed-988f-5925f46200e9.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ f82746cf-f6cd-4fcc-bc9e-7e569ae26211 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f82746cf-f6cd-4fcc-bc9e-7e569ae26211/MeasureReport-ecd1d81f-c8df-4d19-b85f-5bb0d5c9f771.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ f859dd94-f201-4517-a368-32b98dd486c9 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f859dd94-f201-4517-a368-32b98dd486c9/MeasureReport-da236e59-3d0a-46c4-a352-3eec5846dbe6.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | PASS |
| [ f981eba4-4aac-45ce-8c52-f0bc02c9a0dc ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f981eba4-4aac-45ce-8c52-f0bc02c9a0dc/MeasureReport-01143c30-f69f-464f-99fd-405617644ce8.json) | Group_1 | Numerator | 1 | 0 | E-17 — resolution pending | FAIL |


#### CMS0334FHIRPCCesareanBirth
[ [cql] ](../../input/cql/CMS0334FHIRPCCesareanBirth.cql) [ [test results] ](../../input/tests/results/CMS0334FHIRPCCesareanBirth.txt)

QICore: 136 / 2 — has discrepancies (2 mismatched, 0 missing)

Mismatched Test Cases (1 of 138 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ c58acff5-248b-49c9-b18d-69e4a84a08d9 ](../.././input/tests/measure/CMS0334FHIRPCCesareanBirth/c58acff5-248b-49c9-b18d-69e4a84a08d9/MeasureReport-920b0c2e-1f1f-42d3-ab1f-1d7b12fa4bd0.json) | Group_1 | Denominator<br>Denominator Exclusion | 1<br>1 | 0<br>0 | C-11 — resolution pending | FAIL<br>FAIL |


#### CMS347FHIRStatinPreventionTxCVD
[ [cql] ](../../input/cql/CMS347FHIRStatinPreventionTxCVD.cql) [ [test results] ](../../input/tests/results/CMS347FHIRStatinPreventionTxCVD.txt)

QICore: 76 / 112 — has discrepancies (111 mismatched, 4 missing)

Mismatched Test Cases (24 of 188 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_2 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_4 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_3 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_1 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_2 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_4 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_3 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_2 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_4 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 1ba7b147-b701-424c-bade-4e8270547030 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1ba7b147-b701-424c-bade-4e8270547030/MeasureReport-2278c703-994b-4b13-8e3b-c726ba6b8530.json) | Group_4 | Denominator Exception | 0 | 1 | E-16 — resolution pending | PASS |
| [ 2cff757c-4470-46a2-a685-6e23cf82c045 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/2cff757c-4470-46a2-a685-6e23cf82c045/MeasureReport-4cae9687-fba5-4a5d-af12-fe19c1ef3760.json) | Group_4 | Numerator | 0 | 1 | E-19 — resolution pending | PASS |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_1 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_3 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_1 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_2 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_1 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_2 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_4 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_1 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_2 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_3 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_3 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_1 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_2 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_1 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_3 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_1 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_2 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_1 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_3 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_2 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_4 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_3 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_3 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_1 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_2 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_3 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_1 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_2 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-19 — resolution pending | FAIL<br>PASS |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_2 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_3 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_4 | Denominator Exception | 0 | 1 | B-01 — resolution pending<br>E-19 — resolution pending | PASS |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_1 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_2 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_3 | Denominator Exception | 0 | 1 | E-19 — resolution pending | PASS |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | E-19 — resolution pending | PASS<br>PASS |


#### CMS645FHIRBoneDensityPCADTherapy
[ [cql] ](../../input/cql/CMS645FHIRBoneDensityPCADTherapy.cql) [ [test results] ](../../input/tests/results/CMS645FHIRBoneDensityPCADTherapy.txt)

QICore: 15 / 36 — has discrepancies (36 mismatched, 0 missing)

Mismatched Test Cases (3 of 51 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 8c41481d-f89e-4113-ba12-df7c53e93d80 ](../.././input/tests/measure/CMS645FHIRBoneDensityPCADTherapy/8c41481d-f89e-4113-ba12-df7c53e93d80/MeasureReport-5199a981-c1fd-4530-bd20-438541e8993f.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6 ](../.././input/tests/measure/CMS645FHIRBoneDensityPCADTherapy/c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6/MeasureReport-ff0dae36-899e-426e-9f9d-0b7270a49bfb.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL<br>PASS |
| [ d07cf359-d46c-4adf-b2d4-e02a2f43b78e ](../.././input/tests/measure/CMS645FHIRBoneDensityPCADTherapy/d07cf359-d46c-4adf-b2d4-e02a2f43b78e/MeasureReport-2e25820a-ce7b-4c83-b5b6-56eeec0f5577.json) | Group_1 | Numerator | 0 | 1 | B-01 — resolution pending<br>E-21 — resolution pending | PASS |


#### CMS646FHIRIntravesicalBCGTherapy
[ [cql] ](../../input/cql/CMS646FHIRIntravesicalBCGTherapy.cql) [ [test results] ](../../input/tests/results/CMS646FHIRIntravesicalBCGTherapy.txt)

QICore: 28 / 10 — has discrepancies (10 mismatched, 0 missing)

Missing Results (1 of 38 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/342d2bec-0acc-43e5-aaf7-3c9a65b09f91/MeasureReport-12cd358b-deb0-4130-a045-4c6b61e110c0.json) | Group_1 | E-21 — resolution pending |


Mismatched Test Cases (3 of 38 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 10cec7db-41ae-49ad-b883-022f19d92a8b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/10cec7db-41ae-49ad-b883-022f19d92a8b/MeasureReport-b8b4961d-450b-4980-ac8f-95500c6393d4.json) | Group_1 | Denominator Exclusion | 0 | 1 | C-14 — resolution pending | FAIL |
| [ ab48e0c0-6543-4537-8f00-bfcdcba7a81b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/ab48e0c0-6543-4537-8f00-bfcdcba7a81b/MeasureReport-ea6cfef5-54d2-4d6d-a7aa-48cf8e749eaf.json) | Group_1 | Numerator | 0 | 1 | C-14 — resolution pending | PASS |
| [ e648fa70-0532-49b0-92f6-dfb5a6d28d94 ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/e648fa70-0532-49b0-92f6-dfb5a6d28d94/MeasureReport-57107c42-23df-40d4-92fe-5f7fdd475629.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |


#### CMS771FHIRUrinarySymptomScoreBPH
[ [cql] ](../../input/cql/CMS771FHIRUrinarySymptomScoreBPH.cql) [ [test results] ](../../input/tests/results/CMS771FHIRUrinarySymptomScoreBPH.txt)

QICore: 9 / 22 — has discrepancies (22 mismatched, 0 missing)

Mismatched Test Cases (7 of 31 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 051c5977-9f2c-4e8b-8e02-ac3ec0c718d6 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/051c5977-9f2c-4e8b-8e02-ac3ec0c718d6/MeasureReport-13a299d2-1f32-41d7-b226-7380902e41b7.json) | Group_1 | Denominator | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending<br>E-23 — resolution pending | FAIL |
| [ 3ab3ac1d-9b5e-4087-8862-dcb2562fb90f ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/3ab3ac1d-9b5e-4087-8862-dcb2562fb90f/MeasureReport-47dae27e-89cf-4ee5-8c8b-bf1e44997d07.json) | Group_1 | Denominator | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending<br>E-23 — resolution pending | FAIL |
| [ 4c234ec0-3f89-4d55-b767-219d1130f634 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/4c234ec0-3f89-4d55-b767-219d1130f634/MeasureReport-47a91ced-cb5f-44c0-9417-e8efa33a4b08.json) | Group_1 | Numerator | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending<br>E-23 — resolution pending | FAIL |
| [ 9be591a0-517b-4be2-b652-a29be0c75c15 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/9be591a0-517b-4be2-b652-a29be0c75c15/MeasureReport-004d2ae6-6c2e-49f8-bf07-26cada3bbaf3.json) | Group_1 | Numerator | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |
| [ bc79e5bc-237e-44be-b5fc-c5c4efb50286 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/bc79e5bc-237e-44be-b5fc-c5c4efb50286/MeasureReport-621196a7-ca5f-4408-8508-851332413956.json) | Group_1 | Numerator | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |
| [ bf0f8968-c2c0-4416-88db-11ea3e3da968 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/bf0f8968-c2c0-4416-88db-11ea3e3da968/MeasureReport-bcce208a-3ff4-4c82-9d49-c0b64ccb9138.json) | Group_1 | Numerator | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |
| [ e90d90a7-3071-44de-8089-ad7b6f5f3e5d ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/e90d90a7-3071-44de-8089-ad7b6f5f3e5d/MeasureReport-9ef2db11-d78a-49af-a2ac-6536fac264a1.json) | Group_1 | Numerator | 1 | 0 | B-01 — resolution pending<br>E-21 — resolution pending | FAIL |


#### CMS816FHIRHHHypo
[ [cql] ](../../input/cql/CMS816FHIRHHHypo.cql) [ [test results] ](../../input/tests/results/CMS816FHIRHHHypo.txt)

QICore: 16 / 12 — has discrepancies (12 mismatched, 0 missing)

Mismatched Test Cases (12 of 28 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 05c8cd12-addd-4b94-8f92-da093c556a84 ](../.././input/tests/measure/CMS816FHIRHHHypo/05c8cd12-addd-4b94-8f92-da093c556a84/MeasureReport-e66fcfe4-57f5-4259-bb05-540d4f6a864c.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | C-06 — resolution pending | FAIL<br>FAIL |
| [ 1d2bb25a-21a7-4529-9486-a320d4864719 ](../.././input/tests/measure/CMS816FHIRHHHypo/1d2bb25a-21a7-4529-9486-a320d4864719/MeasureReport-b0513b24-8789-4c07-a13d-322d9defbeb8.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | C-06 — resolution pending | FAIL<br>FAIL |
| [ 2adf5469-46a1-4020-be3b-01f91f8acc9d ](../.././input/tests/measure/CMS816FHIRHHHypo/2adf5469-46a1-4020-be3b-01f91f8acc9d/MeasureReport-af8c832f-f1ad-407a-9751-575339d08367.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | C-06 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ 304052f7-e416-4da4-87ae-488e6589cab3 ](../.././input/tests/measure/CMS816FHIRHHHypo/304052f7-e416-4da4-87ae-488e6589cab3/MeasureReport-a754b13e-2ef7-4c69-a205-f9af9a9a089e.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | C-06 — resolution pending | FAIL<br>FAIL |
| [ 339a989b-722c-4452-9d25-454e2d53eea8 ](../.././input/tests/measure/CMS816FHIRHHHypo/339a989b-722c-4452-9d25-454e2d53eea8/MeasureReport-1f48c160-8aba-4e86-bd5d-c5c4bdef1afd.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | C-06 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ 37fd9c7e-bf9e-4769-b448-094ed97bd3e8 ](../.././input/tests/measure/CMS816FHIRHHHypo/37fd9c7e-bf9e-4769-b448-094ed97bd3e8/MeasureReport-6c210a7d-98b1-4d37-a268-45d14a7e7b1d.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | C-06 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ 5bfa3b7e-2b6f-4eb5-b09b-7c6f1145780b ](../.././input/tests/measure/CMS816FHIRHHHypo/5bfa3b7e-2b6f-4eb5-b09b-7c6f1145780b/MeasureReport-0fb98a8a-a7ac-49a3-a1bd-e042373dc1c6.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | C-06 — resolution pending | FAIL<br>FAIL |
| [ 6bc18290-1925-4239-81d7-0118bd062225 ](../.././input/tests/measure/CMS816FHIRHHHypo/6bc18290-1925-4239-81d7-0118bd062225/MeasureReport-1e896d30-3808-482a-b8a3-51198a58d4a6.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | C-06 — resolution pending | FAIL<br>FAIL |
| [ 8301c6c8-e50c-4457-add0-1ebd909c8ca7 ](../.././input/tests/measure/CMS816FHIRHHHypo/8301c6c8-e50c-4457-add0-1ebd909c8ca7/MeasureReport-a821b7fb-7913-45e4-82e2-cf232818d643.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | C-06 — resolution pending | FAIL<br>FAIL |
| [ 974284eb-fc89-452a-9b38-a884c0e0477e ](../.././input/tests/measure/CMS816FHIRHHHypo/974284eb-fc89-452a-9b38-a884c0e0477e/MeasureReport-6244d8f6-995c-4a0e-9d86-9c3abfc3fcb7.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | C-06 — resolution pending | FAIL<br>FAIL |
| [ aa5f21cc-2d56-4749-a190-2828d579f790 ](../.././input/tests/measure/CMS816FHIRHHHypo/aa5f21cc-2d56-4749-a190-2828d579f790/MeasureReport-9eeadd82-4599-4b8b-95a5-f1d59697b451.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | C-06 — resolution pending | FAIL<br>FAIL |
| [ ecde4132-9028-420a-aa7c-d1d14e5c1ab0 ](../.././input/tests/measure/CMS816FHIRHHHypo/ecde4132-9028-420a-aa7c-d1d14e5c1ab0/MeasureReport-b8bedfa5-6f9c-4727-be26-8b53d9a13a5b.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | C-06 — resolution pending | FAIL<br>FAIL |


#### CMS819FHIRHHORAE
[ [cql] ](../../input/cql/CMS819FHIRHHORAE.cql) [ [test results] ](../../input/tests/results/CMS819FHIRHHORAE.txt)

QICore: 24 / 4 — has discrepancies (4 mismatched, 0 missing)

Mismatched Test Cases (2 of 28 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 31b40acc-ca5f-4d1d-bd83-4b1a14eb822e ](../.././input/tests/measure/CMS819FHIRHHORAE/31b40acc-ca5f-4d1d-bd83-4b1a14eb822e/MeasureReport-c93e2b69-18fd-425e-8c71-b52eb967eda0.json) | Group_1 | Initial Population<br>Denominator | 2<br>2 | 1<br>1 | C-09 — resolution pending | FAIL<br>FAIL |
| [ 73b0c1fe-874b-4982-8cb2-3c30520441de ](../.././input/tests/measure/CMS819FHIRHHORAE/73b0c1fe-874b-4982-8cb2-3c30520441de/MeasureReport-15d9e04f-4116-4856-b61a-f7c7b38e3325.json) | Group_1 | Numerator | 1 | 0 | C-09 — resolution pending | FAIL |


#### CMSFHIR844HybridHospitalWideMortality
[ [cql] ](../../input/cql/CMSFHIR844HybridHospitalWideMortality.cql) [ [test results] ](../../input/tests/results/CMSFHIR844HybridHospitalWideMortality.txt)

QICore: 8 / 2 — has discrepancies (2 mismatched, 0 missing)

Mismatched Test Cases (2 of 10 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 6f22a06f-7186-4db1-9310-4f907dc49ff3 ](../.././input/tests/measure/CMSFHIR844HybridHospitalWideMortality/6f22a06f-7186-4db1-9310-4f907dc49ff3/MeasureReport-a02a261f-1274-4f8b-b1f3-5496f7885cbe.json) | Group_1 | Initial Population | 1 | 0 | C-13 — resolution pending | FAIL |
| [ af1b9448-3e7a-4b7f-8934-15bb63258b75 ](../.././input/tests/measure/CMSFHIR844HybridHospitalWideMortality/af1b9448-3e7a-4b7f-8934-15bb63258b75/MeasureReport-7afefb0f-3075-4fb8-8d56-474ba1112c38.json) | Group_1 | Initial Population | 2 | 1 | C-13 — resolution pending | FAIL |


#### CMS871FHIRHHHyper
[ [cql] ](../../input/cql/CMS871FHIRHHHyper.cql) [ [test results] ](../../input/tests/results/CMS871FHIRHHHyper.txt)

QICore: 10 / 16 — has discrepancies (12 mismatched, 4 missing)

Missing Results (4 of 26 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 35719b1a-85bd-4072-b8d5-7218309358c6 ](../.././input/tests/measure/CMS871FHIRHHHyper/35719b1a-85bd-4072-b8d5-7218309358c6/MeasureReport-d5793b30-25e6-4cd6-8f7e-619b1c1802e5.json) | Group_1 | C-07 — resolution pending |
| [ 7507debb-a991-4de0-bd71-634a684ddcd7 ](../.././input/tests/measure/CMS871FHIRHHHyper/7507debb-a991-4de0-bd71-634a684ddcd7/MeasureReport-6b01e3f8-ef51-41c3-8a23-b2868877df06.json) | Group_1 | C-07 — resolution pending |
| [ 98533ccd-24ee-41b3-aab2-ef6cbf89e00d ](../.././input/tests/measure/CMS871FHIRHHHyper/98533ccd-24ee-41b3-aab2-ef6cbf89e00d/MeasureReport-82c8805c-b129-4009-8533-1ed12cf5d18f.json) | Group_1 | C-07 — resolution pending |
| [ fd579f44-757b-4c98-9b09-27b17b935650 ](../.././input/tests/measure/CMS871FHIRHHHyper/fd579f44-757b-4c98-9b09-27b17b935650/MeasureReport-22df2e2a-404d-4ab0-831a-e2ab043197a2.json) | Group_1 | C-07 — resolution pending |


Mismatched Test Cases (12 of 26 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 113a6e72-7049-4a7f-90cf-5ec3435b0dee ](../.././input/tests/measure/CMS871FHIRHHHyper/113a6e72-7049-4a7f-90cf-5ec3435b0dee/MeasureReport-0ac4f7d5-15a2-4c2f-b38d-c2d2ff7775e2.json) | Group_1 | Denominator Observation | 4 | 0 | B-01 — resolution pending<br>C-07 — resolution pending | FAIL |
| [ 48c2ed10-7c2d-4130-a215-a6f72b169111 ](../.././input/tests/measure/CMS871FHIRHHHyper/48c2ed10-7c2d-4130-a215-a6f72b169111/MeasureReport-fa9a16b8-16c8-441f-b6a3-520d3769719f.json) | Group_1 | Denominator Observation | 1 | 0 | C-07 — resolution pending | FAIL |
| [ 4c12355d-2548-471a-a98f-b9a58c2cbfe0 ](../.././input/tests/measure/CMS871FHIRHHHyper/4c12355d-2548-471a-a98f-b9a58c2cbfe0/MeasureReport-13ec3a0e-0600-44a9-be19-99c22f815732.json) | Group_1 | Denominator Observation<br>Numerator Observation | 4<br>1 | 0<br>0 | C-07 — resolution pending | FAIL<br>FAIL |
| [ 7637e69b-5481-4edf-b186-334d4367f740 ](../.././input/tests/measure/CMS871FHIRHHHyper/7637e69b-5481-4edf-b186-334d4367f740/MeasureReport-e0a7ca11-615a-4a8a-842c-1becc018a26a.json) | Group_1 | Denominator Observation | 3 | 0 | C-07 — resolution pending | FAIL |
| [ 76dc2b63-710c-4ac5-afef-f76c2ef07dcd ](../.././input/tests/measure/CMS871FHIRHHHyper/76dc2b63-710c-4ac5-afef-f76c2ef07dcd/MeasureReport-eaa31743-c56c-45e9-8bbd-877d956245a8.json) | Group_1 | Denominator Observation<br>Numerator Observation | 6<br>4 | 0<br>0 | C-07 — resolution pending | FAIL<br>FAIL |
| [ a52510f5-d529-419e-97ae-3979ae9b4d9a ](../.././input/tests/measure/CMS871FHIRHHHyper/a52510f5-d529-419e-97ae-3979ae9b4d9a/MeasureReport-79a43e8f-47d7-4e40-adcb-4e3dc843083d.json) | Group_1 | Denominator Observation | 1 | 0 | C-07 — resolution pending | FAIL |
| [ b7534abb-5837-4f38-83b1-b14e52684f84 ](../.././input/tests/measure/CMS871FHIRHHHyper/b7534abb-5837-4f38-83b1-b14e52684f84/MeasureReport-a8968c87-ea2b-40f7-9064-9b17d657006a.json) | Group_1 | Denominator Observation<br>Numerator Observation | 9<br>1 | 0<br>0 | C-07 — resolution pending | FAIL<br>FAIL |
| [ b8dd3c0c-bc5c-4858-9f96-9faabbbaad45 ](../.././input/tests/measure/CMS871FHIRHHHyper/b8dd3c0c-bc5c-4858-9f96-9faabbbaad45/MeasureReport-b0d599a4-0476-4f92-921b-403084e5676e.json) | Group_1 | Denominator Observation | 3 | 0 | C-07 — resolution pending | FAIL |
| [ c85ad9d3-e24d-47a2-a4ab-8a9b6d3bb9af ](../.././input/tests/measure/CMS871FHIRHHHyper/c85ad9d3-e24d-47a2-a4ab-8a9b6d3bb9af/MeasureReport-e88b5263-dda0-43ca-a6cf-ff775778e782.json) | Group_1 | Denominator Observation | 3 | 0 | C-07 — resolution pending | FAIL |
| [ cb54c026-6ec9-4db5-b8ac-7cb713138ca8 ](../.././input/tests/measure/CMS871FHIRHHHyper/cb54c026-6ec9-4db5-b8ac-7cb713138ca8/MeasureReport-e5b2180c-cc9f-4942-acf3-0f53caf9e904.json) | Group_1 | Denominator Observation | 4 | 0 | C-07 — resolution pending | FAIL |
| [ cff9d7db-ba56-4527-8cd4-3a6412945f16 ](../.././input/tests/measure/CMS871FHIRHHHyper/cff9d7db-ba56-4527-8cd4-3a6412945f16/MeasureReport-eb879220-8c9f-4c3d-be49-9c4baeebd226.json) | Group_1 | Denominator Observation<br>Numerator Observation | 3<br>1 | 0<br>0 | C-07 — resolution pending | FAIL<br>FAIL |
| [ f4e6ce4f-8209-4f79-a9ab-2678d3e0ded8 ](../.././input/tests/measure/CMS871FHIRHHHyper/f4e6ce4f-8209-4f79-a9ab-2678d3e0ded8/MeasureReport-9c64659f-b043-4a04-a74d-08159ec625eb.json) | Group_1 | Denominator Observation | 4 | 0 | C-07 — resolution pending | FAIL |


#### CMS986FHIRMalnutritionScore
[ [cql] ](../../input/cql/CMS986FHIRMalnutritionScore.cql) [ [test results] ](../../input/tests/results/CMS986FHIRMalnutritionScore.txt)

QICore: 27 / 119 — has discrepancies (119 mismatched, 0 missing)

Mismatched Test Cases (120 of 146 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 0389dadc-9e16-4ccc-abe8-5d2b4a899319 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0389dadc-9e16-4ccc-abe8-5d2b4a899319/MeasureReport-78a23405-6a0c-4667-ab38-bc8ee5118683.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 0389dadc-9e16-4ccc-abe8-5d2b4a899319 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0389dadc-9e16-4ccc-abe8-5d2b4a899319/MeasureReport-78a23405-6a0c-4667-ab38-bc8ee5118683.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 0389dadc-9e16-4ccc-abe8-5d2b4a899319 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0389dadc-9e16-4ccc-abe8-5d2b4a899319/MeasureReport-78a23405-6a0c-4667-ab38-bc8ee5118683.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 0389dadc-9e16-4ccc-abe8-5d2b4a899319 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0389dadc-9e16-4ccc-abe8-5d2b4a899319/MeasureReport-78a23405-6a0c-4667-ab38-bc8ee5118683.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 0389dadc-9e16-4ccc-abe8-5d2b4a899319 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0389dadc-9e16-4ccc-abe8-5d2b4a899319/MeasureReport-78a23405-6a0c-4667-ab38-bc8ee5118683.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ 0389dadc-9e16-4ccc-abe8-5d2b4a899319 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0389dadc-9e16-4ccc-abe8-5d2b4a899319/MeasureReport-78a23405-6a0c-4667-ab38-bc8ee5118683.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 06aa2ec3-1620-4f65-90b0-8c5c11c91535 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/06aa2ec3-1620-4f65-90b0-8c5c11c91535/MeasureReport-36e14695-8c7c-4ed6-91ef-bd1713579f19.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 06aa2ec3-1620-4f65-90b0-8c5c11c91535 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/06aa2ec3-1620-4f65-90b0-8c5c11c91535/MeasureReport-36e14695-8c7c-4ed6-91ef-bd1713579f19.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 06aa2ec3-1620-4f65-90b0-8c5c11c91535 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/06aa2ec3-1620-4f65-90b0-8c5c11c91535/MeasureReport-36e14695-8c7c-4ed6-91ef-bd1713579f19.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 06aa2ec3-1620-4f65-90b0-8c5c11c91535 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/06aa2ec3-1620-4f65-90b0-8c5c11c91535/MeasureReport-36e14695-8c7c-4ed6-91ef-bd1713579f19.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 06aa2ec3-1620-4f65-90b0-8c5c11c91535 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/06aa2ec3-1620-4f65-90b0-8c5c11c91535/MeasureReport-36e14695-8c7c-4ed6-91ef-bd1713579f19.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ 06aa2ec3-1620-4f65-90b0-8c5c11c91535 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/06aa2ec3-1620-4f65-90b0-8c5c11c91535/MeasureReport-36e14695-8c7c-4ed6-91ef-bd1713579f19.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 08bd110c-dfda-403c-91cd-34dfc9f16dc8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/08bd110c-dfda-403c-91cd-34dfc9f16dc8/MeasureReport-c83a07fe-a868-4983-9870-dc1a5140bac5.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 08bd110c-dfda-403c-91cd-34dfc9f16dc8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/08bd110c-dfda-403c-91cd-34dfc9f16dc8/MeasureReport-c83a07fe-a868-4983-9870-dc1a5140bac5.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 08bd110c-dfda-403c-91cd-34dfc9f16dc8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/08bd110c-dfda-403c-91cd-34dfc9f16dc8/MeasureReport-c83a07fe-a868-4983-9870-dc1a5140bac5.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 0c3b72aa-9a9b-468d-b2fb-95153a9136bc ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0c3b72aa-9a9b-468d-b2fb-95153a9136bc/MeasureReport-180cacb4-2ef0-4fb6-b8d0-2925e23bd23b.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 0c3b72aa-9a9b-468d-b2fb-95153a9136bc ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0c3b72aa-9a9b-468d-b2fb-95153a9136bc/MeasureReport-180cacb4-2ef0-4fb6-b8d0-2925e23bd23b.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 0c3b72aa-9a9b-468d-b2fb-95153a9136bc ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0c3b72aa-9a9b-468d-b2fb-95153a9136bc/MeasureReport-180cacb4-2ef0-4fb6-b8d0-2925e23bd23b.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 0c3b72aa-9a9b-468d-b2fb-95153a9136bc ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0c3b72aa-9a9b-468d-b2fb-95153a9136bc/MeasureReport-180cacb4-2ef0-4fb6-b8d0-2925e23bd23b.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 0c3df882-87de-42cf-a909-01d7a01eb5ad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0c3df882-87de-42cf-a909-01d7a01eb5ad/MeasureReport-d8ea0079-f0be-4bc1-9d23-1ad593a23631.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 0c3df882-87de-42cf-a909-01d7a01eb5ad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0c3df882-87de-42cf-a909-01d7a01eb5ad/MeasureReport-d8ea0079-f0be-4bc1-9d23-1ad593a23631.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 0c3df882-87de-42cf-a909-01d7a01eb5ad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0c3df882-87de-42cf-a909-01d7a01eb5ad/MeasureReport-d8ea0079-f0be-4bc1-9d23-1ad593a23631.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 0c3df882-87de-42cf-a909-01d7a01eb5ad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0c3df882-87de-42cf-a909-01d7a01eb5ad/MeasureReport-d8ea0079-f0be-4bc1-9d23-1ad593a23631.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 0c3df882-87de-42cf-a909-01d7a01eb5ad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/0c3df882-87de-42cf-a909-01d7a01eb5ad/MeasureReport-d8ea0079-f0be-4bc1-9d23-1ad593a23631.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 165d4163-b24b-416b-8b12-5a3dfca5cd4c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/165d4163-b24b-416b-8b12-5a3dfca5cd4c/MeasureReport-dff85525-6a0a-4ffc-9ade-003aaadd22b4.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 165d4163-b24b-416b-8b12-5a3dfca5cd4c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/165d4163-b24b-416b-8b12-5a3dfca5cd4c/MeasureReport-dff85525-6a0a-4ffc-9ade-003aaadd22b4.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 165d4163-b24b-416b-8b12-5a3dfca5cd4c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/165d4163-b24b-416b-8b12-5a3dfca5cd4c/MeasureReport-dff85525-6a0a-4ffc-9ade-003aaadd22b4.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 165d4163-b24b-416b-8b12-5a3dfca5cd4c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/165d4163-b24b-416b-8b12-5a3dfca5cd4c/MeasureReport-dff85525-6a0a-4ffc-9ade-003aaadd22b4.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 165d4163-b24b-416b-8b12-5a3dfca5cd4c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/165d4163-b24b-416b-8b12-5a3dfca5cd4c/MeasureReport-dff85525-6a0a-4ffc-9ade-003aaadd22b4.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 19d1fe56-f3d5-45cf-a3ad-71fe4f70574d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/19d1fe56-f3d5-45cf-a3ad-71fe4f70574d/MeasureReport-a1964f4a-a1de-4717-8ff2-427042035ee1.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 19d1fe56-f3d5-45cf-a3ad-71fe4f70574d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/19d1fe56-f3d5-45cf-a3ad-71fe4f70574d/MeasureReport-a1964f4a-a1de-4717-8ff2-427042035ee1.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 19d1fe56-f3d5-45cf-a3ad-71fe4f70574d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/19d1fe56-f3d5-45cf-a3ad-71fe4f70574d/MeasureReport-a1964f4a-a1de-4717-8ff2-427042035ee1.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 19d1fe56-f3d5-45cf-a3ad-71fe4f70574d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/19d1fe56-f3d5-45cf-a3ad-71fe4f70574d/MeasureReport-a1964f4a-a1de-4717-8ff2-427042035ee1.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 19d1fe56-f3d5-45cf-a3ad-71fe4f70574d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/19d1fe56-f3d5-45cf-a3ad-71fe4f70574d/MeasureReport-a1964f4a-a1de-4717-8ff2-427042035ee1.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 1ae7790b-bb1d-41d1-9454-deac8ef63ea2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1ae7790b-bb1d-41d1-9454-deac8ef63ea2/MeasureReport-fc8ceb32-0bda-4d92-9188-e239fa8776c7.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1ae7790b-bb1d-41d1-9454-deac8ef63ea2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1ae7790b-bb1d-41d1-9454-deac8ef63ea2/MeasureReport-fc8ceb32-0bda-4d92-9188-e239fa8776c7.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1ae7790b-bb1d-41d1-9454-deac8ef63ea2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1ae7790b-bb1d-41d1-9454-deac8ef63ea2/MeasureReport-fc8ceb32-0bda-4d92-9188-e239fa8776c7.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1ae7790b-bb1d-41d1-9454-deac8ef63ea2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1ae7790b-bb1d-41d1-9454-deac8ef63ea2/MeasureReport-fc8ceb32-0bda-4d92-9188-e239fa8776c7.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 1ae7790b-bb1d-41d1-9454-deac8ef63ea2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1ae7790b-bb1d-41d1-9454-deac8ef63ea2/MeasureReport-fc8ceb32-0bda-4d92-9188-e239fa8776c7.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 1cd7ac5b-a9f0-4fe1-8ebc-7fda2a3df2d5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1cd7ac5b-a9f0-4fe1-8ebc-7fda2a3df2d5/MeasureReport-e4184516-16e1-45f6-ab2d-82d37b23c4be.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1cd7ac5b-a9f0-4fe1-8ebc-7fda2a3df2d5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1cd7ac5b-a9f0-4fe1-8ebc-7fda2a3df2d5/MeasureReport-e4184516-16e1-45f6-ab2d-82d37b23c4be.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1cd7ac5b-a9f0-4fe1-8ebc-7fda2a3df2d5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1cd7ac5b-a9f0-4fe1-8ebc-7fda2a3df2d5/MeasureReport-e4184516-16e1-45f6-ab2d-82d37b23c4be.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 1d160b56-edd7-482f-a2e3-8acc26727f91 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1d160b56-edd7-482f-a2e3-8acc26727f91/MeasureReport-5992575a-58af-4948-980e-35c111984393.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1d160b56-edd7-482f-a2e3-8acc26727f91 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1d160b56-edd7-482f-a2e3-8acc26727f91/MeasureReport-5992575a-58af-4948-980e-35c111984393.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1d160b56-edd7-482f-a2e3-8acc26727f91 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1d160b56-edd7-482f-a2e3-8acc26727f91/MeasureReport-5992575a-58af-4948-980e-35c111984393.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1d160b56-edd7-482f-a2e3-8acc26727f91 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1d160b56-edd7-482f-a2e3-8acc26727f91/MeasureReport-5992575a-58af-4948-980e-35c111984393.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1d160b56-edd7-482f-a2e3-8acc26727f91 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1d160b56-edd7-482f-a2e3-8acc26727f91/MeasureReport-5992575a-58af-4948-980e-35c111984393.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ 1d160b56-edd7-482f-a2e3-8acc26727f91 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1d160b56-edd7-482f-a2e3-8acc26727f91/MeasureReport-5992575a-58af-4948-980e-35c111984393.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 1d73e231-88ed-4333-a085-86a7f1527178 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1d73e231-88ed-4333-a085-86a7f1527178/MeasureReport-39e01d91-9da8-42ac-9f94-c86183689c37.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1d73e231-88ed-4333-a085-86a7f1527178 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1d73e231-88ed-4333-a085-86a7f1527178/MeasureReport-39e01d91-9da8-42ac-9f94-c86183689c37.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1d73e231-88ed-4333-a085-86a7f1527178 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1d73e231-88ed-4333-a085-86a7f1527178/MeasureReport-39e01d91-9da8-42ac-9f94-c86183689c37.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 1d73e231-88ed-4333-a085-86a7f1527178 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1d73e231-88ed-4333-a085-86a7f1527178/MeasureReport-39e01d91-9da8-42ac-9f94-c86183689c37.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 1f8f7d6b-3f4c-4479-b59a-84c89567be81 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1f8f7d6b-3f4c-4479-b59a-84c89567be81/MeasureReport-0d2c26db-b6aa-4581-9b79-f4f9de4fe3b1.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1f8f7d6b-3f4c-4479-b59a-84c89567be81 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1f8f7d6b-3f4c-4479-b59a-84c89567be81/MeasureReport-0d2c26db-b6aa-4581-9b79-f4f9de4fe3b1.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 1f8f7d6b-3f4c-4479-b59a-84c89567be81 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/1f8f7d6b-3f4c-4479-b59a-84c89567be81/MeasureReport-0d2c26db-b6aa-4581-9b79-f4f9de4fe3b1.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 22763f21-3a40-4c07-b4a3-3e74b95cfe54 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/22763f21-3a40-4c07-b4a3-3e74b95cfe54/MeasureReport-7df698a7-9eeb-4a67-b7d1-90cb21de6394.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 22763f21-3a40-4c07-b4a3-3e74b95cfe54 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/22763f21-3a40-4c07-b4a3-3e74b95cfe54/MeasureReport-7df698a7-9eeb-4a67-b7d1-90cb21de6394.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 22763f21-3a40-4c07-b4a3-3e74b95cfe54 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/22763f21-3a40-4c07-b4a3-3e74b95cfe54/MeasureReport-7df698a7-9eeb-4a67-b7d1-90cb21de6394.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 22763f21-3a40-4c07-b4a3-3e74b95cfe54 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/22763f21-3a40-4c07-b4a3-3e74b95cfe54/MeasureReport-7df698a7-9eeb-4a67-b7d1-90cb21de6394.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 240aca16-2f20-4213-b02a-232cec686fe4 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/240aca16-2f20-4213-b02a-232cec686fe4/MeasureReport-5dd5df52-90ba-4509-b7ba-3fed0d46fd7d.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 240aca16-2f20-4213-b02a-232cec686fe4 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/240aca16-2f20-4213-b02a-232cec686fe4/MeasureReport-5dd5df52-90ba-4509-b7ba-3fed0d46fd7d.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 240aca16-2f20-4213-b02a-232cec686fe4 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/240aca16-2f20-4213-b02a-232cec686fe4/MeasureReport-5dd5df52-90ba-4509-b7ba-3fed0d46fd7d.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 250b2590-8b2a-4fc0-a725-227d9bb7556b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/250b2590-8b2a-4fc0-a725-227d9bb7556b/MeasureReport-963158aa-5a7d-46b8-a8d8-bcab39a34d8c.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 250b2590-8b2a-4fc0-a725-227d9bb7556b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/250b2590-8b2a-4fc0-a725-227d9bb7556b/MeasureReport-963158aa-5a7d-46b8-a8d8-bcab39a34d8c.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 250b2590-8b2a-4fc0-a725-227d9bb7556b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/250b2590-8b2a-4fc0-a725-227d9bb7556b/MeasureReport-963158aa-5a7d-46b8-a8d8-bcab39a34d8c.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 250b2590-8b2a-4fc0-a725-227d9bb7556b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/250b2590-8b2a-4fc0-a725-227d9bb7556b/MeasureReport-963158aa-5a7d-46b8-a8d8-bcab39a34d8c.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 2571a635-c375-4aa6-9eef-653062467e10 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2571a635-c375-4aa6-9eef-653062467e10/MeasureReport-cb2dc338-8f8b-4abb-9c5d-beefc62b8c7a.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2571a635-c375-4aa6-9eef-653062467e10 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2571a635-c375-4aa6-9eef-653062467e10/MeasureReport-cb2dc338-8f8b-4abb-9c5d-beefc62b8c7a.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2571a635-c375-4aa6-9eef-653062467e10 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2571a635-c375-4aa6-9eef-653062467e10/MeasureReport-cb2dc338-8f8b-4abb-9c5d-beefc62b8c7a.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 2671a090-11ca-4f99-a1c9-49f44c4ec3a6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2671a090-11ca-4f99-a1c9-49f44c4ec3a6/MeasureReport-6a093324-8621-41c8-9ddd-fb7ce3edb406.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2671a090-11ca-4f99-a1c9-49f44c4ec3a6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2671a090-11ca-4f99-a1c9-49f44c4ec3a6/MeasureReport-6a093324-8621-41c8-9ddd-fb7ce3edb406.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2671a090-11ca-4f99-a1c9-49f44c4ec3a6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2671a090-11ca-4f99-a1c9-49f44c4ec3a6/MeasureReport-6a093324-8621-41c8-9ddd-fb7ce3edb406.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2671a090-11ca-4f99-a1c9-49f44c4ec3a6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2671a090-11ca-4f99-a1c9-49f44c4ec3a6/MeasureReport-6a093324-8621-41c8-9ddd-fb7ce3edb406.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 2671a090-11ca-4f99-a1c9-49f44c4ec3a6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2671a090-11ca-4f99-a1c9-49f44c4ec3a6/MeasureReport-6a093324-8621-41c8-9ddd-fb7ce3edb406.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 269cb313-87d2-4531-a2ae-120c413918ed ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/269cb313-87d2-4531-a2ae-120c413918ed/MeasureReport-7ddfa4a9-ae6e-4383-8077-66f21769fa35.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 269cb313-87d2-4531-a2ae-120c413918ed ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/269cb313-87d2-4531-a2ae-120c413918ed/MeasureReport-7ddfa4a9-ae6e-4383-8077-66f21769fa35.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 269cb313-87d2-4531-a2ae-120c413918ed ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/269cb313-87d2-4531-a2ae-120c413918ed/MeasureReport-7ddfa4a9-ae6e-4383-8077-66f21769fa35.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 2be50408-23f8-454d-97e4-0fbdda2e9057 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2be50408-23f8-454d-97e4-0fbdda2e9057/MeasureReport-78fdcda8-9a32-4d13-bf98-cb9af0ccd10c.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2be50408-23f8-454d-97e4-0fbdda2e9057 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2be50408-23f8-454d-97e4-0fbdda2e9057/MeasureReport-78fdcda8-9a32-4d13-bf98-cb9af0ccd10c.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2be50408-23f8-454d-97e4-0fbdda2e9057 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2be50408-23f8-454d-97e4-0fbdda2e9057/MeasureReport-78fdcda8-9a32-4d13-bf98-cb9af0ccd10c.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 2be50408-23f8-454d-97e4-0fbdda2e9057 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2be50408-23f8-454d-97e4-0fbdda2e9057/MeasureReport-78fdcda8-9a32-4d13-bf98-cb9af0ccd10c.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 2d70f119-a79e-4602-8840-36d26d91cdf6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2d70f119-a79e-4602-8840-36d26d91cdf6/MeasureReport-400f6a98-195a-439e-aa06-19090906a7e8.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2d70f119-a79e-4602-8840-36d26d91cdf6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2d70f119-a79e-4602-8840-36d26d91cdf6/MeasureReport-400f6a98-195a-439e-aa06-19090906a7e8.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2d70f119-a79e-4602-8840-36d26d91cdf6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2d70f119-a79e-4602-8840-36d26d91cdf6/MeasureReport-400f6a98-195a-439e-aa06-19090906a7e8.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2d70f119-a79e-4602-8840-36d26d91cdf6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2d70f119-a79e-4602-8840-36d26d91cdf6/MeasureReport-400f6a98-195a-439e-aa06-19090906a7e8.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 2d70f119-a79e-4602-8840-36d26d91cdf6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2d70f119-a79e-4602-8840-36d26d91cdf6/MeasureReport-400f6a98-195a-439e-aa06-19090906a7e8.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ 2d70f119-a79e-4602-8840-36d26d91cdf6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/2d70f119-a79e-4602-8840-36d26d91cdf6/MeasureReport-400f6a98-195a-439e-aa06-19090906a7e8.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 31ed0db8-1972-40ed-be78-0653f2cd9a95 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/31ed0db8-1972-40ed-be78-0653f2cd9a95/MeasureReport-aad42de7-f25c-444e-8dfa-38018b30fccc.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 31ed0db8-1972-40ed-be78-0653f2cd9a95 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/31ed0db8-1972-40ed-be78-0653f2cd9a95/MeasureReport-aad42de7-f25c-444e-8dfa-38018b30fccc.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 31ed0db8-1972-40ed-be78-0653f2cd9a95 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/31ed0db8-1972-40ed-be78-0653f2cd9a95/MeasureReport-aad42de7-f25c-444e-8dfa-38018b30fccc.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 31ed0db8-1972-40ed-be78-0653f2cd9a95 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/31ed0db8-1972-40ed-be78-0653f2cd9a95/MeasureReport-aad42de7-f25c-444e-8dfa-38018b30fccc.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ 31ed0db8-1972-40ed-be78-0653f2cd9a95 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/31ed0db8-1972-40ed-be78-0653f2cd9a95/MeasureReport-aad42de7-f25c-444e-8dfa-38018b30fccc.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 327a31a9-17cf-41ac-a6c9-10c7ccb10d77 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/327a31a9-17cf-41ac-a6c9-10c7ccb10d77/MeasureReport-9dffb703-3948-4b13-8864-bba4f9a78906.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 327a31a9-17cf-41ac-a6c9-10c7ccb10d77 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/327a31a9-17cf-41ac-a6c9-10c7ccb10d77/MeasureReport-9dffb703-3948-4b13-8864-bba4f9a78906.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 327a31a9-17cf-41ac-a6c9-10c7ccb10d77 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/327a31a9-17cf-41ac-a6c9-10c7ccb10d77/MeasureReport-9dffb703-3948-4b13-8864-bba4f9a78906.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 327a31a9-17cf-41ac-a6c9-10c7ccb10d77 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/327a31a9-17cf-41ac-a6c9-10c7ccb10d77/MeasureReport-9dffb703-3948-4b13-8864-bba4f9a78906.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 33c0f746-578a-4747-8f99-35c44119cebf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/33c0f746-578a-4747-8f99-35c44119cebf/MeasureReport-f00f41bb-32d5-48ce-a91c-a7c11922ae69.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 33c0f746-578a-4747-8f99-35c44119cebf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/33c0f746-578a-4747-8f99-35c44119cebf/MeasureReport-f00f41bb-32d5-48ce-a91c-a7c11922ae69.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 33c0f746-578a-4747-8f99-35c44119cebf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/33c0f746-578a-4747-8f99-35c44119cebf/MeasureReport-f00f41bb-32d5-48ce-a91c-a7c11922ae69.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 33c0f746-578a-4747-8f99-35c44119cebf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/33c0f746-578a-4747-8f99-35c44119cebf/MeasureReport-f00f41bb-32d5-48ce-a91c-a7c11922ae69.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 342ce37f-9c12-4963-9570-878d101ff24f ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/342ce37f-9c12-4963-9570-878d101ff24f/MeasureReport-66067b9e-82d3-4f8a-b15e-f366b5a6c222.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 342ce37f-9c12-4963-9570-878d101ff24f ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/342ce37f-9c12-4963-9570-878d101ff24f/MeasureReport-66067b9e-82d3-4f8a-b15e-f366b5a6c222.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 342ce37f-9c12-4963-9570-878d101ff24f ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/342ce37f-9c12-4963-9570-878d101ff24f/MeasureReport-66067b9e-82d3-4f8a-b15e-f366b5a6c222.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 36a9b107-b6ad-48ce-8b4b-e616b2474ff8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/36a9b107-b6ad-48ce-8b4b-e616b2474ff8/MeasureReport-b046a355-c40e-493e-9ee5-9ccf4d84e18e.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 36a9b107-b6ad-48ce-8b4b-e616b2474ff8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/36a9b107-b6ad-48ce-8b4b-e616b2474ff8/MeasureReport-b046a355-c40e-493e-9ee5-9ccf4d84e18e.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 36a9b107-b6ad-48ce-8b4b-e616b2474ff8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/36a9b107-b6ad-48ce-8b4b-e616b2474ff8/MeasureReport-b046a355-c40e-493e-9ee5-9ccf4d84e18e.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 36a9b107-b6ad-48ce-8b4b-e616b2474ff8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/36a9b107-b6ad-48ce-8b4b-e616b2474ff8/MeasureReport-b046a355-c40e-493e-9ee5-9ccf4d84e18e.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 37b7fdb2-4aa2-4cbb-ab87-5db3cf7bcf33 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/37b7fdb2-4aa2-4cbb-ab87-5db3cf7bcf33/MeasureReport-8dfa5843-3ce4-4152-8064-caf324a4fe84.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 37b7fdb2-4aa2-4cbb-ab87-5db3cf7bcf33 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/37b7fdb2-4aa2-4cbb-ab87-5db3cf7bcf33/MeasureReport-8dfa5843-3ce4-4152-8064-caf324a4fe84.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 37b7fdb2-4aa2-4cbb-ab87-5db3cf7bcf33 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/37b7fdb2-4aa2-4cbb-ab87-5db3cf7bcf33/MeasureReport-8dfa5843-3ce4-4152-8064-caf324a4fe84.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 3a1145a6-7884-4c11-be4d-dd7641fd7beb ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3a1145a6-7884-4c11-be4d-dd7641fd7beb/MeasureReport-b369ebb3-a438-422e-b705-3672e1cd4592.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 3a1145a6-7884-4c11-be4d-dd7641fd7beb ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3a1145a6-7884-4c11-be4d-dd7641fd7beb/MeasureReport-b369ebb3-a438-422e-b705-3672e1cd4592.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 3a1145a6-7884-4c11-be4d-dd7641fd7beb ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3a1145a6-7884-4c11-be4d-dd7641fd7beb/MeasureReport-b369ebb3-a438-422e-b705-3672e1cd4592.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 3c815aab-4636-4cde-9677-7067261287b5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3c815aab-4636-4cde-9677-7067261287b5/MeasureReport-a46ce3df-8345-4013-81b7-6fb6c32d92a4.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 3c815aab-4636-4cde-9677-7067261287b5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3c815aab-4636-4cde-9677-7067261287b5/MeasureReport-a46ce3df-8345-4013-81b7-6fb6c32d92a4.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 3c815aab-4636-4cde-9677-7067261287b5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3c815aab-4636-4cde-9677-7067261287b5/MeasureReport-a46ce3df-8345-4013-81b7-6fb6c32d92a4.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 3c815aab-4636-4cde-9677-7067261287b5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3c815aab-4636-4cde-9677-7067261287b5/MeasureReport-a46ce3df-8345-4013-81b7-6fb6c32d92a4.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 3c815aab-4636-4cde-9677-7067261287b5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3c815aab-4636-4cde-9677-7067261287b5/MeasureReport-a46ce3df-8345-4013-81b7-6fb6c32d92a4.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 3c8279f4-42ee-45a4-aaac-61cf53a121a2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3c8279f4-42ee-45a4-aaac-61cf53a121a2/MeasureReport-d0831a27-05c3-4fa9-aeff-10fa74964cf0.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 3c8279f4-42ee-45a4-aaac-61cf53a121a2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3c8279f4-42ee-45a4-aaac-61cf53a121a2/MeasureReport-d0831a27-05c3-4fa9-aeff-10fa74964cf0.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 3c8279f4-42ee-45a4-aaac-61cf53a121a2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3c8279f4-42ee-45a4-aaac-61cf53a121a2/MeasureReport-d0831a27-05c3-4fa9-aeff-10fa74964cf0.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 3c8279f4-42ee-45a4-aaac-61cf53a121a2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3c8279f4-42ee-45a4-aaac-61cf53a121a2/MeasureReport-d0831a27-05c3-4fa9-aeff-10fa74964cf0.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 3c8279f4-42ee-45a4-aaac-61cf53a121a2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3c8279f4-42ee-45a4-aaac-61cf53a121a2/MeasureReport-d0831a27-05c3-4fa9-aeff-10fa74964cf0.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 3d1544d6-2a0d-4e00-949f-c30783cd23fe ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3d1544d6-2a0d-4e00-949f-c30783cd23fe/MeasureReport-5677707a-c92b-4da2-b04a-a9ea7f532c21.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 3d1544d6-2a0d-4e00-949f-c30783cd23fe ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3d1544d6-2a0d-4e00-949f-c30783cd23fe/MeasureReport-5677707a-c92b-4da2-b04a-a9ea7f532c21.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 3d1544d6-2a0d-4e00-949f-c30783cd23fe ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/3d1544d6-2a0d-4e00-949f-c30783cd23fe/MeasureReport-5677707a-c92b-4da2-b04a-a9ea7f532c21.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 46dc35fd-f1e0-46a0-8e67-8bbf5bd1cbca ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/46dc35fd-f1e0-46a0-8e67-8bbf5bd1cbca/MeasureReport-517683aa-d9eb-4445-aab3-f2438b88f407.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 46dc35fd-f1e0-46a0-8e67-8bbf5bd1cbca ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/46dc35fd-f1e0-46a0-8e67-8bbf5bd1cbca/MeasureReport-517683aa-d9eb-4445-aab3-f2438b88f407.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 46dc35fd-f1e0-46a0-8e67-8bbf5bd1cbca ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/46dc35fd-f1e0-46a0-8e67-8bbf5bd1cbca/MeasureReport-517683aa-d9eb-4445-aab3-f2438b88f407.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 46dc35fd-f1e0-46a0-8e67-8bbf5bd1cbca ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/46dc35fd-f1e0-46a0-8e67-8bbf5bd1cbca/MeasureReport-517683aa-d9eb-4445-aab3-f2438b88f407.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 46dc35fd-f1e0-46a0-8e67-8bbf5bd1cbca ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/46dc35fd-f1e0-46a0-8e67-8bbf5bd1cbca/MeasureReport-517683aa-d9eb-4445-aab3-f2438b88f407.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 47643903-6f24-428e-a354-f4a59d006ddf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/47643903-6f24-428e-a354-f4a59d006ddf/MeasureReport-b565490e-a150-40b4-9815-70fa8f2b3133.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 47643903-6f24-428e-a354-f4a59d006ddf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/47643903-6f24-428e-a354-f4a59d006ddf/MeasureReport-b565490e-a150-40b4-9815-70fa8f2b3133.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 47643903-6f24-428e-a354-f4a59d006ddf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/47643903-6f24-428e-a354-f4a59d006ddf/MeasureReport-b565490e-a150-40b4-9815-70fa8f2b3133.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 47643903-6f24-428e-a354-f4a59d006ddf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/47643903-6f24-428e-a354-f4a59d006ddf/MeasureReport-b565490e-a150-40b4-9815-70fa8f2b3133.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 47963aa4-4cbe-4129-b115-888542ba8b1d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/47963aa4-4cbe-4129-b115-888542ba8b1d/MeasureReport-688dc495-f03f-4976-a6d4-82e8b11b8fc6.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 47963aa4-4cbe-4129-b115-888542ba8b1d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/47963aa4-4cbe-4129-b115-888542ba8b1d/MeasureReport-688dc495-f03f-4976-a6d4-82e8b11b8fc6.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 47963aa4-4cbe-4129-b115-888542ba8b1d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/47963aa4-4cbe-4129-b115-888542ba8b1d/MeasureReport-688dc495-f03f-4976-a6d4-82e8b11b8fc6.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 47963aa4-4cbe-4129-b115-888542ba8b1d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/47963aa4-4cbe-4129-b115-888542ba8b1d/MeasureReport-688dc495-f03f-4976-a6d4-82e8b11b8fc6.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 47963aa4-4cbe-4129-b115-888542ba8b1d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/47963aa4-4cbe-4129-b115-888542ba8b1d/MeasureReport-688dc495-f03f-4976-a6d4-82e8b11b8fc6.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 49601611-a625-48dd-a54a-e28dd3d600e7 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/49601611-a625-48dd-a54a-e28dd3d600e7/MeasureReport-17e4ca58-1bb4-43e8-bb6d-7a400fd92094.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 49601611-a625-48dd-a54a-e28dd3d600e7 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/49601611-a625-48dd-a54a-e28dd3d600e7/MeasureReport-17e4ca58-1bb4-43e8-bb6d-7a400fd92094.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 49601611-a625-48dd-a54a-e28dd3d600e7 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/49601611-a625-48dd-a54a-e28dd3d600e7/MeasureReport-17e4ca58-1bb4-43e8-bb6d-7a400fd92094.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 49601611-a625-48dd-a54a-e28dd3d600e7 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/49601611-a625-48dd-a54a-e28dd3d600e7/MeasureReport-17e4ca58-1bb4-43e8-bb6d-7a400fd92094.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 49601611-a625-48dd-a54a-e28dd3d600e7 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/49601611-a625-48dd-a54a-e28dd3d600e7/MeasureReport-17e4ca58-1bb4-43e8-bb6d-7a400fd92094.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ 49601611-a625-48dd-a54a-e28dd3d600e7 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/49601611-a625-48dd-a54a-e28dd3d600e7/MeasureReport-17e4ca58-1bb4-43e8-bb6d-7a400fd92094.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 4d1811f1-f822-4e7c-9ab1-4752e5fafe79 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4d1811f1-f822-4e7c-9ab1-4752e5fafe79/MeasureReport-86283cdf-83b4-4294-a53a-fd76040f1bf3.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 4d1811f1-f822-4e7c-9ab1-4752e5fafe79 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4d1811f1-f822-4e7c-9ab1-4752e5fafe79/MeasureReport-86283cdf-83b4-4294-a53a-fd76040f1bf3.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 4d1811f1-f822-4e7c-9ab1-4752e5fafe79 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4d1811f1-f822-4e7c-9ab1-4752e5fafe79/MeasureReport-86283cdf-83b4-4294-a53a-fd76040f1bf3.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 4df1538c-c20f-444c-a679-15de516ee684 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4df1538c-c20f-444c-a679-15de516ee684/MeasureReport-fcc8776d-a560-4376-a895-72a7d3e71031.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 4df1538c-c20f-444c-a679-15de516ee684 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4df1538c-c20f-444c-a679-15de516ee684/MeasureReport-fcc8776d-a560-4376-a895-72a7d3e71031.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 4df1538c-c20f-444c-a679-15de516ee684 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4df1538c-c20f-444c-a679-15de516ee684/MeasureReport-fcc8776d-a560-4376-a895-72a7d3e71031.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 4e1967c5-7fe0-4920-97aa-28d02e117c31 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4e1967c5-7fe0-4920-97aa-28d02e117c31/MeasureReport-bb0e1282-7f47-4beb-a023-f4bf0531718a.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 4e1967c5-7fe0-4920-97aa-28d02e117c31 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4e1967c5-7fe0-4920-97aa-28d02e117c31/MeasureReport-bb0e1282-7f47-4beb-a023-f4bf0531718a.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 4e1967c5-7fe0-4920-97aa-28d02e117c31 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4e1967c5-7fe0-4920-97aa-28d02e117c31/MeasureReport-bb0e1282-7f47-4beb-a023-f4bf0531718a.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 4e1967c5-7fe0-4920-97aa-28d02e117c31 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4e1967c5-7fe0-4920-97aa-28d02e117c31/MeasureReport-bb0e1282-7f47-4beb-a023-f4bf0531718a.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 4f90847d-e626-4ebb-8382-ae1c05079579 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4f90847d-e626-4ebb-8382-ae1c05079579/MeasureReport-3d7840dd-5072-4be1-b5c0-9049987ac799.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 4f90847d-e626-4ebb-8382-ae1c05079579 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4f90847d-e626-4ebb-8382-ae1c05079579/MeasureReport-3d7840dd-5072-4be1-b5c0-9049987ac799.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 4f90847d-e626-4ebb-8382-ae1c05079579 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/4f90847d-e626-4ebb-8382-ae1c05079579/MeasureReport-3d7840dd-5072-4be1-b5c0-9049987ac799.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 568283da-a161-4cb8-85d0-e2ef32e60a11 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/568283da-a161-4cb8-85d0-e2ef32e60a11/MeasureReport-e5a30474-0bdd-4a4a-9821-1038d1e8cc46.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 568283da-a161-4cb8-85d0-e2ef32e60a11 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/568283da-a161-4cb8-85d0-e2ef32e60a11/MeasureReport-e5a30474-0bdd-4a4a-9821-1038d1e8cc46.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 568283da-a161-4cb8-85d0-e2ef32e60a11 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/568283da-a161-4cb8-85d0-e2ef32e60a11/MeasureReport-e5a30474-0bdd-4a4a-9821-1038d1e8cc46.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 568283da-a161-4cb8-85d0-e2ef32e60a11 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/568283da-a161-4cb8-85d0-e2ef32e60a11/MeasureReport-e5a30474-0bdd-4a4a-9821-1038d1e8cc46.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 568283da-a161-4cb8-85d0-e2ef32e60a11 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/568283da-a161-4cb8-85d0-e2ef32e60a11/MeasureReport-e5a30474-0bdd-4a4a-9821-1038d1e8cc46.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 5a29e7c9-9e30-4083-9bd2-6a05031d4387 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/5a29e7c9-9e30-4083-9bd2-6a05031d4387/MeasureReport-ab3f4d39-5b0e-4671-978d-e31a09212b53.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 5a29e7c9-9e30-4083-9bd2-6a05031d4387 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/5a29e7c9-9e30-4083-9bd2-6a05031d4387/MeasureReport-ab3f4d39-5b0e-4671-978d-e31a09212b53.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 5a29e7c9-9e30-4083-9bd2-6a05031d4387 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/5a29e7c9-9e30-4083-9bd2-6a05031d4387/MeasureReport-ab3f4d39-5b0e-4671-978d-e31a09212b53.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 5a29e7c9-9e30-4083-9bd2-6a05031d4387 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/5a29e7c9-9e30-4083-9bd2-6a05031d4387/MeasureReport-ab3f4d39-5b0e-4671-978d-e31a09212b53.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 61c2c588-8a0f-44db-bfdb-e4ccc17641ed ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/61c2c588-8a0f-44db-bfdb-e4ccc17641ed/MeasureReport-a822b5f5-bb5b-4b7b-b28f-927b0400f83f.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 61c2c588-8a0f-44db-bfdb-e4ccc17641ed ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/61c2c588-8a0f-44db-bfdb-e4ccc17641ed/MeasureReport-a822b5f5-bb5b-4b7b-b28f-927b0400f83f.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 61c2c588-8a0f-44db-bfdb-e4ccc17641ed ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/61c2c588-8a0f-44db-bfdb-e4ccc17641ed/MeasureReport-a822b5f5-bb5b-4b7b-b28f-927b0400f83f.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 61c2c588-8a0f-44db-bfdb-e4ccc17641ed ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/61c2c588-8a0f-44db-bfdb-e4ccc17641ed/MeasureReport-a822b5f5-bb5b-4b7b-b28f-927b0400f83f.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 61ff8a5e-1936-4411-913f-f45f595d1ce8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/61ff8a5e-1936-4411-913f-f45f595d1ce8/MeasureReport-bc3745d3-b105-47ea-b218-68571cad17df.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 61ff8a5e-1936-4411-913f-f45f595d1ce8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/61ff8a5e-1936-4411-913f-f45f595d1ce8/MeasureReport-bc3745d3-b105-47ea-b218-68571cad17df.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 61ff8a5e-1936-4411-913f-f45f595d1ce8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/61ff8a5e-1936-4411-913f-f45f595d1ce8/MeasureReport-bc3745d3-b105-47ea-b218-68571cad17df.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 62f63de5-1a53-44d4-a980-4406db339714 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/62f63de5-1a53-44d4-a980-4406db339714/MeasureReport-12b8b881-5507-4204-9286-d7c3a7d53e9e.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 62f63de5-1a53-44d4-a980-4406db339714 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/62f63de5-1a53-44d4-a980-4406db339714/MeasureReport-12b8b881-5507-4204-9286-d7c3a7d53e9e.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 62f63de5-1a53-44d4-a980-4406db339714 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/62f63de5-1a53-44d4-a980-4406db339714/MeasureReport-12b8b881-5507-4204-9286-d7c3a7d53e9e.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 62f63de5-1a53-44d4-a980-4406db339714 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/62f63de5-1a53-44d4-a980-4406db339714/MeasureReport-12b8b881-5507-4204-9286-d7c3a7d53e9e.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 62f63de5-1a53-44d4-a980-4406db339714 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/62f63de5-1a53-44d4-a980-4406db339714/MeasureReport-12b8b881-5507-4204-9286-d7c3a7d53e9e.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ 62f63de5-1a53-44d4-a980-4406db339714 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/62f63de5-1a53-44d4-a980-4406db339714/MeasureReport-12b8b881-5507-4204-9286-d7c3a7d53e9e.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 6432b1e4-0953-459f-932e-9f6a92a9fb0c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/6432b1e4-0953-459f-932e-9f6a92a9fb0c/MeasureReport-d93d6f05-2a58-440c-8a5d-b8c4ac6ce069.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 6432b1e4-0953-459f-932e-9f6a92a9fb0c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/6432b1e4-0953-459f-932e-9f6a92a9fb0c/MeasureReport-d93d6f05-2a58-440c-8a5d-b8c4ac6ce069.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 6432b1e4-0953-459f-932e-9f6a92a9fb0c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/6432b1e4-0953-459f-932e-9f6a92a9fb0c/MeasureReport-d93d6f05-2a58-440c-8a5d-b8c4ac6ce069.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 64a7c79b-1858-4cc5-a662-b6773e796a29 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/64a7c79b-1858-4cc5-a662-b6773e796a29/MeasureReport-2302a70b-be36-413f-8143-3ae568bbca85.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 64a7c79b-1858-4cc5-a662-b6773e796a29 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/64a7c79b-1858-4cc5-a662-b6773e796a29/MeasureReport-2302a70b-be36-413f-8143-3ae568bbca85.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 64a7c79b-1858-4cc5-a662-b6773e796a29 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/64a7c79b-1858-4cc5-a662-b6773e796a29/MeasureReport-2302a70b-be36-413f-8143-3ae568bbca85.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 64a7c79b-1858-4cc5-a662-b6773e796a29 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/64a7c79b-1858-4cc5-a662-b6773e796a29/MeasureReport-2302a70b-be36-413f-8143-3ae568bbca85.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 67a96ee1-da0c-4ec6-859d-da4c287c4443 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/67a96ee1-da0c-4ec6-859d-da4c287c4443/MeasureReport-ebd838d0-41e3-4d29-8f3e-667d97014043.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 67a96ee1-da0c-4ec6-859d-da4c287c4443 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/67a96ee1-da0c-4ec6-859d-da4c287c4443/MeasureReport-ebd838d0-41e3-4d29-8f3e-667d97014043.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 67a96ee1-da0c-4ec6-859d-da4c287c4443 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/67a96ee1-da0c-4ec6-859d-da4c287c4443/MeasureReport-ebd838d0-41e3-4d29-8f3e-667d97014043.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 67d405db-521a-41bc-8928-ca1a4b79d40c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/67d405db-521a-41bc-8928-ca1a4b79d40c/MeasureReport-5275aa77-4d78-4648-93f3-971f7a5662c4.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 67d405db-521a-41bc-8928-ca1a4b79d40c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/67d405db-521a-41bc-8928-ca1a4b79d40c/MeasureReport-5275aa77-4d78-4648-93f3-971f7a5662c4.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 67d405db-521a-41bc-8928-ca1a4b79d40c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/67d405db-521a-41bc-8928-ca1a4b79d40c/MeasureReport-5275aa77-4d78-4648-93f3-971f7a5662c4.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 67d405db-521a-41bc-8928-ca1a4b79d40c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/67d405db-521a-41bc-8928-ca1a4b79d40c/MeasureReport-5275aa77-4d78-4648-93f3-971f7a5662c4.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 6ddcc1ce-fd55-4bcf-b154-2555cbaa71ae ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/6ddcc1ce-fd55-4bcf-b154-2555cbaa71ae/MeasureReport-115f1eaa-c6c2-4177-90ba-19b2cf75f584.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 6ddcc1ce-fd55-4bcf-b154-2555cbaa71ae ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/6ddcc1ce-fd55-4bcf-b154-2555cbaa71ae/MeasureReport-115f1eaa-c6c2-4177-90ba-19b2cf75f584.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 6ddcc1ce-fd55-4bcf-b154-2555cbaa71ae ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/6ddcc1ce-fd55-4bcf-b154-2555cbaa71ae/MeasureReport-115f1eaa-c6c2-4177-90ba-19b2cf75f584.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 70591696-5925-43a0-ba94-6f9af6613d9d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/70591696-5925-43a0-ba94-6f9af6613d9d/MeasureReport-11a60483-4e41-4ab7-8cd9-469b9a0b0615.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 70591696-5925-43a0-ba94-6f9af6613d9d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/70591696-5925-43a0-ba94-6f9af6613d9d/MeasureReport-11a60483-4e41-4ab7-8cd9-469b9a0b0615.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 70591696-5925-43a0-ba94-6f9af6613d9d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/70591696-5925-43a0-ba94-6f9af6613d9d/MeasureReport-11a60483-4e41-4ab7-8cd9-469b9a0b0615.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 72cacd71-fdea-4b34-91b0-04928e79222c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/72cacd71-fdea-4b34-91b0-04928e79222c/MeasureReport-85bc052a-aeda-4034-9cb3-29455b86f2b0.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 72cacd71-fdea-4b34-91b0-04928e79222c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/72cacd71-fdea-4b34-91b0-04928e79222c/MeasureReport-85bc052a-aeda-4034-9cb3-29455b86f2b0.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 72cacd71-fdea-4b34-91b0-04928e79222c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/72cacd71-fdea-4b34-91b0-04928e79222c/MeasureReport-85bc052a-aeda-4034-9cb3-29455b86f2b0.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 72cacd71-fdea-4b34-91b0-04928e79222c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/72cacd71-fdea-4b34-91b0-04928e79222c/MeasureReport-85bc052a-aeda-4034-9cb3-29455b86f2b0.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 756e6c64-6ee6-4e02-816c-a0612ef36bed ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/756e6c64-6ee6-4e02-816c-a0612ef36bed/MeasureReport-0f9a40cb-b1e2-49e0-9188-27636ad9c688.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 756e6c64-6ee6-4e02-816c-a0612ef36bed ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/756e6c64-6ee6-4e02-816c-a0612ef36bed/MeasureReport-0f9a40cb-b1e2-49e0-9188-27636ad9c688.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 756e6c64-6ee6-4e02-816c-a0612ef36bed ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/756e6c64-6ee6-4e02-816c-a0612ef36bed/MeasureReport-0f9a40cb-b1e2-49e0-9188-27636ad9c688.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 75d19c2d-5ae9-476e-93f5-e84e668512a8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/75d19c2d-5ae9-476e-93f5-e84e668512a8/MeasureReport-6e8aa31c-2306-4133-a291-43123dd5c5eb.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 75d19c2d-5ae9-476e-93f5-e84e668512a8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/75d19c2d-5ae9-476e-93f5-e84e668512a8/MeasureReport-6e8aa31c-2306-4133-a291-43123dd5c5eb.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 75d19c2d-5ae9-476e-93f5-e84e668512a8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/75d19c2d-5ae9-476e-93f5-e84e668512a8/MeasureReport-6e8aa31c-2306-4133-a291-43123dd5c5eb.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 78a3078e-68a7-4715-9490-60eaf10111a4 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/78a3078e-68a7-4715-9490-60eaf10111a4/MeasureReport-552b96d1-6d31-4b98-ad3c-dc287ba83ee7.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 78a3078e-68a7-4715-9490-60eaf10111a4 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/78a3078e-68a7-4715-9490-60eaf10111a4/MeasureReport-552b96d1-6d31-4b98-ad3c-dc287ba83ee7.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 78a3078e-68a7-4715-9490-60eaf10111a4 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/78a3078e-68a7-4715-9490-60eaf10111a4/MeasureReport-552b96d1-6d31-4b98-ad3c-dc287ba83ee7.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 78a3078e-68a7-4715-9490-60eaf10111a4 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/78a3078e-68a7-4715-9490-60eaf10111a4/MeasureReport-552b96d1-6d31-4b98-ad3c-dc287ba83ee7.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 78a3078e-68a7-4715-9490-60eaf10111a4 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/78a3078e-68a7-4715-9490-60eaf10111a4/MeasureReport-552b96d1-6d31-4b98-ad3c-dc287ba83ee7.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ 78a3078e-68a7-4715-9490-60eaf10111a4 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/78a3078e-68a7-4715-9490-60eaf10111a4/MeasureReport-552b96d1-6d31-4b98-ad3c-dc287ba83ee7.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 7a61226f-6ed7-4fa9-9055-5e9b0234db9b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/7a61226f-6ed7-4fa9-9055-5e9b0234db9b/MeasureReport-321da364-04b0-4680-869c-43e17e6038cd.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 7a61226f-6ed7-4fa9-9055-5e9b0234db9b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/7a61226f-6ed7-4fa9-9055-5e9b0234db9b/MeasureReport-321da364-04b0-4680-869c-43e17e6038cd.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 7a61226f-6ed7-4fa9-9055-5e9b0234db9b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/7a61226f-6ed7-4fa9-9055-5e9b0234db9b/MeasureReport-321da364-04b0-4680-869c-43e17e6038cd.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 7a61226f-6ed7-4fa9-9055-5e9b0234db9b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/7a61226f-6ed7-4fa9-9055-5e9b0234db9b/MeasureReport-321da364-04b0-4680-869c-43e17e6038cd.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 7a61226f-6ed7-4fa9-9055-5e9b0234db9b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/7a61226f-6ed7-4fa9-9055-5e9b0234db9b/MeasureReport-321da364-04b0-4680-869c-43e17e6038cd.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ 7a61226f-6ed7-4fa9-9055-5e9b0234db9b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/7a61226f-6ed7-4fa9-9055-5e9b0234db9b/MeasureReport-321da364-04b0-4680-869c-43e17e6038cd.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 7fc738cc-7bdf-4f45-8239-959c0492102b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/7fc738cc-7bdf-4f45-8239-959c0492102b/MeasureReport-5b9cc675-04f5-4295-b6ab-218580ff5fe3.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 7fc738cc-7bdf-4f45-8239-959c0492102b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/7fc738cc-7bdf-4f45-8239-959c0492102b/MeasureReport-5b9cc675-04f5-4295-b6ab-218580ff5fe3.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 7fc738cc-7bdf-4f45-8239-959c0492102b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/7fc738cc-7bdf-4f45-8239-959c0492102b/MeasureReport-5b9cc675-04f5-4295-b6ab-218580ff5fe3.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 7fc738cc-7bdf-4f45-8239-959c0492102b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/7fc738cc-7bdf-4f45-8239-959c0492102b/MeasureReport-5b9cc675-04f5-4295-b6ab-218580ff5fe3.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 8090a147-702c-452a-b59d-524c2bcbaaad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8090a147-702c-452a-b59d-524c2bcbaaad/MeasureReport-7a759ea4-3444-4e87-ade0-4ff67b9ba989.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8090a147-702c-452a-b59d-524c2bcbaaad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8090a147-702c-452a-b59d-524c2bcbaaad/MeasureReport-7a759ea4-3444-4e87-ade0-4ff67b9ba989.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8090a147-702c-452a-b59d-524c2bcbaaad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8090a147-702c-452a-b59d-524c2bcbaaad/MeasureReport-7a759ea4-3444-4e87-ade0-4ff67b9ba989.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 81470689-a3f6-4580-88e0-5cddc5813c79 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/81470689-a3f6-4580-88e0-5cddc5813c79/MeasureReport-405de7e9-5fa3-49fd-a578-5afad2bcf224.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 81470689-a3f6-4580-88e0-5cddc5813c79 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/81470689-a3f6-4580-88e0-5cddc5813c79/MeasureReport-405de7e9-5fa3-49fd-a578-5afad2bcf224.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 81470689-a3f6-4580-88e0-5cddc5813c79 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/81470689-a3f6-4580-88e0-5cddc5813c79/MeasureReport-405de7e9-5fa3-49fd-a578-5afad2bcf224.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 81470689-a3f6-4580-88e0-5cddc5813c79 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/81470689-a3f6-4580-88e0-5cddc5813c79/MeasureReport-405de7e9-5fa3-49fd-a578-5afad2bcf224.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 81470689-a3f6-4580-88e0-5cddc5813c79 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/81470689-a3f6-4580-88e0-5cddc5813c79/MeasureReport-405de7e9-5fa3-49fd-a578-5afad2bcf224.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ 81470689-a3f6-4580-88e0-5cddc5813c79 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/81470689-a3f6-4580-88e0-5cddc5813c79/MeasureReport-405de7e9-5fa3-49fd-a578-5afad2bcf224.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 8269bfc3-071c-40e9-b523-07f615752097 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8269bfc3-071c-40e9-b523-07f615752097/MeasureReport-c7ded068-b8bc-4588-a1a1-26c8f0502036.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8269bfc3-071c-40e9-b523-07f615752097 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8269bfc3-071c-40e9-b523-07f615752097/MeasureReport-c7ded068-b8bc-4588-a1a1-26c8f0502036.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8269bfc3-071c-40e9-b523-07f615752097 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8269bfc3-071c-40e9-b523-07f615752097/MeasureReport-c7ded068-b8bc-4588-a1a1-26c8f0502036.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8269bfc3-071c-40e9-b523-07f615752097 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8269bfc3-071c-40e9-b523-07f615752097/MeasureReport-c7ded068-b8bc-4588-a1a1-26c8f0502036.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 8269bfc3-071c-40e9-b523-07f615752097 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8269bfc3-071c-40e9-b523-07f615752097/MeasureReport-c7ded068-b8bc-4588-a1a1-26c8f0502036.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 88000c93-b6da-4afc-be0d-43a5653485c1 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/88000c93-b6da-4afc-be0d-43a5653485c1/MeasureReport-56677a22-5799-408b-8e49-95dd110bb800.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 88000c93-b6da-4afc-be0d-43a5653485c1 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/88000c93-b6da-4afc-be0d-43a5653485c1/MeasureReport-56677a22-5799-408b-8e49-95dd110bb800.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 88000c93-b6da-4afc-be0d-43a5653485c1 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/88000c93-b6da-4afc-be0d-43a5653485c1/MeasureReport-56677a22-5799-408b-8e49-95dd110bb800.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 8802defe-e5c2-4fa7-81e6-107c145256c1 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8802defe-e5c2-4fa7-81e6-107c145256c1/MeasureReport-2171f8ee-7f9f-49e0-9167-87ef21ff09a4.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8802defe-e5c2-4fa7-81e6-107c145256c1 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8802defe-e5c2-4fa7-81e6-107c145256c1/MeasureReport-2171f8ee-7f9f-49e0-9167-87ef21ff09a4.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8802defe-e5c2-4fa7-81e6-107c145256c1 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8802defe-e5c2-4fa7-81e6-107c145256c1/MeasureReport-2171f8ee-7f9f-49e0-9167-87ef21ff09a4.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 8802defe-e5c2-4fa7-81e6-107c145256c1 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8802defe-e5c2-4fa7-81e6-107c145256c1/MeasureReport-2171f8ee-7f9f-49e0-9167-87ef21ff09a4.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 8c09f733-5ece-4f90-8ed6-7574dd841d68 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8c09f733-5ece-4f90-8ed6-7574dd841d68/MeasureReport-e2c982fe-147d-47f4-a06c-8bde09f0fcfe.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8c09f733-5ece-4f90-8ed6-7574dd841d68 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8c09f733-5ece-4f90-8ed6-7574dd841d68/MeasureReport-e2c982fe-147d-47f4-a06c-8bde09f0fcfe.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8c09f733-5ece-4f90-8ed6-7574dd841d68 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8c09f733-5ece-4f90-8ed6-7574dd841d68/MeasureReport-e2c982fe-147d-47f4-a06c-8bde09f0fcfe.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 8c09f733-5ece-4f90-8ed6-7574dd841d68 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8c09f733-5ece-4f90-8ed6-7574dd841d68/MeasureReport-e2c982fe-147d-47f4-a06c-8bde09f0fcfe.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 8dcae048-a632-4b49-b0d1-0d9355e57078 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8dcae048-a632-4b49-b0d1-0d9355e57078/MeasureReport-e1467661-016f-48e7-afca-5865ba12bed0.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8dcae048-a632-4b49-b0d1-0d9355e57078 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8dcae048-a632-4b49-b0d1-0d9355e57078/MeasureReport-e1467661-016f-48e7-afca-5865ba12bed0.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 8dcae048-a632-4b49-b0d1-0d9355e57078 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8dcae048-a632-4b49-b0d1-0d9355e57078/MeasureReport-e1467661-016f-48e7-afca-5865ba12bed0.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 8dcae048-a632-4b49-b0d1-0d9355e57078 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/8dcae048-a632-4b49-b0d1-0d9355e57078/MeasureReport-e1467661-016f-48e7-afca-5865ba12bed0.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ 91c95de4-b745-4258-948d-5ce03a901e5e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/91c95de4-b745-4258-948d-5ce03a901e5e/MeasureReport-1bdbe04a-89af-4725-8e6f-abd034a24f75.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 91c95de4-b745-4258-948d-5ce03a901e5e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/91c95de4-b745-4258-948d-5ce03a901e5e/MeasureReport-1bdbe04a-89af-4725-8e6f-abd034a24f75.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 91c95de4-b745-4258-948d-5ce03a901e5e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/91c95de4-b745-4258-948d-5ce03a901e5e/MeasureReport-1bdbe04a-89af-4725-8e6f-abd034a24f75.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 91c95de4-b745-4258-948d-5ce03a901e5e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/91c95de4-b745-4258-948d-5ce03a901e5e/MeasureReport-1bdbe04a-89af-4725-8e6f-abd034a24f75.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 94c2361a-0926-449e-8d97-317d3db41f80 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/94c2361a-0926-449e-8d97-317d3db41f80/MeasureReport-8d4e2729-ea7a-4858-b755-0334cf7bbcdd.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 94c2361a-0926-449e-8d97-317d3db41f80 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/94c2361a-0926-449e-8d97-317d3db41f80/MeasureReport-8d4e2729-ea7a-4858-b755-0334cf7bbcdd.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 94c2361a-0926-449e-8d97-317d3db41f80 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/94c2361a-0926-449e-8d97-317d3db41f80/MeasureReport-8d4e2729-ea7a-4858-b755-0334cf7bbcdd.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 94c2361a-0926-449e-8d97-317d3db41f80 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/94c2361a-0926-449e-8d97-317d3db41f80/MeasureReport-8d4e2729-ea7a-4858-b755-0334cf7bbcdd.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 94c2361a-0926-449e-8d97-317d3db41f80 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/94c2361a-0926-449e-8d97-317d3db41f80/MeasureReport-8d4e2729-ea7a-4858-b755-0334cf7bbcdd.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 95c5116d-15f0-4367-b467-dca152f6b43c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/95c5116d-15f0-4367-b467-dca152f6b43c/MeasureReport-e7148037-d239-4e3e-bcfb-3758cb2a5a30.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 95c5116d-15f0-4367-b467-dca152f6b43c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/95c5116d-15f0-4367-b467-dca152f6b43c/MeasureReport-e7148037-d239-4e3e-bcfb-3758cb2a5a30.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 95c5116d-15f0-4367-b467-dca152f6b43c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/95c5116d-15f0-4367-b467-dca152f6b43c/MeasureReport-e7148037-d239-4e3e-bcfb-3758cb2a5a30.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 95c5116d-15f0-4367-b467-dca152f6b43c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/95c5116d-15f0-4367-b467-dca152f6b43c/MeasureReport-e7148037-d239-4e3e-bcfb-3758cb2a5a30.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 95c5116d-15f0-4367-b467-dca152f6b43c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/95c5116d-15f0-4367-b467-dca152f6b43c/MeasureReport-e7148037-d239-4e3e-bcfb-3758cb2a5a30.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 96c85f39-db15-4895-a38b-9c18ea2e6cdf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/96c85f39-db15-4895-a38b-9c18ea2e6cdf/MeasureReport-c3fac542-ebbe-4e6a-85a8-b78dacb31005.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 96c85f39-db15-4895-a38b-9c18ea2e6cdf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/96c85f39-db15-4895-a38b-9c18ea2e6cdf/MeasureReport-c3fac542-ebbe-4e6a-85a8-b78dacb31005.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 96c85f39-db15-4895-a38b-9c18ea2e6cdf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/96c85f39-db15-4895-a38b-9c18ea2e6cdf/MeasureReport-c3fac542-ebbe-4e6a-85a8-b78dacb31005.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ 96c85f39-db15-4895-a38b-9c18ea2e6cdf ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/96c85f39-db15-4895-a38b-9c18ea2e6cdf/MeasureReport-c3fac542-ebbe-4e6a-85a8-b78dacb31005.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 98a1289e-18e0-4606-be91-5434433cd259 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/98a1289e-18e0-4606-be91-5434433cd259/MeasureReport-4a6f9948-03fb-4235-896e-32adc7881bab.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 98a1289e-18e0-4606-be91-5434433cd259 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/98a1289e-18e0-4606-be91-5434433cd259/MeasureReport-4a6f9948-03fb-4235-896e-32adc7881bab.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 98a1289e-18e0-4606-be91-5434433cd259 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/98a1289e-18e0-4606-be91-5434433cd259/MeasureReport-4a6f9948-03fb-4235-896e-32adc7881bab.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 98a1289e-18e0-4606-be91-5434433cd259 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/98a1289e-18e0-4606-be91-5434433cd259/MeasureReport-4a6f9948-03fb-4235-896e-32adc7881bab.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ 98a1289e-18e0-4606-be91-5434433cd259 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/98a1289e-18e0-4606-be91-5434433cd259/MeasureReport-4a6f9948-03fb-4235-896e-32adc7881bab.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ 997a0126-0a02-4157-a081-53ee4013a412 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/997a0126-0a02-4157-a081-53ee4013a412/MeasureReport-88364e84-e8ce-4479-88d3-fb1656b3006f.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 997a0126-0a02-4157-a081-53ee4013a412 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/997a0126-0a02-4157-a081-53ee4013a412/MeasureReport-88364e84-e8ce-4479-88d3-fb1656b3006f.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 997a0126-0a02-4157-a081-53ee4013a412 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/997a0126-0a02-4157-a081-53ee4013a412/MeasureReport-88364e84-e8ce-4479-88d3-fb1656b3006f.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ 9fe3ca47-1605-4187-93a0-be6618160cc7 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/9fe3ca47-1605-4187-93a0-be6618160cc7/MeasureReport-ac0cea05-50c9-4d8a-bf70-9447b60f9604.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 9fe3ca47-1605-4187-93a0-be6618160cc7 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/9fe3ca47-1605-4187-93a0-be6618160cc7/MeasureReport-ac0cea05-50c9-4d8a-bf70-9447b60f9604.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ 9fe3ca47-1605-4187-93a0-be6618160cc7 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/9fe3ca47-1605-4187-93a0-be6618160cc7/MeasureReport-ac0cea05-50c9-4d8a-bf70-9447b60f9604.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ a3389623-74d2-4a33-a946-08787313abf2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a3389623-74d2-4a33-a946-08787313abf2/MeasureReport-ce0b0a48-eb10-4407-b439-0675b657a0a5.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ a3389623-74d2-4a33-a946-08787313abf2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a3389623-74d2-4a33-a946-08787313abf2/MeasureReport-ce0b0a48-eb10-4407-b439-0675b657a0a5.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ a3389623-74d2-4a33-a946-08787313abf2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a3389623-74d2-4a33-a946-08787313abf2/MeasureReport-ce0b0a48-eb10-4407-b439-0675b657a0a5.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ a4d4b3e9-171d-456f-9282-3db4bb9852f6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4d4b3e9-171d-456f-9282-3db4bb9852f6/MeasureReport-39634e10-ade7-4976-9479-257871b0c4e1.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ a4d4b3e9-171d-456f-9282-3db4bb9852f6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4d4b3e9-171d-456f-9282-3db4bb9852f6/MeasureReport-39634e10-ade7-4976-9479-257871b0c4e1.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ a4d4b3e9-171d-456f-9282-3db4bb9852f6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4d4b3e9-171d-456f-9282-3db4bb9852f6/MeasureReport-39634e10-ade7-4976-9479-257871b0c4e1.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_1 | Measure Population Exclusion | 1 | 0 | C-03 — resolution pending | PASS |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_2 | Measure Population Exclusion | 1 | 0 | C-03 — resolution pending | PASS |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_3 | Measure Population Exclusion | 1 | 0 | C-03 — resolution pending | PASS |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_4 | Measure Population Exclusion | 1 | 0 | C-03 — resolution pending | PASS |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_5 | Measure Population Exclusion | 1 | 0 | C-03 — resolution pending | PASS |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_6 | Measure Population Exclusion | 1 | 0 | C-03 — resolution pending | PASS |
| [ a589fdb4-a3c9-4374-a231-5118b8ff434c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a589fdb4-a3c9-4374-a231-5118b8ff434c/MeasureReport-ab35dfc9-868a-460c-a653-30d5ad640110.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ a589fdb4-a3c9-4374-a231-5118b8ff434c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a589fdb4-a3c9-4374-a231-5118b8ff434c/MeasureReport-ab35dfc9-868a-460c-a653-30d5ad640110.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ a589fdb4-a3c9-4374-a231-5118b8ff434c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a589fdb4-a3c9-4374-a231-5118b8ff434c/MeasureReport-ab35dfc9-868a-460c-a653-30d5ad640110.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ a589fdb4-a3c9-4374-a231-5118b8ff434c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a589fdb4-a3c9-4374-a231-5118b8ff434c/MeasureReport-ab35dfc9-868a-460c-a653-30d5ad640110.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ a646d170-0582-4930-bb7b-2b6d942ead1e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a646d170-0582-4930-bb7b-2b6d942ead1e/MeasureReport-536e5c94-8049-4d79-aab1-35891c9cfddd.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ a646d170-0582-4930-bb7b-2b6d942ead1e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a646d170-0582-4930-bb7b-2b6d942ead1e/MeasureReport-536e5c94-8049-4d79-aab1-35891c9cfddd.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ a646d170-0582-4930-bb7b-2b6d942ead1e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a646d170-0582-4930-bb7b-2b6d942ead1e/MeasureReport-536e5c94-8049-4d79-aab1-35891c9cfddd.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ a646d170-0582-4930-bb7b-2b6d942ead1e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a646d170-0582-4930-bb7b-2b6d942ead1e/MeasureReport-536e5c94-8049-4d79-aab1-35891c9cfddd.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ a9aa059a-44ff-4092-a717-62ffba4ba9f3 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a9aa059a-44ff-4092-a717-62ffba4ba9f3/MeasureReport-e0b69e9a-0997-4d00-a52d-ad6e6a865a94.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ a9aa059a-44ff-4092-a717-62ffba4ba9f3 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a9aa059a-44ff-4092-a717-62ffba4ba9f3/MeasureReport-e0b69e9a-0997-4d00-a52d-ad6e6a865a94.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ a9aa059a-44ff-4092-a717-62ffba4ba9f3 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a9aa059a-44ff-4092-a717-62ffba4ba9f3/MeasureReport-e0b69e9a-0997-4d00-a52d-ad6e6a865a94.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ ab327433-5304-4665-b195-dcf90754a581 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ab327433-5304-4665-b195-dcf90754a581/MeasureReport-7041966a-cff2-41ed-a776-08858f8bc55b.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ab327433-5304-4665-b195-dcf90754a581 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ab327433-5304-4665-b195-dcf90754a581/MeasureReport-7041966a-cff2-41ed-a776-08858f8bc55b.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ab327433-5304-4665-b195-dcf90754a581 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ab327433-5304-4665-b195-dcf90754a581/MeasureReport-7041966a-cff2-41ed-a776-08858f8bc55b.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ab327433-5304-4665-b195-dcf90754a581 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ab327433-5304-4665-b195-dcf90754a581/MeasureReport-7041966a-cff2-41ed-a776-08858f8bc55b.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ab327433-5304-4665-b195-dcf90754a581 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ab327433-5304-4665-b195-dcf90754a581/MeasureReport-7041966a-cff2-41ed-a776-08858f8bc55b.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ ab327433-5304-4665-b195-dcf90754a581 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ab327433-5304-4665-b195-dcf90754a581/MeasureReport-7041966a-cff2-41ed-a776-08858f8bc55b.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ abe408d9-f5a5-4bdc-952b-7162a2aa7d31 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/abe408d9-f5a5-4bdc-952b-7162a2aa7d31/MeasureReport-9bea3df7-91c0-4cef-99e9-18e75c25ce6f.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ abe408d9-f5a5-4bdc-952b-7162a2aa7d31 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/abe408d9-f5a5-4bdc-952b-7162a2aa7d31/MeasureReport-9bea3df7-91c0-4cef-99e9-18e75c25ce6f.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ abe408d9-f5a5-4bdc-952b-7162a2aa7d31 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/abe408d9-f5a5-4bdc-952b-7162a2aa7d31/MeasureReport-9bea3df7-91c0-4cef-99e9-18e75c25ce6f.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ abe408d9-f5a5-4bdc-952b-7162a2aa7d31 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/abe408d9-f5a5-4bdc-952b-7162a2aa7d31/MeasureReport-9bea3df7-91c0-4cef-99e9-18e75c25ce6f.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ abe408d9-f5a5-4bdc-952b-7162a2aa7d31 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/abe408d9-f5a5-4bdc-952b-7162a2aa7d31/MeasureReport-9bea3df7-91c0-4cef-99e9-18e75c25ce6f.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ abe408d9-f5a5-4bdc-952b-7162a2aa7d31 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/abe408d9-f5a5-4bdc-952b-7162a2aa7d31/MeasureReport-9bea3df7-91c0-4cef-99e9-18e75c25ce6f.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ ac18bf5e-8bd1-4a85-a48e-e1b27fb88950 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ac18bf5e-8bd1-4a85-a48e-e1b27fb88950/MeasureReport-a18c81a3-02f0-49b0-9256-7611b64e0f5a.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ac18bf5e-8bd1-4a85-a48e-e1b27fb88950 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ac18bf5e-8bd1-4a85-a48e-e1b27fb88950/MeasureReport-a18c81a3-02f0-49b0-9256-7611b64e0f5a.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ac18bf5e-8bd1-4a85-a48e-e1b27fb88950 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ac18bf5e-8bd1-4a85-a48e-e1b27fb88950/MeasureReport-a18c81a3-02f0-49b0-9256-7611b64e0f5a.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ ac2adb48-5f93-46dc-ba35-11abf0b20c49 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ac2adb48-5f93-46dc-ba35-11abf0b20c49/MeasureReport-3e105aee-5ec7-4f3e-a010-66611eaf792f.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ac2adb48-5f93-46dc-ba35-11abf0b20c49 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ac2adb48-5f93-46dc-ba35-11abf0b20c49/MeasureReport-3e105aee-5ec7-4f3e-a010-66611eaf792f.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ac2adb48-5f93-46dc-ba35-11abf0b20c49 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ac2adb48-5f93-46dc-ba35-11abf0b20c49/MeasureReport-3e105aee-5ec7-4f3e-a010-66611eaf792f.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ac2adb48-5f93-46dc-ba35-11abf0b20c49 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ac2adb48-5f93-46dc-ba35-11abf0b20c49/MeasureReport-3e105aee-5ec7-4f3e-a010-66611eaf792f.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ ac2adb48-5f93-46dc-ba35-11abf0b20c49 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ac2adb48-5f93-46dc-ba35-11abf0b20c49/MeasureReport-3e105aee-5ec7-4f3e-a010-66611eaf792f.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ ae722bd4-32c2-4e5f-a1a8-743bdd9800f6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ae722bd4-32c2-4e5f-a1a8-743bdd9800f6/MeasureReport-5f2419bb-6cc1-48ad-bc30-aa1c5acd6d9e.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ae722bd4-32c2-4e5f-a1a8-743bdd9800f6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ae722bd4-32c2-4e5f-a1a8-743bdd9800f6/MeasureReport-5f2419bb-6cc1-48ad-bc30-aa1c5acd6d9e.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ae722bd4-32c2-4e5f-a1a8-743bdd9800f6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ae722bd4-32c2-4e5f-a1a8-743bdd9800f6/MeasureReport-5f2419bb-6cc1-48ad-bc30-aa1c5acd6d9e.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ b06b5114-a397-433a-9b32-b9b91f54d08c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b06b5114-a397-433a-9b32-b9b91f54d08c/MeasureReport-0c67907e-3ecd-4812-80f0-7b28362dcd52.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b06b5114-a397-433a-9b32-b9b91f54d08c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b06b5114-a397-433a-9b32-b9b91f54d08c/MeasureReport-0c67907e-3ecd-4812-80f0-7b28362dcd52.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b06b5114-a397-433a-9b32-b9b91f54d08c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b06b5114-a397-433a-9b32-b9b91f54d08c/MeasureReport-0c67907e-3ecd-4812-80f0-7b28362dcd52.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ b06b5114-a397-433a-9b32-b9b91f54d08c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b06b5114-a397-433a-9b32-b9b91f54d08c/MeasureReport-0c67907e-3ecd-4812-80f0-7b28362dcd52.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ b13bed65-b8cd-4905-b3c7-81a9c11dffc2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b13bed65-b8cd-4905-b3c7-81a9c11dffc2/MeasureReport-eb6fe542-dc18-4106-bdbd-d548c8274b70.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b13bed65-b8cd-4905-b3c7-81a9c11dffc2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b13bed65-b8cd-4905-b3c7-81a9c11dffc2/MeasureReport-eb6fe542-dc18-4106-bdbd-d548c8274b70.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b13bed65-b8cd-4905-b3c7-81a9c11dffc2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b13bed65-b8cd-4905-b3c7-81a9c11dffc2/MeasureReport-eb6fe542-dc18-4106-bdbd-d548c8274b70.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b13bed65-b8cd-4905-b3c7-81a9c11dffc2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b13bed65-b8cd-4905-b3c7-81a9c11dffc2/MeasureReport-eb6fe542-dc18-4106-bdbd-d548c8274b70.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b13bed65-b8cd-4905-b3c7-81a9c11dffc2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b13bed65-b8cd-4905-b3c7-81a9c11dffc2/MeasureReport-eb6fe542-dc18-4106-bdbd-d548c8274b70.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ b13bed65-b8cd-4905-b3c7-81a9c11dffc2 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b13bed65-b8cd-4905-b3c7-81a9c11dffc2/MeasureReport-eb6fe542-dc18-4106-bdbd-d548c8274b70.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ b1b79c28-6fb7-4900-8332-1378089486dd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b1b79c28-6fb7-4900-8332-1378089486dd/MeasureReport-9bcb4ca4-e083-4799-a67c-9820f1945dbd.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b1b79c28-6fb7-4900-8332-1378089486dd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b1b79c28-6fb7-4900-8332-1378089486dd/MeasureReport-9bcb4ca4-e083-4799-a67c-9820f1945dbd.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b1b79c28-6fb7-4900-8332-1378089486dd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b1b79c28-6fb7-4900-8332-1378089486dd/MeasureReport-9bcb4ca4-e083-4799-a67c-9820f1945dbd.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b1b79c28-6fb7-4900-8332-1378089486dd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b1b79c28-6fb7-4900-8332-1378089486dd/MeasureReport-9bcb4ca4-e083-4799-a67c-9820f1945dbd.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b1b79c28-6fb7-4900-8332-1378089486dd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b1b79c28-6fb7-4900-8332-1378089486dd/MeasureReport-9bcb4ca4-e083-4799-a67c-9820f1945dbd.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ b1b79c28-6fb7-4900-8332-1378089486dd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b1b79c28-6fb7-4900-8332-1378089486dd/MeasureReport-9bcb4ca4-e083-4799-a67c-9820f1945dbd.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ b3ee3e77-b813-42ac-949a-5d85e3df5002 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b3ee3e77-b813-42ac-949a-5d85e3df5002/MeasureReport-e02f6363-d36d-41fc-b0b7-14431bf6a4ec.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b3ee3e77-b813-42ac-949a-5d85e3df5002 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b3ee3e77-b813-42ac-949a-5d85e3df5002/MeasureReport-e02f6363-d36d-41fc-b0b7-14431bf6a4ec.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b3ee3e77-b813-42ac-949a-5d85e3df5002 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b3ee3e77-b813-42ac-949a-5d85e3df5002/MeasureReport-e02f6363-d36d-41fc-b0b7-14431bf6a4ec.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ b438eddf-9781-4236-840f-25f45be7030d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b438eddf-9781-4236-840f-25f45be7030d/MeasureReport-df919ee9-8717-419c-908a-3afe8b88a62c.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b438eddf-9781-4236-840f-25f45be7030d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b438eddf-9781-4236-840f-25f45be7030d/MeasureReport-df919ee9-8717-419c-908a-3afe8b88a62c.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b438eddf-9781-4236-840f-25f45be7030d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b438eddf-9781-4236-840f-25f45be7030d/MeasureReport-df919ee9-8717-419c-908a-3afe8b88a62c.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ b438eddf-9781-4236-840f-25f45be7030d ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b438eddf-9781-4236-840f-25f45be7030d/MeasureReport-df919ee9-8717-419c-908a-3afe8b88a62c.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ b5c1bc9e-c944-4ea8-b570-640609906df3 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b5c1bc9e-c944-4ea8-b570-640609906df3/MeasureReport-ca7cd0a5-01b1-4058-b117-6b1fee7883c9.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b5c1bc9e-c944-4ea8-b570-640609906df3 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b5c1bc9e-c944-4ea8-b570-640609906df3/MeasureReport-ca7cd0a5-01b1-4058-b117-6b1fee7883c9.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b5c1bc9e-c944-4ea8-b570-640609906df3 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b5c1bc9e-c944-4ea8-b570-640609906df3/MeasureReport-ca7cd0a5-01b1-4058-b117-6b1fee7883c9.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ b625bf76-24fe-4114-a609-434271f750ec ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b625bf76-24fe-4114-a609-434271f750ec/MeasureReport-5fe2e6c6-5acb-48e5-9889-d384e7eab88d.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b625bf76-24fe-4114-a609-434271f750ec ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b625bf76-24fe-4114-a609-434271f750ec/MeasureReport-5fe2e6c6-5acb-48e5-9889-d384e7eab88d.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b625bf76-24fe-4114-a609-434271f750ec ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b625bf76-24fe-4114-a609-434271f750ec/MeasureReport-5fe2e6c6-5acb-48e5-9889-d384e7eab88d.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ b625bf76-24fe-4114-a609-434271f750ec ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b625bf76-24fe-4114-a609-434271f750ec/MeasureReport-5fe2e6c6-5acb-48e5-9889-d384e7eab88d.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ b625bf76-24fe-4114-a609-434271f750ec ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/b625bf76-24fe-4114-a609-434271f750ec/MeasureReport-5fe2e6c6-5acb-48e5-9889-d384e7eab88d.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ bded9082-97bf-42d0-8c35-cbf18df5938f ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bded9082-97bf-42d0-8c35-cbf18df5938f/MeasureReport-77bf3a6f-fbc3-4b2c-aea8-2144ea18e273.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ bded9082-97bf-42d0-8c35-cbf18df5938f ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bded9082-97bf-42d0-8c35-cbf18df5938f/MeasureReport-77bf3a6f-fbc3-4b2c-aea8-2144ea18e273.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ bded9082-97bf-42d0-8c35-cbf18df5938f ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bded9082-97bf-42d0-8c35-cbf18df5938f/MeasureReport-77bf3a6f-fbc3-4b2c-aea8-2144ea18e273.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ bded9082-97bf-42d0-8c35-cbf18df5938f ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bded9082-97bf-42d0-8c35-cbf18df5938f/MeasureReport-77bf3a6f-fbc3-4b2c-aea8-2144ea18e273.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ bded9082-97bf-42d0-8c35-cbf18df5938f ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bded9082-97bf-42d0-8c35-cbf18df5938f/MeasureReport-77bf3a6f-fbc3-4b2c-aea8-2144ea18e273.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ bfb5338b-1ade-4488-9fdb-0cb974388e18 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bfb5338b-1ade-4488-9fdb-0cb974388e18/MeasureReport-e717f932-2153-4a09-80cb-02ebe97babe8.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ bfb5338b-1ade-4488-9fdb-0cb974388e18 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bfb5338b-1ade-4488-9fdb-0cb974388e18/MeasureReport-e717f932-2153-4a09-80cb-02ebe97babe8.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ bfb5338b-1ade-4488-9fdb-0cb974388e18 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bfb5338b-1ade-4488-9fdb-0cb974388e18/MeasureReport-e717f932-2153-4a09-80cb-02ebe97babe8.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ bfb5338b-1ade-4488-9fdb-0cb974388e18 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bfb5338b-1ade-4488-9fdb-0cb974388e18/MeasureReport-e717f932-2153-4a09-80cb-02ebe97babe8.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ bfb5338b-1ade-4488-9fdb-0cb974388e18 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bfb5338b-1ade-4488-9fdb-0cb974388e18/MeasureReport-e717f932-2153-4a09-80cb-02ebe97babe8.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ bfb5338b-1ade-4488-9fdb-0cb974388e18 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/bfb5338b-1ade-4488-9fdb-0cb974388e18/MeasureReport-e717f932-2153-4a09-80cb-02ebe97babe8.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ c1a50cef-42e4-4473-8aa4-704e9743b1ca ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c1a50cef-42e4-4473-8aa4-704e9743b1ca/MeasureReport-bd9cbd13-1c38-4617-a68f-88b0789aea85.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ c1a50cef-42e4-4473-8aa4-704e9743b1ca ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c1a50cef-42e4-4473-8aa4-704e9743b1ca/MeasureReport-bd9cbd13-1c38-4617-a68f-88b0789aea85.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ c1a50cef-42e4-4473-8aa4-704e9743b1ca ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c1a50cef-42e4-4473-8aa4-704e9743b1ca/MeasureReport-bd9cbd13-1c38-4617-a68f-88b0789aea85.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ c44bb375-9561-47d1-a48d-179750f8a676 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c44bb375-9561-47d1-a48d-179750f8a676/MeasureReport-6afb9c76-c369-4e48-9881-d73e34caddac.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ c44bb375-9561-47d1-a48d-179750f8a676 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c44bb375-9561-47d1-a48d-179750f8a676/MeasureReport-6afb9c76-c369-4e48-9881-d73e34caddac.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ c44bb375-9561-47d1-a48d-179750f8a676 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c44bb375-9561-47d1-a48d-179750f8a676/MeasureReport-6afb9c76-c369-4e48-9881-d73e34caddac.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ c44bb375-9561-47d1-a48d-179750f8a676 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c44bb375-9561-47d1-a48d-179750f8a676/MeasureReport-6afb9c76-c369-4e48-9881-d73e34caddac.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ c44bb375-9561-47d1-a48d-179750f8a676 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c44bb375-9561-47d1-a48d-179750f8a676/MeasureReport-6afb9c76-c369-4e48-9881-d73e34caddac.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ c44bb375-9561-47d1-a48d-179750f8a676 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c44bb375-9561-47d1-a48d-179750f8a676/MeasureReport-6afb9c76-c369-4e48-9881-d73e34caddac.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ c5dd5da0-c5e3-4504-9dda-05c729757735 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c5dd5da0-c5e3-4504-9dda-05c729757735/MeasureReport-520ab06e-d1b0-41cc-bbe7-2964fe5428d1.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ c5dd5da0-c5e3-4504-9dda-05c729757735 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c5dd5da0-c5e3-4504-9dda-05c729757735/MeasureReport-520ab06e-d1b0-41cc-bbe7-2964fe5428d1.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ c5dd5da0-c5e3-4504-9dda-05c729757735 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c5dd5da0-c5e3-4504-9dda-05c729757735/MeasureReport-520ab06e-d1b0-41cc-bbe7-2964fe5428d1.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ c6dfe659-1bc4-46fd-8893-ebac7a586cc6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c6dfe659-1bc4-46fd-8893-ebac7a586cc6/MeasureReport-b55f4df8-1be9-4fe5-8346-e7e93065576c.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ c6dfe659-1bc4-46fd-8893-ebac7a586cc6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c6dfe659-1bc4-46fd-8893-ebac7a586cc6/MeasureReport-b55f4df8-1be9-4fe5-8346-e7e93065576c.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ c6dfe659-1bc4-46fd-8893-ebac7a586cc6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c6dfe659-1bc4-46fd-8893-ebac7a586cc6/MeasureReport-b55f4df8-1be9-4fe5-8346-e7e93065576c.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ c6dfe659-1bc4-46fd-8893-ebac7a586cc6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/c6dfe659-1bc4-46fd-8893-ebac7a586cc6/MeasureReport-b55f4df8-1be9-4fe5-8346-e7e93065576c.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ cf475a5c-e979-46c8-a596-8671d8a7acc5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/cf475a5c-e979-46c8-a596-8671d8a7acc5/MeasureReport-8aabfd93-5d8b-44fd-8b30-e74bf1efc0b9.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ cf475a5c-e979-46c8-a596-8671d8a7acc5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/cf475a5c-e979-46c8-a596-8671d8a7acc5/MeasureReport-8aabfd93-5d8b-44fd-8b30-e74bf1efc0b9.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ cf475a5c-e979-46c8-a596-8671d8a7acc5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/cf475a5c-e979-46c8-a596-8671d8a7acc5/MeasureReport-8aabfd93-5d8b-44fd-8b30-e74bf1efc0b9.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ cf475a5c-e979-46c8-a596-8671d8a7acc5 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/cf475a5c-e979-46c8-a596-8671d8a7acc5/MeasureReport-8aabfd93-5d8b-44fd-8b30-e74bf1efc0b9.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ d5946dae-72bb-4b13-984d-ce4da0add583 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d5946dae-72bb-4b13-984d-ce4da0add583/MeasureReport-692b6212-9c8c-4511-b6b1-ffd675c4b198.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ d5946dae-72bb-4b13-984d-ce4da0add583 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d5946dae-72bb-4b13-984d-ce4da0add583/MeasureReport-692b6212-9c8c-4511-b6b1-ffd675c4b198.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ d5946dae-72bb-4b13-984d-ce4da0add583 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d5946dae-72bb-4b13-984d-ce4da0add583/MeasureReport-692b6212-9c8c-4511-b6b1-ffd675c4b198.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ d5946dae-72bb-4b13-984d-ce4da0add583 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d5946dae-72bb-4b13-984d-ce4da0add583/MeasureReport-692b6212-9c8c-4511-b6b1-ffd675c4b198.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ d5946dae-72bb-4b13-984d-ce4da0add583 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d5946dae-72bb-4b13-984d-ce4da0add583/MeasureReport-692b6212-9c8c-4511-b6b1-ffd675c4b198.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ d5946dae-72bb-4b13-984d-ce4da0add583 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d5946dae-72bb-4b13-984d-ce4da0add583/MeasureReport-692b6212-9c8c-4511-b6b1-ffd675c4b198.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ d638571a-bc86-48a8-9792-0885a26e68fc ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d638571a-bc86-48a8-9792-0885a26e68fc/MeasureReport-eb7c29a1-7719-41c8-b5a9-3236ae4a83e2.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ d638571a-bc86-48a8-9792-0885a26e68fc ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d638571a-bc86-48a8-9792-0885a26e68fc/MeasureReport-eb7c29a1-7719-41c8-b5a9-3236ae4a83e2.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ d638571a-bc86-48a8-9792-0885a26e68fc ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d638571a-bc86-48a8-9792-0885a26e68fc/MeasureReport-eb7c29a1-7719-41c8-b5a9-3236ae4a83e2.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ d638571a-bc86-48a8-9792-0885a26e68fc ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d638571a-bc86-48a8-9792-0885a26e68fc/MeasureReport-eb7c29a1-7719-41c8-b5a9-3236ae4a83e2.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ d7b8ad65-50f2-442f-8afc-1de38844a5ad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d7b8ad65-50f2-442f-8afc-1de38844a5ad/MeasureReport-1c0df8d1-ca19-442a-8ace-1d4f49f4d7d2.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ d7b8ad65-50f2-442f-8afc-1de38844a5ad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d7b8ad65-50f2-442f-8afc-1de38844a5ad/MeasureReport-1c0df8d1-ca19-442a-8ace-1d4f49f4d7d2.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ d7b8ad65-50f2-442f-8afc-1de38844a5ad ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/d7b8ad65-50f2-442f-8afc-1de38844a5ad/MeasureReport-1c0df8d1-ca19-442a-8ace-1d4f49f4d7d2.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ da25ee5a-4502-4bf6-95fe-087bd0a2c1fd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/da25ee5a-4502-4bf6-95fe-087bd0a2c1fd/MeasureReport-6ecd76a7-3318-449d-b458-df2f65c7a63f.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ da25ee5a-4502-4bf6-95fe-087bd0a2c1fd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/da25ee5a-4502-4bf6-95fe-087bd0a2c1fd/MeasureReport-6ecd76a7-3318-449d-b458-df2f65c7a63f.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ da25ee5a-4502-4bf6-95fe-087bd0a2c1fd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/da25ee5a-4502-4bf6-95fe-087bd0a2c1fd/MeasureReport-6ecd76a7-3318-449d-b458-df2f65c7a63f.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ da25ee5a-4502-4bf6-95fe-087bd0a2c1fd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/da25ee5a-4502-4bf6-95fe-087bd0a2c1fd/MeasureReport-6ecd76a7-3318-449d-b458-df2f65c7a63f.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ dade6a4b-6099-476b-b163-ae119af82b9b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dade6a4b-6099-476b-b163-ae119af82b9b/MeasureReport-6647e195-d891-4399-9a6d-76d9b7aade14.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ dade6a4b-6099-476b-b163-ae119af82b9b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dade6a4b-6099-476b-b163-ae119af82b9b/MeasureReport-6647e195-d891-4399-9a6d-76d9b7aade14.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ dade6a4b-6099-476b-b163-ae119af82b9b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dade6a4b-6099-476b-b163-ae119af82b9b/MeasureReport-6647e195-d891-4399-9a6d-76d9b7aade14.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ dade6a4b-6099-476b-b163-ae119af82b9b ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dade6a4b-6099-476b-b163-ae119af82b9b/MeasureReport-6647e195-d891-4399-9a6d-76d9b7aade14.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ dc0b5670-62cb-4ab3-9f8e-ac2dad078c20 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dc0b5670-62cb-4ab3-9f8e-ac2dad078c20/MeasureReport-5f01734a-0984-4fed-8e8e-9452a8985b11.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ dc0b5670-62cb-4ab3-9f8e-ac2dad078c20 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dc0b5670-62cb-4ab3-9f8e-ac2dad078c20/MeasureReport-5f01734a-0984-4fed-8e8e-9452a8985b11.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ dc0b5670-62cb-4ab3-9f8e-ac2dad078c20 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dc0b5670-62cb-4ab3-9f8e-ac2dad078c20/MeasureReport-5f01734a-0984-4fed-8e8e-9452a8985b11.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ dc0b5670-62cb-4ab3-9f8e-ac2dad078c20 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dc0b5670-62cb-4ab3-9f8e-ac2dad078c20/MeasureReport-5f01734a-0984-4fed-8e8e-9452a8985b11.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ dc0b5670-62cb-4ab3-9f8e-ac2dad078c20 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dc0b5670-62cb-4ab3-9f8e-ac2dad078c20/MeasureReport-5f01734a-0984-4fed-8e8e-9452a8985b11.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ dc38b27c-8138-4d45-8540-f7fe92317375 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dc38b27c-8138-4d45-8540-f7fe92317375/MeasureReport-6ec3a1cd-107a-4b24-811b-6935ba5ab6bb.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ dc38b27c-8138-4d45-8540-f7fe92317375 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dc38b27c-8138-4d45-8540-f7fe92317375/MeasureReport-6ec3a1cd-107a-4b24-811b-6935ba5ab6bb.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ dc38b27c-8138-4d45-8540-f7fe92317375 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dc38b27c-8138-4d45-8540-f7fe92317375/MeasureReport-6ec3a1cd-107a-4b24-811b-6935ba5ab6bb.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ dc38b27c-8138-4d45-8540-f7fe92317375 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dc38b27c-8138-4d45-8540-f7fe92317375/MeasureReport-6ec3a1cd-107a-4b24-811b-6935ba5ab6bb.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ dc38b27c-8138-4d45-8540-f7fe92317375 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/dc38b27c-8138-4d45-8540-f7fe92317375/MeasureReport-6ec3a1cd-107a-4b24-811b-6935ba5ab6bb.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ e0747546-a821-4475-9ba3-e963ac80e963 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e0747546-a821-4475-9ba3-e963ac80e963/MeasureReport-8ef0e0a7-da47-4193-bbe5-1efb3af7f985.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e0747546-a821-4475-9ba3-e963ac80e963 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e0747546-a821-4475-9ba3-e963ac80e963/MeasureReport-8ef0e0a7-da47-4193-bbe5-1efb3af7f985.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e0747546-a821-4475-9ba3-e963ac80e963 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e0747546-a821-4475-9ba3-e963ac80e963/MeasureReport-8ef0e0a7-da47-4193-bbe5-1efb3af7f985.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e0747546-a821-4475-9ba3-e963ac80e963 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e0747546-a821-4475-9ba3-e963ac80e963/MeasureReport-8ef0e0a7-da47-4193-bbe5-1efb3af7f985.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e0747546-a821-4475-9ba3-e963ac80e963 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e0747546-a821-4475-9ba3-e963ac80e963/MeasureReport-8ef0e0a7-da47-4193-bbe5-1efb3af7f985.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ e0747546-a821-4475-9ba3-e963ac80e963 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e0747546-a821-4475-9ba3-e963ac80e963/MeasureReport-8ef0e0a7-da47-4193-bbe5-1efb3af7f985.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ e0f9a2cf-7ab8-4e40-b36a-b10ce147f8a8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e0f9a2cf-7ab8-4e40-b36a-b10ce147f8a8/MeasureReport-81af822d-ff3b-4f95-af93-b6c745385cef.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e0f9a2cf-7ab8-4e40-b36a-b10ce147f8a8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e0f9a2cf-7ab8-4e40-b36a-b10ce147f8a8/MeasureReport-81af822d-ff3b-4f95-af93-b6c745385cef.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e0f9a2cf-7ab8-4e40-b36a-b10ce147f8a8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e0f9a2cf-7ab8-4e40-b36a-b10ce147f8a8/MeasureReport-81af822d-ff3b-4f95-af93-b6c745385cef.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ e0f9a2cf-7ab8-4e40-b36a-b10ce147f8a8 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e0f9a2cf-7ab8-4e40-b36a-b10ce147f8a8/MeasureReport-81af822d-ff3b-4f95-af93-b6c745385cef.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ e2664b2b-8c66-4621-a0c1-a95862fba95a ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e2664b2b-8c66-4621-a0c1-a95862fba95a/MeasureReport-2a0570fc-f572-4ee4-8a43-b872ed6303df.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e2664b2b-8c66-4621-a0c1-a95862fba95a ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e2664b2b-8c66-4621-a0c1-a95862fba95a/MeasureReport-2a0570fc-f572-4ee4-8a43-b872ed6303df.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e2664b2b-8c66-4621-a0c1-a95862fba95a ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e2664b2b-8c66-4621-a0c1-a95862fba95a/MeasureReport-2a0570fc-f572-4ee4-8a43-b872ed6303df.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e2664b2b-8c66-4621-a0c1-a95862fba95a ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e2664b2b-8c66-4621-a0c1-a95862fba95a/MeasureReport-2a0570fc-f572-4ee4-8a43-b872ed6303df.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e2664b2b-8c66-4621-a0c1-a95862fba95a ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e2664b2b-8c66-4621-a0c1-a95862fba95a/MeasureReport-2a0570fc-f572-4ee4-8a43-b872ed6303df.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ e2664b2b-8c66-4621-a0c1-a95862fba95a ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e2664b2b-8c66-4621-a0c1-a95862fba95a/MeasureReport-2a0570fc-f572-4ee4-8a43-b872ed6303df.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ e5582d8a-91d9-4234-910d-db7a6fa68785 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e5582d8a-91d9-4234-910d-db7a6fa68785/MeasureReport-51be9302-99d8-4af1-b46a-8556ade7c69a.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e5582d8a-91d9-4234-910d-db7a6fa68785 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e5582d8a-91d9-4234-910d-db7a6fa68785/MeasureReport-51be9302-99d8-4af1-b46a-8556ade7c69a.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e5582d8a-91d9-4234-910d-db7a6fa68785 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e5582d8a-91d9-4234-910d-db7a6fa68785/MeasureReport-51be9302-99d8-4af1-b46a-8556ade7c69a.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ e5582d8a-91d9-4234-910d-db7a6fa68785 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e5582d8a-91d9-4234-910d-db7a6fa68785/MeasureReport-51be9302-99d8-4af1-b46a-8556ade7c69a.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ e7536d99-81b0-4980-b27a-037c49753a9e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e7536d99-81b0-4980-b27a-037c49753a9e/MeasureReport-7a141761-3f2d-4207-9082-e719fff25d8f.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e7536d99-81b0-4980-b27a-037c49753a9e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e7536d99-81b0-4980-b27a-037c49753a9e/MeasureReport-7a141761-3f2d-4207-9082-e719fff25d8f.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e7536d99-81b0-4980-b27a-037c49753a9e ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e7536d99-81b0-4980-b27a-037c49753a9e/MeasureReport-7a141761-3f2d-4207-9082-e719fff25d8f.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ e87069c9-7889-4954-858f-33d6ad24cd7a ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e87069c9-7889-4954-858f-33d6ad24cd7a/MeasureReport-cdbedc1d-933b-4278-8634-e2c340f664c9.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e87069c9-7889-4954-858f-33d6ad24cd7a ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e87069c9-7889-4954-858f-33d6ad24cd7a/MeasureReport-cdbedc1d-933b-4278-8634-e2c340f664c9.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ e87069c9-7889-4954-858f-33d6ad24cd7a ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/e87069c9-7889-4954-858f-33d6ad24cd7a/MeasureReport-cdbedc1d-933b-4278-8634-e2c340f664c9.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ eb03b6f2-c098-4428-b31e-96fc9d968947 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/eb03b6f2-c098-4428-b31e-96fc9d968947/MeasureReport-ccc64341-9894-462c-82cc-542e1d9f2c3e.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ eb03b6f2-c098-4428-b31e-96fc9d968947 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/eb03b6f2-c098-4428-b31e-96fc9d968947/MeasureReport-ccc64341-9894-462c-82cc-542e1d9f2c3e.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ eb03b6f2-c098-4428-b31e-96fc9d968947 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/eb03b6f2-c098-4428-b31e-96fc9d968947/MeasureReport-ccc64341-9894-462c-82cc-542e1d9f2c3e.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ eb03b6f2-c098-4428-b31e-96fc9d968947 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/eb03b6f2-c098-4428-b31e-96fc9d968947/MeasureReport-ccc64341-9894-462c-82cc-542e1d9f2c3e.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ ebc944c4-c40a-4d80-8bb6-0cc698f07681 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ebc944c4-c40a-4d80-8bb6-0cc698f07681/MeasureReport-2218b8d5-657d-487e-9dfa-d171b2408742.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ebc944c4-c40a-4d80-8bb6-0cc698f07681 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ebc944c4-c40a-4d80-8bb6-0cc698f07681/MeasureReport-2218b8d5-657d-487e-9dfa-d171b2408742.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ebc944c4-c40a-4d80-8bb6-0cc698f07681 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ebc944c4-c40a-4d80-8bb6-0cc698f07681/MeasureReport-2218b8d5-657d-487e-9dfa-d171b2408742.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ ebc944c4-c40a-4d80-8bb6-0cc698f07681 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ebc944c4-c40a-4d80-8bb6-0cc698f07681/MeasureReport-2218b8d5-657d-487e-9dfa-d171b2408742.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ ebc944c4-c40a-4d80-8bb6-0cc698f07681 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/ebc944c4-c40a-4d80-8bb6-0cc698f07681/MeasureReport-2218b8d5-657d-487e-9dfa-d171b2408742.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ eeebf78e-439d-445b-a4e1-74d3b6eaeb08 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/eeebf78e-439d-445b-a4e1-74d3b6eaeb08/MeasureReport-f69b93da-ab40-4a0a-8b75-602828125808.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ eeebf78e-439d-445b-a4e1-74d3b6eaeb08 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/eeebf78e-439d-445b-a4e1-74d3b6eaeb08/MeasureReport-f69b93da-ab40-4a0a-8b75-602828125808.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ eeebf78e-439d-445b-a4e1-74d3b6eaeb08 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/eeebf78e-439d-445b-a4e1-74d3b6eaeb08/MeasureReport-f69b93da-ab40-4a0a-8b75-602828125808.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ eeebf78e-439d-445b-a4e1-74d3b6eaeb08 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/eeebf78e-439d-445b-a4e1-74d3b6eaeb08/MeasureReport-f69b93da-ab40-4a0a-8b75-602828125808.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ eeebf78e-439d-445b-a4e1-74d3b6eaeb08 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/eeebf78e-439d-445b-a4e1-74d3b6eaeb08/MeasureReport-f69b93da-ab40-4a0a-8b75-602828125808.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ f06bb07a-3410-410d-a859-8bd57a79cf51 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f06bb07a-3410-410d-a859-8bd57a79cf51/MeasureReport-cfc229db-5ba1-4fd8-aef4-fcd369834901.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f06bb07a-3410-410d-a859-8bd57a79cf51 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f06bb07a-3410-410d-a859-8bd57a79cf51/MeasureReport-cfc229db-5ba1-4fd8-aef4-fcd369834901.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f06bb07a-3410-410d-a859-8bd57a79cf51 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f06bb07a-3410-410d-a859-8bd57a79cf51/MeasureReport-cfc229db-5ba1-4fd8-aef4-fcd369834901.json) | Group_3 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f06bb07a-3410-410d-a859-8bd57a79cf51 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f06bb07a-3410-410d-a859-8bd57a79cf51/MeasureReport-cfc229db-5ba1-4fd8-aef4-fcd369834901.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f06bb07a-3410-410d-a859-8bd57a79cf51 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f06bb07a-3410-410d-a859-8bd57a79cf51/MeasureReport-cfc229db-5ba1-4fd8-aef4-fcd369834901.json) | Group_5 | Measure Observation | 4 | 0 | C-03 — resolution pending | FAIL |
| [ f06bb07a-3410-410d-a859-8bd57a79cf51 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f06bb07a-3410-410d-a859-8bd57a79cf51/MeasureReport-cfc229db-5ba1-4fd8-aef4-fcd369834901.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ f1d12297-45c4-4984-8e28-02bf21d84efd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f1d12297-45c4-4984-8e28-02bf21d84efd/MeasureReport-3ece9790-e531-4dca-b382-f580a08a2873.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f1d12297-45c4-4984-8e28-02bf21d84efd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f1d12297-45c4-4984-8e28-02bf21d84efd/MeasureReport-3ece9790-e531-4dca-b382-f580a08a2873.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f1d12297-45c4-4984-8e28-02bf21d84efd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f1d12297-45c4-4984-8e28-02bf21d84efd/MeasureReport-3ece9790-e531-4dca-b382-f580a08a2873.json) | Group_5 | Measure Observation | 2 | 0 | C-03 — resolution pending | FAIL |
| [ f1d12297-45c4-4984-8e28-02bf21d84efd ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f1d12297-45c4-4984-8e28-02bf21d84efd/MeasureReport-3ece9790-e531-4dca-b382-f580a08a2873.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ f2125d25-eea4-4130-8118-6d32b7bf4690 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f2125d25-eea4-4130-8118-6d32b7bf4690/MeasureReport-1bdfd944-4292-406f-8e84-60e2dcfa44df.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f2125d25-eea4-4130-8118-6d32b7bf4690 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f2125d25-eea4-4130-8118-6d32b7bf4690/MeasureReport-1bdfd944-4292-406f-8e84-60e2dcfa44df.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f2125d25-eea4-4130-8118-6d32b7bf4690 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f2125d25-eea4-4130-8118-6d32b7bf4690/MeasureReport-1bdfd944-4292-406f-8e84-60e2dcfa44df.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ f3b2a15e-a120-4190-9a08-e23b61fae693 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f3b2a15e-a120-4190-9a08-e23b61fae693/MeasureReport-997a68d9-d37f-4f4a-9ea1-a90dd1b147f4.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f3b2a15e-a120-4190-9a08-e23b61fae693 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f3b2a15e-a120-4190-9a08-e23b61fae693/MeasureReport-997a68d9-d37f-4f4a-9ea1-a90dd1b147f4.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f3b2a15e-a120-4190-9a08-e23b61fae693 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f3b2a15e-a120-4190-9a08-e23b61fae693/MeasureReport-997a68d9-d37f-4f4a-9ea1-a90dd1b147f4.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ f48547b0-823f-4d85-93d5-0b19bc8aa7ba ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f48547b0-823f-4d85-93d5-0b19bc8aa7ba/MeasureReport-7b10f501-34d4-4b6e-8576-a96f84edf75c.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f48547b0-823f-4d85-93d5-0b19bc8aa7ba ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f48547b0-823f-4d85-93d5-0b19bc8aa7ba/MeasureReport-7b10f501-34d4-4b6e-8576-a96f84edf75c.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f48547b0-823f-4d85-93d5-0b19bc8aa7ba ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f48547b0-823f-4d85-93d5-0b19bc8aa7ba/MeasureReport-7b10f501-34d4-4b6e-8576-a96f84edf75c.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ f524a276-6788-435a-99f7-cd0ef48553e6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f524a276-6788-435a-99f7-cd0ef48553e6/MeasureReport-1fb2fff0-6856-4ce5-bb77-a65f186eff99.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f524a276-6788-435a-99f7-cd0ef48553e6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f524a276-6788-435a-99f7-cd0ef48553e6/MeasureReport-1fb2fff0-6856-4ce5-bb77-a65f186eff99.json) | Group_2 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f524a276-6788-435a-99f7-cd0ef48553e6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f524a276-6788-435a-99f7-cd0ef48553e6/MeasureReport-1fb2fff0-6856-4ce5-bb77-a65f186eff99.json) | Group_4 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f524a276-6788-435a-99f7-cd0ef48553e6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f524a276-6788-435a-99f7-cd0ef48553e6/MeasureReport-1fb2fff0-6856-4ce5-bb77-a65f186eff99.json) | Group_5 | Measure Observation | 3 | 0 | C-03 — resolution pending | FAIL |
| [ f524a276-6788-435a-99f7-cd0ef48553e6 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f524a276-6788-435a-99f7-cd0ef48553e6/MeasureReport-1fb2fff0-6856-4ce5-bb77-a65f186eff99.json) | Group_6 | Measure Observation | 75 | 0 | C-03 — resolution pending | FAIL |
| [ f6a15d18-bde3-49cd-88b6-45430eb0bd42 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f6a15d18-bde3-49cd-88b6-45430eb0bd42/MeasureReport-cf626065-f1c9-46a4-a571-0a4db0e4bfe8.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f6a15d18-bde3-49cd-88b6-45430eb0bd42 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f6a15d18-bde3-49cd-88b6-45430eb0bd42/MeasureReport-cf626065-f1c9-46a4-a571-0a4db0e4bfe8.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f6a15d18-bde3-49cd-88b6-45430eb0bd42 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f6a15d18-bde3-49cd-88b6-45430eb0bd42/MeasureReport-cf626065-f1c9-46a4-a571-0a4db0e4bfe8.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |
| [ f7730b43-fab5-4fee-a1ba-34dd41279a20 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f7730b43-fab5-4fee-a1ba-34dd41279a20/MeasureReport-92436316-09ab-4eaf-95cd-1cae8d199c50.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f7730b43-fab5-4fee-a1ba-34dd41279a20 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f7730b43-fab5-4fee-a1ba-34dd41279a20/MeasureReport-92436316-09ab-4eaf-95cd-1cae8d199c50.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f7730b43-fab5-4fee-a1ba-34dd41279a20 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f7730b43-fab5-4fee-a1ba-34dd41279a20/MeasureReport-92436316-09ab-4eaf-95cd-1cae8d199c50.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ f7e36aca-e995-4d53-b52f-598243f38cb9 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f7e36aca-e995-4d53-b52f-598243f38cb9/MeasureReport-cd8822fd-0697-4bf3-a97e-fa80022d3717.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f7e36aca-e995-4d53-b52f-598243f38cb9 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f7e36aca-e995-4d53-b52f-598243f38cb9/MeasureReport-cd8822fd-0697-4bf3-a97e-fa80022d3717.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ f7e36aca-e995-4d53-b52f-598243f38cb9 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/f7e36aca-e995-4d53-b52f-598243f38cb9/MeasureReport-cd8822fd-0697-4bf3-a97e-fa80022d3717.json) | Group_6 | Measure Observation | 50 | 0 | C-03 — resolution pending | FAIL |
| [ fa4b9fb0-93e7-4f81-9a51-1cbc0814703c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/fa4b9fb0-93e7-4f81-9a51-1cbc0814703c/MeasureReport-880e5165-432a-4490-9443-83c3711772fc.json) | Group_1 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ fa4b9fb0-93e7-4f81-9a51-1cbc0814703c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/fa4b9fb0-93e7-4f81-9a51-1cbc0814703c/MeasureReport-880e5165-432a-4490-9443-83c3711772fc.json) | Group_5 | Measure Observation | 1 | 0 | C-03 — resolution pending | FAIL |
| [ fa4b9fb0-93e7-4f81-9a51-1cbc0814703c ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/fa4b9fb0-93e7-4f81-9a51-1cbc0814703c/MeasureReport-880e5165-432a-4490-9443-83c3711772fc.json) | Group_6 | Measure Observation | 100 | 0 | C-03 — resolution pending | FAIL |


#### CMS996FHIRAptTxforSTEMI
[ [cql] ](../../input/cql/CMS996FHIRAptTxforSTEMI.cql) [ [test results] ](../../input/tests/results/CMS996FHIRAptTxforSTEMI.txt)

QICore: 108 / 6 — has discrepancies (6 mismatched, 0 missing)

Mismatched Test Cases (5 of 114 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 60823d79-b37f-4358-819f-f39b4e885c6d ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/60823d79-b37f-4358-819f-f39b4e885c6d/MeasureReport-96a1323f-d99d-4b31-aace-c90b90f8af7a.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ 7edab122-3af3-4172-9231-7c1470ecc1e0 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/7edab122-3af3-4172-9231-7c1470ecc1e0/MeasureReport-9d0666d5-6e19-4f7f-b284-1af640b254f3.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ 8bb7c40b-7447-42ca-b662-161a7026ed8f ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/8bb7c40b-7447-42ca-b662-161a7026ed8f/MeasureReport-bb15a071-2c69-428e-ac66-6405f7d75d07.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ ccc7deaf-98b7-4dad-b190-8fee10f2cf77 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/ccc7deaf-98b7-4dad-b190-8fee10f2cf77/MeasureReport-9d6a333f-3243-42df-9063-031aa80e74ff.json) | Group_1 | Denominator Exception | 1 | 0 | E-21 — resolution pending | PASS |
| [ f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847/MeasureReport-f2a63299-25e1-4d91-8e5c-1bdf3b60e9cb.json) | Group_1 | Denominator Exclusion | 0 | 1 | E-21 — resolution pending | PASS |


#### CMS1017FHIRHHFI
[ [cql] ](../../input/cql/CMS1017FHIRHHFI.cql) [ [test results] ](../../input/tests/results/CMS1017FHIRHHFI.txt)

QICore: 10 / 55 — has discrepancies (51 mismatched, 0 missing)

Missing Populations (4 of 65 test cases)
| Test Case | Group | Population |
| --- | --- | --- |
| [ 0884b17b-baa4-47c0-a2b6-5849230dcf43 ](../.././input/tests/measure/CMS1017FHIRHHFI/0884b17b-baa4-47c0-a2b6-5849230dcf43/MeasureReport-83cc8c85-b1c5-4c9c-825d-547009c2e658.json) | Group_1 | Measure Observation |
| [ 58079a8d-2808-4029-b1fb-67ab4a841aa9 ](../.././input/tests/measure/CMS1017FHIRHHFI/58079a8d-2808-4029-b1fb-67ab4a841aa9/MeasureReport-1801d1c6-48ce-4c5e-bc64-9c318eb6709f.json) | Group_1 | Measure Observation |
| [ 5926d62e-8eb9-4742-9bdf-efdcbd08ab7b ](../.././input/tests/measure/CMS1017FHIRHHFI/5926d62e-8eb9-4742-9bdf-efdcbd08ab7b/MeasureReport-b0fb695d-3be9-4ab8-8bbd-b44cd2691bcb.json) | Group_1 | Measure Observation |
| [ 7c4f1e55-2462-45e5-9062-5cf8d04b40e2 ](../.././input/tests/measure/CMS1017FHIRHHFI/7c4f1e55-2462-45e5-9062-5cf8d04b40e2/MeasureReport-417ae819-fffd-441f-b1a2-ab427e0d51af.json) | Group_1 | Measure Observation |


Mismatched Test Cases (51 of 65 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 02d5c5f5-9487-42af-bb5e-dfc3aaeb70eb ](../.././input/tests/measure/CMS1017FHIRHHFI/02d5c5f5-9487-42af-bb5e-dfc3aaeb70eb/MeasureReport-ff263746-408d-42f7-8afa-36508c44d892.json) | Group_1 | Denominator Observation<br>Numerator Observation | 2<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 0653f9dc-8461-484e-a901-f17709f04776 ](../.././input/tests/measure/CMS1017FHIRHHFI/0653f9dc-8461-484e-a901-f17709f04776/MeasureReport-2d7280f0-6a18-4665-861d-0bbb57384e39.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 0dfafc1a-cf94-4ca1-becf-c1b843896810 ](../.././input/tests/measure/CMS1017FHIRHHFI/0dfafc1a-cf94-4ca1-becf-c1b843896810/MeasureReport-cd491c44-6ed1-483f-8775-516f92b9c16d.json) | Group_1 | Numerator Exclusion<br>Denominator Observation<br>Numerator Observation | 0<br>7<br>1 | 1<br>0<br>0 | C-04 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ 113d43da-4cad-4907-804e-63afb7652b27 ](../.././input/tests/measure/CMS1017FHIRHHFI/113d43da-4cad-4907-804e-63afb7652b27/MeasureReport-bd009887-fb29-450b-9dfb-3cf01211624f.json) | Group_1 | Denominator Observation<br>Numerator Observation | 4<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 1ac96e3e-856c-417a-9c68-0df860ce73c8 ](../.././input/tests/measure/CMS1017FHIRHHFI/1ac96e3e-856c-417a-9c68-0df860ce73c8/MeasureReport-cdca174a-a8d1-4caf-b711-732355c746e9.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 1b700def-98b0-418c-b0a7-80ecb275597f ](../.././input/tests/measure/CMS1017FHIRHHFI/1b700def-98b0-418c-b0a7-80ecb275597f/MeasureReport-73e347c6-4fee-446b-8f5d-f03f3094172d.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 1e2ddc95-76d4-4be7-8273-2b35371a727b ](../.././input/tests/measure/CMS1017FHIRHHFI/1e2ddc95-76d4-4be7-8273-2b35371a727b/MeasureReport-2d7f4331-e5f7-43db-9072-a6921d9993e2.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 25c753a7-b6b4-4335-bd7c-05b68b0324a4 ](../.././input/tests/measure/CMS1017FHIRHHFI/25c753a7-b6b4-4335-bd7c-05b68b0324a4/MeasureReport-5e77fa9f-873a-42e3-a0a5-76183c60a3a6.json) | Group_1 | Denominator Observation<br>Numerator Observation | 2<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 28684639-1aa3-429c-9cac-4e8217995b13 ](../.././input/tests/measure/CMS1017FHIRHHFI/28684639-1aa3-429c-9cac-4e8217995b13/MeasureReport-4f50177e-ee99-41d1-ac09-5b8312f5e6fe.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 3279a237-292f-47b1-9039-ba46b57e765a ](../.././input/tests/measure/CMS1017FHIRHHFI/3279a237-292f-47b1-9039-ba46b57e765a/MeasureReport-f5d23a7a-6563-4866-9fed-520eb3083644.json) | Group_1 | Denominator Observation | 4 | 0 | C-04 — resolution pending | FAIL |
| [ 365ed821-88d1-4459-9d32-dd4fa6426335 ](../.././input/tests/measure/CMS1017FHIRHHFI/365ed821-88d1-4459-9d32-dd4fa6426335/MeasureReport-9e88fbe0-1835-4e9d-ad06-a92252e3efeb.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 38d7ec48-dc28-4875-8f24-451ecd3dab5a ](../.././input/tests/measure/CMS1017FHIRHHFI/38d7ec48-dc28-4875-8f24-451ecd3dab5a/MeasureReport-423878a3-c1ef-46cf-a527-3da605013c68.json) | Group_1 | Denominator Observation | 3 | 0 | C-04 — resolution pending | FAIL |
| [ 3c34a5af-1ef1-4b86-a41f-ac1d44e96ca0 ](../.././input/tests/measure/CMS1017FHIRHHFI/3c34a5af-1ef1-4b86-a41f-ac1d44e96ca0/MeasureReport-1edcc821-5daf-4930-bad1-a323f0437344.json) | Group_1 | Numerator Observation | 1 | 0 | C-04 — resolution pending | FAIL |
| [ 3ee27450-2fd5-4930-bfbb-e718074e4087 ](../.././input/tests/measure/CMS1017FHIRHHFI/3ee27450-2fd5-4930-bfbb-e718074e4087/MeasureReport-e222355e-0881-49a6-9386-7f28402016f7.json) | Group_1 | Denominator Observation | 1 | 0 | C-04 — resolution pending | FAIL |
| [ 404570c9-b21f-4fa2-be5d-6d02c910fea6 ](../.././input/tests/measure/CMS1017FHIRHHFI/404570c9-b21f-4fa2-be5d-6d02c910fea6/MeasureReport-be4537a3-fcab-48b3-b04f-fced6ef56cbc.json) | Group_1 | Denominator Observation | 4 | 0 | C-04 — resolution pending | FAIL |
| [ 4402a9b9-3d48-4472-a000-579b7baa88fa ](../.././input/tests/measure/CMS1017FHIRHHFI/4402a9b9-3d48-4472-a000-579b7baa88fa/MeasureReport-58735c7c-d169-4918-b688-f5a907e09834.json) | Group_1 | Denominator Observation<br>Numerator Observation | 12<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 55931284-058b-4776-882c-720caddd3366 ](../.././input/tests/measure/CMS1017FHIRHHFI/55931284-058b-4776-882c-720caddd3366/MeasureReport-7a7c74f8-7efb-407b-a39b-012bec4e1038.json) | Group_1 | Denominator Observation<br>Numerator Observation | 2<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 55b18e1a-c949-4d85-b1c5-caa91fc6ac4b ](../.././input/tests/measure/CMS1017FHIRHHFI/55b18e1a-c949-4d85-b1c5-caa91fc6ac4b/MeasureReport-a02d7a51-5000-46ce-992a-344f91edf5d0.json) | Group_1 | Denominator Observation<br>Numerator Observation | 2<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 56d5fe48-53b2-4599-870c-58bfc4ba0145 ](../.././input/tests/measure/CMS1017FHIRHHFI/56d5fe48-53b2-4599-870c-58bfc4ba0145/MeasureReport-b2feea6f-8dcc-47d3-97ed-0b8ce61fdbb7.json) | Group_1 | Denominator Observation<br>Numerator Observation | 12<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 5d7d44f5-aa75-4889-89d6-e7586ac5de4a ](../.././input/tests/measure/CMS1017FHIRHHFI/5d7d44f5-aa75-4889-89d6-e7586ac5de4a/MeasureReport-f8efe70f-adf2-424e-adbc-4c2d0f393767.json) | Group_1 | Denominator Observation<br>Numerator Observation | 2<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 5ff2713d-ca89-42ae-91bb-cba3e1d9a487 ](../.././input/tests/measure/CMS1017FHIRHHFI/5ff2713d-ca89-42ae-91bb-cba3e1d9a487/MeasureReport-74f8c3e3-881b-4ba8-bfdb-ceef555ed020.json) | Group_1 | Numerator Exclusion<br>Denominator Observation<br>Numerator Observation | 0<br>5<br>1 | 1<br>0<br>0 | C-04 — resolution pending | FAIL<br>FAIL<br>FAIL |
| [ 632475eb-a20a-43d6-baa7-f377ef8c5324 ](../.././input/tests/measure/CMS1017FHIRHHFI/632475eb-a20a-43d6-baa7-f377ef8c5324/MeasureReport-f5be6280-c43b-49e9-9113-e628bc5c7646.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 6442de45-b65d-4bda-8143-0e9c28d19485 ](../.././input/tests/measure/CMS1017FHIRHHFI/6442de45-b65d-4bda-8143-0e9c28d19485/MeasureReport-b81497c1-2782-497e-b242-04fe0f4eb196.json) | Group_1 | Denominator Observation | 2 | 0 | C-04 — resolution pending | FAIL |
| [ 65163106-0b19-4548-a994-f44b35e162e0 ](../.././input/tests/measure/CMS1017FHIRHHFI/65163106-0b19-4548-a994-f44b35e162e0/MeasureReport-a2537d3f-2d92-4e2d-9a9e-f228af25b987.json) | Group_1 | Denominator Observation<br>Numerator Observation | 2<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 66ee842d-d852-42dd-928e-7f9bff5c52cd ](../.././input/tests/measure/CMS1017FHIRHHFI/66ee842d-d852-42dd-928e-7f9bff5c52cd/MeasureReport-1fc4754f-fdf7-4e69-ac2a-f7c4f04418d4.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 6916a60d-ffa4-4d2c-8d96-73a31aa18854 ](../.././input/tests/measure/CMS1017FHIRHHFI/6916a60d-ffa4-4d2c-8d96-73a31aa18854/MeasureReport-b7ea39de-abdd-45a6-ab14-c7c6f13fc0bd.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 735a7993-89b8-4b21-87c2-d5f57df0f5a8 ](../.././input/tests/measure/CMS1017FHIRHHFI/735a7993-89b8-4b21-87c2-d5f57df0f5a8/MeasureReport-ddadfa0f-ba70-4faa-a0d6-acff5965af5a.json) | Group_1 | Denominator Observation<br>Numerator Observation | 4<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 8045dee0-645e-497b-a5e8-ad659cdcf8c6 ](../.././input/tests/measure/CMS1017FHIRHHFI/8045dee0-645e-497b-a5e8-ad659cdcf8c6/MeasureReport-9418fb72-ca02-4a6f-9a0f-067090c8e5f1.json) | Group_1 | Denominator Observation<br>Numerator Observation | 4<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 8552c09f-c2db-4069-9d15-41efafe4711a ](../.././input/tests/measure/CMS1017FHIRHHFI/8552c09f-c2db-4069-9d15-41efafe4711a/MeasureReport-bf211560-ad9c-424f-8a14-0c5339a0a1bf.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 8b607dee-4e17-492d-9949-0c69c10587e3 ](../.././input/tests/measure/CMS1017FHIRHHFI/8b607dee-4e17-492d-9949-0c69c10587e3/MeasureReport-72cfb143-00c9-43a6-b714-665608bec6d4.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ 8b63d691-46a8-4ce6-8dee-60aea7f34f82 ](../.././input/tests/measure/CMS1017FHIRHHFI/8b63d691-46a8-4ce6-8dee-60aea7f34f82/MeasureReport-6c1e7b95-da26-42f0-8128-73f364441235.json) | Group_1 | Denominator Observation | 1 | 0 | C-04 — resolution pending | FAIL |
| [ 966cc666-8f6e-4d6e-93a9-0c4b6345e966 ](../.././input/tests/measure/CMS1017FHIRHHFI/966cc666-8f6e-4d6e-93a9-0c4b6345e966/MeasureReport-a43ccb17-9884-413b-9d84-b5e095be1599.json) | Group_1 | Denominator Observation<br>Numerator Observation | 4<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 972573e8-bd51-4b77-a954-39babec1a055 ](../.././input/tests/measure/CMS1017FHIRHHFI/972573e8-bd51-4b77-a954-39babec1a055/MeasureReport-bbd9b46e-e1fe-4003-95fc-1754c130fda3.json) | Group_1 | Denominator Observation<br>Numerator Observation | 3<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ 9897529f-07ba-42c6-a7b2-5d856a925a6a ](../.././input/tests/measure/CMS1017FHIRHHFI/9897529f-07ba-42c6-a7b2-5d856a925a6a/MeasureReport-e3d20846-af96-45f7-88c8-d85c74a4ba32.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ a0c41f8f-c8d9-4ae5-a07d-e87b16ae1367 ](../.././input/tests/measure/CMS1017FHIRHHFI/a0c41f8f-c8d9-4ae5-a07d-e87b16ae1367/MeasureReport-802900b9-4f29-4203-93aa-a9e085c90c62.json) | Group_1 | Denominator Observation<br>Numerator Observation | 11<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ a2b51ea0-5a72-4bda-abe5-dd393bfa0545 ](../.././input/tests/measure/CMS1017FHIRHHFI/a2b51ea0-5a72-4bda-abe5-dd393bfa0545/MeasureReport-95ac321c-2b89-4714-996a-50f4e392bce9.json) | Group_1 | Denominator Observation<br>Numerator Observation | 12<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ c9413f99-840f-449d-b4ab-427fb7de6aa0 ](../.././input/tests/measure/CMS1017FHIRHHFI/c9413f99-840f-449d-b4ab-427fb7de6aa0/MeasureReport-bef81bad-92d6-4634-ad90-0290df332c0b.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ ca728a1a-9a25-46b6-80bc-bfffae233f6c ](../.././input/tests/measure/CMS1017FHIRHHFI/ca728a1a-9a25-46b6-80bc-bfffae233f6c/MeasureReport-fa3bd160-6278-475a-9c38-e81af83ebb9d.json) | Group_1 | Denominator Observation<br>Numerator Observation | 12<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ d305cce9-ad3c-4259-9f9d-6026974fa847 ](../.././input/tests/measure/CMS1017FHIRHHFI/d305cce9-ad3c-4259-9f9d-6026974fa847/MeasureReport-a73c73db-78f6-428b-89ae-9f1907b86dfc.json) | Group_1 | Denominator Observation<br>Numerator Observation | 4<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ d62aa527-2547-48a1-aece-d649b05c7d6e ](../.././input/tests/measure/CMS1017FHIRHHFI/d62aa527-2547-48a1-aece-d649b05c7d6e/MeasureReport-3cebbd69-4c88-4eda-9587-9854ed2a85e3.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ e287cd76-85bd-4f51-9c41-f19551e83c14 ](../.././input/tests/measure/CMS1017FHIRHHFI/e287cd76-85bd-4f51-9c41-f19551e83c14/MeasureReport-0123f66f-22d0-4297-89fe-228eb29f2363.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ e3212bfa-f3a9-4323-8993-9ef74c2f8d89 ](../.././input/tests/measure/CMS1017FHIRHHFI/e3212bfa-f3a9-4323-8993-9ef74c2f8d89/MeasureReport-4556891c-6318-48fc-99f7-646bf5926c0e.json) | Group_1 | Denominator Observation<br>Numerator Observation | 4<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ e6383b7c-aa91-42f7-8dc8-693a8c7dcaf3 ](../.././input/tests/measure/CMS1017FHIRHHFI/e6383b7c-aa91-42f7-8dc8-693a8c7dcaf3/MeasureReport-4f8aa5d5-93df-4ae6-9187-a41f96326284.json) | Group_1 | Denominator Observation<br>Numerator Observation | 4<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ e6d91b78-a66f-4fcc-a9a7-edb3d862803e ](../.././input/tests/measure/CMS1017FHIRHHFI/e6d91b78-a66f-4fcc-a9a7-edb3d862803e/MeasureReport-ccce2151-63ad-4f73-aa29-a3f52cd6ba54.json) | Group_1 | Denominator Observation<br>Numerator Observation | 3<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ ea6b3f44-c6cd-4417-ae60-e97564bf24f9 ](../.././input/tests/measure/CMS1017FHIRHHFI/ea6b3f44-c6cd-4417-ae60-e97564bf24f9/MeasureReport-1452dcd8-c12d-4d5f-82b8-c3014feff056.json) | Group_1 | Denominator Observation | 5 | 0 | C-04 — resolution pending | FAIL |
| [ f18417e1-5990-40b7-b927-5b50015380a2 ](../.././input/tests/measure/CMS1017FHIRHHFI/f18417e1-5990-40b7-b927-5b50015380a2/MeasureReport-1819fe1f-72fe-426e-b952-f48eea7162e4.json) | Group_1 | Denominator Observation<br>Numerator Observation | 2<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ f4708272-e5d3-4b6a-9431-a3cd2eb6809f ](../.././input/tests/measure/CMS1017FHIRHHFI/f4708272-e5d3-4b6a-9431-a3cd2eb6809f/MeasureReport-959d93a9-fc01-48e7-989b-15b3f2fba3c7.json) | Group_1 | Denominator Observation | 6 | 0 | C-04 — resolution pending | FAIL |
| [ f771a339-74f0-4651-90f3-1c820edea547 ](../.././input/tests/measure/CMS1017FHIRHHFI/f771a339-74f0-4651-90f3-1c820edea547/MeasureReport-283ac8a7-cd94-455f-af78-066c7397562e.json) | Group_1 | Denominator Observation<br>Numerator Observation | 12<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ f90151aa-9bd6-4c0c-bed8-5d43fe7fb8bb ](../.././input/tests/measure/CMS1017FHIRHHFI/f90151aa-9bd6-4c0c-bed8-5d43fe7fb8bb/MeasureReport-f4b4dd8f-02cb-4ed2-9318-79e72ed07d4a.json) | Group_1 | Denominator Observation | 2 | 0 | C-04 — resolution pending | FAIL |
| [ fbbd3f4a-3e8e-40b9-ac83-0e80b2c129ec ](../.././input/tests/measure/CMS1017FHIRHHFI/fbbd3f4a-3e8e-40b9-ac83-0e80b2c129ec/MeasureReport-66785c4c-992c-404b-b8fb-39d71a2a1f16.json) | Group_1 | Denominator Observation<br>Numerator Observation | 2<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |
| [ fd4ea84b-cd39-4d03-9641-9ca8d10bbe67 ](../.././input/tests/measure/CMS1017FHIRHHFI/fd4ea84b-cd39-4d03-9641-9ca8d10bbe67/MeasureReport-bed5a065-8af6-4c78-a165-460571622469.json) | Group_1 | Denominator Observation<br>Numerator Observation | 4<br>1 | 0<br>0 | C-04 — resolution pending | FAIL<br>FAIL |


#### CMS1028FHIRPCSevereOBComps
[ [cql] ](../../input/cql/CMS1028FHIRPCSevereOBComps.cql) [ [test results] ](../../input/tests/results/CMS1028FHIRPCSevereOBComps.txt)

QICore: 136 / 5 — has discrepancies (5 mismatched, 0 missing)

Mismatched Test Cases (1 of 141 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 763d86f9-d93f-4873-8b64-8439566b242e ](../.././input/tests/measure/CMS1028FHIRPCSevereOBComps/763d86f9-d93f-4873-8b64-8439566b242e/MeasureReport-7ca90ad8-935e-4d56-80d9-5470c8a98481.json) | Group_1 | Numerator | 2 | 1 | C-14 — resolution pending | FAIL |
| [ 763d86f9-d93f-4873-8b64-8439566b242e ](../.././input/tests/measure/CMS1028FHIRPCSevereOBComps/763d86f9-d93f-4873-8b64-8439566b242e/MeasureReport-7ca90ad8-935e-4d56-80d9-5470c8a98481.json) | Group_2 | Numerator | 2 | 1 | C-14 — resolution pending | FAIL |


#### CMS1154ScreeningPrediabetesFHIR
[ [cql] ](../../input/cql/CMS1154ScreeningPrediabetesFHIR.cql) [ [test results] ](../../input/tests/results/CMS1154ScreeningPrediabetesFHIR.txt)

QICore: 9 / 1 — has discrepancies (1 mismatched, 0 missing)

Mismatched Test Cases (1 of 10 test cases)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ bc9c82ca-72b5-41c4-a9a3-7e3860a9ac2d ](../.././input/tests/measure/CMS1154ScreeningPrediabetesFHIR/bc9c82ca-72b5-41c4-a9a3-7e3860a9ac2d/MeasureReport-466dec57-6ceb-4f37-8daa-40f26f14a191.json) | Group_1 | Denominator Exclusion | 1 | 0 | E-16 — resolution pending | FAIL |


## Engine Diff: CMS vs QI-Core (qicore-2025)

_Where the CMS engine's actual results differ from the QI-Core engine's (source of truth) on the same test case and population. "Missing in CMS" rows are populations the QI-Core engine produced that CMS did not; "Missing in QI-Core" is the reverse._

| Measure | Mismatch | Missing in CMS | Missing in QI-Core |
| --- | ---: | ---: | ---: |
| CMS2FHIRPCSDepScreenAndFollowUp | 8 | 0 | 0 |
| CMS22FHIRPCSBPScreeningFollowUp | 15 | 0 | 0 |
| CMS68FHIRDocumentationCurrentMeds | 0 | 4 | 0 |
| CMS71FHIRSTKAnticoagAFFlutter | 13 | 0 | 0 |
| CMS72FHIRSTKAntithromboticDay2 | 252 | 0 | 0 |
| CMS104FHIRSTKDCAntithrombotic | 175 | 0 | 0 |
| CMS108FHIRVTEProphylaxis | 26 | 0 | 0 |
| CMS129FHIRProstCaBoneScanUse | 67 | 0 | 0 |
| CMS135FHIRACEIorARBorARNIforHF | 81 | 0 | 0 |
| CMS144FHIRHFBetaBlockerForLVSD | 112 | 0 | 0 |
| CMS145FHIRCADBBlockerTPMIorLVSD | 158 | 0 | 0 |
| CMS149FHIRDementiaCognitiveAssess | 21 | 0 | 0 |
| CMS177FHIRChildMDDSuicideAssmt | 2 | 0 | 0 |
| CMS190FHIRVTEProphylaxisICU | 33 | 0 | 0 |
| CMS0334FHIRPCCesareanBirth | 1 | 0 | 0 |
| CMS347FHIRStatinPreventionTxCVD | 342 | 0 | 20 |
| CMS506FHIRSafeUseofOpioids | 5 | 0 | 0 |
| CMS645FHIRBoneDensityPCADTherapy | 80 | 0 | 0 |
| CMS646FHIRIntravesicalBCGTherapy | 10 | 5 | 0 |
| CMS771FHIRUrinarySymptomScoreBPH | 47 | 0 | 0 |
| CMS819FHIRHHORAE | 2 | 0 | 0 |
| CMS871FHIRHHHyper | 2 | 0 | 0 |
| CMS986FHIRMalnutritionScore | 6 | 0 | 0 |
| CMS996FHIRAptTxforSTEMI | 13 | 0 | 0 |
| CMS1028FHIRPCSevereOBComps | 12 | 0 | 0 |
| CMS1173FHIRDiagnosticDelayVTE | 4 | 0 | 0 |
| CMS1188FHIRHIVSTITesting | 2 | 0 | 0 |
| CMS1264FHIRECATREHQR | 152 | 0 | 0 |
| NHSNAcuteCareHospitalMonthlyInitialPopulation1 | 27 | 0 | 0 |
| NHSNGlycemicControlHypoglycemiaInitialPopulation | 4 | 0 | 0 |

| **Total** | **1672** | **9** | **20** |

### CMS2FHIRPCSDepScreenAndFollowUp

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0e463fc3-d1bf-4e19-882b-fad6342aa668 | Denominator Exception | 0 | 1 | mismatch |
| 12786a64-c20e-4542-a4c0-bf3129d6a9e0 | Denominator Exception | 0 | 1 | mismatch |
| 41df0dbe-ae84-4496-b355-320ff8707a85 | Denominator Exception | 0 | 1 | mismatch |
| 6078e73e-3265-4022-ae63-216c096b6246 | Denominator Exception | 0 | 1 | mismatch |
| 6aaff09e-4a7b-4efa-93f8-13033e95c230 | Denominator Exception | 0 | 1 | mismatch |
| 86ca7528-efcb-44ed-9203-6f21f37f4332 | Denominator Exception | 0 | 1 | mismatch |
| d0ba1182-26fa-4cfa-9f91-960503b7fe53 | Denominator Exception | 0 | 1 | mismatch |
| f29e2786-fade-4dca-b14d-7037a34ef498 | Denominator Exception | 0 | 1 | mismatch |

### CMS22FHIRPCSBPScreeningFollowUp

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0278fdf0-f067-46e8-aeb1-fb96dff3c947 | Denominator Exception | 0 | 1 | mismatch |
| 1f16120b-56c9-4d72-8dd4-01d8a0175d77 | Denominator Exception | 0 | 1 | mismatch |
| 435d23cc-57ef-4f7f-aec3-c7ef524d5cd1 | Denominator Exclusion | 1 | 0 | mismatch |
| 695cee04-cf12-411e-a258-99e430093a4e | Denominator Exception | 0 | 2 | mismatch |
| 86618b52-e0cc-4e90-b48c-cd64bbae8973 | Denominator Exception | 0 | 1 | mismatch |
| 9ed1ecf5-2d93-4bde-a293-5d5fbf209475 | Denominator Exception | 0 | 1 | mismatch |
| a55c6265-a05c-4fad-beb4-c5338420d1b1 | Denominator Exception | 0 | 1 | mismatch |
| ad737f80-c9ea-41fd-a142-78d9c80a9c7c | Denominator Exception | 0 | 1 | mismatch |
| afdeaa75-d332-40f2-9b30-0b6ddf7e7c14 | Denominator Exception | 0 | 1 | mismatch |
| c41f9946-cb0f-4489-8367-581a5b876165 | Denominator Exception | 1 | 2 | mismatch |
| c41f9946-cb0f-4489-8367-581a5b876165 | Numerator | 1 | 0 | mismatch |
| dda022c0-3234-4ad7-ad6e-d696b0b57440 | Denominator Exception | 0 | 1 | mismatch |
| ef9a58ac-e252-480a-bed8-2309c503587d | Denominator Exception | 0 | 1 | mismatch |
| f9417a57-54e8-4a0b-a516-ab62b8d4aae0 | Denominator Exception | 0 | 2 | mismatch |
| f9417a57-54e8-4a0b-a516-ab62b8d4aae0 | Numerator | 1 | 0 | mismatch |

### CMS68FHIRDocumentationCurrentMeds

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| f2e2e1c0-9e35-4592-9579-72a236cb2f56 | Denominator | — | — | missing-in-cms |
| f2e2e1c0-9e35-4592-9579-72a236cb2f56 | Denominator Exception | — | — | missing-in-cms |
| f2e2e1c0-9e35-4592-9579-72a236cb2f56 | Initial Population | — | — | missing-in-cms |
| f2e2e1c0-9e35-4592-9579-72a236cb2f56 | Numerator | — | — | missing-in-cms |

### CMS71FHIRSTKAnticoagAFFlutter

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0587a75d-0dcc-4c6b-bfc0-f5727342ec1f | Denominator | 0 | 1 | mismatch |
| 0587a75d-0dcc-4c6b-bfc0-f5727342ec1f | Numerator | 0 | 1 | mismatch |
| 4151585a-f621-4de5-91df-4e4227e2165e | Denominator | 1 | 0 | mismatch |
| 56ae006d-ab1b-428d-8614-2ccd5d962650 | Denominator | 0 | 1 | mismatch |
| 56ae006d-ab1b-428d-8614-2ccd5d962650 | Numerator | 0 | 1 | mismatch |
| 595ebfd1-fe6a-4b4b-96a1-23a72f6a70da | Denominator Exception | 0 | 1 | mismatch |
| 595ebfd1-fe6a-4b4b-96a1-23a72f6a70da | Numerator | 1 | 0 | mismatch |
| b29204ac-96ce-4be0-90ad-ae8ecfa4f245 | Denominator Exception | 0 | 1 | mismatch |
| b29204ac-96ce-4be0-90ad-ae8ecfa4f245 | Numerator | 1 | 0 | mismatch |
| c640ff8f-5b2a-448e-85a2-e739af7a8dc4 | Denominator Exception | 0 | 1 | mismatch |
| c640ff8f-5b2a-448e-85a2-e739af7a8dc4 | Numerator | 1 | 0 | mismatch |
| e20b4e76-8523-43ab-abc2-a4f4137a84bb | Denominator Exception | 0 | 1 | mismatch |
| e20b4e76-8523-43ab-abc2-a4f4137a84bb | Numerator | 1 | 0 | mismatch |

### CMS72FHIRSTKAntithromboticDay2

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 036c2c6b-c5b7-4e1f-8a85-ac0787e3a15d | Denominator | 1 | 0 | mismatch |
| 036c2c6b-c5b7-4e1f-8a85-ac0787e3a15d | Initial Population | 1 | 0 | mismatch |
| 05ec524f-1d2d-4f9e-8eaa-cc2662030fc6 | Denominator | 1 | 0 | mismatch |
| 05ec524f-1d2d-4f9e-8eaa-cc2662030fc6 | Denominator Exception | 1 | 0 | mismatch |
| 05ec524f-1d2d-4f9e-8eaa-cc2662030fc6 | Initial Population | 1 | 0 | mismatch |
| 072fc02e-93db-449c-a293-2e8525a49694 | Denominator | 1 | 0 | mismatch |
| 072fc02e-93db-449c-a293-2e8525a49694 | Denominator Exception | 1 | 0 | mismatch |
| 072fc02e-93db-449c-a293-2e8525a49694 | Initial Population | 1 | 0 | mismatch |
| 09a132b9-b03c-4a8d-a09f-f18c544bb660 | Denominator | 1 | 0 | mismatch |
| 09a132b9-b03c-4a8d-a09f-f18c544bb660 | Denominator Exception | 1 | 0 | mismatch |
| 09a132b9-b03c-4a8d-a09f-f18c544bb660 | Initial Population | 1 | 0 | mismatch |
| 09a4fe70-dc7a-48ed-9b97-47f0a119eabd | Denominator | 1 | 0 | mismatch |
| 09a4fe70-dc7a-48ed-9b97-47f0a119eabd | Initial Population | 1 | 0 | mismatch |
| 0bc6d4d8-81a0-42a2-9efb-9efbfc3ebe2a | Denominator Exclusion | 1 | 0 | mismatch |
| 0c8a299c-b082-4383-b0b4-aebbb0fa9fb4 | Denominator | 1 | 0 | mismatch |
| 0c8a299c-b082-4383-b0b4-aebbb0fa9fb4 | Initial Population | 1 | 0 | mismatch |
| 0eecd949-77bf-4ded-bb95-40e11c2116c7 | Denominator | 1 | 0 | mismatch |
| 0eecd949-77bf-4ded-bb95-40e11c2116c7 | Initial Population | 1 | 0 | mismatch |
| 11fc1901-7cc7-46c6-bbd0-58b614082170 | Denominator | 1 | 0 | mismatch |
| 11fc1901-7cc7-46c6-bbd0-58b614082170 | Initial Population | 1 | 0 | mismatch |
| 1216ea1e-aee7-4c75-8e5d-7f712f6ba3f7 | Denominator | 1 | 0 | mismatch |
| 1216ea1e-aee7-4c75-8e5d-7f712f6ba3f7 | Initial Population | 1 | 0 | mismatch |
| 144370a9-c9cf-43db-ba18-f92f4f8cec29 | Denominator | 1 | 0 | mismatch |
| 144370a9-c9cf-43db-ba18-f92f4f8cec29 | Denominator Exception | 1 | 0 | mismatch |
| 144370a9-c9cf-43db-ba18-f92f4f8cec29 | Initial Population | 1 | 0 | mismatch |
| 155afb0b-baef-4e1a-8255-dd3bc96c9c0d | Denominator | 1 | 0 | mismatch |
| 155afb0b-baef-4e1a-8255-dd3bc96c9c0d | Initial Population | 1 | 0 | mismatch |
| 1ef5e77a-dea5-4f1f-873b-44ea79810330 | Denominator | 1 | 0 | mismatch |
| 1ef5e77a-dea5-4f1f-873b-44ea79810330 | Initial Population | 1 | 0 | mismatch |
| 1ef5e77a-dea5-4f1f-873b-44ea79810330 | Numerator | 1 | 0 | mismatch |
| 2402a44f-a5bb-4010-9045-123f5cfb976a | Denominator Exclusion | 1 | 0 | mismatch |
| 2a1812bc-465a-438c-934c-e85a3591512a | Denominator | 1 | 0 | mismatch |
| 2a1812bc-465a-438c-934c-e85a3591512a | Denominator Exception | 1 | 0 | mismatch |
| 2a1812bc-465a-438c-934c-e85a3591512a | Initial Population | 1 | 0 | mismatch |
| 2ecbb381-211e-421a-8053-21c820f33043 | Denominator | 1 | 0 | mismatch |
| 2ecbb381-211e-421a-8053-21c820f33043 | Denominator Exception | 1 | 0 | mismatch |
| 2ecbb381-211e-421a-8053-21c820f33043 | Initial Population | 1 | 0 | mismatch |
| 2f7681fa-66b0-4395-aa35-7622e37709ae | Denominator | 1 | 0 | mismatch |
| 2f7681fa-66b0-4395-aa35-7622e37709ae | Initial Population | 1 | 0 | mismatch |
| 3264d587-3c02-45ff-b989-044fcc30abae | Denominator | 1 | 0 | mismatch |
| 3264d587-3c02-45ff-b989-044fcc30abae | Denominator Exception | 1 | 0 | mismatch |
| 3264d587-3c02-45ff-b989-044fcc30abae | Initial Population | 1 | 0 | mismatch |
| 3432dedb-7130-4614-9283-6c1569fab90f | Denominator | 1 | 0 | mismatch |
| 3432dedb-7130-4614-9283-6c1569fab90f | Initial Population | 1 | 0 | mismatch |
| 388557b1-cf25-4750-88b2-751e475b433f | Denominator | 1 | 0 | mismatch |
| 388557b1-cf25-4750-88b2-751e475b433f | Initial Population | 1 | 0 | mismatch |
| 3ab85f43-dd45-4827-8f13-ad9d1208d2e0 | Denominator | 1 | 0 | mismatch |
| 3ab85f43-dd45-4827-8f13-ad9d1208d2e0 | Denominator Exclusion | 1 | 0 | mismatch |
| 3ab85f43-dd45-4827-8f13-ad9d1208d2e0 | Initial Population | 1 | 0 | mismatch |
| 3fbd6ec2-8d40-45e7-90d7-747d7c491c1a | Denominator Exclusion | 1 | 0 | mismatch |
| 4b6a9c86-3aad-4828-be61-bab6cd0c3140 | Denominator | 1 | 0 | mismatch |
| 4b6a9c86-3aad-4828-be61-bab6cd0c3140 | Denominator Exception | 1 | 0 | mismatch |
| 4b6a9c86-3aad-4828-be61-bab6cd0c3140 | Initial Population | 1 | 0 | mismatch |
| 4f8b0ca2-baf1-4ce6-8b9a-c3220097cf7c | Denominator | 1 | 0 | mismatch |
| 4f8b0ca2-baf1-4ce6-8b9a-c3220097cf7c | Denominator Exception | 1 | 0 | mismatch |
| 4f8b0ca2-baf1-4ce6-8b9a-c3220097cf7c | Initial Population | 1 | 0 | mismatch |
| 54381296-da32-4474-85b7-209d99c52e7e | Denominator | 1 | 0 | mismatch |
| 54381296-da32-4474-85b7-209d99c52e7e | Initial Population | 1 | 0 | mismatch |
| 5736982d-6c82-4815-b0d2-3416ebe105f4 | Denominator | 1 | 0 | mismatch |
| 5736982d-6c82-4815-b0d2-3416ebe105f4 | Denominator Exclusion | 1 | 0 | mismatch |
| 5736982d-6c82-4815-b0d2-3416ebe105f4 | Initial Population | 1 | 0 | mismatch |
| 58169ea2-037f-4302-9c37-4239fe24f73d | Denominator | 1 | 0 | mismatch |
| 58169ea2-037f-4302-9c37-4239fe24f73d | Denominator Exception | 1 | 0 | mismatch |
| 58169ea2-037f-4302-9c37-4239fe24f73d | Initial Population | 1 | 0 | mismatch |
| 5a329008-fcc1-4168-ab9c-89cb5dd6ff32 | Denominator | 1 | 0 | mismatch |
| 5a329008-fcc1-4168-ab9c-89cb5dd6ff32 | Denominator Exclusion | 1 | 0 | mismatch |
| 5a329008-fcc1-4168-ab9c-89cb5dd6ff32 | Initial Population | 1 | 0 | mismatch |
| 5adf0120-b2f5-415f-b1ff-1684d9f4af7a | Denominator | 1 | 0 | mismatch |
| 5adf0120-b2f5-415f-b1ff-1684d9f4af7a | Initial Population | 1 | 0 | mismatch |
| 64a75df8-8bed-49ea-9c90-ee3569d233df | Denominator | 1 | 0 | mismatch |
| 64a75df8-8bed-49ea-9c90-ee3569d233df | Initial Population | 1 | 0 | mismatch |
| 64a75df8-8bed-49ea-9c90-ee3569d233df | Numerator | 1 | 0 | mismatch |
| 6678ed6f-3c94-4630-a7c5-d35a003b4535 | Denominator | 1 | 0 | mismatch |
| 6678ed6f-3c94-4630-a7c5-d35a003b4535 | Initial Population | 1 | 0 | mismatch |
| 6bbc3f38-7f5f-4da9-9beb-eb32874fd1ed | Denominator | 1 | 0 | mismatch |
| 6bbc3f38-7f5f-4da9-9beb-eb32874fd1ed | Initial Population | 1 | 0 | mismatch |
| 70e86911-43d6-41de-bfb9-933d8f539b98 | Denominator | 1 | 0 | mismatch |
| 70e86911-43d6-41de-bfb9-933d8f539b98 | Denominator Exception | 1 | 0 | mismatch |
| 70e86911-43d6-41de-bfb9-933d8f539b98 | Initial Population | 1 | 0 | mismatch |
| 7317795b-638b-4d0c-9e9e-b55ade45958c | Denominator | 1 | 0 | mismatch |
| 7317795b-638b-4d0c-9e9e-b55ade45958c | Denominator Exclusion | 1 | 0 | mismatch |
| 7317795b-638b-4d0c-9e9e-b55ade45958c | Initial Population | 1 | 0 | mismatch |
| 763c581d-7398-47e7-ba78-eaa5853df551 | Denominator | 1 | 0 | mismatch |
| 763c581d-7398-47e7-ba78-eaa5853df551 | Initial Population | 1 | 0 | mismatch |
| 77a6cd7b-4322-4c29-b248-64d8af106ce7 | Denominator | 1 | 0 | mismatch |
| 77a6cd7b-4322-4c29-b248-64d8af106ce7 | Initial Population | 1 | 0 | mismatch |
| 77bba430-02fc-4ac7-ab49-f57fd73daa9b | Denominator | 1 | 0 | mismatch |
| 77bba430-02fc-4ac7-ab49-f57fd73daa9b | Initial Population | 1 | 0 | mismatch |
| 79a2dd53-a342-41d9-a5c9-1b565bd06fe7 | Denominator | 1 | 0 | mismatch |
| 79a2dd53-a342-41d9-a5c9-1b565bd06fe7 | Denominator Exclusion | 1 | 0 | mismatch |
| 79a2dd53-a342-41d9-a5c9-1b565bd06fe7 | Initial Population | 1 | 0 | mismatch |
| 79f6bb60-1bdb-4dff-857d-65311e9ccea5 | Denominator | 1 | 0 | mismatch |
| 79f6bb60-1bdb-4dff-857d-65311e9ccea5 | Initial Population | 1 | 0 | mismatch |
| 7abd0282-c461-4c61-9669-f261a689f485 | Denominator | 1 | 0 | mismatch |
| 7abd0282-c461-4c61-9669-f261a689f485 | Denominator Exclusion | 1 | 0 | mismatch |
| 7abd0282-c461-4c61-9669-f261a689f485 | Initial Population | 1 | 0 | mismatch |
| 7ce35cc8-ed8f-46f0-9ba1-8421a760bdc8 | Denominator | 1 | 0 | mismatch |
| 7ce35cc8-ed8f-46f0-9ba1-8421a760bdc8 | Initial Population | 1 | 0 | mismatch |
| 7d9affce-5c31-4fcb-b9e5-c0304c3f9406 | Denominator | 1 | 0 | mismatch |
| 7d9affce-5c31-4fcb-b9e5-c0304c3f9406 | Denominator Exception | 1 | 0 | mismatch |
| 7d9affce-5c31-4fcb-b9e5-c0304c3f9406 | Initial Population | 1 | 0 | mismatch |
| 7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8 | Denominator | 1 | 0 | mismatch |
| 7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8 | Initial Population | 1 | 0 | mismatch |
| 7e3bf20a-7a5b-4d50-aa34-267ab19da7b2 | Denominator | 1 | 0 | mismatch |
| 7e3bf20a-7a5b-4d50-aa34-267ab19da7b2 | Initial Population | 1 | 0 | mismatch |
| 7e3bf20a-7a5b-4d50-aa34-267ab19da7b2 | Numerator | 1 | 0 | mismatch |
| 82399522-ba6c-4997-afc9-23f55bb7da89 | Denominator | 1 | 0 | mismatch |
| 82399522-ba6c-4997-afc9-23f55bb7da89 | Initial Population | 1 | 0 | mismatch |
| 82fd75d8-4816-4d24-b18c-0e454c430eb5 | Denominator | 1 | 0 | mismatch |
| 82fd75d8-4816-4d24-b18c-0e454c430eb5 | Denominator Exception | 1 | 0 | mismatch |
| 82fd75d8-4816-4d24-b18c-0e454c430eb5 | Initial Population | 1 | 0 | mismatch |
| 844d9440-ab79-4206-9893-bcf9a786970e | Denominator | 1 | 0 | mismatch |
| 844d9440-ab79-4206-9893-bcf9a786970e | Initial Population | 1 | 0 | mismatch |
| 844d9440-ab79-4206-9893-bcf9a786970e | Numerator | 1 | 0 | mismatch |
| 8873f644-9b17-4ee1-9152-8c77c7374bd1 | Denominator Exclusion | 1 | 0 | mismatch |
| 89275dc4-f4c1-41b5-a215-9c7228933cc0 | Denominator | 1 | 0 | mismatch |
| 89275dc4-f4c1-41b5-a215-9c7228933cc0 | Denominator Exclusion | 1 | 0 | mismatch |
| 89275dc4-f4c1-41b5-a215-9c7228933cc0 | Initial Population | 1 | 0 | mismatch |
| 93798745-af1c-4eb6-8dc4-446a531c05a4 | Denominator | 1 | 0 | mismatch |
| 93798745-af1c-4eb6-8dc4-446a531c05a4 | Initial Population | 1 | 0 | mismatch |
| 93798745-af1c-4eb6-8dc4-446a531c05a4 | Numerator | 1 | 0 | mismatch |
| 96266910-a2b3-4294-9dc5-8a812622b70b | Denominator | 1 | 0 | mismatch |
| 96266910-a2b3-4294-9dc5-8a812622b70b | Initial Population | 1 | 0 | mismatch |
| 9843e92a-751f-4b3c-86b8-50397a64c8fd | Denominator | 1 | 0 | mismatch |
| 9843e92a-751f-4b3c-86b8-50397a64c8fd | Denominator Exception | 1 | 0 | mismatch |
| 9843e92a-751f-4b3c-86b8-50397a64c8fd | Initial Population | 1 | 0 | mismatch |
| 9915271c-d175-41e1-8550-2f698a2b9dc7 | Denominator Exclusion | 1 | 0 | mismatch |
| 9a297d79-90eb-46f1-9068-1a7c7b6c7147 | Denominator | 1 | 0 | mismatch |
| 9a297d79-90eb-46f1-9068-1a7c7b6c7147 | Initial Population | 1 | 0 | mismatch |
| 9a42c820-29ec-464e-b2f5-eb8114985a0c | Denominator | 1 | 0 | mismatch |
| 9a42c820-29ec-464e-b2f5-eb8114985a0c | Denominator Exception | 1 | 0 | mismatch |
| 9a42c820-29ec-464e-b2f5-eb8114985a0c | Initial Population | 1 | 0 | mismatch |
| 9a8c51a0-bf53-42b6-927d-c1f90b81a31a | Denominator | 1 | 0 | mismatch |
| 9a8c51a0-bf53-42b6-927d-c1f90b81a31a | Initial Population | 1 | 0 | mismatch |
| 9bfee327-99be-48de-ba09-5b64e4435f8d | Denominator | 1 | 0 | mismatch |
| 9bfee327-99be-48de-ba09-5b64e4435f8d | Denominator Exclusion | 1 | 0 | mismatch |
| 9bfee327-99be-48de-ba09-5b64e4435f8d | Initial Population | 1 | 0 | mismatch |
| 9f4bc5cc-b5a4-4d67-a11f-9f171b62fd9f | Denominator | 1 | 0 | mismatch |
| 9f4bc5cc-b5a4-4d67-a11f-9f171b62fd9f | Initial Population | 1 | 0 | mismatch |
| a0ced1fb-191d-404b-80f4-761e51cf9de2 | Denominator | 1 | 0 | mismatch |
| a0ced1fb-191d-404b-80f4-761e51cf9de2 | Denominator Exception | 1 | 0 | mismatch |
| a0ced1fb-191d-404b-80f4-761e51cf9de2 | Initial Population | 1 | 0 | mismatch |
| a1a37483-1a67-4dd9-a8ca-b4d49a28a19d | Denominator | 1 | 0 | mismatch |
| a1a37483-1a67-4dd9-a8ca-b4d49a28a19d | Initial Population | 1 | 0 | mismatch |
| a2cb4956-d7e5-45a9-8007-80dcb893203c | Denominator | 1 | 0 | mismatch |
| a2cb4956-d7e5-45a9-8007-80dcb893203c | Denominator Exception | 1 | 0 | mismatch |
| a2cb4956-d7e5-45a9-8007-80dcb893203c | Initial Population | 1 | 0 | mismatch |
| a5feebb4-d3c0-4435-aed5-9579b75a8a52 | Denominator | 1 | 0 | mismatch |
| a5feebb4-d3c0-4435-aed5-9579b75a8a52 | Initial Population | 1 | 0 | mismatch |
| a938e0ff-51b3-4001-b33e-5fd2c00a9147 | Denominator | 1 | 0 | mismatch |
| a938e0ff-51b3-4001-b33e-5fd2c00a9147 | Initial Population | 1 | 0 | mismatch |
| a938e0ff-51b3-4001-b33e-5fd2c00a9147 | Numerator | 1 | 0 | mismatch |
| aadbfade-4898-4931-9e11-e5d7ba64ab27 | Denominator | 1 | 0 | mismatch |
| aadbfade-4898-4931-9e11-e5d7ba64ab27 | Denominator Exception | 1 | 0 | mismatch |
| aadbfade-4898-4931-9e11-e5d7ba64ab27 | Initial Population | 1 | 0 | mismatch |
| ab024aef-425c-43ba-a856-882a3e3c91f1 | Denominator | 1 | 0 | mismatch |
| ab024aef-425c-43ba-a856-882a3e3c91f1 | Denominator Exception | 1 | 0 | mismatch |
| ab024aef-425c-43ba-a856-882a3e3c91f1 | Initial Population | 1 | 0 | mismatch |
| ab28178c-eadb-41a3-861e-ee22c8f12d16 | Denominator | 1 | 0 | mismatch |
| ab28178c-eadb-41a3-861e-ee22c8f12d16 | Initial Population | 1 | 0 | mismatch |
| ac23e6a6-3f36-49db-9eba-2da744a41c57 | Denominator | 1 | 0 | mismatch |
| ac23e6a6-3f36-49db-9eba-2da744a41c57 | Denominator Exception | 1 | 0 | mismatch |
| ac23e6a6-3f36-49db-9eba-2da744a41c57 | Initial Population | 1 | 0 | mismatch |
| ad35c913-a8ba-4d29-b6e9-8652aa5ca20c | Denominator | 1 | 0 | mismatch |
| ad35c913-a8ba-4d29-b6e9-8652aa5ca20c | Denominator Exclusion | 1 | 0 | mismatch |
| ad35c913-a8ba-4d29-b6e9-8652aa5ca20c | Initial Population | 1 | 0 | mismatch |
| b3043789-f91a-42f6-848d-6bfd7df331fe | Denominator | 1 | 0 | mismatch |
| b3043789-f91a-42f6-848d-6bfd7df331fe | Denominator Exclusion | 1 | 0 | mismatch |
| b3043789-f91a-42f6-848d-6bfd7df331fe | Initial Population | 1 | 0 | mismatch |
| b4cd9b20-6d41-4034-907c-b24e362a0699 | Denominator | 1 | 0 | mismatch |
| b4cd9b20-6d41-4034-907c-b24e362a0699 | Denominator Exception | 1 | 0 | mismatch |
| b4cd9b20-6d41-4034-907c-b24e362a0699 | Initial Population | 1 | 0 | mismatch |
| b569157b-b263-4b72-ab40-132bea1d8f71 | Denominator | 1 | 0 | mismatch |
| b569157b-b263-4b72-ab40-132bea1d8f71 | Initial Population | 1 | 0 | mismatch |
| b569157b-b263-4b72-ab40-132bea1d8f71 | Numerator | 1 | 0 | mismatch |
| b86e54d1-f8ca-44b6-99a5-d455c5649104 | Denominator | 1 | 0 | mismatch |
| b86e54d1-f8ca-44b6-99a5-d455c5649104 | Initial Population | 1 | 0 | mismatch |
| bda91aac-a815-4a22-b505-36cef1080d49 | Denominator | 1 | 0 | mismatch |
| bda91aac-a815-4a22-b505-36cef1080d49 | Denominator Exception | 1 | 0 | mismatch |
| bda91aac-a815-4a22-b505-36cef1080d49 | Initial Population | 1 | 0 | mismatch |
| c014ff5d-792f-45c9-9659-4999537005b0 | Denominator | 1 | 0 | mismatch |
| c014ff5d-792f-45c9-9659-4999537005b0 | Denominator Exception | 1 | 0 | mismatch |
| c014ff5d-792f-45c9-9659-4999537005b0 | Initial Population | 1 | 0 | mismatch |
| c1ee2b07-f5a7-451c-8bc5-cb97b1cbcca2 | Denominator | 1 | 0 | mismatch |
| c1ee2b07-f5a7-451c-8bc5-cb97b1cbcca2 | Initial Population | 1 | 0 | mismatch |
| c48c3487-44cf-4a09-bc17-e60e66d19002 | Denominator | 1 | 0 | mismatch |
| c48c3487-44cf-4a09-bc17-e60e66d19002 | Denominator Exception | 1 | 0 | mismatch |
| c48c3487-44cf-4a09-bc17-e60e66d19002 | Initial Population | 1 | 0 | mismatch |
| c5085136-65ef-498f-8aa9-449bf48f6a63 | Denominator | 1 | 0 | mismatch |
| c5085136-65ef-498f-8aa9-449bf48f6a63 | Initial Population | 1 | 0 | mismatch |
| c7382fb6-053b-4424-b5c2-87d79179b016 | Denominator | 1 | 0 | mismatch |
| c7382fb6-053b-4424-b5c2-87d79179b016 | Initial Population | 1 | 0 | mismatch |
| c787d9c8-9645-4da6-a607-85dbefdf129e | Denominator | 1 | 0 | mismatch |
| c787d9c8-9645-4da6-a607-85dbefdf129e | Denominator Exception | 1 | 0 | mismatch |
| c787d9c8-9645-4da6-a607-85dbefdf129e | Initial Population | 1 | 0 | mismatch |
| c84cc10b-29f5-41cb-84a7-fbb23f52e0d5 | Denominator | 1 | 0 | mismatch |
| c84cc10b-29f5-41cb-84a7-fbb23f52e0d5 | Initial Population | 1 | 0 | mismatch |
| cb7c95fc-6d6b-4e07-81e8-a79385142b94 | Denominator | 1 | 0 | mismatch |
| cb7c95fc-6d6b-4e07-81e8-a79385142b94 | Denominator Exclusion | 1 | 0 | mismatch |
| cb7c95fc-6d6b-4e07-81e8-a79385142b94 | Initial Population | 1 | 0 | mismatch |
| cc23329d-6635-4347-8669-a98c921f4381 | Denominator | 1 | 0 | mismatch |
| cc23329d-6635-4347-8669-a98c921f4381 | Denominator Exception | 1 | 0 | mismatch |
| cc23329d-6635-4347-8669-a98c921f4381 | Initial Population | 1 | 0 | mismatch |
| d0a59b97-c3ab-4028-9109-a31359a93c47 | Denominator | 1 | 0 | mismatch |
| d0a59b97-c3ab-4028-9109-a31359a93c47 | Initial Population | 1 | 0 | mismatch |
| d0a59b97-c3ab-4028-9109-a31359a93c47 | Numerator | 1 | 0 | mismatch |
| d496f08e-c55b-44b1-97a7-f86cf9ead1e2 | Denominator | 1 | 0 | mismatch |
| d496f08e-c55b-44b1-97a7-f86cf9ead1e2 | Initial Population | 1 | 0 | mismatch |
| d82d5f38-a1b7-4f28-a3db-25f42f7e64b2 | Denominator | 1 | 0 | mismatch |
| d82d5f38-a1b7-4f28-a3db-25f42f7e64b2 | Denominator Exception | 1 | 0 | mismatch |
| d82d5f38-a1b7-4f28-a3db-25f42f7e64b2 | Initial Population | 1 | 0 | mismatch |
| da480fb9-7501-46f5-9575-f15a638bc751 | Denominator | 1 | 0 | mismatch |
| da480fb9-7501-46f5-9575-f15a638bc751 | Initial Population | 1 | 0 | mismatch |
| dc187313-245c-4ed6-b6bb-fcb94c117fec | Denominator | 1 | 0 | mismatch |
| dc187313-245c-4ed6-b6bb-fcb94c117fec | Initial Population | 1 | 0 | mismatch |
| dd40e582-8c3f-44a2-b781-84acead6120f | Denominator | 1 | 0 | mismatch |
| dd40e582-8c3f-44a2-b781-84acead6120f | Denominator Exception | 1 | 0 | mismatch |
| dd40e582-8c3f-44a2-b781-84acead6120f | Initial Population | 1 | 0 | mismatch |
| de4005d0-549c-40bb-93b9-26650c194d04 | Denominator | 1 | 0 | mismatch |
| de4005d0-549c-40bb-93b9-26650c194d04 | Initial Population | 1 | 0 | mismatch |
| e0286677-4610-4138-b9fe-3ed648ed45f8 | Denominator | 1 | 0 | mismatch |
| e0286677-4610-4138-b9fe-3ed648ed45f8 | Initial Population | 1 | 0 | mismatch |
| e0bae301-79a4-48b0-a174-5eb1bd6cf19e | Denominator Exclusion | 1 | 0 | mismatch |
| e89c4eae-404c-44b9-8be5-c8a8b481813a | Denominator | 1 | 0 | mismatch |
| e89c4eae-404c-44b9-8be5-c8a8b481813a | Initial Population | 1 | 0 | mismatch |
| ea737165-ca06-4304-9964-c157d504c3ee | Denominator | 1 | 0 | mismatch |
| ea737165-ca06-4304-9964-c157d504c3ee | Denominator Exclusion | 1 | 0 | mismatch |
| ea737165-ca06-4304-9964-c157d504c3ee | Initial Population | 1 | 0 | mismatch |
| eafd6c1f-c099-48b8-8101-b24b4a49cd0b | Denominator | 1 | 0 | mismatch |
| eafd6c1f-c099-48b8-8101-b24b4a49cd0b | Denominator Exclusion | 1 | 0 | mismatch |
| eafd6c1f-c099-48b8-8101-b24b4a49cd0b | Initial Population | 1 | 0 | mismatch |
| ed638412-155e-4349-8461-4550fd4fae3b | Denominator | 1 | 0 | mismatch |
| ed638412-155e-4349-8461-4550fd4fae3b | Initial Population | 1 | 0 | mismatch |
| edd8668f-d541-4869-9bb6-02e9e2c2509d | Denominator Exclusion | 1 | 0 | mismatch |
| f0d37c4e-7377-4876-8533-f955963f96f9 | Denominator | 1 | 0 | mismatch |
| f0d37c4e-7377-4876-8533-f955963f96f9 | Initial Population | 1 | 0 | mismatch |
| f25baf5f-2980-416c-a8ef-3b9e42d751c3 | Denominator | 1 | 0 | mismatch |
| f25baf5f-2980-416c-a8ef-3b9e42d751c3 | Initial Population | 1 | 0 | mismatch |
| f5f317c7-69f1-4a89-850a-8a58789c80f2 | Denominator | 1 | 0 | mismatch |
| f5f317c7-69f1-4a89-850a-8a58789c80f2 | Initial Population | 1 | 0 | mismatch |
| fac8c35b-89c4-4ff0-bf5b-32147a59e18f | Denominator Exclusion | 1 | 0 | mismatch |
| fba66844-a0b7-4a8b-a33b-d75df55e9fea | Denominator Exclusion | 1 | 0 | mismatch |
| febd4b3e-99bc-4c55-bba9-3b2136c2160b | Denominator | 1 | 0 | mismatch |
| febd4b3e-99bc-4c55-bba9-3b2136c2160b | Initial Population | 1 | 0 | mismatch |
| febd4b3e-99bc-4c55-bba9-3b2136c2160b | Numerator | 1 | 0 | mismatch |
| fed17706-6d92-4092-a9b1-9b7e47847f2a | Denominator | 1 | 0 | mismatch |
| fed17706-6d92-4092-a9b1-9b7e47847f2a | Denominator Exclusion | 1 | 0 | mismatch |
| fed17706-6d92-4092-a9b1-9b7e47847f2a | Initial Population | 1 | 0 | mismatch |
| fed7bfb0-5746-4029-a64c-f40cc30ce946 | Denominator | 1 | 0 | mismatch |
| fed7bfb0-5746-4029-a64c-f40cc30ce946 | Initial Population | 1 | 0 | mismatch |
| ff9ea2c7-7a68-486d-809e-f0e2cd94d6eb | Denominator | 1 | 0 | mismatch |
| ff9ea2c7-7a68-486d-809e-f0e2cd94d6eb | Initial Population | 1 | 0 | mismatch |

### CMS104FHIRSTKDCAntithrombotic

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 003b2da3-b46a-4b24-91be-65ef27eef3bc | Denominator | 1 | 0 | mismatch |
| 003b2da3-b46a-4b24-91be-65ef27eef3bc | Initial Population | 1 | 0 | mismatch |
| 0852e05c-94f3-4467-ad2c-255ffc5050e9 | Denominator | 1 | 0 | mismatch |
| 0852e05c-94f3-4467-ad2c-255ffc5050e9 | Initial Population | 1 | 0 | mismatch |
| 0edb029c-ae5a-492a-ad4c-79ea0f8059d4 | Denominator | 1 | 0 | mismatch |
| 0edb029c-ae5a-492a-ad4c-79ea0f8059d4 | Denominator Exclusion | 1 | 0 | mismatch |
| 0edb029c-ae5a-492a-ad4c-79ea0f8059d4 | Initial Population | 1 | 0 | mismatch |
| 146a6714-8663-4f45-826a-01110ff34490 | Denominator | 1 | 0 | mismatch |
| 146a6714-8663-4f45-826a-01110ff34490 | Initial Population | 1 | 0 | mismatch |
| 146a6714-8663-4f45-826a-01110ff34490 | Numerator | 1 | 0 | mismatch |
| 15e67912-9913-4b22-9f1b-3e86879e1d6d | Denominator | 1 | 0 | mismatch |
| 15e67912-9913-4b22-9f1b-3e86879e1d6d | Denominator Exclusion | 1 | 0 | mismatch |
| 15e67912-9913-4b22-9f1b-3e86879e1d6d | Initial Population | 1 | 0 | mismatch |
| 162a5913-9989-42f2-8d6a-ae460e245e4c | Denominator | 1 | 0 | mismatch |
| 162a5913-9989-42f2-8d6a-ae460e245e4c | Initial Population | 1 | 0 | mismatch |
| 1ec7f3ad-fe6d-486b-829b-101ebb721824 | Denominator | 1 | 0 | mismatch |
| 1ec7f3ad-fe6d-486b-829b-101ebb721824 | Initial Population | 1 | 0 | mismatch |
| 2326f161-b68e-4034-91cb-4eae3c2ba587 | Denominator | 1 | 0 | mismatch |
| 2326f161-b68e-4034-91cb-4eae3c2ba587 | Initial Population | 1 | 0 | mismatch |
| 264ec8d1-8e92-4b73-a6cb-e8856b22890d | Denominator | 1 | 0 | mismatch |
| 264ec8d1-8e92-4b73-a6cb-e8856b22890d | Denominator Exclusion | 1 | 0 | mismatch |
| 264ec8d1-8e92-4b73-a6cb-e8856b22890d | Initial Population | 1 | 0 | mismatch |
| 2d54a94c-edf1-4f92-baf8-3813a8ef452d | Denominator | 1 | 0 | mismatch |
| 2d54a94c-edf1-4f92-baf8-3813a8ef452d | Initial Population | 1 | 0 | mismatch |
| 2d54a94c-edf1-4f92-baf8-3813a8ef452d | Numerator | 1 | 0 | mismatch |
| 2e0b5b75-22d9-4607-b8fe-f31c86620554 | Denominator | 1 | 0 | mismatch |
| 2e0b5b75-22d9-4607-b8fe-f31c86620554 | Initial Population | 1 | 0 | mismatch |
| 2ffdd04b-5cee-4904-9ce8-2f68dada9941 | Denominator | 1 | 0 | mismatch |
| 2ffdd04b-5cee-4904-9ce8-2f68dada9941 | Denominator Exclusion | 1 | 0 | mismatch |
| 2ffdd04b-5cee-4904-9ce8-2f68dada9941 | Initial Population | 1 | 0 | mismatch |
| 302f7629-15c3-4e52-86df-5677eab6770c | Denominator | 1 | 0 | mismatch |
| 302f7629-15c3-4e52-86df-5677eab6770c | Denominator Exclusion | 1 | 0 | mismatch |
| 302f7629-15c3-4e52-86df-5677eab6770c | Initial Population | 1 | 0 | mismatch |
| 348471db-5aaa-4bf3-a280-75222f20d599 | Denominator | 1 | 0 | mismatch |
| 348471db-5aaa-4bf3-a280-75222f20d599 | Denominator Exception | 1 | 0 | mismatch |
| 348471db-5aaa-4bf3-a280-75222f20d599 | Initial Population | 1 | 0 | mismatch |
| 34d3361c-95b3-43bf-a2a8-380914e06acb | Denominator | 1 | 0 | mismatch |
| 34d3361c-95b3-43bf-a2a8-380914e06acb | Denominator Exclusion | 1 | 0 | mismatch |
| 34d3361c-95b3-43bf-a2a8-380914e06acb | Initial Population | 1 | 0 | mismatch |
| 3da60e55-4952-4341-b2eb-a79707f4ec3e | Denominator | 1 | 0 | mismatch |
| 3da60e55-4952-4341-b2eb-a79707f4ec3e | Denominator Exclusion | 1 | 0 | mismatch |
| 3da60e55-4952-4341-b2eb-a79707f4ec3e | Initial Population | 1 | 0 | mismatch |
| 3f089430-0edb-485d-9844-b2c58fb715e2 | Denominator | 1 | 0 | mismatch |
| 3f089430-0edb-485d-9844-b2c58fb715e2 | Initial Population | 1 | 0 | mismatch |
| 451b6853-3734-4c1c-b37e-5904629e0350 | Denominator | 1 | 0 | mismatch |
| 451b6853-3734-4c1c-b37e-5904629e0350 | Denominator Exclusion | 1 | 0 | mismatch |
| 451b6853-3734-4c1c-b37e-5904629e0350 | Initial Population | 1 | 0 | mismatch |
| 48952352-d74c-491c-9420-6e999e60f52a | Denominator | 1 | 0 | mismatch |
| 48952352-d74c-491c-9420-6e999e60f52a | Initial Population | 1 | 0 | mismatch |
| 48952352-d74c-491c-9420-6e999e60f52a | Numerator | 1 | 0 | mismatch |
| 4d94ffcd-39a0-4e40-83c1-6093ff82d641 | Denominator | 1 | 0 | mismatch |
| 4d94ffcd-39a0-4e40-83c1-6093ff82d641 | Initial Population | 1 | 0 | mismatch |
| 52a258e1-0a79-4bb7-8f50-1aa519aa4e00 | Denominator | 1 | 0 | mismatch |
| 52a258e1-0a79-4bb7-8f50-1aa519aa4e00 | Denominator Exclusion | 1 | 0 | mismatch |
| 52a258e1-0a79-4bb7-8f50-1aa519aa4e00 | Initial Population | 1 | 0 | mismatch |
| 591c23ea-1ddd-4800-9203-4b6946979818 | Denominator | 1 | 0 | mismatch |
| 591c23ea-1ddd-4800-9203-4b6946979818 | Initial Population | 1 | 0 | mismatch |
| 591c23ea-1ddd-4800-9203-4b6946979818 | Numerator | 1 | 0 | mismatch |
| 593382e8-4ad5-4300-b0ad-26c8954281c6 | Denominator | 1 | 0 | mismatch |
| 593382e8-4ad5-4300-b0ad-26c8954281c6 | Initial Population | 1 | 0 | mismatch |
| 593382e8-4ad5-4300-b0ad-26c8954281c6 | Numerator | 1 | 0 | mismatch |
| 5adc911a-c2a1-475c-a347-9da4ee98c6df | Denominator | 1 | 0 | mismatch |
| 5adc911a-c2a1-475c-a347-9da4ee98c6df | Initial Population | 1 | 0 | mismatch |
| 5adc911a-c2a1-475c-a347-9da4ee98c6df | Numerator | 1 | 0 | mismatch |
| 5aee33a0-e42c-4a79-97b7-40e7ac8b270e | Denominator | 1 | 0 | mismatch |
| 5aee33a0-e42c-4a79-97b7-40e7ac8b270e | Denominator Exception | 1 | 0 | mismatch |
| 5aee33a0-e42c-4a79-97b7-40e7ac8b270e | Initial Population | 1 | 0 | mismatch |
| 65ef54b4-48ea-4fc0-a9a7-79b3be807393 | Denominator | 1 | 0 | mismatch |
| 65ef54b4-48ea-4fc0-a9a7-79b3be807393 | Denominator Exclusion | 1 | 0 | mismatch |
| 65ef54b4-48ea-4fc0-a9a7-79b3be807393 | Initial Population | 1 | 0 | mismatch |
| 6abe0474-e60b-438d-b661-4be178e6b4bd | Denominator | 1 | 0 | mismatch |
| 6abe0474-e60b-438d-b661-4be178e6b4bd | Denominator Exclusion | 1 | 0 | mismatch |
| 6abe0474-e60b-438d-b661-4be178e6b4bd | Initial Population | 1 | 0 | mismatch |
| 6cf51e7c-99f4-4c6d-9b1c-6e371c96b742 | Denominator | 1 | 0 | mismatch |
| 6cf51e7c-99f4-4c6d-9b1c-6e371c96b742 | Initial Population | 1 | 0 | mismatch |
| 6e82e823-f955-43fa-8b8a-b9cd4ae27778 | Denominator | 1 | 0 | mismatch |
| 6e82e823-f955-43fa-8b8a-b9cd4ae27778 | Initial Population | 1 | 0 | mismatch |
| 728a543b-9149-4b2a-9e65-3fb41ce3f35b | Denominator | 1 | 0 | mismatch |
| 728a543b-9149-4b2a-9e65-3fb41ce3f35b | Initial Population | 1 | 0 | mismatch |
| 7b1ac1a8-b7be-41ec-a77f-db545af22263 | Denominator | 1 | 0 | mismatch |
| 7b1ac1a8-b7be-41ec-a77f-db545af22263 | Initial Population | 1 | 0 | mismatch |
| 7b1ac1a8-b7be-41ec-a77f-db545af22263 | Numerator | 1 | 0 | mismatch |
| 7c3ee345-c9da-4ce2-97e8-727de2e5023a | Denominator | 1 | 0 | mismatch |
| 7c3ee345-c9da-4ce2-97e8-727de2e5023a | Denominator Exclusion | 1 | 0 | mismatch |
| 7c3ee345-c9da-4ce2-97e8-727de2e5023a | Initial Population | 1 | 0 | mismatch |
| 7e22eabf-ac1f-4209-a8f6-dcc8b548b71c | Denominator | 1 | 0 | mismatch |
| 7e22eabf-ac1f-4209-a8f6-dcc8b548b71c | Initial Population | 1 | 0 | mismatch |
| 8493a3fb-9501-4aa2-83a3-39fbafa6644c | Denominator | 1 | 0 | mismatch |
| 8493a3fb-9501-4aa2-83a3-39fbafa6644c | Initial Population | 1 | 0 | mismatch |
| 87b7df35-0de4-4c6a-a030-8afac02454f2 | Denominator | 1 | 0 | mismatch |
| 87b7df35-0de4-4c6a-a030-8afac02454f2 | Initial Population | 1 | 0 | mismatch |
| 88c4fed3-bef0-450a-b9ff-d736d4568838 | Denominator | 1 | 0 | mismatch |
| 88c4fed3-bef0-450a-b9ff-d736d4568838 | Initial Population | 1 | 0 | mismatch |
| 8e28076e-2fc9-4170-95e9-a4de9e04fd5e | Denominator | 1 | 0 | mismatch |
| 8e28076e-2fc9-4170-95e9-a4de9e04fd5e | Initial Population | 1 | 0 | mismatch |
| 93459ee6-e397-477e-b7da-250fb75f5974 | Denominator | 1 | 0 | mismatch |
| 93459ee6-e397-477e-b7da-250fb75f5974 | Initial Population | 1 | 0 | mismatch |
| 93459ee6-e397-477e-b7da-250fb75f5974 | Numerator | 1 | 0 | mismatch |
| 964f8143-6ff7-4b80-ad76-4dc59de2af37 | Denominator | 1 | 0 | mismatch |
| 964f8143-6ff7-4b80-ad76-4dc59de2af37 | Initial Population | 1 | 0 | mismatch |
| 999617b0-b41a-4a82-910d-f707ce1d7779 | Denominator | 1 | 0 | mismatch |
| 999617b0-b41a-4a82-910d-f707ce1d7779 | Denominator Exclusion | 1 | 0 | mismatch |
| 999617b0-b41a-4a82-910d-f707ce1d7779 | Initial Population | 1 | 0 | mismatch |
| 9f18a5c2-e59f-4582-91b5-401a86234284 | Denominator | 1 | 0 | mismatch |
| 9f18a5c2-e59f-4582-91b5-401a86234284 | Initial Population | 1 | 0 | mismatch |
| a2b8327c-eaf4-4552-863e-851426e729d4 | Denominator | 1 | 0 | mismatch |
| a2b8327c-eaf4-4552-863e-851426e729d4 | Initial Population | 1 | 0 | mismatch |
| a2b8327c-eaf4-4552-863e-851426e729d4 | Numerator | 1 | 0 | mismatch |
| a7b90108-4f50-4164-87b9-73817e9fdac2 | Denominator | 1 | 0 | mismatch |
| a7b90108-4f50-4164-87b9-73817e9fdac2 | Denominator Exclusion | 1 | 0 | mismatch |
| a7b90108-4f50-4164-87b9-73817e9fdac2 | Initial Population | 1 | 0 | mismatch |
| a86dcf01-3c5f-43ca-a426-c118d5974332 | Denominator | 1 | 0 | mismatch |
| a86dcf01-3c5f-43ca-a426-c118d5974332 | Initial Population | 1 | 0 | mismatch |
| a9c3e62b-fd84-4701-8024-7e3e60af9ed1 | Denominator | 1 | 0 | mismatch |
| a9c3e62b-fd84-4701-8024-7e3e60af9ed1 | Initial Population | 1 | 0 | mismatch |
| a9c3e62b-fd84-4701-8024-7e3e60af9ed1 | Numerator | 1 | 0 | mismatch |
| ac56c496-c5d6-4c23-be20-130ee8327fd2 | Denominator | 1 | 0 | mismatch |
| ac56c496-c5d6-4c23-be20-130ee8327fd2 | Initial Population | 1 | 0 | mismatch |
| ac56c496-c5d6-4c23-be20-130ee8327fd2 | Numerator | 1 | 0 | mismatch |
| ad8c4056-7c25-4dba-a861-ec201afd16fb | Denominator | 1 | 0 | mismatch |
| ad8c4056-7c25-4dba-a861-ec201afd16fb | Denominator Exclusion | 1 | 0 | mismatch |
| ad8c4056-7c25-4dba-a861-ec201afd16fb | Initial Population | 1 | 0 | mismatch |
| b536acae-02c7-4c6e-914b-4ea199d98f79 | Denominator | 1 | 0 | mismatch |
| b536acae-02c7-4c6e-914b-4ea199d98f79 | Denominator Exclusion | 1 | 0 | mismatch |
| b536acae-02c7-4c6e-914b-4ea199d98f79 | Initial Population | 1 | 0 | mismatch |
| b9d52b97-7602-457d-a96d-a1950a01b42a | Denominator | 1 | 0 | mismatch |
| b9d52b97-7602-457d-a96d-a1950a01b42a | Initial Population | 1 | 0 | mismatch |
| ba8bb5f1-966b-4ac1-a311-b2550c0e4858 | Denominator | 1 | 0 | mismatch |
| ba8bb5f1-966b-4ac1-a311-b2550c0e4858 | Denominator Exclusion | 1 | 0 | mismatch |
| ba8bb5f1-966b-4ac1-a311-b2550c0e4858 | Initial Population | 1 | 0 | mismatch |
| c15bee15-84c1-494a-ac82-2159b06da175 | Denominator | 1 | 0 | mismatch |
| c15bee15-84c1-494a-ac82-2159b06da175 | Denominator Exception | 1 | 0 | mismatch |
| c15bee15-84c1-494a-ac82-2159b06da175 | Initial Population | 1 | 0 | mismatch |
| cf0c5672-d86d-47fa-b13b-9bdb299c1d47 | Denominator | 1 | 0 | mismatch |
| cf0c5672-d86d-47fa-b13b-9bdb299c1d47 | Denominator Exclusion | 1 | 0 | mismatch |
| cf0c5672-d86d-47fa-b13b-9bdb299c1d47 | Initial Population | 1 | 0 | mismatch |
| cfe6d907-c9fa-4d4c-9889-803315e8f707 | Denominator | 1 | 0 | mismatch |
| cfe6d907-c9fa-4d4c-9889-803315e8f707 | Denominator Exclusion | 1 | 0 | mismatch |
| cfe6d907-c9fa-4d4c-9889-803315e8f707 | Initial Population | 1 | 0 | mismatch |
| d21be273-87ad-4ab5-a936-9de820872e73 | Denominator | 1 | 0 | mismatch |
| d21be273-87ad-4ab5-a936-9de820872e73 | Initial Population | 1 | 0 | mismatch |
| d8ea50e2-e1a9-41ae-ac73-480bb198d963 | Denominator | 1 | 0 | mismatch |
| d8ea50e2-e1a9-41ae-ac73-480bb198d963 | Initial Population | 1 | 0 | mismatch |
| d8ea50e2-e1a9-41ae-ac73-480bb198d963 | Numerator | 1 | 0 | mismatch |
| db5afa02-02e2-4c0d-88c8-d3c0682333a1 | Denominator | 1 | 0 | mismatch |
| db5afa02-02e2-4c0d-88c8-d3c0682333a1 | Initial Population | 1 | 0 | mismatch |
| dd6c17ad-396b-4ff5-9538-e06da5f0a39c | Denominator | 1 | 0 | mismatch |
| dd6c17ad-396b-4ff5-9538-e06da5f0a39c | Initial Population | 1 | 0 | mismatch |
| e081bee5-67f8-464f-9356-9b287e32a35a | Denominator | 1 | 0 | mismatch |
| e081bee5-67f8-464f-9356-9b287e32a35a | Initial Population | 1 | 0 | mismatch |
| e081bee5-67f8-464f-9356-9b287e32a35a | Numerator | 1 | 0 | mismatch |
| e13ab79b-1b28-4a37-96cc-e63baa5f88cd | Denominator | 1 | 0 | mismatch |
| e13ab79b-1b28-4a37-96cc-e63baa5f88cd | Denominator Exclusion | 1 | 0 | mismatch |
| e13ab79b-1b28-4a37-96cc-e63baa5f88cd | Initial Population | 1 | 0 | mismatch |
| e6f270ed-ddb3-43cf-a2f7-ef26df352d4d | Denominator | 1 | 0 | mismatch |
| e6f270ed-ddb3-43cf-a2f7-ef26df352d4d | Denominator Exclusion | 1 | 0 | mismatch |
| e6f270ed-ddb3-43cf-a2f7-ef26df352d4d | Initial Population | 1 | 0 | mismatch |
| e8e62dbf-ce54-4f04-b2d9-f574b8ded2c4 | Denominator | 1 | 0 | mismatch |
| e8e62dbf-ce54-4f04-b2d9-f574b8ded2c4 | Denominator Exclusion | 1 | 0 | mismatch |
| e8e62dbf-ce54-4f04-b2d9-f574b8ded2c4 | Initial Population | 1 | 0 | mismatch |
| e9074892-9513-48d7-999e-afeace427512 | Denominator | 1 | 0 | mismatch |
| e9074892-9513-48d7-999e-afeace427512 | Initial Population | 1 | 0 | mismatch |
| ea41e48d-7b6d-4c9e-a8a1-f9c4bcf30785 | Denominator | 1 | 0 | mismatch |
| ea41e48d-7b6d-4c9e-a8a1-f9c4bcf30785 | Initial Population | 1 | 0 | mismatch |
| eb5173bb-769a-4c95-b0e9-362a271f72ea | Denominator | 1 | 0 | mismatch |
| eb5173bb-769a-4c95-b0e9-362a271f72ea | Denominator Exclusion | 1 | 0 | mismatch |
| eb5173bb-769a-4c95-b0e9-362a271f72ea | Initial Population | 1 | 0 | mismatch |
| ed5ae1dd-5a2e-4b69-9044-1f4cbed2fcfc | Denominator | 1 | 0 | mismatch |
| ed5ae1dd-5a2e-4b69-9044-1f4cbed2fcfc | Initial Population | 1 | 0 | mismatch |
| f705cc70-0d7d-4dc1-88f7-9b37ab5290d2 | Denominator | 1 | 0 | mismatch |
| f705cc70-0d7d-4dc1-88f7-9b37ab5290d2 | Initial Population | 1 | 0 | mismatch |
| f705cc70-0d7d-4dc1-88f7-9b37ab5290d2 | Numerator | 1 | 0 | mismatch |
| fdd3fe25-b12c-4417-a999-91e4583f6cd4 | Denominator | 1 | 0 | mismatch |
| fdd3fe25-b12c-4417-a999-91e4583f6cd4 | Denominator Exclusion | 1 | 0 | mismatch |
| fdd3fe25-b12c-4417-a999-91e4583f6cd4 | Initial Population | 1 | 0 | mismatch |

### CMS108FHIRVTEProphylaxis

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 068814f1-4270-4e10-b470-9a5433bceb3e | Numerator | 1 | 0 | mismatch |
| 0ddb05b5-03af-4d2a-9d9c-0be8034d1ff4 | Denominator Exclusion | 1 | 0 | mismatch |
| 1b450176-8caa-4133-bc9a-c066969f72ce | Denominator Exclusion | 1 | 0 | mismatch |
| 33d162ce-3bc7-4b0a-8c04-fec0a42a6263 | Numerator | 0 | 1 | mismatch |
| 3db5c5a1-2eec-4e01-8e59-ac389a0a2179 | Numerator | 0 | 1 | mismatch |
| 41f2785f-4c4f-4497-a46b-e17fd8b5ee3f | Denominator Exclusion | 1 | 0 | mismatch |
| 52790be5-0f6e-4ebd-85f5-57f35db8b56b | Numerator | 1 | 0 | mismatch |
| 543248c8-b6af-407d-b435-7e867c4770b4 | Numerator | 1 | 0 | mismatch |
| 5741c41a-04ec-4967-83b2-b0d746bd0ed5 | Numerator | 0 | 1 | mismatch |
| 610c90c9-f387-40f8-9bd7-710d20dfd6f0 | Numerator | 1 | 0 | mismatch |
| 70a5b41a-14ac-4e08-b661-d5523ad80fbf | Denominator Exclusion | 1 | 0 | mismatch |
| 73673965-9b35-446e-bad7-1701991e6906 | Numerator | 1 | 0 | mismatch |
| 77c1bf41-fce8-4044-9eeb-c205b8fdc0a9 | Numerator | 1 | 0 | mismatch |
| 8bb999a1-696a-497b-a5f4-aa55e146a16e | Numerator | 0 | 1 | mismatch |
| 900f47c2-3615-4ffe-a9e0-3e7e70469ffb | Denominator Exclusion | 1 | 0 | mismatch |
| 96c7b8e3-2c28-4b46-a579-f56d644cf762 | Denominator Exclusion | 1 | 0 | mismatch |
| a3e0cca4-bdb1-4972-b6ca-84841cb66859 | Numerator | 1 | 0 | mismatch |
| a8083d97-85af-4e1e-8770-30c49a287194 | Numerator | 1 | 0 | mismatch |
| afad0252-21ef-48ee-9a5a-33dab92d8709 | Denominator Exclusion | 1 | 0 | mismatch |
| cbd1de91-441a-4588-b000-2e589d099ab6 | Numerator | 1 | 0 | mismatch |
| d9b7ffa9-ed78-484c-8880-b4cbf2b4b6a1 | Numerator | 0 | 1 | mismatch |
| dc0dcb01-87f0-4e65-9c36-8cf6174abef1 | Numerator | 0 | 1 | mismatch |
| dd5a1e46-1b99-45a3-b4d3-1fde205d8a11 | Numerator | 0 | 1 | mismatch |
| ea49dc35-7378-4436-aa37-53ec9f13b05d | Numerator | 2 | 1 | mismatch |
| eb754c68-82c7-48cd-a2f0-26ee1cd92544 | Denominator Exclusion | 1 | 0 | mismatch |
| ef0bebdc-61bf-4233-abd3-1f3c99a2cd8d | Numerator | 1 | 0 | mismatch |

### CMS129FHIRProstCaBoneScanUse

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 019de843-7347-4a25-ad9e-2a4ca3a84054 | Initial Population | 1 | 0 | mismatch |
| 056a27fa-04fc-45d6-bf3f-07482f8db4a8 | Initial Population | 1 | 0 | mismatch |
| 0c0256d2-a6d4-4ed3-bd95-ac7d88108b6c | Initial Population | 1 | 0 | mismatch |
| 187cc99d-9cb5-442f-8201-3695e5358101 | Denominator | 1 | 0 | mismatch |
| 187cc99d-9cb5-442f-8201-3695e5358101 | Denominator Exception | 1 | 0 | mismatch |
| 24a5102c-7e6e-4ec6-8433-737b0fe8c854 | Initial Population | 1 | 0 | mismatch |
| 2ae1ab8a-7ef3-407f-a218-d6b304c8c298 | Denominator | 1 | 0 | mismatch |
| 2ae1ab8a-7ef3-407f-a218-d6b304c8c298 | Numerator | 1 | 0 | mismatch |
| 2d132665-a998-4d23-b316-3112cc046a67 | Denominator | 1 | 0 | mismatch |
| 2d132665-a998-4d23-b316-3112cc046a67 | Initial Population | 1 | 0 | mismatch |
| 2fbd48f4-a5c7-4dd9-8318-745f49cc469c | Initial Population | 1 | 0 | mismatch |
| 402afca0-63e5-4ede-9110-fef8edbba947 | Initial Population | 1 | 0 | mismatch |
| 49c6ee4d-15cd-422c-a511-b7ba3d55b1f6 | Denominator | 1 | 0 | mismatch |
| 49c6ee4d-15cd-422c-a511-b7ba3d55b1f6 | Numerator | 1 | 0 | mismatch |
| 4c552455-4056-4466-af06-298b4399c6f7 | Initial Population | 1 | 0 | mismatch |
| 4e7be08e-1f6f-4d44-bdd2-807439ce367a | Denominator | 1 | 0 | mismatch |
| 528c61cc-7733-4dfe-aa51-61652a12b2a9 | Initial Population | 1 | 0 | mismatch |
| 54b4b9a9-00fd-453f-b8c1-61c324fa68da | Denominator | 1 | 0 | mismatch |
| 54b4b9a9-00fd-453f-b8c1-61c324fa68da | Initial Population | 1 | 0 | mismatch |
| 597f0a09-29f9-4141-804a-56a1fca5352e | Initial Population | 1 | 0 | mismatch |
| 5dfa0337-c807-4847-8960-94fe1718ae1d | Denominator | 1 | 0 | mismatch |
| 5dfa0337-c807-4847-8960-94fe1718ae1d | Denominator Exception | 1 | 0 | mismatch |
| 675ba5dd-80e3-4a2b-bd96-0b0ae9df1533 | Denominator | 1 | 0 | mismatch |
| 675ba5dd-80e3-4a2b-bd96-0b0ae9df1533 | Initial Population | 1 | 0 | mismatch |
| 830e69d7-3820-46d6-9222-9d79323d0194 | Initial Population | 1 | 0 | mismatch |
| 84ed0de7-7cfb-4ac3-96a3-3c854eff391a | Denominator | 1 | 0 | mismatch |
| 84ed0de7-7cfb-4ac3-96a3-3c854eff391a | Initial Population | 1 | 0 | mismatch |
| 8559361b-b9c4-4819-88ea-985c27c2ad51 | Denominator | 1 | 0 | mismatch |
| 8d89307b-7ec1-4262-93cc-e1b4ef76e326 | Initial Population | 1 | 0 | mismatch |
| 8fabf398-d258-4613-b8d8-12bcbc273dc8 | Denominator | 1 | 0 | mismatch |
| 8fabf398-d258-4613-b8d8-12bcbc273dc8 | Numerator | 1 | 0 | mismatch |
| 991879e8-a1e3-4014-a71b-7c6bdfbc9748 | Initial Population | 1 | 0 | mismatch |
| 9d5d4ffe-710e-4b5d-b84a-ba4ed2de06dd | Denominator | 1 | 0 | mismatch |
| a20036b7-297c-4adb-a7f4-4762c75f44f5 | Initial Population | 1 | 0 | mismatch |
| a5ee17bd-75d5-4cdc-b2eb-bf22014b7ba6 | Denominator | 1 | 0 | mismatch |
| a5ee17bd-75d5-4cdc-b2eb-bf22014b7ba6 | Initial Population | 1 | 0 | mismatch |
| ab43793d-af43-4960-8b9f-012f23a08822 | Initial Population | 1 | 0 | mismatch |
| b7a34d3f-446e-4a40-adff-595b8614977c | Denominator | 1 | 0 | mismatch |
| b7a34d3f-446e-4a40-adff-595b8614977c | Denominator Exception | 1 | 0 | mismatch |
| ba2a14bb-ba5f-4fd9-b676-ed0808280f23 | Initial Population | 1 | 0 | mismatch |
| bfed65eb-7ece-4d24-8470-4e9803b6b7f7 | Initial Population | 1 | 0 | mismatch |
| c0736fec-a1e6-4d86-b39e-2d2bbca87a09 | Denominator | 1 | 0 | mismatch |
| c0736fec-a1e6-4d86-b39e-2d2bbca87a09 | Initial Population | 1 | 0 | mismatch |
| c123c20a-c9c3-4acd-bead-8cc61e2ba29f | Initial Population | 1 | 0 | mismatch |
| c143148d-84ca-433a-a312-37c3f92b61cc | Initial Population | 1 | 0 | mismatch |
| c3180c8f-a14b-4c8f-8a3f-6f092f3df8c3 | Denominator | 1 | 0 | mismatch |
| c3180c8f-a14b-4c8f-8a3f-6f092f3df8c3 | Initial Population | 1 | 0 | mismatch |
| c55f6f6d-e355-4280-9e5d-d21fc00b5c3e | Denominator | 1 | 0 | mismatch |
| c55f6f6d-e355-4280-9e5d-d21fc00b5c3e | Numerator | 1 | 0 | mismatch |
| d6c602b4-8853-486f-8189-386d08799146 | Initial Population | 1 | 0 | mismatch |
| d8b7ff4e-67bb-480d-b2c1-77db6ab0ef1e | Denominator | 1 | 0 | mismatch |
| d8b7ff4e-67bb-480d-b2c1-77db6ab0ef1e | Numerator | 1 | 0 | mismatch |
| dcdb253e-ebd6-4c16-b632-9a56bcec4541 | Denominator | 1 | 0 | mismatch |
| dcdb253e-ebd6-4c16-b632-9a56bcec4541 | Numerator | 1 | 0 | mismatch |
| e290d85d-647e-4776-975a-0cbd7eaffbf4 | Initial Population | 1 | 0 | mismatch |
| eb735e04-483b-4fa9-ac2d-e73918acd50e | Initial Population | 1 | 0 | mismatch |
| ed027d3a-1e21-4c66-8357-cfe1d05ae918 | Initial Population | 1 | 0 | mismatch |
| eeaa3999-225d-45e8-befa-56076faffc78 | Denominator | 1 | 0 | mismatch |
| ef8cad07-6254-4461-ba1a-86aaf95eeb6e | Denominator | 1 | 0 | mismatch |
| ef8cad07-6254-4461-ba1a-86aaf95eeb6e | Initial Population | 1 | 0 | mismatch |
| f01072cb-c5a0-4c51-a24a-3fa503fe41fb | Denominator | 1 | 0 | mismatch |
| f4f5d8a4-99b7-4ace-af89-75e6af754713 | Denominator | 1 | 0 | mismatch |
| f4f5d8a4-99b7-4ace-af89-75e6af754713 | Initial Population | 1 | 0 | mismatch |
| f561d914-95ab-4519-9b02-b9570ea69d48 | Denominator | 1 | 0 | mismatch |
| f561d914-95ab-4519-9b02-b9570ea69d48 | Numerator | 1 | 0 | mismatch |
| fdadfa9f-9e7b-4d80-a00e-56e8759b47c1 | Denominator | 1 | 0 | mismatch |
| fdadfa9f-9e7b-4d80-a00e-56e8759b47c1 | Initial Population | 1 | 0 | mismatch |

### CMS135FHIRACEIorARBorARNIforHF

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 072be19e-9540-452c-9d7f-03c104cffa97 | Denominator | 1 | 0 | mismatch |
| 072be19e-9540-452c-9d7f-03c104cffa97 | Denominator Exception | 1 | 0 | mismatch |
| 072be19e-9540-452c-9d7f-03c104cffa97 | Initial Population | 1 | 0 | mismatch |
| 149c3a7c-2b80-47f8-b50d-5c1d233eedb7 | Denominator | 1 | 0 | mismatch |
| 149c3a7c-2b80-47f8-b50d-5c1d233eedb7 | Initial Population | 1 | 0 | mismatch |
| 149c3a7c-2b80-47f8-b50d-5c1d233eedb7 | Numerator | 1 | 0 | mismatch |
| 1f64a697-a90b-4aaf-a315-fa84168ac2b4 | Denominator Exception | 0 | 1 | mismatch |
| 1f64a697-a90b-4aaf-a315-fa84168ac2b4 | Numerator | 1 | 0 | mismatch |
| 27111f10-365a-4ec3-a918-f856c687211e | Denominator | 1 | 0 | mismatch |
| 27111f10-365a-4ec3-a918-f856c687211e | Denominator Exclusion | 1 | 0 | mismatch |
| 27111f10-365a-4ec3-a918-f856c687211e | Initial Population | 1 | 0 | mismatch |
| 298d5342-fa0a-4386-bf48-b9c977a1c367 | Denominator | 1 | 0 | mismatch |
| 298d5342-fa0a-4386-bf48-b9c977a1c367 | Initial Population | 1 | 0 | mismatch |
| 298d5342-fa0a-4386-bf48-b9c977a1c367 | Numerator | 1 | 0 | mismatch |
| 2b0d7791-29b6-4f10-b72a-b6a50667770d | Denominator | 1 | 0 | mismatch |
| 2b0d7791-29b6-4f10-b72a-b6a50667770d | Denominator Exclusion | 1 | 0 | mismatch |
| 2b0d7791-29b6-4f10-b72a-b6a50667770d | Initial Population | 1 | 0 | mismatch |
| 3a4004a0-b352-488d-8c18-8fd023308646 | Denominator | 1 | 0 | mismatch |
| 3a4004a0-b352-488d-8c18-8fd023308646 | Denominator Exclusion | 1 | 0 | mismatch |
| 3a4004a0-b352-488d-8c18-8fd023308646 | Initial Population | 1 | 0 | mismatch |
| 3b1c14dc-b18c-40d9-b848-c750ea4ffe5e | Denominator | 1 | 0 | mismatch |
| 3b1c14dc-b18c-40d9-b848-c750ea4ffe5e | Initial Population | 1 | 0 | mismatch |
| 4bc4883f-0770-4a68-824a-5fa4dba72638 | Denominator | 1 | 0 | mismatch |
| 4bc4883f-0770-4a68-824a-5fa4dba72638 | Initial Population | 1 | 0 | mismatch |
| 4bc4883f-0770-4a68-824a-5fa4dba72638 | Numerator | 1 | 0 | mismatch |
| 4cc0d4b2-9c45-4f7d-9dd9-b8588b27e2f1 | Denominator | 1 | 0 | mismatch |
| 4cc0d4b2-9c45-4f7d-9dd9-b8588b27e2f1 | Denominator Exclusion | 1 | 0 | mismatch |
| 4cc0d4b2-9c45-4f7d-9dd9-b8588b27e2f1 | Initial Population | 1 | 0 | mismatch |
| 5b7e720f-e2fc-4779-9b1c-3f34a0241482 | Denominator | 1 | 0 | mismatch |
| 5b7e720f-e2fc-4779-9b1c-3f34a0241482 | Initial Population | 1 | 0 | mismatch |
| 64e76766-9760-4385-a977-cbe8136ce425 | Denominator Exception | 0 | 1 | mismatch |
| 64e76766-9760-4385-a977-cbe8136ce425 | Numerator | 1 | 0 | mismatch |
| 6a86918d-3f69-43c8-8863-1d0bf835a2c7 | Denominator | 1 | 0 | mismatch |
| 6a86918d-3f69-43c8-8863-1d0bf835a2c7 | Denominator Exception | 1 | 0 | mismatch |
| 6a86918d-3f69-43c8-8863-1d0bf835a2c7 | Initial Population | 1 | 0 | mismatch |
| 9709553b-acb2-4267-a57f-ae9d51e82659 | Denominator | 1 | 0 | mismatch |
| 9709553b-acb2-4267-a57f-ae9d51e82659 | Denominator Exception | 1 | 0 | mismatch |
| 9709553b-acb2-4267-a57f-ae9d51e82659 | Initial Population | 1 | 0 | mismatch |
| a40966e2-5640-409f-9596-65c03db31424 | Denominator | 1 | 0 | mismatch |
| a40966e2-5640-409f-9596-65c03db31424 | Initial Population | 1 | 0 | mismatch |
| a486908c-84c0-42af-96d2-efe65453833f | Denominator | 1 | 0 | mismatch |
| a486908c-84c0-42af-96d2-efe65453833f | Denominator Exception | 1 | 0 | mismatch |
| a486908c-84c0-42af-96d2-efe65453833f | Initial Population | 1 | 0 | mismatch |
| a995bd9f-dec6-44fd-9b1f-458509443b91 | Denominator | 1 | 0 | mismatch |
| a995bd9f-dec6-44fd-9b1f-458509443b91 | Denominator Exclusion | 1 | 0 | mismatch |
| a995bd9f-dec6-44fd-9b1f-458509443b91 | Initial Population | 1 | 0 | mismatch |
| ab7f5ace-9ac6-4077-bee9-444abeac4f51 | Denominator | 1 | 0 | mismatch |
| ab7f5ace-9ac6-4077-bee9-444abeac4f51 | Denominator Exception | 1 | 0 | mismatch |
| ab7f5ace-9ac6-4077-bee9-444abeac4f51 | Initial Population | 1 | 0 | mismatch |
| ad471f87-60ee-48b9-95b8-80f35eb230dd | Denominator | 1 | 0 | mismatch |
| ad471f87-60ee-48b9-95b8-80f35eb230dd | Initial Population | 1 | 0 | mismatch |
| b2abcd36-fabd-4d6e-93c4-2dd430f4969e | Denominator | 1 | 0 | mismatch |
| b2abcd36-fabd-4d6e-93c4-2dd430f4969e | Initial Population | 1 | 0 | mismatch |
| b6db1aac-34f4-4883-a728-ba988a26c3de | Denominator | 1 | 0 | mismatch |
| b6db1aac-34f4-4883-a728-ba988a26c3de | Denominator Exception | 1 | 0 | mismatch |
| b6db1aac-34f4-4883-a728-ba988a26c3de | Initial Population | 1 | 0 | mismatch |
| b7d36e19-a42e-4b09-a83f-4525cd7de100 | Denominator | 1 | 0 | mismatch |
| b7d36e19-a42e-4b09-a83f-4525cd7de100 | Denominator Exclusion | 1 | 0 | mismatch |
| b7d36e19-a42e-4b09-a83f-4525cd7de100 | Initial Population | 1 | 0 | mismatch |
| d18e37a6-7b66-4e7c-b305-692872c13f8d | Denominator | 1 | 0 | mismatch |
| d18e37a6-7b66-4e7c-b305-692872c13f8d | Initial Population | 1 | 0 | mismatch |
| d297e68e-3f02-42a8-a59f-a5a4cecbd47d | Denominator Exception | 0 | 1 | mismatch |
| d297e68e-3f02-42a8-a59f-a5a4cecbd47d | Numerator | 1 | 0 | mismatch |
| d7067d60-b722-4bba-b78e-682d86c0b48e | Denominator | 1 | 0 | mismatch |
| d7067d60-b722-4bba-b78e-682d86c0b48e | Denominator Exception | 1 | 0 | mismatch |
| d7067d60-b722-4bba-b78e-682d86c0b48e | Initial Population | 1 | 0 | mismatch |
| e1182b17-1292-4473-bd9e-c70e3846ca0b | Denominator | 1 | 0 | mismatch |
| e1182b17-1292-4473-bd9e-c70e3846ca0b | Denominator Exclusion | 1 | 0 | mismatch |
| e1182b17-1292-4473-bd9e-c70e3846ca0b | Initial Population | 1 | 0 | mismatch |
| ed36c619-3ef8-44ca-86fa-1993920d76ef | Denominator | 1 | 0 | mismatch |
| ed36c619-3ef8-44ca-86fa-1993920d76ef | Denominator Exception | 1 | 0 | mismatch |
| ed36c619-3ef8-44ca-86fa-1993920d76ef | Initial Population | 1 | 0 | mismatch |
| ed37e316-c721-4a98-91d7-862f5ece7187 | Denominator | 1 | 0 | mismatch |
| ed37e316-c721-4a98-91d7-862f5ece7187 | Denominator Exclusion | 1 | 0 | mismatch |
| ed37e316-c721-4a98-91d7-862f5ece7187 | Initial Population | 1 | 0 | mismatch |
| ef395d2c-85a3-41a2-b6cb-37c4ff5217dc | Denominator | 1 | 0 | mismatch |
| ef395d2c-85a3-41a2-b6cb-37c4ff5217dc | Denominator Exclusion | 1 | 0 | mismatch |
| ef395d2c-85a3-41a2-b6cb-37c4ff5217dc | Initial Population | 1 | 0 | mismatch |
| fb38d345-4723-46b4-a269-8b4fd8982bc7 | Denominator | 1 | 0 | mismatch |
| fb38d345-4723-46b4-a269-8b4fd8982bc7 | Denominator Exception | 1 | 0 | mismatch |
| fb38d345-4723-46b4-a269-8b4fd8982bc7 | Initial Population | 1 | 0 | mismatch |

### CMS144FHIRHFBetaBlockerForLVSD

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 00cd231f-4460-4d84-8e04-d8b0a04d8afd | Denominator | 1 | 0 | mismatch |
| 00cd231f-4460-4d84-8e04-d8b0a04d8afd | Denominator Exception | 1 | 0 | mismatch |
| 00cd231f-4460-4d84-8e04-d8b0a04d8afd | Initial Population | 1 | 0 | mismatch |
| 07efd4bb-b45d-4bfd-aeb2-08de49742d91 | Denominator | 1 | 0 | mismatch |
| 07efd4bb-b45d-4bfd-aeb2-08de49742d91 | Initial Population | 1 | 0 | mismatch |
| 07efd4bb-b45d-4bfd-aeb2-08de49742d91 | Numerator | 1 | 0 | mismatch |
| 0d2e80d0-d70a-4eba-9bfe-23d1dfeb546e | Denominator | 1 | 0 | mismatch |
| 0d2e80d0-d70a-4eba-9bfe-23d1dfeb546e | Denominator Exception | 1 | 0 | mismatch |
| 0d2e80d0-d70a-4eba-9bfe-23d1dfeb546e | Initial Population | 1 | 0 | mismatch |
| 1d166b9e-0d11-4247-a9a8-f610f36a74f2 | Denominator | 1 | 0 | mismatch |
| 1d166b9e-0d11-4247-a9a8-f610f36a74f2 | Denominator Exception | 1 | 0 | mismatch |
| 1d166b9e-0d11-4247-a9a8-f610f36a74f2 | Initial Population | 1 | 0 | mismatch |
| 2333a56a-8346-4afc-93a2-0626a9a1ac2d | Denominator | 1 | 0 | mismatch |
| 2333a56a-8346-4afc-93a2-0626a9a1ac2d | Denominator Exception | 1 | 0 | mismatch |
| 2333a56a-8346-4afc-93a2-0626a9a1ac2d | Initial Population | 1 | 0 | mismatch |
| 23f874f8-e7e6-4bee-aa0c-140afae4ac16 | Denominator | 1 | 0 | mismatch |
| 23f874f8-e7e6-4bee-aa0c-140afae4ac16 | Denominator Exception | 1 | 0 | mismatch |
| 23f874f8-e7e6-4bee-aa0c-140afae4ac16 | Initial Population | 1 | 0 | mismatch |
| 29effff8-4b34-43a2-8619-c69f02688f41 | Denominator | 1 | 0 | mismatch |
| 29effff8-4b34-43a2-8619-c69f02688f41 | Denominator Exception | 1 | 0 | mismatch |
| 29effff8-4b34-43a2-8619-c69f02688f41 | Initial Population | 1 | 0 | mismatch |
| 2f695158-3d0a-42ac-b747-dc355ed01e94 | Denominator | 1 | 0 | mismatch |
| 2f695158-3d0a-42ac-b747-dc355ed01e94 | Initial Population | 1 | 0 | mismatch |
| 37e926e5-d9d4-4066-b991-e9b3d58fd8ca | Denominator | 1 | 0 | mismatch |
| 37e926e5-d9d4-4066-b991-e9b3d58fd8ca | Initial Population | 1 | 0 | mismatch |
| 426369f7-d9ec-4923-8628-465229c5f958 | Denominator | 1 | 0 | mismatch |
| 426369f7-d9ec-4923-8628-465229c5f958 | Initial Population | 1 | 0 | mismatch |
| 426369f7-d9ec-4923-8628-465229c5f958 | Numerator | 1 | 0 | mismatch |
| 442a40d5-a18e-434b-ad30-7760e73adac2 | Denominator | 1 | 0 | mismatch |
| 442a40d5-a18e-434b-ad30-7760e73adac2 | Initial Population | 1 | 0 | mismatch |
| 4d13ba5c-740b-4263-ae55-3a40a3f73846 | Denominator | 1 | 0 | mismatch |
| 4d13ba5c-740b-4263-ae55-3a40a3f73846 | Denominator Exception | 1 | 0 | mismatch |
| 4d13ba5c-740b-4263-ae55-3a40a3f73846 | Initial Population | 1 | 0 | mismatch |
| 559a2377-0faa-44f8-bf81-bbf3df4d3edf | Denominator | 1 | 0 | mismatch |
| 559a2377-0faa-44f8-bf81-bbf3df4d3edf | Initial Population | 1 | 0 | mismatch |
| 5914457a-816c-43fe-86da-07fcc42ebe61 | Denominator | 1 | 0 | mismatch |
| 5914457a-816c-43fe-86da-07fcc42ebe61 | Initial Population | 1 | 0 | mismatch |
| 5adba588-db7d-4921-be95-4fb872be59de | Denominator | 1 | 0 | mismatch |
| 5adba588-db7d-4921-be95-4fb872be59de | Initial Population | 1 | 0 | mismatch |
| 5e965079-8e53-4011-b4e8-cb3829257450 | Denominator | 1 | 0 | mismatch |
| 5e965079-8e53-4011-b4e8-cb3829257450 | Denominator Exception | 1 | 0 | mismatch |
| 5e965079-8e53-4011-b4e8-cb3829257450 | Initial Population | 1 | 0 | mismatch |
| 66ef197c-3807-41cf-acd0-f188836e2762 | Denominator | 1 | 0 | mismatch |
| 66ef197c-3807-41cf-acd0-f188836e2762 | Initial Population | 1 | 0 | mismatch |
| 67779bc6-07ee-42cf-8ca7-e71302915dba | Denominator | 1 | 0 | mismatch |
| 67779bc6-07ee-42cf-8ca7-e71302915dba | Initial Population | 1 | 0 | mismatch |
| 67779bc6-07ee-42cf-8ca7-e71302915dba | Numerator | 1 | 0 | mismatch |
| 704612ca-2d86-4f6b-88d6-d8f3e9cb031f | Initial Population | 1 | 0 | mismatch |
| 71dd3794-ce93-473b-8d90-9d68c0dac6d8 | Denominator | 1 | 0 | mismatch |
| 71dd3794-ce93-473b-8d90-9d68c0dac6d8 | Initial Population | 1 | 0 | mismatch |
| 729c5ee9-d55f-47f2-aee1-d8d9fb25fe38 | Denominator | 1 | 0 | mismatch |
| 729c5ee9-d55f-47f2-aee1-d8d9fb25fe38 | Initial Population | 1 | 0 | mismatch |
| 798bb898-32dc-4f83-b700-792efe0a65db | Denominator | 1 | 0 | mismatch |
| 798bb898-32dc-4f83-b700-792efe0a65db | Initial Population | 1 | 0 | mismatch |
| 7b8885c5-ad14-4361-9755-c76a6e3b8530 | Denominator | 1 | 0 | mismatch |
| 7b8885c5-ad14-4361-9755-c76a6e3b8530 | Initial Population | 1 | 0 | mismatch |
| 7b8885c5-ad14-4361-9755-c76a6e3b8530 | Numerator | 1 | 0 | mismatch |
| 7fc4e721-68e5-4ed8-ab41-b5fb0d1ff43a | Denominator | 1 | 0 | mismatch |
| 7fc4e721-68e5-4ed8-ab41-b5fb0d1ff43a | Initial Population | 1 | 0 | mismatch |
| 84b7f540-8e30-41e8-9691-f9ccd6924e0d | Denominator | 1 | 0 | mismatch |
| 84b7f540-8e30-41e8-9691-f9ccd6924e0d | Denominator Exclusion | 1 | 0 | mismatch |
| 84b7f540-8e30-41e8-9691-f9ccd6924e0d | Initial Population | 1 | 0 | mismatch |
| 8c717fb3-e820-44c6-b4ef-e9a7f46cd85b | Denominator | 1 | 0 | mismatch |
| 8c717fb3-e820-44c6-b4ef-e9a7f46cd85b | Denominator Exception | 1 | 0 | mismatch |
| 8c717fb3-e820-44c6-b4ef-e9a7f46cd85b | Initial Population | 1 | 0 | mismatch |
| 9038ae82-f60c-467a-94b4-76e644e707e3 | Denominator | 1 | 0 | mismatch |
| 9038ae82-f60c-467a-94b4-76e644e707e3 | Denominator Exception | 1 | 0 | mismatch |
| 9038ae82-f60c-467a-94b4-76e644e707e3 | Initial Population | 1 | 0 | mismatch |
| 9d197057-c39b-4799-ab3a-b81df7a6199b | Denominator | 1 | 0 | mismatch |
| 9d197057-c39b-4799-ab3a-b81df7a6199b | Denominator Exclusion | 1 | 0 | mismatch |
| 9d197057-c39b-4799-ab3a-b81df7a6199b | Initial Population | 1 | 0 | mismatch |
| a245856e-f7bf-46a8-8742-f84d87c6d5ef | Denominator | 1 | 0 | mismatch |
| a245856e-f7bf-46a8-8742-f84d87c6d5ef | Initial Population | 1 | 0 | mismatch |
| a45954f4-f511-4409-8d7a-6ad606d1e5e2 | Denominator | 1 | 0 | mismatch |
| a45954f4-f511-4409-8d7a-6ad606d1e5e2 | Denominator Exception | 1 | 0 | mismatch |
| a45954f4-f511-4409-8d7a-6ad606d1e5e2 | Initial Population | 1 | 0 | mismatch |
| a6c8dee0-0110-4814-ae0d-37a2a03b2f72 | Denominator | 1 | 0 | mismatch |
| a6c8dee0-0110-4814-ae0d-37a2a03b2f72 | Denominator Exception | 1 | 0 | mismatch |
| a6c8dee0-0110-4814-ae0d-37a2a03b2f72 | Initial Population | 1 | 0 | mismatch |
| b6995e3c-319c-45f1-8aca-4db81b6730f6 | Initial Population | 1 | 0 | mismatch |
| b7060d53-0742-484c-a9b9-3ff63fbb8d5c | Denominator | 1 | 0 | mismatch |
| b7060d53-0742-484c-a9b9-3ff63fbb8d5c | Denominator Exception | 1 | 0 | mismatch |
| b7060d53-0742-484c-a9b9-3ff63fbb8d5c | Initial Population | 1 | 0 | mismatch |
| b74609f2-2fe7-4969-8b2b-215e7c21dfce | Initial Population | 1 | 0 | mismatch |
| bc710459-b3f7-4a3b-a640-3bf541fea336 | Denominator | 1 | 0 | mismatch |
| bc710459-b3f7-4a3b-a640-3bf541fea336 | Denominator Exclusion | 1 | 0 | mismatch |
| bc710459-b3f7-4a3b-a640-3bf541fea336 | Initial Population | 1 | 0 | mismatch |
| c58e088d-0b16-4e3d-aaad-3577cdec75d3 | Denominator | 1 | 0 | mismatch |
| c58e088d-0b16-4e3d-aaad-3577cdec75d3 | Initial Population | 1 | 0 | mismatch |
| cdcbc425-203d-4ebb-81f3-8d5f50253eed | Denominator | 1 | 0 | mismatch |
| cdcbc425-203d-4ebb-81f3-8d5f50253eed | Denominator Exception | 1 | 0 | mismatch |
| cdcbc425-203d-4ebb-81f3-8d5f50253eed | Initial Population | 1 | 0 | mismatch |
| ce22375e-abe0-4fd5-a4ad-07d8a6056045 | Denominator | 1 | 0 | mismatch |
| ce22375e-abe0-4fd5-a4ad-07d8a6056045 | Denominator Exclusion | 1 | 0 | mismatch |
| ce22375e-abe0-4fd5-a4ad-07d8a6056045 | Initial Population | 1 | 0 | mismatch |
| de470536-41fa-4b2f-8355-2790e90ece22 | Denominator | 1 | 0 | mismatch |
| de470536-41fa-4b2f-8355-2790e90ece22 | Denominator Exclusion | 1 | 0 | mismatch |
| de470536-41fa-4b2f-8355-2790e90ece22 | Initial Population | 1 | 0 | mismatch |
| ec0aa07d-f1b1-4c5d-a9f3-e5bbdd77cdfa | Denominator | 1 | 0 | mismatch |
| ec0aa07d-f1b1-4c5d-a9f3-e5bbdd77cdfa | Denominator Exclusion | 1 | 0 | mismatch |
| ec0aa07d-f1b1-4c5d-a9f3-e5bbdd77cdfa | Initial Population | 1 | 0 | mismatch |
| f00cb244-a7cc-408f-ab47-7c3ac82469b1 | Denominator | 1 | 0 | mismatch |
| f00cb244-a7cc-408f-ab47-7c3ac82469b1 | Denominator Exception | 1 | 0 | mismatch |
| f00cb244-a7cc-408f-ab47-7c3ac82469b1 | Initial Population | 1 | 0 | mismatch |
| f5c049d5-d1b8-413b-bdad-ff54a5d20df3 | Denominator | 1 | 0 | mismatch |
| f5c049d5-d1b8-413b-bdad-ff54a5d20df3 | Initial Population | 1 | 0 | mismatch |
| f6cdc95d-7796-4dad-a5da-d2e1fddc8687 | Denominator | 1 | 0 | mismatch |
| f6cdc95d-7796-4dad-a5da-d2e1fddc8687 | Denominator Exclusion | 1 | 0 | mismatch |
| f6cdc95d-7796-4dad-a5da-d2e1fddc8687 | Initial Population | 1 | 0 | mismatch |
| faf3e31d-533a-484f-9b9d-4ded0c32c84f | Denominator | 1 | 0 | mismatch |
| faf3e31d-533a-484f-9b9d-4ded0c32c84f | Denominator Exclusion | 1 | 0 | mismatch |
| faf3e31d-533a-484f-9b9d-4ded0c32c84f | Initial Population | 1 | 0 | mismatch |

### CMS145FHIRCADBBlockerTPMIorLVSD

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 10a6f006-eabc-4a40-80bd-2a135e45e597 | Initial Population | 1 | 0 | mismatch |
| 10a6f006-eabc-4a40-80bd-2a135e45e597 | Denominator | 1 | 0 | mismatch |
| 10a6f006-eabc-4a40-80bd-2a135e45e597 | Denominator Exception | 1 | 0 | mismatch |
| 10a6f006-eabc-4a40-80bd-2a135e45e597 | Initial Population | 1 | 0 | mismatch |
| 12fda733-0b5d-4f3d-93b2-59c35ba85898 | Denominator | 1 | 0 | mismatch |
| 12fda733-0b5d-4f3d-93b2-59c35ba85898 | Numerator | 1 | 0 | mismatch |
| 13ce1b7d-3af6-4ef3-a4ae-6cd7c3075be8 | Denominator | 1 | 0 | mismatch |
| 13ce1b7d-3af6-4ef3-a4ae-6cd7c3075be8 | Initial Population | 1 | 0 | mismatch |
| 13ce1b7d-3af6-4ef3-a4ae-6cd7c3075be8 | Initial Population | 1 | 0 | mismatch |
| 1bbf669f-a5f6-4dde-a8b2-1be100394e18 | Initial Population | 1 | 0 | mismatch |
| 1bbf669f-a5f6-4dde-a8b2-1be100394e18 | Denominator | 1 | 0 | mismatch |
| 1bbf669f-a5f6-4dde-a8b2-1be100394e18 | Denominator Exception | 1 | 0 | mismatch |
| 1bbf669f-a5f6-4dde-a8b2-1be100394e18 | Initial Population | 1 | 0 | mismatch |
| 1f58a561-bf40-43dd-89a3-8d590013ee1c | Initial Population | 1 | 0 | mismatch |
| 1f58a561-bf40-43dd-89a3-8d590013ee1c | Denominator | 1 | 0 | mismatch |
| 1f58a561-bf40-43dd-89a3-8d590013ee1c | Denominator Exception | 1 | 0 | mismatch |
| 1f58a561-bf40-43dd-89a3-8d590013ee1c | Initial Population | 1 | 0 | mismatch |
| 1f70822b-c513-4c3a-8162-49f0bb9c914b | Initial Population | 1 | 0 | mismatch |
| 1f70822b-c513-4c3a-8162-49f0bb9c914b | Denominator | 1 | 0 | mismatch |
| 1f70822b-c513-4c3a-8162-49f0bb9c914b | Denominator Exception | 1 | 0 | mismatch |
| 1f70822b-c513-4c3a-8162-49f0bb9c914b | Initial Population | 1 | 0 | mismatch |
| 2dbbdad5-c2dd-42a4-9f6d-f8815bc89a0a | Initial Population | 1 | 0 | mismatch |
| 2dbbdad5-c2dd-42a4-9f6d-f8815bc89a0a | Denominator | 1 | 0 | mismatch |
| 2dbbdad5-c2dd-42a4-9f6d-f8815bc89a0a | Denominator Exception | 1 | 0 | mismatch |
| 2dbbdad5-c2dd-42a4-9f6d-f8815bc89a0a | Initial Population | 1 | 0 | mismatch |
| 314e78ef-a347-41bb-bce3-4b4ed18999ad | Denominator | 1 | 0 | mismatch |
| 314e78ef-a347-41bb-bce3-4b4ed18999ad | Denominator Exception | 1 | 0 | mismatch |
| 314e78ef-a347-41bb-bce3-4b4ed18999ad | Initial Population | 1 | 0 | mismatch |
| 314e78ef-a347-41bb-bce3-4b4ed18999ad | Initial Population | 1 | 0 | mismatch |
| 40500bf1-592a-4afd-8b0d-cc1fe3947190 | Initial Population | 1 | 0 | mismatch |
| 40500bf1-592a-4afd-8b0d-cc1fe3947190 | Initial Population | 1 | 0 | mismatch |
| 40b4796d-dba8-4232-8cb4-8daf5d13a554 | Denominator | 1 | 0 | mismatch |
| 40b4796d-dba8-4232-8cb4-8daf5d13a554 | Initial Population | 1 | 0 | mismatch |
| 40b4796d-dba8-4232-8cb4-8daf5d13a554 | Initial Population | 1 | 0 | mismatch |
| 4a3086cd-63f3-41c3-8ce9-f75b4b18b85c | Denominator | 1 | 0 | mismatch |
| 4e2ad83d-ea31-45e7-92cb-07fe319aaf38 | Initial Population | 1 | 0 | mismatch |
| 4e2ad83d-ea31-45e7-92cb-07fe319aaf38 | Denominator | 1 | 0 | mismatch |
| 4e2ad83d-ea31-45e7-92cb-07fe319aaf38 | Initial Population | 1 | 0 | mismatch |
| 4e9c7cf3-c1ae-4110-9ead-86fadfbbf3e4 | Initial Population | 1 | 0 | mismatch |
| 4e9c7cf3-c1ae-4110-9ead-86fadfbbf3e4 | Denominator | 1 | 0 | mismatch |
| 4e9c7cf3-c1ae-4110-9ead-86fadfbbf3e4 | Initial Population | 1 | 0 | mismatch |
| 4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e | Initial Population | 1 | 0 | mismatch |
| 4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e | Denominator | 1 | 0 | mismatch |
| 4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e | Initial Population | 1 | 0 | mismatch |
| 5a85d133-acfb-40fa-a3ab-0c557d21e67c | Denominator | 1 | 0 | mismatch |
| 5a85d133-acfb-40fa-a3ab-0c557d21e67c | Denominator Exception | 1 | 0 | mismatch |
| 5a85d133-acfb-40fa-a3ab-0c557d21e67c | Initial Population | 1 | 0 | mismatch |
| 5a85d133-acfb-40fa-a3ab-0c557d21e67c | Initial Population | 1 | 0 | mismatch |
| 5fd0d626-e9c5-4e6c-a10d-1a1183fa7702 | Denominator | 1 | 0 | mismatch |
| 5fd0d626-e9c5-4e6c-a10d-1a1183fa7702 | Initial Population | 1 | 0 | mismatch |
| 5fd0d626-e9c5-4e6c-a10d-1a1183fa7702 | Initial Population | 1 | 0 | mismatch |
| 61306767-0e74-44b8-ac06-1339c3783355 | Denominator | 1 | 0 | mismatch |
| 61306767-0e74-44b8-ac06-1339c3783355 | Initial Population | 1 | 0 | mismatch |
| 61306767-0e74-44b8-ac06-1339c3783355 | Initial Population | 1 | 0 | mismatch |
| 641aaa7a-d227-475a-a648-9c048522b62f | Initial Population | 1 | 0 | mismatch |
| 641aaa7a-d227-475a-a648-9c048522b62f | Denominator | 1 | 0 | mismatch |
| 641aaa7a-d227-475a-a648-9c048522b62f | Initial Population | 1 | 0 | mismatch |
| 68c4dacc-cb3c-423b-9fde-899dfd05b5f6 | Initial Population | 1 | 0 | mismatch |
| 68c4dacc-cb3c-423b-9fde-899dfd05b5f6 | Denominator | 1 | 0 | mismatch |
| 68c4dacc-cb3c-423b-9fde-899dfd05b5f6 | Initial Population | 1 | 0 | mismatch |
| 6919fd23-aa69-4a1a-a07e-4752cdc53f46 | Initial Population | 1 | 0 | mismatch |
| 6919fd23-aa69-4a1a-a07e-4752cdc53f46 | Denominator | 1 | 0 | mismatch |
| 6919fd23-aa69-4a1a-a07e-4752cdc53f46 | Denominator Exception | 1 | 0 | mismatch |
| 6919fd23-aa69-4a1a-a07e-4752cdc53f46 | Initial Population | 1 | 0 | mismatch |
| 69f6fe89-2983-4d35-a12a-b1ae92cc9c2f | Initial Population | 1 | 0 | mismatch |
| 69f6fe89-2983-4d35-a12a-b1ae92cc9c2f | Initial Population | 1 | 0 | mismatch |
| 6cf089cd-0c1a-4e4f-b8d9-11c0db757c4b | Initial Population | 1 | 0 | mismatch |
| 6cf089cd-0c1a-4e4f-b8d9-11c0db757c4b | Denominator | 1 | 0 | mismatch |
| 6cf089cd-0c1a-4e4f-b8d9-11c0db757c4b | Denominator Exception | 1 | 0 | mismatch |
| 6cf089cd-0c1a-4e4f-b8d9-11c0db757c4b | Initial Population | 1 | 0 | mismatch |
| 7111ea79-9b4a-4452-86cf-22b8852bc5eb | Denominator | 1 | 0 | mismatch |
| 7111ea79-9b4a-4452-86cf-22b8852bc5eb | Denominator Exception | 1 | 0 | mismatch |
| 7111ea79-9b4a-4452-86cf-22b8852bc5eb | Initial Population | 1 | 0 | mismatch |
| 7111ea79-9b4a-4452-86cf-22b8852bc5eb | Initial Population | 1 | 0 | mismatch |
| 72845b60-dc5c-4a93-8b86-2caca155cd9c | Initial Population | 1 | 0 | mismatch |
| 72845b60-dc5c-4a93-8b86-2caca155cd9c | Initial Population | 1 | 0 | mismatch |
| 75fc7993-6b07-49f8-a8aa-747dfcbb3d8f | Initial Population | 1 | 0 | mismatch |
| 75fc7993-6b07-49f8-a8aa-747dfcbb3d8f | Denominator | 1 | 0 | mismatch |
| 75fc7993-6b07-49f8-a8aa-747dfcbb3d8f | Denominator Exception | 1 | 0 | mismatch |
| 75fc7993-6b07-49f8-a8aa-747dfcbb3d8f | Initial Population | 1 | 0 | mismatch |
| 79e4e453-f52d-41a1-a86b-4229a89733c4 | Denominator | 1 | 0 | mismatch |
| 79e4e453-f52d-41a1-a86b-4229a89733c4 | Initial Population | 1 | 0 | mismatch |
| 79e4e453-f52d-41a1-a86b-4229a89733c4 | Initial Population | 1 | 0 | mismatch |
| 7fb4cb03-569b-4c0a-8898-abe49d609c0e | Initial Population | 1 | 0 | mismatch |
| 7fb4cb03-569b-4c0a-8898-abe49d609c0e | Denominator | 1 | 0 | mismatch |
| 7fb4cb03-569b-4c0a-8898-abe49d609c0e | Initial Population | 1 | 0 | mismatch |
| 8fc3dac2-816b-4ef2-b14c-85264bb3f2c4 | Initial Population | 1 | 0 | mismatch |
| 8fc3dac2-816b-4ef2-b14c-85264bb3f2c4 | Denominator | 1 | 0 | mismatch |
| 8fc3dac2-816b-4ef2-b14c-85264bb3f2c4 | Initial Population | 1 | 0 | mismatch |
| 9377a6c4-cb7a-4e87-a8b2-c598a5b0e2df | Denominator | 1 | 0 | mismatch |
| 9377a6c4-cb7a-4e87-a8b2-c598a5b0e2df | Denominator Exception | 1 | 0 | mismatch |
| 9377a6c4-cb7a-4e87-a8b2-c598a5b0e2df | Initial Population | 1 | 0 | mismatch |
| 9377a6c4-cb7a-4e87-a8b2-c598a5b0e2df | Initial Population | 1 | 0 | mismatch |
| 9cc50dc3-f081-4ab6-bf6d-cbfabb5a2e89 | Initial Population | 1 | 0 | mismatch |
| 9cc50dc3-f081-4ab6-bf6d-cbfabb5a2e89 | Initial Population | 1 | 0 | mismatch |
| a579d7e2-37c4-47ea-937d-524bba26ba2f | Denominator | 1 | 0 | mismatch |
| a579d7e2-37c4-47ea-937d-524bba26ba2f | Denominator Exception | 1 | 0 | mismatch |
| a579d7e2-37c4-47ea-937d-524bba26ba2f | Initial Population | 1 | 0 | mismatch |
| a579d7e2-37c4-47ea-937d-524bba26ba2f | Initial Population | 1 | 0 | mismatch |
| a994dd9f-d7b0-441d-8358-1287d75836de | Initial Population | 1 | 0 | mismatch |
| a994dd9f-d7b0-441d-8358-1287d75836de | Denominator | 1 | 0 | mismatch |
| a994dd9f-d7b0-441d-8358-1287d75836de | Denominator Exception | 1 | 0 | mismatch |
| a994dd9f-d7b0-441d-8358-1287d75836de | Initial Population | 1 | 0 | mismatch |
| aafbe467-9603-443a-8397-d5c337ebbdc6 | Denominator | 1 | 0 | mismatch |
| aafbe467-9603-443a-8397-d5c337ebbdc6 | Denominator Exception | 1 | 0 | mismatch |
| aafbe467-9603-443a-8397-d5c337ebbdc6 | Initial Population | 1 | 0 | mismatch |
| aafbe467-9603-443a-8397-d5c337ebbdc6 | Initial Population | 1 | 0 | mismatch |
| b05ea6cd-9b35-4798-b15d-edc8593c2d33 | Denominator | 1 | 0 | mismatch |
| b05ea6cd-9b35-4798-b15d-edc8593c2d33 | Denominator Exception | 1 | 0 | mismatch |
| b05ea6cd-9b35-4798-b15d-edc8593c2d33 | Initial Population | 1 | 0 | mismatch |
| b05ea6cd-9b35-4798-b15d-edc8593c2d33 | Initial Population | 1 | 0 | mismatch |
| b19af44d-fb1c-4e78-a0dd-170c6b61718f | Initial Population | 1 | 0 | mismatch |
| b19af44d-fb1c-4e78-a0dd-170c6b61718f | Denominator | 1 | 0 | mismatch |
| b19af44d-fb1c-4e78-a0dd-170c6b61718f | Denominator Exception | 1 | 0 | mismatch |
| b19af44d-fb1c-4e78-a0dd-170c6b61718f | Initial Population | 1 | 0 | mismatch |
| b65680a0-9768-4ce4-b08d-972fcd84e28e | Initial Population | 1 | 0 | mismatch |
| b65680a0-9768-4ce4-b08d-972fcd84e28e | Denominator | 1 | 0 | mismatch |
| b65680a0-9768-4ce4-b08d-972fcd84e28e | Initial Population | 1 | 0 | mismatch |
| bfacfb96-e784-4346-bf29-a20359dc0ba5 | Denominator | 1 | 0 | mismatch |
| bfacfb96-e784-4346-bf29-a20359dc0ba5 | Denominator Exception | 1 | 0 | mismatch |
| bfacfb96-e784-4346-bf29-a20359dc0ba5 | Initial Population | 1 | 0 | mismatch |
| bfacfb96-e784-4346-bf29-a20359dc0ba5 | Initial Population | 1 | 0 | mismatch |
| c9a8b77a-d6fb-4c6d-a5cb-6c71bcf6baeb | Initial Population | 1 | 0 | mismatch |
| c9a8b77a-d6fb-4c6d-a5cb-6c71bcf6baeb | Denominator | 1 | 0 | mismatch |
| c9a8b77a-d6fb-4c6d-a5cb-6c71bcf6baeb | Initial Population | 1 | 0 | mismatch |
| d1b682be-c26e-44cc-8ac4-18c81760a4a3 | Denominator | 1 | 0 | mismatch |
| d1b682be-c26e-44cc-8ac4-18c81760a4a3 | Initial Population | 1 | 0 | mismatch |
| d1b682be-c26e-44cc-8ac4-18c81760a4a3 | Initial Population | 1 | 0 | mismatch |
| d280cb74-871e-4191-b06a-4b700959a974 | Denominator | 1 | 0 | mismatch |
| dad1f2c7-c45e-4287-9abf-e7e6ff90f2ff | Denominator | 1 | 0 | mismatch |
| dad1f2c7-c45e-4287-9abf-e7e6ff90f2ff | Initial Population | 1 | 0 | mismatch |
| dad1f2c7-c45e-4287-9abf-e7e6ff90f2ff | Numerator | 1 | 0 | mismatch |
| dad1f2c7-c45e-4287-9abf-e7e6ff90f2ff | Initial Population | 1 | 0 | mismatch |
| dbb4d4b0-06a5-4354-9dbc-072c01709a3a | Initial Population | 1 | 0 | mismatch |
| dbb4d4b0-06a5-4354-9dbc-072c01709a3a | Denominator | 1 | 0 | mismatch |
| dbb4d4b0-06a5-4354-9dbc-072c01709a3a | Denominator Exception | 1 | 0 | mismatch |
| dbb4d4b0-06a5-4354-9dbc-072c01709a3a | Initial Population | 1 | 0 | mismatch |
| dd4e465a-3796-4d5d-af53-3e2ab1e4041b | Denominator | 1 | 0 | mismatch |
| dd4e465a-3796-4d5d-af53-3e2ab1e4041b | Denominator Exception | 1 | 0 | mismatch |
| e64fd85a-d643-423d-9e3f-d4838e181aa9 | Initial Population | 1 | 0 | mismatch |
| e64fd85a-d643-423d-9e3f-d4838e181aa9 | Denominator | 1 | 0 | mismatch |
| e64fd85a-d643-423d-9e3f-d4838e181aa9 | Initial Population | 1 | 0 | mismatch |
| e64fd85a-d643-423d-9e3f-d4838e181aa9 | Numerator | 1 | 0 | mismatch |
| f929563b-a5a0-4d55-9613-d75d383d3c6a | Denominator Exception | 1 | 0 | mismatch |
| f9a24bdb-a835-4851-8e50-eb11a5c9fb50 | Initial Population | 1 | 0 | mismatch |
| f9a24bdb-a835-4851-8e50-eb11a5c9fb50 | Denominator | 1 | 0 | mismatch |
| f9a24bdb-a835-4851-8e50-eb11a5c9fb50 | Denominator Exception | 1 | 0 | mismatch |
| f9a24bdb-a835-4851-8e50-eb11a5c9fb50 | Initial Population | 1 | 0 | mismatch |
| fd5fb311-a466-4c59-966d-48fa7aa88931 | Denominator | 1 | 0 | mismatch |
| fd5fb311-a466-4c59-966d-48fa7aa88931 | Initial Population | 1 | 0 | mismatch |
| fd5fb311-a466-4c59-966d-48fa7aa88931 | Initial Population | 1 | 0 | mismatch |
| fdbc9919-4ab0-446a-8c0b-29ac687d8d3b | Denominator | 1 | 0 | mismatch |
| fdbc9919-4ab0-446a-8c0b-29ac687d8d3b | Denominator Exception | 1 | 0 | mismatch |
| fdbc9919-4ab0-446a-8c0b-29ac687d8d3b | Initial Population | 1 | 0 | mismatch |
| fdbc9919-4ab0-446a-8c0b-29ac687d8d3b | Initial Population | 1 | 0 | mismatch |
| ffab1562-be09-428c-9e96-e30868867493 | Initial Population | 1 | 0 | mismatch |
| ffab1562-be09-428c-9e96-e30868867493 | Denominator | 1 | 0 | mismatch |
| ffab1562-be09-428c-9e96-e30868867493 | Initial Population | 1 | 0 | mismatch |

### CMS149FHIRDementiaCognitiveAssess

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0405033f-c6a4-4619-93da-14c9c5613d7b | Denominator | 1 | 0 | mismatch |
| 0405033f-c6a4-4619-93da-14c9c5613d7b | Initial Population | 1 | 0 | mismatch |
| 0405033f-c6a4-4619-93da-14c9c5613d7b | Numerator | 1 | 0 | mismatch |
| 04c67cc9-bf23-4f31-988c-8bac7e96f938 | Denominator | 1 | 0 | mismatch |
| 04c67cc9-bf23-4f31-988c-8bac7e96f938 | Initial Population | 1 | 0 | mismatch |
| 051c9480-438e-48d5-b91f-5f8f980b1f8b | Denominator | 1 | 0 | mismatch |
| 051c9480-438e-48d5-b91f-5f8f980b1f8b | Initial Population | 1 | 0 | mismatch |
| 67e19058-917d-43f8-98d3-d16730fc7d32 | Denominator | 1 | 0 | mismatch |
| 67e19058-917d-43f8-98d3-d16730fc7d32 | Initial Population | 1 | 0 | mismatch |
| 980e3550-6c75-4c4d-a64d-0657107e7cec | Denominator | 1 | 0 | mismatch |
| 980e3550-6c75-4c4d-a64d-0657107e7cec | Initial Population | 1 | 0 | mismatch |
| 99f28510-d75f-48d3-9f36-69739bc27419 | Denominator | 1 | 0 | mismatch |
| 99f28510-d75f-48d3-9f36-69739bc27419 | Initial Population | 1 | 0 | mismatch |
| 99f28510-d75f-48d3-9f36-69739bc27419 | Numerator | 1 | 0 | mismatch |
| e00c927a-f454-4611-97b2-e3e2bdfed182 | Denominator | 1 | 0 | mismatch |
| e00c927a-f454-4611-97b2-e3e2bdfed182 | Initial Population | 1 | 0 | mismatch |
| e1e5ecba-2f9f-41c6-9bd2-2a1bc26a0273 | Denominator | 1 | 0 | mismatch |
| e1e5ecba-2f9f-41c6-9bd2-2a1bc26a0273 | Initial Population | 1 | 0 | mismatch |
| fd115ded-69a6-4766-bdd9-d6364347401e | Denominator | 1 | 0 | mismatch |
| fd115ded-69a6-4766-bdd9-d6364347401e | Initial Population | 1 | 0 | mismatch |
| fd115ded-69a6-4766-bdd9-d6364347401e | Numerator | 1 | 0 | mismatch |

### CMS177FHIRChildMDDSuicideAssmt

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 85e6225c-a9bb-4338-a228-297564e38c4d | Denominator | 1 | 0 | mismatch |
| 85e6225c-a9bb-4338-a228-297564e38c4d | Initial Population | 1 | 0 | mismatch |

### CMS190FHIRVTEProphylaxisICU

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 002fcec4-89ed-42be-a7b7-41fa0acddf8c | Numerator | 1 | 0 | mismatch |
| 0d48b3b3-c5f0-4955-810d-eb4f1b8714ca | Numerator | 1 | 0 | mismatch |
| 208cb0f9-a6e9-4207-b6a4-3325fb463099 | Numerator | 0 | 1 | mismatch |
| 24068f4d-3179-4f2f-872e-02e3b362cac0 | Denominator Exclusion | 1 | 0 | mismatch |
| 25265601-4e27-4fe0-8d35-68ebac8f9894 | Numerator | 1 | 0 | mismatch |
| 298f3f92-5047-46fc-ad92-9f27887ffc55 | Numerator | 1 | 0 | mismatch |
| 2e1ee160-9c41-4c6f-b368-56c074cfb592 | Denominator Exclusion | 1 | 0 | mismatch |
| 324eb2e7-2beb-4c84-88ff-bdf9fa5d8171 | Numerator | 1 | 0 | mismatch |
| 4c32b73b-abba-431b-a352-f0f454e7c9dd | Numerator | 0 | 1 | mismatch |
| 4d76c150-b6a5-4a1a-8801-81c24ce398b7 | Numerator | 1 | 0 | mismatch |
| 4fc421c7-e490-4d4e-a326-53d08635efb9 | Numerator | 0 | 1 | mismatch |
| 632831b0-1ebf-47b5-b439-3a124cd77c37 | Numerator | 0 | 1 | mismatch |
| 67df96df-45ff-4387-bc4d-04055a95483f | Numerator | 1 | 0 | mismatch |
| 7e7f4563-a628-40ab-990b-ca0837313759 | Numerator | 0 | 1 | mismatch |
| 8de031c4-05db-42db-b0e5-902696c6e495 | Denominator Exclusion | 1 | 0 | mismatch |
| 95a54d01-197e-48ef-bb48-d3d398aecbe8 | Numerator | 0 | 1 | mismatch |
| 99fbf673-e18e-43db-bccf-6edc67fcb13f | Numerator | 1 | 0 | mismatch |
| 9ddea16c-55d3-4dda-a1d8-a256fbff0b64 | Numerator | 0 | 1 | mismatch |
| a2563ca7-fda7-4cfd-a5cf-a688225749e2 | Numerator | 1 | 0 | mismatch |
| a82cd0c1-900e-4ab3-a498-840ac1608486 | Denominator Exclusion | 1 | 0 | mismatch |
| a9c75661-be1c-41b2-aa15-222cc7d2ca81 | Numerator | 0 | 1 | mismatch |
| c0481b47-738b-4a09-8901-915ece2beb7e | Numerator | 0 | 1 | mismatch |
| d5d8e66a-01da-462b-95b6-e47b16285b1f | Denominator Exclusion | 1 | 0 | mismatch |
| d665c40d-2323-471f-9642-983472d2be7b | Denominator Exclusion | 1 | 0 | mismatch |
| d70dcaf9-4e69-4395-9612-ee1e156f6184 | Numerator | 2 | 1 | mismatch |
| e8931859-4ad8-49c8-9cdd-8697293456a2 | Numerator | 0 | 1 | mismatch |
| efad4991-4382-4ceb-8bc4-32e8ac088e47 | Numerator | 1 | 0 | mismatch |
| f00f3778-6ad1-466d-a3bd-bcbc63d62b55 | Numerator | 0 | 1 | mismatch |
| f028ed8f-f83b-4c77-bb08-2c4df7694a12 | Denominator Exclusion | 1 | 0 | mismatch |
| f035a977-30d0-487c-b542-a596e718420c | Numerator | 0 | 1 | mismatch |
| f82746cf-f6cd-4fcc-bc9e-7e569ae26211 | Numerator | 0 | 1 | mismatch |
| f859dd94-f201-4517-a368-32b98dd486c9 | Numerator | 0 | 1 | mismatch |
| f8f09c1f-37fc-4088-a39d-710971b626b7 | Denominator Exclusion | 1 | 0 | mismatch |

### CMS0334FHIRPCCesareanBirth

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 912e076d-3b5d-46cc-b2cb-c78172b295a3 | Initial Population | 1 | 0 | mismatch |

### CMS347FHIRStatinPreventionTxCVD

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0045ec92-0b70-4961-8a7c-41b5c43d53a1 | Denominator Exception | 1 | 0 | mismatch |
| 022c05d8-8337-4f1a-9d69-abb6500b1be5 | Denominator | 1 | 0 | mismatch |
| 022c05d8-8337-4f1a-9d69-abb6500b1be5 | Initial Population | 1 | 0 | mismatch |
| 031e746c-9c2c-4eea-acca-a26c8862c9d5 | Denominator | 1 | 0 | mismatch |
| 031e746c-9c2c-4eea-acca-a26c8862c9d5 | Initial Population | 1 | 0 | mismatch |
| 0438e6ec-b6c0-422d-b8c9-074e5f8d9af5 | Denominator Exclusion | 1 | 0 | mismatch |
| 06f036ce-62f0-4807-88d2-f3f8e70d2f31 | Denominator Exception | 1 | 0 | mismatch |
| 078ef6a8-509f-4f36-98f3-977174636356 | Denominator | 1 | 0 | mismatch |
| 078ef6a8-509f-4f36-98f3-977174636356 | Initial Population | 1 | 0 | mismatch |
| 078ef6a8-509f-4f36-98f3-977174636356 | Numerator | 1 | 0 | mismatch |
| 08a2c605-1316-4d3c-b26e-2b40a28a2e44 | Denominator | 1 | 0 | mismatch |
| 08a2c605-1316-4d3c-b26e-2b40a28a2e44 | Initial Population | 1 | 0 | mismatch |
| 08a2c605-1316-4d3c-b26e-2b40a28a2e44 | Numerator | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 0 | 1 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Numerator | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 1 | 0 | mismatch |
| 08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a | Denominator | 1 | 0 | mismatch |
| 08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a | Initial Population | 1 | 0 | mismatch |
| 08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a | Numerator | 1 | 0 | mismatch |
| 0aaae01e-d3b0-4b76-abf8-a044fd4f5d80 | Denominator Exception | 1 | 0 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Denominator | 1 | 0 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Initial Population | 1 | 0 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Numerator | 1 | 0 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Denominator Exception | 1 | 0 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Denominator Exception | 1 | 0 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Denominator Exception | 1 | 0 | mismatch |
| 0ce81150-5908-49a1-bef9-21406359af63 | Denominator Exception | 1 | 0 | mismatch |
| 0ce81150-5908-49a1-bef9-21406359af63 | Denominator Exception | 0 | 1 | mismatch |
| 0ce81150-5908-49a1-bef9-21406359af63 | Numerator | 1 | 0 | mismatch |
| 0ce81150-5908-49a1-bef9-21406359af63 | Denominator Exception | 1 | 0 | mismatch |
| 0ce81150-5908-49a1-bef9-21406359af63 | Denominator Exception | 1 | 0 | mismatch |
| 0e334f85-c298-401d-95ab-bad7ae13ced8 | Denominator | 1 | 0 | mismatch |
| 0e334f85-c298-401d-95ab-bad7ae13ced8 | Initial Population | 1 | 0 | mismatch |
| 0e334f85-c298-401d-95ab-bad7ae13ced8 | Numerator | 1 | 0 | mismatch |
| 0f204e98-0782-43a3-ae53-b516cc8d5797 | Denominator Exception | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Initial Population | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Numerator | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator Exception | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator Exception | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator Exception | 1 | 0 | mismatch |
| 1051c571-b7e4-48d1-8e77-02b1da164b73 | Denominator | 1 | 0 | mismatch |
| 1051c571-b7e4-48d1-8e77-02b1da164b73 | Initial Population | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 0 | 1 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Numerator | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 1 | 0 | mismatch |
| 13d790be-84c6-438c-b571-842698654db7 | Denominator | 1 | 0 | mismatch |
| 13d790be-84c6-438c-b571-842698654db7 | Initial Population | 1 | 0 | mismatch |
| 13d790be-84c6-438c-b571-842698654db7 | Numerator | 1 | 0 | mismatch |
| 15d7fcaa-773f-4888-8b13-bc077cbfdf4a | Denominator | 1 | 0 | mismatch |
| 15d7fcaa-773f-4888-8b13-bc077cbfdf4a | Denominator Exception | 1 | 0 | mismatch |
| 15d7fcaa-773f-4888-8b13-bc077cbfdf4a | Initial Population | 1 | 0 | mismatch |
| 16082e3d-b9c6-4823-87cf-d079e65f073f | Denominator | 1 | 0 | mismatch |
| 16082e3d-b9c6-4823-87cf-d079e65f073f | Denominator Exception | 1 | 0 | mismatch |
| 16082e3d-b9c6-4823-87cf-d079e65f073f | Initial Population | 1 | 0 | mismatch |
| 184b56d3-9ebd-4802-8e3b-cdaa95a5f50a | Denominator Exception | 1 | 0 | mismatch |
| 1ba7b147-b701-424c-bade-4e8270547030 | Denominator Exception | 1 | 0 | mismatch |
| 1d3021bb-b593-4efc-af5b-320243bbe9b7 | Denominator | 1 | 0 | mismatch |
| 1d3021bb-b593-4efc-af5b-320243bbe9b7 | Denominator Exception | 1 | 0 | mismatch |
| 1d3021bb-b593-4efc-af5b-320243bbe9b7 | Initial Population | 1 | 0 | mismatch |
| 20922873-db29-4914-a413-eed415e4504b | Denominator | 1 | 0 | mismatch |
| 20922873-db29-4914-a413-eed415e4504b | Initial Population | 1 | 0 | mismatch |
| 20922873-db29-4914-a413-eed415e4504b | Numerator | 1 | 0 | mismatch |
| 231a16e4-7d60-4e2c-943b-2f4c98994808 | Denominator | 1 | 0 | mismatch |
| 231a16e4-7d60-4e2c-943b-2f4c98994808 | Initial Population | 1 | 0 | mismatch |
| 231a16e4-7d60-4e2c-943b-2f4c98994808 | Numerator | 1 | 0 | mismatch |
| 24acf4a1-d67f-4584-9b6b-6c8025ffcc0a | Denominator | 1 | 0 | mismatch |
| 24acf4a1-d67f-4584-9b6b-6c8025ffcc0a | Initial Population | 1 | 0 | mismatch |
| 24acf4a1-d67f-4584-9b6b-6c8025ffcc0a | Numerator | 1 | 0 | mismatch |
| 26101306-010f-48c5-aa83-8a94f280f755 | Denominator Exception | 1 | 0 | mismatch |
| 2727681a-5857-4de1-a892-0cd4e531541c | Denominator | 1 | 0 | mismatch |
| 2727681a-5857-4de1-a892-0cd4e531541c | Denominator Exclusion | 1 | 0 | mismatch |
| 2727681a-5857-4de1-a892-0cd4e531541c | Initial Population | 1 | 0 | mismatch |
| 2c5a09d4-18c9-4128-86fb-bd49871f9231 | Denominator | 1 | 0 | mismatch |
| 2c5a09d4-18c9-4128-86fb-bd49871f9231 | Denominator Exception | 1 | 0 | mismatch |
| 2c5a09d4-18c9-4128-86fb-bd49871f9231 | Initial Population | 1 | 0 | mismatch |
| 2cff757c-4470-46a2-a685-6e23cf82c045 | Numerator | 1 | 0 | mismatch |
| 3137d292-5094-49ef-82da-d9809b599030 | Denominator | 1 | 0 | mismatch |
| 3137d292-5094-49ef-82da-d9809b599030 | Denominator Exception | 1 | 0 | mismatch |
| 3137d292-5094-49ef-82da-d9809b599030 | Initial Population | 1 | 0 | mismatch |
| 35999af4-f52b-4e73-8f05-4bfca8dee7ec | Denominator | 1 | 0 | mismatch |
| 35999af4-f52b-4e73-8f05-4bfca8dee7ec | Initial Population | 1 | 0 | mismatch |
| 35d9e119-50ef-4df1-b303-f348596657ad | Denominator | 1 | 0 | mismatch |
| 35d9e119-50ef-4df1-b303-f348596657ad | Initial Population | 1 | 0 | mismatch |
| 35d9e119-50ef-4df1-b303-f348596657ad | Numerator | 1 | 0 | mismatch |
| 36408f0f-58eb-47fe-8e64-1b98e47e5c36 | Denominator | 1 | 0 | mismatch |
| 36408f0f-58eb-47fe-8e64-1b98e47e5c36 | Initial Population | 1 | 0 | mismatch |
| 38aac591-8983-4d7c-b29e-c8d145e7ffaa | Denominator | 1 | 0 | mismatch |
| 38aac591-8983-4d7c-b29e-c8d145e7ffaa | Denominator Exception | 1 | 0 | mismatch |
| 38aac591-8983-4d7c-b29e-c8d145e7ffaa | Initial Population | 1 | 0 | mismatch |
| 39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f | Denominator | 1 | 0 | mismatch |
| 39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f | Initial Population | 1 | 0 | mismatch |
| 39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f | Numerator | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Initial Population | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Numerator | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 1 | 0 | mismatch |
| 3c4aa676-8ef0-415c-a71e-09289d57cbfa | Denominator Exception | 1 | 0 | mismatch |
| 3e09af44-0445-4077-b73c-6896fdbe49c5 | Denominator | 1 | 0 | mismatch |
| 3e09af44-0445-4077-b73c-6896fdbe49c5 | Denominator Exception | 1 | 0 | mismatch |
| 3e09af44-0445-4077-b73c-6896fdbe49c5 | Initial Population | 1 | 0 | mismatch |
| 40aa228f-ff55-4653-8bbe-125dc0fb5983 | Denominator Exclusion | 1 | 0 | mismatch |
| 4120512a-d0f4-4ffa-acd9-0191db3b7f46 | Denominator | 1 | 0 | mismatch |
| 4120512a-d0f4-4ffa-acd9-0191db3b7f46 | Denominator Exception | 1 | 0 | mismatch |
| 4120512a-d0f4-4ffa-acd9-0191db3b7f46 | Initial Population | 1 | 0 | mismatch |
| 4c8a6a20-c5cc-496b-950f-68d6997bf4d1 | Denominator Exception | 1 | 0 | mismatch |
| 4d6fb0e2-636d-426f-802b-5ecb4f059440 | Denominator Exception | 1 | 0 | mismatch |
| 4d6fb0e2-636d-426f-802b-5ecb4f059440 | Denominator Exception | 1 | 0 | mismatch |
| 4d6fb0e2-636d-426f-802b-5ecb4f059440 | Denominator Exception | 1 | 0 | mismatch |
| 4d6fb0e2-636d-426f-802b-5ecb4f059440 | Denominator Exception | 0 | 1 | mismatch |
| 4d6fb0e2-636d-426f-802b-5ecb4f059440 | Numerator | 1 | 0 | mismatch |
| 4e72d245-e401-4be7-a743-84ab6a842871 | Denominator Exception | 1 | 0 | mismatch |
| 4e72d245-e401-4be7-a743-84ab6a842871 | Denominator Exception | 0 | 1 | mismatch |
| 4e72d245-e401-4be7-a743-84ab6a842871 | Numerator | 1 | 0 | mismatch |
| 4e72d245-e401-4be7-a743-84ab6a842871 | Denominator Exception | 1 | 0 | mismatch |
| 4e72d245-e401-4be7-a743-84ab6a842871 | Denominator Exception | 1 | 0 | mismatch |
| 4ea5e47c-48de-4f1f-a7bb-499753983f9b | Denominator | 1 | 0 | mismatch |
| 4ea5e47c-48de-4f1f-a7bb-499753983f9b | Initial Population | 1 | 0 | mismatch |
| 50c7b2fc-879b-4088-88bf-36a9f8c0baf0 | Denominator Exception | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator Exception | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Initial Population | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Numerator | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator Exception | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator Exception | 1 | 0 | mismatch |
| 5355d1bc-f8b4-4063-945a-0717e9530281 | Denominator | 1 | 0 | mismatch |
| 5355d1bc-f8b4-4063-945a-0717e9530281 | Initial Population | 1 | 0 | mismatch |
| 5355d1bc-f8b4-4063-945a-0717e9530281 | Numerator | 1 | 0 | mismatch |
| 537d14db-6ced-4cd2-9553-e88bd6551771 | Denominator | 1 | 0 | mismatch |
| 537d14db-6ced-4cd2-9553-e88bd6551771 | Denominator Exception | 1 | 0 | mismatch |
| 537d14db-6ced-4cd2-9553-e88bd6551771 | Initial Population | 1 | 0 | mismatch |
| 59715b85-2d66-4627-ad73-d91e5862cb5b | Denominator Exclusion | 1 | 0 | mismatch |
| 59d6bb14-b82e-4295-baf1-d96be73e1e38 | Denominator | 1 | 0 | mismatch |
| 59d6bb14-b82e-4295-baf1-d96be73e1e38 | Denominator Exception | 1 | 0 | mismatch |
| 59d6bb14-b82e-4295-baf1-d96be73e1e38 | Initial Population | 1 | 0 | mismatch |
| 5b37b5a5-0e28-4b28-9889-8878d41ff9cf | Denominator | 1 | 0 | mismatch |
| 5b37b5a5-0e28-4b28-9889-8878d41ff9cf | Denominator Exception | 1 | 0 | mismatch |
| 5b37b5a5-0e28-4b28-9889-8878d41ff9cf | Initial Population | 1 | 0 | mismatch |
| 5bbad8cc-56b9-4802-a5da-7de376a461f0 | Denominator | 1 | 0 | mismatch |
| 5bbad8cc-56b9-4802-a5da-7de376a461f0 | Initial Population | 1 | 0 | mismatch |
| 5cebab0f-d32e-4adc-bef3-90812d6c5819 | Denominator Exception | 1 | 0 | mismatch |
| 5d926cc3-70dc-4c82-9513-d39f01765baf | Denominator | 1 | 0 | mismatch |
| 5d926cc3-70dc-4c82-9513-d39f01765baf | Denominator Exception | 1 | 0 | mismatch |
| 5d926cc3-70dc-4c82-9513-d39f01765baf | Initial Population | 1 | 0 | mismatch |
| 65c0a8c4-c562-4f73-a534-d7f7a976e42f | Denominator | 1 | 0 | mismatch |
| 65c0a8c4-c562-4f73-a534-d7f7a976e42f | Denominator Exception | 1 | 0 | mismatch |
| 65c0a8c4-c562-4f73-a534-d7f7a976e42f | Initial Population | 1 | 0 | mismatch |
| 694248de-4f73-4557-816b-f6a932f15793 | Denominator | 1 | 0 | mismatch |
| 694248de-4f73-4557-816b-f6a932f15793 | Initial Population | 1 | 0 | mismatch |
| 694248de-4f73-4557-816b-f6a932f15793 | Numerator | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Initial Population | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Numerator | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 1 | 0 | mismatch |
| 759a89b4-51ed-4622-adae-6b0930701ebb | Denominator | 1 | 0 | mismatch |
| 759a89b4-51ed-4622-adae-6b0930701ebb | Denominator Exception | 1 | 0 | mismatch |
| 759a89b4-51ed-4622-adae-6b0930701ebb | Initial Population | 1 | 0 | mismatch |
| 7a273d18-942a-40d1-9bf6-a12275337aae | Denominator | 1 | 0 | mismatch |
| 7a273d18-942a-40d1-9bf6-a12275337aae | Denominator Exception | 1 | 0 | mismatch |
| 7a273d18-942a-40d1-9bf6-a12275337aae | Initial Population | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 0 | 1 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Numerator | 1 | 0 | mismatch |
| 7bc28f33-e1e6-4122-8a38-e9c36685a6ba | Denominator | 1 | 0 | mismatch |
| 7bc28f33-e1e6-4122-8a38-e9c36685a6ba | Denominator Exclusion | 1 | 0 | mismatch |
| 7bc28f33-e1e6-4122-8a38-e9c36685a6ba | Initial Population | 1 | 0 | mismatch |
| 821087e5-a030-49ac-95b5-5b9ab38e88da | Denominator | 1 | 0 | mismatch |
| 821087e5-a030-49ac-95b5-5b9ab38e88da | Denominator Exception | 1 | 0 | mismatch |
| 821087e5-a030-49ac-95b5-5b9ab38e88da | Initial Population | 1 | 0 | mismatch |
| 86bacb29-41c3-4ea8-8e4b-3e13c075e557 | Denominator | 1 | 0 | mismatch |
| 86bacb29-41c3-4ea8-8e4b-3e13c075e557 | Denominator Exception | 1 | 0 | mismatch |
| 86bacb29-41c3-4ea8-8e4b-3e13c075e557 | Initial Population | 1 | 0 | mismatch |
| 87b32275-37d7-4adf-afa4-8a4518964de0 | Denominator | 1 | 0 | mismatch |
| 87b32275-37d7-4adf-afa4-8a4518964de0 | Initial Population | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Initial Population | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Numerator | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 1 | 0 | mismatch |
| 8c357499-cb9a-41c9-9060-1bbbefb0fd7e | Denominator Exception | 1 | 0 | mismatch |
| 93aea3e2-4736-4be0-830f-54c1ef6df6d5 | Denominator | 1 | 0 | mismatch |
| 93aea3e2-4736-4be0-830f-54c1ef6df6d5 | Denominator Exception | 1 | 0 | mismatch |
| 93aea3e2-4736-4be0-830f-54c1ef6df6d5 | Initial Population | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 0 | 1 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Numerator | 1 | 0 | mismatch |
| 95fad34f-db86-4e4a-a8a2-42a3b7ac15dc | Denominator Exception | 1 | 0 | mismatch |
| 9933efe1-3258-4c1b-8162-258f85831467 | Denominator Exception | 1 | 0 | mismatch |
| 99ab4b63-b8b7-432c-91a6-fb38ba7203dd | Denominator | 1 | 0 | mismatch |
| 99ab4b63-b8b7-432c-91a6-fb38ba7203dd | Denominator Exception | 1 | 0 | mismatch |
| 99ab4b63-b8b7-432c-91a6-fb38ba7203dd | Initial Population | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Initial Population | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Numerator | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 1 | 0 | mismatch |
| 9c2afd42-581e-418b-9eaa-3ddf4918c9ac | Denominator Exception | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 0 | 1 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Numerator | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 1 | 0 | mismatch |
| a0202aaf-756f-4d08-8329-8fd585ddda63 | Denominator | 1 | 0 | mismatch |
| a0202aaf-756f-4d08-8329-8fd585ddda63 | Initial Population | 1 | 0 | mismatch |
| a03e2988-3bed-4fc5-b1e7-70eac99f0612 | Denominator | 1 | 0 | mismatch |
| a03e2988-3bed-4fc5-b1e7-70eac99f0612 | Initial Population | 1 | 0 | mismatch |
| a3169726-4d3d-4a3f-8175-67fd795191a5 | Denominator Exception | 1 | 0 | mismatch |
| a779556b-5041-4d85-9c5f-9af223961ff2 | Denominator Exception | 1 | 0 | mismatch |
| a7f7eb97-a44f-4394-bff6-0485ae59bc9e | Denominator Exception | 1 | 0 | mismatch |
| afd6fd51-72ff-41fa-9cec-7591ab6f5a51 | Denominator Exception | 1 | 0 | mismatch |
| b35ba523-abea-4848-8dac-256c1727447c | Denominator Exception | 1 | 0 | mismatch |
| b708e603-c09f-4798-9631-4603653c1380 | Denominator | 1 | 0 | mismatch |
| b708e603-c09f-4798-9631-4603653c1380 | Initial Population | 1 | 0 | mismatch |
| be29ff82-9191-4b5f-91ca-cc5590fea905 | Denominator Exception | 1 | 0 | mismatch |
| bf38398e-4c04-4808-af1c-ea0c86b44d45 | Denominator | 1 | 0 | mismatch |
| bf38398e-4c04-4808-af1c-ea0c86b44d45 | Denominator Exception | 1 | 0 | mismatch |
| bf38398e-4c04-4808-af1c-ea0c86b44d45 | Initial Population | 1 | 0 | mismatch |
| bfb8c317-cc95-41cc-9d3d-e1e66dd5b168 | Denominator | 1 | 0 | mismatch |
| bfb8c317-cc95-41cc-9d3d-e1e66dd5b168 | Denominator Exclusion | 1 | 0 | mismatch |
| bfb8c317-cc95-41cc-9d3d-e1e66dd5b168 | Initial Population | 1 | 0 | mismatch |
| c00d7354-2160-48f4-a251-1fcf892d1b42 | Denominator | 1 | 0 | mismatch |
| c00d7354-2160-48f4-a251-1fcf892d1b42 | Initial Population | 1 | 0 | mismatch |
| c00d7354-2160-48f4-a251-1fcf892d1b42 | Numerator | 1 | 0 | mismatch |
| c686053c-d4b7-45b7-9ebb-19080a24f031 | Denominator | 1 | 0 | mismatch |
| c686053c-d4b7-45b7-9ebb-19080a24f031 | Denominator Exclusion | 1 | 0 | mismatch |
| c686053c-d4b7-45b7-9ebb-19080a24f031 | Initial Population | 1 | 0 | mismatch |
| c77c84ce-f0a9-4949-a8d7-4413565db083 | Denominator | 1 | 0 | mismatch |
| c77c84ce-f0a9-4949-a8d7-4413565db083 | Initial Population | 1 | 0 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Denominator | 1 | 0 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Initial Population | 1 | 0 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Numerator | 1 | 0 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Denominator Exception | 1 | 0 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Denominator Exception | 1 | 0 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Denominator Exception | 1 | 0 | mismatch |
| cbc1d484-f7a2-43f3-b091-e362d9bb770e | Denominator Exception | 1 | 0 | mismatch |
| cc9d23e6-2322-4b1c-8a8b-29f6f92c89f1 | Denominator Exception | 1 | 0 | mismatch |
| cf1d9246-dbe4-4c59-a955-e2301e37732b | Denominator | 1 | 0 | mismatch |
| cf1d9246-dbe4-4c59-a955-e2301e37732b | Denominator Exception | 1 | 0 | mismatch |
| cf1d9246-dbe4-4c59-a955-e2301e37732b | Initial Population | 1 | 0 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Denominator | 1 | 0 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Initial Population | 1 | 0 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Numerator | 1 | 0 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Denominator Exception | 1 | 0 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Denominator Exception | 1 | 0 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Denominator Exception | 1 | 0 | mismatch |
| d2c7d463-775a-4c8d-bcb0-35ea689b2d20 | Denominator Exception | 1 | 0 | mismatch |
| d2c7d463-775a-4c8d-bcb0-35ea689b2d20 | Denominator Exception | 1 | 0 | mismatch |
| d2c7d463-775a-4c8d-bcb0-35ea689b2d20 | Denominator Exception | 1 | 0 | mismatch |
| d2c7d463-775a-4c8d-bcb0-35ea689b2d20 | Denominator Exception | 0 | 1 | mismatch |
| d2c7d463-775a-4c8d-bcb0-35ea689b2d20 | Numerator | 1 | 0 | mismatch |
| d3a48d69-2269-472a-9c27-da2c658e8c68 | Denominator | 1 | 0 | mismatch |
| d3a48d69-2269-472a-9c27-da2c658e8c68 | Denominator Exception | 1 | 0 | mismatch |
| d3a48d69-2269-472a-9c27-da2c658e8c68 | Initial Population | 1 | 0 | mismatch |
| d9d151d1-9bd3-40ce-a2c1-cb8a985328fc | Denominator | 1 | 0 | mismatch |
| d9d151d1-9bd3-40ce-a2c1-cb8a985328fc | Initial Population | 1 | 0 | mismatch |
| d9f94b3d-5bba-4965-8364-1d7c87957c3e | Denominator | 1 | 0 | mismatch |
| d9f94b3d-5bba-4965-8364-1d7c87957c3e | Initial Population | 1 | 0 | mismatch |
| da34c14f-672e-4f66-827b-3eb08d97559e | Denominator Exception | 1 | 0 | mismatch |
| da5f94c9-9d0c-42ea-ab7f-dd3a92a04ceb | Denominator | 1 | 0 | mismatch |
| da5f94c9-9d0c-42ea-ab7f-dd3a92a04ceb | Initial Population | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Initial Population | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Numerator | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 1 | 0 | mismatch |
| df05b853-3e6d-4a12-b1db-fd9d0ec790a2 | Denominator | 1 | 0 | mismatch |
| df05b853-3e6d-4a12-b1db-fd9d0ec790a2 | Denominator Exception | 1 | 0 | mismatch |
| df05b853-3e6d-4a12-b1db-fd9d0ec790a2 | Initial Population | 1 | 0 | mismatch |
| e0813324-b2e0-4138-99f4-696f03c3db30 | Denominator | 1 | 0 | mismatch |
| e0813324-b2e0-4138-99f4-696f03c3db30 | Initial Population | 1 | 0 | mismatch |
| e0813324-b2e0-4138-99f4-696f03c3db30 | Numerator | 1 | 0 | mismatch |
| e1c47dc2-2705-4c32-8000-415987028df9 | Denominator | 1 | 0 | mismatch |
| e1c47dc2-2705-4c32-8000-415987028df9 | Denominator Exception | 1 | 0 | mismatch |
| e1c47dc2-2705-4c32-8000-415987028df9 | Initial Population | 1 | 0 | mismatch |
| e20a62fd-329e-44d7-8767-1951f9392396 | Denominator Exclusion | 1 | 0 | mismatch |
| e2edb18a-fb70-43cc-b680-6f933af7d182 | Denominator | 1 | 0 | mismatch |
| e2edb18a-fb70-43cc-b680-6f933af7d182 | Denominator Exclusion | 1 | 0 | mismatch |
| e2edb18a-fb70-43cc-b680-6f933af7d182 | Initial Population | 1 | 0 | mismatch |
| e55d9fc4-44e6-4f00-bf53-b82a5b646222 | Denominator | 1 | 0 | mismatch |
| e55d9fc4-44e6-4f00-bf53-b82a5b646222 | Initial Population | 1 | 0 | mismatch |
| e55d9fc4-44e6-4f00-bf53-b82a5b646222 | Numerator | 1 | 0 | mismatch |
| e656adac-2016-40a4-833f-0c5a02952ba3 | Denominator | 1 | 0 | mismatch |
| e656adac-2016-40a4-833f-0c5a02952ba3 | Denominator Exception | 1 | 0 | mismatch |
| e656adac-2016-40a4-833f-0c5a02952ba3 | Initial Population | 1 | 0 | mismatch |
| e7908699-646c-410f-9c1f-76539b412955 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 0 | 1 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Numerator | 1 | 0 | mismatch |
| eea87300-5d3f-4c9f-8835-9245b4e19059 | Denominator Exception | 1 | 0 | mismatch |
| ef3f90d1-4954-40bd-b230-e44ffa98ed29 | Denominator | 1 | 0 | mismatch |
| ef3f90d1-4954-40bd-b230-e44ffa98ed29 | Initial Population | 1 | 0 | mismatch |
| ef3f90d1-4954-40bd-b230-e44ffa98ed29 | Numerator | 1 | 0 | mismatch |
| f101bf69-38b2-4c86-9978-727c665dfb31 | Denominator | 1 | 0 | mismatch |
| f101bf69-38b2-4c86-9978-727c665dfb31 | Initial Population | 1 | 0 | mismatch |
| f120b2b6-40ba-4ae3-b087-c64e8e3bdf11 | Denominator | 1 | 0 | mismatch |
| f120b2b6-40ba-4ae3-b087-c64e8e3bdf11 | Denominator Exception | 1 | 0 | mismatch |
| f120b2b6-40ba-4ae3-b087-c64e8e3bdf11 | Initial Population | 1 | 0 | mismatch |
| f3b17514-f40d-43f9-baa9-a0418142ca98 | Denominator | 1 | 0 | mismatch |
| f3b17514-f40d-43f9-baa9-a0418142ca98 | Initial Population | 1 | 0 | mismatch |
| f51f9a0a-9895-4c16-9fc5-fcbe5e9cc79d | Denominator Exception | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Initial Population | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Numerator | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 1 | 0 | mismatch |
| f925afe3-4a77-404d-ba92-e78740f37d15 | Denominator | 1 | 0 | mismatch |
| f925afe3-4a77-404d-ba92-e78740f37d15 | Initial Population | 1 | 0 | mismatch |
| f925afe3-4a77-404d-ba92-e78740f37d15 | Numerator | 1 | 0 | mismatch |
| f944825a-367c-46c5-b753-d59f088038d2 | Denominator Exception | 1 | 0 | mismatch |
| fa446b35-031d-4eb5-b7f1-5782580e5209 | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 0 | 1 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Numerator | 1 | 0 | mismatch |
| fc82f4cb-7c62-41bd-9779-dd0f2e6e437f | Denominator | 1 | 0 | mismatch |
| fc82f4cb-7c62-41bd-9779-dd0f2e6e437f | Denominator Exclusion | 1 | 0 | mismatch |
| fc82f4cb-7c62-41bd-9779-dd0f2e6e437f | Initial Population | 1 | 0 | mismatch |
| fcd4fe20-9013-4d1c-965b-1445f0088624 | Denominator | 1 | 0 | mismatch |
| fcd4fe20-9013-4d1c-965b-1445f0088624 | Denominator Exception | 1 | 0 | mismatch |
| fcd4fe20-9013-4d1c-965b-1445f0088624 | Initial Population | 1 | 0 | mismatch |
| fe38b06e-b202-4620-a5ac-e2d0d99591d7 | Denominator | 1 | 0 | mismatch |
| fe38b06e-b202-4620-a5ac-e2d0d99591d7 | Denominator Exception | 1 | 0 | mismatch |
| fe38b06e-b202-4620-a5ac-e2d0d99591d7 | Initial Population | 1 | 0 | mismatch |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator Exception | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator Exclusion | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Initial Population | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Numerator | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator Exception | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator Exclusion | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Initial Population | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Numerator | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator Exception | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator Exclusion | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Initial Population | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Numerator | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator Exception | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Denominator Exclusion | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Initial Population | — | — | missing-in-qicore |
| 6da189af-7eb0-47b0-8c77-905944706aa1 | Numerator | — | — | missing-in-qicore |

### CMS506FHIRSafeUseofOpioids

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 57da77da-de92-4ef5-9311-1d86e67c9de4 | Denominator Exclusion | 1 | 0 | mismatch |
| 5e76e39a-8a30-4035-8d44-3362f4826aa7 | Denominator Exclusion | 1 | 0 | mismatch |
| 81dc5bb4-3273-492a-beff-2f7b0394f3c8 | Denominator Exclusion | 1 | 0 | mismatch |
| cdb80ca8-b110-436d-83e8-b339ebc09a44 | Denominator Exclusion | 1 | 0 | mismatch |
| e24d8c71-61dc-4e0d-bfc1-a5ebc186706d | Denominator Exclusion | 1 | 0 | mismatch |

### CMS645FHIRBoneDensityPCADTherapy

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 05afd17d-f9a0-4588-bb3a-ffefd2f6c271 | Numerator | 1 | 0 | mismatch |
| 1dc53422-497d-492a-8aa4-8a165264a14d | Numerator | 1 | 0 | mismatch |
| 2585c4a2-7b38-48e4-9317-19ddbbcfa107 | Denominator | 1 | 0 | mismatch |
| 2585c4a2-7b38-48e4-9317-19ddbbcfa107 | Initial Population | 1 | 0 | mismatch |
| 2585c4a2-7b38-48e4-9317-19ddbbcfa107 | Numerator | 1 | 0 | mismatch |
| 27fca7ba-ef00-44ec-8d97-919908f42495 | Denominator | 1 | 0 | mismatch |
| 27fca7ba-ef00-44ec-8d97-919908f42495 | Initial Population | 1 | 0 | mismatch |
| 27fca7ba-ef00-44ec-8d97-919908f42495 | Numerator | 1 | 0 | mismatch |
| 2a1d8b51-131f-4552-90f7-59ca5a7979ce | Denominator | 1 | 0 | mismatch |
| 2a1d8b51-131f-4552-90f7-59ca5a7979ce | Initial Population | 1 | 0 | mismatch |
| 2a1d8b51-131f-4552-90f7-59ca5a7979ce | Numerator | 1 | 0 | mismatch |
| 2ac59cce-81b8-4060-9452-9a35fe580c6a | Denominator | 1 | 0 | mismatch |
| 2ac59cce-81b8-4060-9452-9a35fe580c6a | Initial Population | 1 | 0 | mismatch |
| 319cbdd5-a6ea-437b-8162-a7af346daa63 | Denominator | 1 | 0 | mismatch |
| 319cbdd5-a6ea-437b-8162-a7af346daa63 | Initial Population | 1 | 0 | mismatch |
| 326c4d57-1c4e-498e-8975-ceb9913c28f8 | Denominator | 1 | 0 | mismatch |
| 326c4d57-1c4e-498e-8975-ceb9913c28f8 | Initial Population | 1 | 0 | mismatch |
| 3bbbeb07-97d4-4f25-936f-a0c1e81303e0 | Denominator | 1 | 0 | mismatch |
| 3bbbeb07-97d4-4f25-936f-a0c1e81303e0 | Initial Population | 1 | 0 | mismatch |
| 449eb3e6-3c46-439c-95cd-125aadd27e82 | Denominator | 1 | 0 | mismatch |
| 449eb3e6-3c46-439c-95cd-125aadd27e82 | Initial Population | 1 | 0 | mismatch |
| 449eb3e6-3c46-439c-95cd-125aadd27e82 | Numerator | 1 | 0 | mismatch |
| 4a7a2cf4-6073-47a0-9012-ea9b32e6e9db | Denominator | 1 | 0 | mismatch |
| 4a7a2cf4-6073-47a0-9012-ea9b32e6e9db | Initial Population | 1 | 0 | mismatch |
| 59743016-0222-4c22-bda3-48fa09e5ceb9 | Numerator | 1 | 0 | mismatch |
| 5e4e034a-475f-476a-a8f7-cb0d361508ab | Denominator | 1 | 0 | mismatch |
| 5e4e034a-475f-476a-a8f7-cb0d361508ab | Initial Population | 1 | 0 | mismatch |
| 72a0fde9-7145-4b6c-ac08-75d41d04f910 | Denominator | 1 | 0 | mismatch |
| 72a0fde9-7145-4b6c-ac08-75d41d04f910 | Initial Population | 1 | 0 | mismatch |
| 77b28b84-d0aa-4aa8-9a07-2922938394de | Denominator | 1 | 0 | mismatch |
| 77b28b84-d0aa-4aa8-9a07-2922938394de | Initial Population | 1 | 0 | mismatch |
| 7d6436cb-995e-4672-b5bb-04cb996b2949 | Denominator | 1 | 0 | mismatch |
| 7d6436cb-995e-4672-b5bb-04cb996b2949 | Initial Population | 1 | 0 | mismatch |
| 7dada0a7-61dd-4375-9863-38d08bd6d676 | Denominator | 1 | 0 | mismatch |
| 7dada0a7-61dd-4375-9863-38d08bd6d676 | Initial Population | 1 | 0 | mismatch |
| 8068a81d-feca-4719-aa75-cb45df5428e7 | Denominator | 1 | 0 | mismatch |
| 8068a81d-feca-4719-aa75-cb45df5428e7 | Initial Population | 1 | 0 | mismatch |
| 878aa680-2642-45b8-b103-5ef96188b5ea | Denominator | 1 | 0 | mismatch |
| 878aa680-2642-45b8-b103-5ef96188b5ea | Initial Population | 1 | 0 | mismatch |
| 8c41481d-f89e-4113-ba12-df7c53e93d80 | Denominator | 1 | 0 | mismatch |
| 8c41481d-f89e-4113-ba12-df7c53e93d80 | Initial Population | 1 | 0 | mismatch |
| 8c41481d-f89e-4113-ba12-df7c53e93d80 | Numerator | 1 | 0 | mismatch |
| 92e567b3-9d68-4c50-9be6-36e0ca7b96f5 | Denominator | 1 | 0 | mismatch |
| 92e567b3-9d68-4c50-9be6-36e0ca7b96f5 | Initial Population | 1 | 0 | mismatch |
| 92e567b3-9d68-4c50-9be6-36e0ca7b96f5 | Numerator | 1 | 0 | mismatch |
| 959743cc-af58-48ff-afa3-68428e69f0f5 | Denominator | 1 | 0 | mismatch |
| 959743cc-af58-48ff-afa3-68428e69f0f5 | Initial Population | 1 | 0 | mismatch |
| 97b72f42-08ae-4f6e-a7c9-1aa42f33ca90 | Denominator | 1 | 0 | mismatch |
| 97b72f42-08ae-4f6e-a7c9-1aa42f33ca90 | Initial Population | 1 | 0 | mismatch |
| 97b72f42-08ae-4f6e-a7c9-1aa42f33ca90 | Numerator | 1 | 0 | mismatch |
| 9bff7002-9697-407d-a42b-9debdafc9695 | Denominator | 1 | 0 | mismatch |
| 9bff7002-9697-407d-a42b-9debdafc9695 | Initial Population | 1 | 0 | mismatch |
| 9eb59f31-41dc-401b-9057-b8c8361f116c | Denominator | 1 | 0 | mismatch |
| 9eb59f31-41dc-401b-9057-b8c8361f116c | Initial Population | 1 | 0 | mismatch |
| 9eb59f31-41dc-401b-9057-b8c8361f116c | Numerator | 1 | 0 | mismatch |
| a1b4a442-e924-4565-a942-42ad54d2b14f | Denominator | 1 | 0 | mismatch |
| a1b4a442-e924-4565-a942-42ad54d2b14f | Initial Population | 1 | 0 | mismatch |
| a5307581-654f-415f-912f-cec5e9dc00dc | Denominator | 1 | 0 | mismatch |
| a5307581-654f-415f-912f-cec5e9dc00dc | Initial Population | 1 | 0 | mismatch |
| b2fd08f5-6a75-4221-8b2d-eb28f45ed981 | Denominator | 1 | 0 | mismatch |
| b2fd08f5-6a75-4221-8b2d-eb28f45ed981 | Initial Population | 1 | 0 | mismatch |
| b3d69ee8-375d-4039-9b92-c829cf29e154 | Denominator | 1 | 0 | mismatch |
| b3d69ee8-375d-4039-9b92-c829cf29e154 | Initial Population | 1 | 0 | mismatch |
| c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6 | Denominator | 1 | 0 | mismatch |
| c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6 | Initial Population | 1 | 0 | mismatch |
| c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6 | Numerator | 1 | 0 | mismatch |
| d07cf359-d46c-4adf-b2d4-e02a2f43b78e | Denominator | 1 | 0 | mismatch |
| d07cf359-d46c-4adf-b2d4-e02a2f43b78e | Initial Population | 1 | 0 | mismatch |
| d07cf359-d46c-4adf-b2d4-e02a2f43b78e | Numerator | 1 | 0 | mismatch |
| e0997418-e22b-4aa9-805b-dde2c398787b | Denominator | 1 | 0 | mismatch |
| e0997418-e22b-4aa9-805b-dde2c398787b | Initial Population | 1 | 0 | mismatch |
| e0997418-e22b-4aa9-805b-dde2c398787b | Numerator | 1 | 0 | mismatch |
| ee053d16-adcb-4760-9305-6a553d789d9a | Denominator | 1 | 0 | mismatch |
| ee053d16-adcb-4760-9305-6a553d789d9a | Initial Population | 1 | 0 | mismatch |
| eeea0041-7128-42e5-bb00-3b842ea97c83 | Denominator | 1 | 0 | mismatch |
| eeea0041-7128-42e5-bb00-3b842ea97c83 | Initial Population | 1 | 0 | mismatch |
| f3f9227e-50eb-4752-af8a-464072cc60c2 | Denominator | 1 | 0 | mismatch |
| f3f9227e-50eb-4752-af8a-464072cc60c2 | Initial Population | 1 | 0 | mismatch |
| f3f9227e-50eb-4752-af8a-464072cc60c2 | Numerator | 1 | 0 | mismatch |
| f67164ab-356d-4fb6-afcb-169aaa7fe3f4 | Numerator | 1 | 0 | mismatch |

### CMS646FHIRIntravesicalBCGTherapy

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 132a7880-8f4a-4b0c-a7d6-3f5a8d6df0b3 | Denominator Exclusion | 1 | 0 | mismatch |
| 25bd7c4f-0cd4-4634-a84e-53377e5701f1 | Denominator Exclusion | 1 | 0 | mismatch |
| 362e32e9-4f68-4917-811d-7a39eedb4b66 | Denominator Exclusion | 1 | 0 | mismatch |
| 38ed8b42-1631-4673-aa53-6d2a4db163f3 | Denominator Exclusion | 1 | 0 | mismatch |
| 7488d3a7-f4e1-445f-8375-d3fff29f080d | Denominator Exclusion | 1 | 0 | mismatch |
| 790f8993-7a03-4ed7-94d7-c0e95587afa0 | Numerator | 1 | 0 | mismatch |
| ab48e0c0-6543-4537-8f00-bfcdcba7a81b | Numerator | 1 | 0 | mismatch |
| b786e9d7-b4ae-4cd1-b7eb-a6d4f789424e | Denominator Exclusion | 1 | 0 | mismatch |
| e648fa70-0532-49b0-92f6-dfb5a6d28d94 | Denominator Exception | 0 | 1 | mismatch |
| f5c2b6b4-4458-4be5-8c3b-20d2fb0ad36c | Denominator Exclusion | 1 | 0 | mismatch |
| 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 | Denominator | — | — | missing-in-cms |
| 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 | Denominator Exception | — | — | missing-in-cms |
| 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 | Denominator Exclusion | — | — | missing-in-cms |
| 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 | Initial Population | — | — | missing-in-cms |
| 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 | Numerator | — | — | missing-in-cms |

### CMS771FHIRUrinarySymptomScoreBPH

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 051c5977-9f2c-4e8b-8e02-ac3ec0c718d6 | Initial Population | 1 | 0 | mismatch |
| 2105cba2-6e61-487d-a737-3efe876028e8 | Denominator | 1 | 0 | mismatch |
| 2105cba2-6e61-487d-a737-3efe876028e8 | Denominator Exclusion | 1 | 0 | mismatch |
| 2105cba2-6e61-487d-a737-3efe876028e8 | Initial Population | 1 | 0 | mismatch |
| 228562c7-76c5-42e1-b4b6-0b952faa75c4 | Denominator | 1 | 0 | mismatch |
| 228562c7-76c5-42e1-b4b6-0b952faa75c4 | Initial Population | 1 | 0 | mismatch |
| 3ab3ac1d-9b5e-4087-8862-dcb2562fb90f | Initial Population | 1 | 0 | mismatch |
| 4c234ec0-3f89-4d55-b767-219d1130f634 | Denominator | 1 | 0 | mismatch |
| 4c234ec0-3f89-4d55-b767-219d1130f634 | Initial Population | 1 | 0 | mismatch |
| 5339dd62-76a1-43b3-9965-d8b61478236a | Denominator | 1 | 0 | mismatch |
| 5339dd62-76a1-43b3-9965-d8b61478236a | Denominator Exclusion | 1 | 0 | mismatch |
| 5339dd62-76a1-43b3-9965-d8b61478236a | Initial Population | 1 | 0 | mismatch |
| 623a7f40-c265-4e42-a5c5-6f54e20b19df | Denominator | 1 | 0 | mismatch |
| 623a7f40-c265-4e42-a5c5-6f54e20b19df | Initial Population | 1 | 0 | mismatch |
| 6552cc29-c4e2-441f-9ae5-41d3a2f5aea4 | Denominator | 1 | 0 | mismatch |
| 6552cc29-c4e2-441f-9ae5-41d3a2f5aea4 | Initial Population | 1 | 0 | mismatch |
| 71846c52-343b-4e31-95e1-b1f44cca0128 | Denominator | 1 | 0 | mismatch |
| 71846c52-343b-4e31-95e1-b1f44cca0128 | Denominator Exclusion | 1 | 0 | mismatch |
| 71846c52-343b-4e31-95e1-b1f44cca0128 | Initial Population | 1 | 0 | mismatch |
| 7f62a1c0-a39c-41b7-98b6-5877db9755b0 | Denominator | 1 | 0 | mismatch |
| 7f62a1c0-a39c-41b7-98b6-5877db9755b0 | Initial Population | 1 | 0 | mismatch |
| 836cdd6c-bc29-4752-9beb-c336d26f0ed2 | Denominator | 1 | 0 | mismatch |
| 836cdd6c-bc29-4752-9beb-c336d26f0ed2 | Initial Population | 1 | 0 | mismatch |
| 844dab9e-f34d-41dc-bd89-1440551471a6 | Denominator | 1 | 0 | mismatch |
| 844dab9e-f34d-41dc-bd89-1440551471a6 | Initial Population | 1 | 0 | mismatch |
| 8bb45bbf-4685-45ed-ab5d-004f87214748 | Denominator | 1 | 0 | mismatch |
| 8bb45bbf-4685-45ed-ab5d-004f87214748 | Denominator Exclusion | 1 | 0 | mismatch |
| 8bb45bbf-4685-45ed-ab5d-004f87214748 | Initial Population | 1 | 0 | mismatch |
| 9b637e49-9b8a-48c1-8304-ec5984bc47fa | Denominator | 1 | 0 | mismatch |
| 9b637e49-9b8a-48c1-8304-ec5984bc47fa | Initial Population | 1 | 0 | mismatch |
| 9be591a0-517b-4be2-b652-a29be0c75c15 | Denominator | 1 | 0 | mismatch |
| 9be591a0-517b-4be2-b652-a29be0c75c15 | Initial Population | 1 | 0 | mismatch |
| ae8b8236-bf9b-47f9-9c71-4655bca14aba | Denominator | 1 | 0 | mismatch |
| ae8b8236-bf9b-47f9-9c71-4655bca14aba | Denominator Exclusion | 1 | 0 | mismatch |
| ae8b8236-bf9b-47f9-9c71-4655bca14aba | Initial Population | 1 | 0 | mismatch |
| bc79e5bc-237e-44be-b5fc-c5c4efb50286 | Denominator | 1 | 0 | mismatch |
| bc79e5bc-237e-44be-b5fc-c5c4efb50286 | Initial Population | 1 | 0 | mismatch |
| bf0200ab-3e37-4e90-8517-ffb351f2e563 | Denominator | 1 | 0 | mismatch |
| bf0200ab-3e37-4e90-8517-ffb351f2e563 | Initial Population | 1 | 0 | mismatch |
| bf0f8968-c2c0-4416-88db-11ea3e3da968 | Denominator | 1 | 0 | mismatch |
| bf0f8968-c2c0-4416-88db-11ea3e3da968 | Initial Population | 1 | 0 | mismatch |
| cb9497a6-968f-4034-b85c-d254c07e34e5 | Denominator | 1 | 0 | mismatch |
| cb9497a6-968f-4034-b85c-d254c07e34e5 | Initial Population | 1 | 0 | mismatch |
| e90d90a7-3071-44de-8089-ad7b6f5f3e5d | Denominator | 1 | 0 | mismatch |
| e90d90a7-3071-44de-8089-ad7b6f5f3e5d | Initial Population | 1 | 0 | mismatch |
| f1ccd667-ada1-4ca8-b4dc-fe4a9674d81a | Denominator | 1 | 0 | mismatch |
| f1ccd667-ada1-4ca8-b4dc-fe4a9674d81a | Initial Population | 1 | 0 | mismatch |

### CMS819FHIRHHORAE

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 1994e69a-472b-4e32-80c1-5c692f36acce | Numerator | 1 | 0 | mismatch |
| 93064568-a739-403d-853f-a3150bd8f752 | Numerator | 1 | 0 | mismatch |

### CMS871FHIRHHHyper

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 113a6e72-7049-4a7f-90cf-5ec3435b0dee | Denominator | 1 | 0 | mismatch |
| 113a6e72-7049-4a7f-90cf-5ec3435b0dee | Initial Population | 1 | 0 | mismatch |

### CMS986FHIRMalnutritionScore

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| a4f53b12-e0e3-4faf-8e66-6ce8193a6477 | Measure Population Exclusion | 0 | 1 | mismatch |
| a4f53b12-e0e3-4faf-8e66-6ce8193a6477 | Measure Population Exclusion | 0 | 1 | mismatch |
| a4f53b12-e0e3-4faf-8e66-6ce8193a6477 | Measure Population Exclusion | 0 | 1 | mismatch |
| a4f53b12-e0e3-4faf-8e66-6ce8193a6477 | Measure Population Exclusion | 0 | 1 | mismatch |
| a4f53b12-e0e3-4faf-8e66-6ce8193a6477 | Measure Population Exclusion | 0 | 1 | mismatch |
| a4f53b12-e0e3-4faf-8e66-6ce8193a6477 | Measure Population Exclusion | 0 | 1 | mismatch |

### CMS996FHIRAptTxforSTEMI

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 60823d79-b37f-4358-819f-f39b4e885c6d | Denominator Exception | 0 | 1 | mismatch |
| 7edab122-3af3-4172-9231-7c1470ecc1e0 | Denominator Exception | 0 | 1 | mismatch |
| 83e7cc74-5ae5-4fb9-922f-15faa555890a | Denominator Exclusion | 1 | 0 | mismatch |
| 8bb7c40b-7447-42ca-b662-161a7026ed8f | Denominator Exception | 0 | 1 | mismatch |
| a0de0e88-9054-45d4-a417-3b9ea5ebe78a | Denominator Exclusion | 1 | 0 | mismatch |
| b4219e21-be97-4f81-8a31-fee0035179c8 | Denominator | 1 | 0 | mismatch |
| b4219e21-be97-4f81-8a31-fee0035179c8 | Initial Population | 1 | 0 | mismatch |
| b4219e21-be97-4f81-8a31-fee0035179c8 | Numerator | 1 | 0 | mismatch |
| ccc7deaf-98b7-4dad-b190-8fee10f2cf77 | Denominator Exception | 0 | 1 | mismatch |
| ef443a3d-6cde-467d-b374-d90a2f244e83 | Denominator Exclusion | 1 | 0 | mismatch |
| f4df05b5-547b-45d2-bc18-8fcbd5afbaf7 | Denominator Exclusion | 1 | 0 | mismatch |
| f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847 | Denominator Exclusion | 1 | 0 | mismatch |
| f7d55d17-b25e-4923-a880-dd79ef092ba6 | Denominator Exclusion | 1 | 0 | mismatch |

### CMS1028FHIRPCSevereOBComps

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 02cdc116-49ce-4277-ad9e-de6bc2a3274d | Denominator | 1 | 0 | mismatch |
| 02cdc116-49ce-4277-ad9e-de6bc2a3274d | Initial Population | 1 | 0 | mismatch |
| 02cdc116-49ce-4277-ad9e-de6bc2a3274d | Denominator | 1 | 0 | mismatch |
| 02cdc116-49ce-4277-ad9e-de6bc2a3274d | Initial Population | 1 | 0 | mismatch |
| 3d4b6868-31ce-42f8-87c1-ab06d851d53f | Initial Population | 1 | 0 | mismatch |
| 3d4b6868-31ce-42f8-87c1-ab06d851d53f | Initial Population | 1 | 0 | mismatch |
| 4911c0c6-22e1-45ad-b39d-7e4d88c200d8 | Numerator | 1 | 0 | mismatch |
| 4911c0c6-22e1-45ad-b39d-7e4d88c200d8 | Numerator | 1 | 0 | mismatch |
| 4e9a1928-a33f-4be3-aa05-c69e9fc4bff7 | Denominator | 1 | 0 | mismatch |
| 4e9a1928-a33f-4be3-aa05-c69e9fc4bff7 | Initial Population | 1 | 0 | mismatch |
| 4e9a1928-a33f-4be3-aa05-c69e9fc4bff7 | Denominator | 1 | 0 | mismatch |
| 4e9a1928-a33f-4be3-aa05-c69e9fc4bff7 | Initial Population | 1 | 0 | mismatch |

### CMS1173FHIRDiagnosticDelayVTE

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 3739ae38-6a2c-4197-bda6-e493c9df60e3 | Denominator Exclusion | 1 | 0 | mismatch |
| 3739ae38-6a2c-4197-bda6-e493c9df60e3 | Numerator | 0 | 1 | mismatch |
| cfa235c3-3b8b-4cbb-a78f-5c4fd2af04df | Denominator Exclusion | 1 | 0 | mismatch |
| cfa235c3-3b8b-4cbb-a78f-5c4fd2af04df | Numerator | 0 | 1 | mismatch |

### CMS1188FHIRHIVSTITesting

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 6c08efb1-922f-4e66-98bc-0de25182e723 | Numerator | 1 | 0 | mismatch |
| 77973e2d-625a-4c4f-aa69-2c716af0ad3c | Numerator | 1 | 0 | mismatch |

### CMS1264FHIRECATREHQR

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 01959faf-5ea5-41cb-b960-b74da18cca85 | Denominator | 1 | 0 | mismatch |
| 01959faf-5ea5-41cb-b960-b74da18cca85 | Initial Population | 1 | 0 | mismatch |
| 01959faf-5ea5-41cb-b960-b74da18cca85 | Numerator | 1 | 0 | mismatch |
| 040dc7b1-27f9-43a3-82c9-b1a514db3071 | Denominator | 1 | 0 | mismatch |
| 040dc7b1-27f9-43a3-82c9-b1a514db3071 | Initial Population | 1 | 0 | mismatch |
| 040dc7b1-27f9-43a3-82c9-b1a514db3071 | Numerator | 1 | 0 | mismatch |
| 048b1f27-6343-4bcd-950d-e228de06aa9c | Denominator | 2 | 0 | mismatch |
| 048b1f27-6343-4bcd-950d-e228de06aa9c | Initial Population | 2 | 0 | mismatch |
| 048b1f27-6343-4bcd-950d-e228de06aa9c | Numerator | 2 | 0 | mismatch |
| 11703274-1218-440d-bb98-08502a794179 | Denominator | 1 | 0 | mismatch |
| 11703274-1218-440d-bb98-08502a794179 | Initial Population | 1 | 0 | mismatch |
| 11703274-1218-440d-bb98-08502a794179 | Numerator | 1 | 0 | mismatch |
| 16cffb87-15ea-48b7-bd68-f211f48d6f19 | Denominator | 1 | 0 | mismatch |
| 16cffb87-15ea-48b7-bd68-f211f48d6f19 | Initial Population | 1 | 0 | mismatch |
| 16cffb87-15ea-48b7-bd68-f211f48d6f19 | Numerator | 1 | 0 | mismatch |
| 1f8035de-4255-434e-a32f-b97039ec57ff | Denominator | 1 | 0 | mismatch |
| 1f8035de-4255-434e-a32f-b97039ec57ff | Initial Population | 1 | 0 | mismatch |
| 1f8035de-4255-434e-a32f-b97039ec57ff | Numerator | 1 | 0 | mismatch |
| 21b841f6-b863-4c1d-8798-41c527b04a92 | Denominator | 1 | 0 | mismatch |
| 21b841f6-b863-4c1d-8798-41c527b04a92 | Initial Population | 1 | 0 | mismatch |
| 221f787f-b5b1-4e16-ab64-6ab9d3e8744f | Denominator | 1 | 0 | mismatch |
| 221f787f-b5b1-4e16-ab64-6ab9d3e8744f | Initial Population | 1 | 0 | mismatch |
| 221f787f-b5b1-4e16-ab64-6ab9d3e8744f | Numerator | 1 | 0 | mismatch |
| 2c2a7958-4d1a-4142-9360-8045067a1c5b | Denominator | 1 | 0 | mismatch |
| 2c2a7958-4d1a-4142-9360-8045067a1c5b | Initial Population | 1 | 0 | mismatch |
| 2c2a7958-4d1a-4142-9360-8045067a1c5b | Numerator | 1 | 0 | mismatch |
| 2fc54731-4fd9-4884-aba5-9a8385111375 | Denominator | 1 | 0 | mismatch |
| 2fc54731-4fd9-4884-aba5-9a8385111375 | Initial Population | 1 | 0 | mismatch |
| 2fc54731-4fd9-4884-aba5-9a8385111375 | Numerator | 1 | 0 | mismatch |
| 3302c6ff-8767-4be7-9c81-f1d98351b247 | Denominator | 2 | 0 | mismatch |
| 3302c6ff-8767-4be7-9c81-f1d98351b247 | Initial Population | 2 | 0 | mismatch |
| 3302c6ff-8767-4be7-9c81-f1d98351b247 | Numerator | 1 | 0 | mismatch |
| 35fd427f-1233-4f3c-b8b3-9e400755da8f | Denominator | 1 | 0 | mismatch |
| 35fd427f-1233-4f3c-b8b3-9e400755da8f | Initial Population | 1 | 0 | mismatch |
| 35fd427f-1233-4f3c-b8b3-9e400755da8f | Numerator | 1 | 0 | mismatch |
| 404c928b-a752-4792-91c4-8a1fd0656759 | Denominator | 1 | 0 | mismatch |
| 404c928b-a752-4792-91c4-8a1fd0656759 | Initial Population | 1 | 0 | mismatch |
| 42be9d46-4c2f-4493-8299-d33dcbb7170e | Denominator | 1 | 0 | mismatch |
| 42be9d46-4c2f-4493-8299-d33dcbb7170e | Initial Population | 1 | 0 | mismatch |
| 4c95d881-2e7e-4e81-bb4c-b1ae680ff286 | Denominator | 1 | 0 | mismatch |
| 4c95d881-2e7e-4e81-bb4c-b1ae680ff286 | Initial Population | 1 | 0 | mismatch |
| 4c95d881-2e7e-4e81-bb4c-b1ae680ff286 | Numerator | 1 | 0 | mismatch |
| 50270eff-f1ed-4cb3-b22b-467d89937c3a | Denominator | 1 | 0 | mismatch |
| 50270eff-f1ed-4cb3-b22b-467d89937c3a | Initial Population | 1 | 0 | mismatch |
| 540b665b-e89c-466a-9ef8-758b3883a37c | Denominator | 1 | 0 | mismatch |
| 540b665b-e89c-466a-9ef8-758b3883a37c | Initial Population | 1 | 0 | mismatch |
| 5ae9589c-1301-45a0-af30-ac7b679b649f | Denominator | 1 | 0 | mismatch |
| 5ae9589c-1301-45a0-af30-ac7b679b649f | Initial Population | 1 | 0 | mismatch |
| 5ae9589c-1301-45a0-af30-ac7b679b649f | Numerator | 1 | 0 | mismatch |
| 5fb0b78c-ffd3-47c3-91a3-252bc4a70177 | Denominator | 1 | 0 | mismatch |
| 5fb0b78c-ffd3-47c3-91a3-252bc4a70177 | Initial Population | 1 | 0 | mismatch |
| 6252a858-2362-4c63-8d7d-6db0b7ac9299 | Denominator | 1 | 0 | mismatch |
| 6252a858-2362-4c63-8d7d-6db0b7ac9299 | Initial Population | 1 | 0 | mismatch |
| 63cea3d6-d2e0-4736-a035-87633ca960bd | Denominator | 1 | 0 | mismatch |
| 63cea3d6-d2e0-4736-a035-87633ca960bd | Initial Population | 1 | 0 | mismatch |
| 666528ac-0d94-4b09-8e6c-c5930b7dd17c | Denominator | 1 | 0 | mismatch |
| 666528ac-0d94-4b09-8e6c-c5930b7dd17c | Initial Population | 1 | 0 | mismatch |
| 666528ac-0d94-4b09-8e6c-c5930b7dd17c | Numerator | 1 | 0 | mismatch |
| 66803f75-5dc5-43fb-9844-f18d765a64ec | Denominator | 1 | 0 | mismatch |
| 66803f75-5dc5-43fb-9844-f18d765a64ec | Initial Population | 1 | 0 | mismatch |
| 66803f75-5dc5-43fb-9844-f18d765a64ec | Numerator | 1 | 0 | mismatch |
| 74855a5c-bb3b-438a-9eb9-7fdc1994d06d | Denominator | 1 | 0 | mismatch |
| 74855a5c-bb3b-438a-9eb9-7fdc1994d06d | Initial Population | 1 | 0 | mismatch |
| 78cbc6ac-f30d-404b-b539-6b903c7cfeba | Denominator | 1 | 0 | mismatch |
| 78cbc6ac-f30d-404b-b539-6b903c7cfeba | Initial Population | 1 | 0 | mismatch |
| 78cbc6ac-f30d-404b-b539-6b903c7cfeba | Numerator | 1 | 0 | mismatch |
| 7bcd79b7-7898-437d-b563-cfb9068df210 | Denominator | 1 | 0 | mismatch |
| 7bcd79b7-7898-437d-b563-cfb9068df210 | Initial Population | 1 | 0 | mismatch |
| 7bcd79b7-7898-437d-b563-cfb9068df210 | Numerator | 1 | 0 | mismatch |
| 7bee402e-2687-4813-9b39-37d723663d18 | Denominator | 1 | 0 | mismatch |
| 7bee402e-2687-4813-9b39-37d723663d18 | Initial Population | 1 | 0 | mismatch |
| 7dd19e80-23c6-4e31-86a9-bb833cfc676b | Denominator | 1 | 0 | mismatch |
| 7dd19e80-23c6-4e31-86a9-bb833cfc676b | Initial Population | 1 | 0 | mismatch |
| 7fbb7e37-228b-4b3b-8974-871a3e798720 | Denominator | 1 | 0 | mismatch |
| 7fbb7e37-228b-4b3b-8974-871a3e798720 | Initial Population | 1 | 0 | mismatch |
| 7fbb7e37-228b-4b3b-8974-871a3e798720 | Numerator | 1 | 0 | mismatch |
| 7fd4f9cd-8fbb-4935-9bfd-959c538166b2 | Denominator | 1 | 0 | mismatch |
| 7fd4f9cd-8fbb-4935-9bfd-959c538166b2 | Initial Population | 1 | 0 | mismatch |
| 8e43bc64-4242-494d-b47f-fdbbd3372bbe | Denominator | 1 | 0 | mismatch |
| 8e43bc64-4242-494d-b47f-fdbbd3372bbe | Initial Population | 1 | 0 | mismatch |
| 8e43bc64-4242-494d-b47f-fdbbd3372bbe | Numerator | 1 | 0 | mismatch |
| 9098f676-4f4e-402c-80e3-331aabb6d414 | Denominator | 1 | 0 | mismatch |
| 9098f676-4f4e-402c-80e3-331aabb6d414 | Initial Population | 1 | 0 | mismatch |
| 9098f676-4f4e-402c-80e3-331aabb6d414 | Numerator | 1 | 0 | mismatch |
| 91d5385d-09ac-4206-b009-0c7feffc22ff | Denominator | 1 | 0 | mismatch |
| 91d5385d-09ac-4206-b009-0c7feffc22ff | Initial Population | 1 | 0 | mismatch |
| 91d5385d-09ac-4206-b009-0c7feffc22ff | Numerator | 1 | 0 | mismatch |
| 9b5e4d84-366b-4082-8409-b7e18e0a3c45 | Denominator | 1 | 0 | mismatch |
| 9b5e4d84-366b-4082-8409-b7e18e0a3c45 | Initial Population | 1 | 0 | mismatch |
| 9b5e4d84-366b-4082-8409-b7e18e0a3c45 | Numerator | 1 | 0 | mismatch |
| 9bac5045-01af-4350-b54f-63ab17f3ba9f | Denominator | 0 | 1 | mismatch |
| 9bac5045-01af-4350-b54f-63ab17f3ba9f | Initial Population | 0 | 1 | mismatch |
| 9bac5045-01af-4350-b54f-63ab17f3ba9f | Numerator | 0 | 1 | mismatch |
| 9ec1a135-fb47-4c1c-8f6b-98afab15274e | Denominator | 1 | 0 | mismatch |
| 9ec1a135-fb47-4c1c-8f6b-98afab15274e | Initial Population | 1 | 0 | mismatch |
| 9f77830b-ff7c-4060-bf38-295b215ab56d | Denominator | 1 | 0 | mismatch |
| 9f77830b-ff7c-4060-bf38-295b215ab56d | Initial Population | 1 | 0 | mismatch |
| 9f77830b-ff7c-4060-bf38-295b215ab56d | Numerator | 1 | 0 | mismatch |
| a11dce52-c6b3-46e5-bc01-8994b0c8f471 | Denominator | 1 | 0 | mismatch |
| a11dce52-c6b3-46e5-bc01-8994b0c8f471 | Initial Population | 1 | 0 | mismatch |
| a11dce52-c6b3-46e5-bc01-8994b0c8f471 | Numerator | 1 | 0 | mismatch |
| a3dd602c-cd84-4e7a-aa37-eae4b15fdf4e | Denominator | 1 | 0 | mismatch |
| a3dd602c-cd84-4e7a-aa37-eae4b15fdf4e | Initial Population | 1 | 0 | mismatch |
| a3dd602c-cd84-4e7a-aa37-eae4b15fdf4e | Numerator | 1 | 0 | mismatch |
| a42d4cc2-24ca-4637-889f-276bcdd1e7cf | Denominator | 1 | 0 | mismatch |
| a42d4cc2-24ca-4637-889f-276bcdd1e7cf | Initial Population | 1 | 0 | mismatch |
| a42d4cc2-24ca-4637-889f-276bcdd1e7cf | Numerator | 1 | 0 | mismatch |
| b312fbc9-083f-4832-8d7c-d3e64df4145b | Denominator | 1 | 0 | mismatch |
| b312fbc9-083f-4832-8d7c-d3e64df4145b | Initial Population | 1 | 0 | mismatch |
| b312fbc9-083f-4832-8d7c-d3e64df4145b | Numerator | 1 | 0 | mismatch |
| bfc497aa-308c-4113-9a36-21c6e17c3802 | Denominator | 2 | 0 | mismatch |
| bfc497aa-308c-4113-9a36-21c6e17c3802 | Initial Population | 2 | 0 | mismatch |
| bfc497aa-308c-4113-9a36-21c6e17c3802 | Numerator | 2 | 0 | mismatch |
| c3284314-fe9b-408a-9b26-a21830f84432 | Denominator | 1 | 0 | mismatch |
| c3284314-fe9b-408a-9b26-a21830f84432 | Initial Population | 1 | 0 | mismatch |
| cc00e728-de5f-4df8-abcb-1e610496be66 | Denominator | 1 | 0 | mismatch |
| cc00e728-de5f-4df8-abcb-1e610496be66 | Initial Population | 1 | 0 | mismatch |
| cc01e29c-7ebb-4876-b63a-29de550c62f9 | Denominator | 1 | 0 | mismatch |
| cc01e29c-7ebb-4876-b63a-29de550c62f9 | Initial Population | 1 | 0 | mismatch |
| cc01e29c-7ebb-4876-b63a-29de550c62f9 | Numerator | 1 | 0 | mismatch |
| cee26b56-54cf-444e-8944-6edfbd6d2b93 | Denominator | 1 | 0 | mismatch |
| cee26b56-54cf-444e-8944-6edfbd6d2b93 | Initial Population | 1 | 0 | mismatch |
| cee26b56-54cf-444e-8944-6edfbd6d2b93 | Numerator | 1 | 0 | mismatch |
| d1b64acd-58bc-4831-b150-a80b4240d6b1 | Denominator | 1 | 0 | mismatch |
| d1b64acd-58bc-4831-b150-a80b4240d6b1 | Initial Population | 1 | 0 | mismatch |
| d1b64acd-58bc-4831-b150-a80b4240d6b1 | Numerator | 1 | 0 | mismatch |
| d3a7a6b7-bbbc-4c08-bd8c-ce1e1cbdc8a8 | Denominator | 2 | 0 | mismatch |
| d3a7a6b7-bbbc-4c08-bd8c-ce1e1cbdc8a8 | Initial Population | 2 | 0 | mismatch |
| d3a7a6b7-bbbc-4c08-bd8c-ce1e1cbdc8a8 | Numerator | 1 | 0 | mismatch |
| d5fe6f9c-6036-4004-9993-290f3a2be34a | Denominator | 1 | 0 | mismatch |
| d5fe6f9c-6036-4004-9993-290f3a2be34a | Initial Population | 1 | 0 | mismatch |
| d5fe6f9c-6036-4004-9993-290f3a2be34a | Numerator | 1 | 0 | mismatch |
| d8832769-c838-4f1b-9c1e-fa4ed3a3efb9 | Denominator | 1 | 0 | mismatch |
| d8832769-c838-4f1b-9c1e-fa4ed3a3efb9 | Initial Population | 1 | 0 | mismatch |
| dac89c3d-536e-4dca-9871-570a0bcd8d16 | Denominator | 2 | 0 | mismatch |
| dac89c3d-536e-4dca-9871-570a0bcd8d16 | Initial Population | 2 | 0 | mismatch |
| dac89c3d-536e-4dca-9871-570a0bcd8d16 | Numerator | 2 | 0 | mismatch |
| dad5b672-1e5b-437c-91fe-1f69b5d58c70 | Denominator | 1 | 0 | mismatch |
| dad5b672-1e5b-437c-91fe-1f69b5d58c70 | Initial Population | 1 | 0 | mismatch |
| dad5b672-1e5b-437c-91fe-1f69b5d58c70 | Numerator | 1 | 0 | mismatch |
| dfd5dc6b-3299-4e4f-ae02-45f251e1f75b | Denominator | 1 | 0 | mismatch |
| dfd5dc6b-3299-4e4f-ae02-45f251e1f75b | Initial Population | 1 | 0 | mismatch |
| e982ec87-76b0-4fe2-b437-ac0503cf2159 | Denominator | 1 | 0 | mismatch |
| e982ec87-76b0-4fe2-b437-ac0503cf2159 | Initial Population | 1 | 0 | mismatch |
| e982ec87-76b0-4fe2-b437-ac0503cf2159 | Numerator | 1 | 0 | mismatch |
| eabe386d-5bca-4fdd-acb0-8228b4df83c0 | Denominator | 1 | 0 | mismatch |
| eabe386d-5bca-4fdd-acb0-8228b4df83c0 | Initial Population | 1 | 0 | mismatch |
| ed5fa616-8b70-4016-b40d-6f87983e2776 | Denominator | 1 | 0 | mismatch |
| ed5fa616-8b70-4016-b40d-6f87983e2776 | Initial Population | 1 | 0 | mismatch |
| ed5fa616-8b70-4016-b40d-6f87983e2776 | Numerator | 1 | 0 | mismatch |
| ee13a2d8-61d9-4d2f-8f13-1423bd271950 | Denominator | 1 | 0 | mismatch |
| ee13a2d8-61d9-4d2f-8f13-1423bd271950 | Initial Population | 1 | 0 | mismatch |

### NHSNAcuteCareHospitalMonthlyInitialPopulation1

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 025529dc-5384-4544-acb2-c2b6f7c9a23c | Initial Population | 1 | 0 | mismatch |
| 0353da56-ca21-45d3-8f96-8954167143ae | Initial Population | 1 | 0 | mismatch |
| 09431e3b-b1d9-491a-b6a3-76b3868e6213 | Initial Population | 1 | 0 | mismatch |
| 16acd0ee-60e7-4573-b433-5a9c335c145b | Initial Population | 1 | 0 | mismatch |
| 19feaae6-8985-4444-9182-d3c785698710 | Initial Population | 1 | 0 | mismatch |
| 1c06a652-f116-4307-80b7-342c16d20de1 | Initial Population | 1 | 0 | mismatch |
| 24ab1538-bc59-454b-bd24-961288f4eea8 | Initial Population | 1 | 0 | mismatch |
| 2ce50e7f-4e04-4d5b-9d9a-2243958c2a92 | Initial Population | 1 | 0 | mismatch |
| 2ea03a1a-cefe-4eac-9e34-7bf434b30d2b | Initial Population | 1 | 0 | mismatch |
| 36e30d76-0d86-4b72-ba89-4ebaacf48b31 | Initial Population | 1 | 0 | mismatch |
| 3e86234e-4999-4e8e-a4a2-420d1343b079 | Initial Population | 1 | 0 | mismatch |
| 4974042e-fff4-4a3d-905e-548c6593ce40 | Initial Population | 1 | 0 | mismatch |
| 4c8f4dd1-193e-4239-80ac-63e9ac2bd053 | Initial Population | 1 | 0 | mismatch |
| 4d192f80-7649-4afd-a842-528ef60fc904 | Initial Population | 1 | 0 | mismatch |
| 55f7d07e-a8ec-4abf-9bb3-b9b3f81d38d5 | Initial Population | 1 | 0 | mismatch |
| 5efcd4e7-f71b-48a6-badb-b1b88c02f161 | Initial Population | 1 | 0 | mismatch |
| 6409f1eb-d338-4bf6-a3df-4da1eb997c48 | Initial Population | 1 | 0 | mismatch |
| 70306180-c713-4fa4-9c39-ae3b15e15d22 | Initial Population | 1 | 0 | mismatch |
| 7f26eb5a-f877-458b-b960-5de7ffa5b4d0 | Initial Population | 1 | 0 | mismatch |
| 8a407b28-6668-43be-9148-31ed08b8c0c4 | Initial Population | 1 | 0 | mismatch |
| 8ffa77ff-8591-442d-84b1-6c6cb86fd09e | Initial Population | 1 | 0 | mismatch |
| 98561005-400a-4b9d-8902-f04605b6b168 | Initial Population | 1 | 0 | mismatch |
| a1ec5d8e-4926-456a-8523-786a93f2348b | Initial Population | 1 | 0 | mismatch |
| bf9e53b4-e10c-4a11-a9be-8d5b944c1d51 | Initial Population | 1 | 0 | mismatch |
| c1b0ea0e-73e8-4b74-bbae-4cf2504fa9e4 | Initial Population | 1 | 0 | mismatch |
| d7aad5bd-638e-402a-92b3-2fb7f3f91151 | Initial Population | 1 | 0 | mismatch |
| ec296057-82c9-41b2-9e32-8ec2ea4f3687 | Initial Population | 1 | 0 | mismatch |

### NHSNGlycemicControlHypoglycemiaInitialPopulation

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 40b66b90-4811-4f6f-8eec-c46d1a5e6eeb | Initial Population | 1 | 0 | mismatch |
| 7bcb6920-f361-4121-82a5-31fed13e65d6 | Initial Population | 1 | 0 | mismatch |
| c4d785a5-a3fa-463b-b9ca-91d60eeb3dab | Initial Population | 1 | 0 | mismatch |
| f3a5ce49-9f1f-455b-8f44-2389c61d5850 | Initial Population | 1 | 0 | mismatch |


---

_See [catalog_issue_details.md](./catalog_issue_details.md) for per-issue detail on every catalog issue cited above._
