# Discrepancy Report
| Details | Value |
| --- | --- |
| Generated | 2026-09-03 16:54:45.384070 |
| Total Measures | 74 |
| Total Test Cases | 3964 |
| Measures with Discrepancies | 40 |
| Known Issues (resolution pending) | 11 issues / 4 test cases |
| Pass Count (all) | 22449 (94.63%) |
| Fail Count (all) | 1273 (5.37%) |
| Pass Count (excl. resolution-pending) | 22449 (94.71%) |
| Fail Count (excl. resolution-pending) | 1254 (5.29%) |
| QICore Pass Count | 22678 (95.60%) |
| QICore Fail Count | 1044 (4.40%) |
| QICore Measures with Discrepancies | 35 |



## Known Issues (resolution-pending)

| ID | Issue | Category | Status | Affected measures | Tracked test cases |
|---|------|---------|-------|-----|------|
| E-01 | `Min()` over DateTime throws | engine | **Confirmed** | CMS1173, CMS871, CMS645, CMS646, CMS156 | 0 |
| E-02 | Raw `FHIR.dateTime` / choice-typed `X.effective` in temporal operators fails | engine | **Confirmed** | CMS1173, CMS156 | 0 |
| E-11 | `Unable to extract codes from fhirType Reference` | engine | **Confirmed** | CMS135, CMS165 | 4 |
| E-12 | Union branch evaluates empty despite correct data | engine | **Confirmed** | CMS104 | 0 |
| E-13 | Union of `ConditionProblemsHealthConcerns` ∪ `ConditionEncounterDiagnosis` → `Choice<...>` fed to `prevalenceInterval()` mis-resolves: missing FHIRCommon Choice overload + translator cannot resolve the call (the Choice should coerce to base `Condition` — engine/translator issue) | engine | **Confirmed / Applied** | CMS347, CMS117, CMS138, CMS153, CMS136, CMS155, CMS69, CMS645, CMS1154, CMS1157, CMS75, CMS142, CMS143, CMS771, CMS1188, CMS124, CMS349, CMS90, CMS646, CMS314, CMS129, CMS951, CMS128, CMS56, CMS131, CMS159, CMS133, CMS996, CMS157, CMS156, CMS22, CMS71 | 0 |
| E-14 | `PCMaternal.cql` cast type change (`.value as DateTime` → `.value as FHIR.dateTime`) | engine | **Suspected** | CMS0334, CMS1028 | 0 |
| E-16 | `overlaps` on a half-open null-high interval (`[start, null)`) evaluates false — `FHIRCommon.prevalenceInterval()` inactive branch | engine | **Confirmed** | CMS1154 | 0 |
| E-17 | `ObservationScreeningAssessment` profile retrieve returns empty despite qualifying observations (`isAssessmentPerformed()` / profile-retrieve gap) — CMS56 Numerator assessments (`Date {HOOS,HOOSJr,PROMIS10,VR12} Total Assessment Completed` = `[]` for all 58 cases, incl. fixtures that satisfy the logic); CMS131 DenExcl corroboration | engine | **Confirmed** | CMS56, CMS131 | 0 |
| E-18 | Raw `FHIR.dateTime` returned from a define feeding `sort` and a mixed-type `Interval` endpoint throws `"Values FHIR.dateTime and FHIR.dateTime are not comparable"` (CMS156 Index Prescription Start Date — the post-E-13 reappearance of the E-01/E-02 family) | engine | **Confirmed** | CMS156 | 0 |
| M-05 | `AHAOverall.cql` Choice narrowing dropped `ConditionProblemsHealthConcerns` support (CMS144) | migration | Not fixed | CMS144 | 0 |
| C-02 | CMS157 — Cancer diagnosis coded in ICD-10-CM vs SNOMED-only valueset | content | Not fixed | CMS157 | 0 |


| Discrepancy Summary | Measure Count | Test Case Count |
|---|:---:|:---:|
| Missing Results | 6 | 14 |
| Missing Populations | 0 | 0 |
| Mismatched Test Cases | 38 | 845 |



_Note: Measures can have multiple discrepancies, so the Measures with Discrepancies count may not match the summary counts._
## CMS vs QICore Comparison

| Measure | CMS Pass / Fail | QICore Pass / Fail | Notes |
|---|:---:|:---:|---|
| CMS2FHIRPCSDepScreenAndFollowUp | 172 / 8 | 173 / 7 | Both have discrepancies |
| CMS22FHIRPCSBPScreeningFollowUp | 206 / 14 | 220 / 0 | CMS has discrepancies, QICore passes |
| CMS50FHIRReceiptofSpecialistReport | 99 / 0 | 99 / 0 | Match — both pass |
| CMS56FHIRFuncStatHipReplacement | 232 / 0 | 232 / 0 | Match — both pass |
| CMS68FHIRDocumentationCurrentMeds | 72 / 4 | 76 / 0 | CMS has discrepancies, QICore passes |
| CMS69FHIRPCSBMIScreenAndFollowUp | 260 / 55 | 315 / 0 | CMS has discrepancies, QICore passes |
| CMS71FHIRSTKAnticoagAFFlutter | 399 / 16 | 411 / 4 | Both have discrepancies |
| CMS72FHIRSTKAntithromboticDay2 | 768 / 22 | 532 / 258 | Both have discrepancies |
| CMS74FHIRDentalCariesPrevention | 80 / 0 | 80 / 0 | Match — both pass |
| CMS75FHIRChildrenDentalDecay | 80 / 0 | 80 / 0 | Match — both pass |
| CMS90FHIRFSAforHeartFailure | 148 / 0 | 148 / 0 | Match — both pass |
| CMS104FHIRSTKDCAntithrombotic | 372 / 38 | 225 / 185 | Both have discrepancies |
| CMS108FHIRVTEProphylaxis | 536 / 24 | 548 / 12 | Both have discrepancies |
| CMS117FHIRChildImmunStatus | 180 / 0 | 180 / 0 | Match — both pass |
| CMS122FHIRDiabetesAssessGT9Pct | 169 / 51 | 208 / 12 | Both have discrepancies |
| CMS124FHIRCervicalCancerScreen | 118 / 18 | 136 / 0 | CMS has discrepancies, QICore passes |
| CMS125FHIRBreastCancerScreen | 237 / 27 | 256 / 8 | Both have discrepancies |
| CMS128FHIRAntidepressantMgmt | 232 / 0 | 98 / 134 | CMS passes, QICore has discrepancies |
| CMS129FHIRProstCaBoneScanUse | 204 / 0 | 204 / 0 | Match — both pass |
| CMS130FHIRColorectalCancerScrn | 211 / 45 | 255 / 1 | Both have discrepancies |
| CMS131FHIRDiabetesEyeExam | 211 / 41 | 246 / 6 | Both have discrepancies |
| CMS133FHIRCataracts2040BCVA90Days | 292 / 0 | 292 / 0 | Match — both pass |
| CMS135FHIRACEIorARBorARNIforHF | 170 / 30 | 185 / 15 | Both have discrepancies |
| CMS136FHIRChildADHDMedFollowUp | 512 / 0 | 507 / 5 | CMS passes, QICore has discrepancies |
| CMS137FHIRSUDTxInitEngagement | 360 / 0 | 360 / 0 | Match — both pass |
| CMS138FHIRTobaccoScrnCessation | 564 / 0 | 564 / 0 | Match — both pass |
| CMS139FHIRFallRiskScreening | 116 / 0 | 116 / 0 | Match — both pass |
| CMS142FHIRCommWithDrManagingDiab | 123 / 5 | 123 / 5 | Both have discrepancies |
| CMS143FHIRPOAGOpticNerveEval | 128 / 0 | 128 / 0 | Match — both pass |
| CMS144FHIRHFBetaBlockerForLVSD | 235 / 5 | 240 / 0 | CMS has discrepancies, QICore passes |
| CMS145FHIRCADBBlockerTPMIorLVSD | 418 / 6 | 422 / 2 | Both have discrepancies |
| CMS146FHIRApproTestPharyngitis | 152 / 0 | 152 / 0 | Match — both pass |
| CMS149FHIRDementiaCognitiveAssess | 132 / 0 | 132 / 0 | Match — both pass |
| CMS153FHIRChlamydiaScreening | 128 / 0 | 126 / 2 | CMS passes, QICore has discrepancies |
| CMS154FHIRAppropriateTxforURI | 132 / 0 | 132 / 0 | Match — both pass |
| CMS155FHIRWgtAssessCounseling | 408 / 0 | 408 / 0 | Match — both pass |
| CMS156FHIRHighRiskMedsElderly | 509 / 199 | 702 / 6 | Both have discrepancies |
| CMS157FHIRPainIntensityQuantified | 332 / 46 | 332 / 46 | Both have discrepancies |
| CMS159FHIRDepRemissionat12Months | 264 / 4 | 264 / 4 | Both have discrepancies |
| CMS165FHIRControllingHighBP | 237 / 35 | 259 / 13 | Both have discrepancies |
| CMS177FHIRChildMDDSuicideAssmt | 121 / 2 | 123 / 0 | CMS has discrepancies, QICore passes |
| CMS190FHIRVTEProphylaxisICU | 600 / 25 | 613 / 12 | Both have discrepancies |
| CMS314FHIRHIVViralSuppression | 129 / 0 | 129 / 0 | Match — both pass |
| CMS0334FHIRPCCesareanBirth | 550 / 2 | 550 / 2 | Both have discrepancies |
| CMS347FHIRStatinPreventionTxCVD | 3486 / 274 | 3705 / 55 | Both have discrepancies |
| CMS349FHIRHIVScreening | 180 / 0 | 180 / 0 | Match — both pass |
| CMS506FHIRSafeUseofOpioids | 204 / 0 | 204 / 0 | Match — both pass |
| CMSFHIR529HybridHospitalWideReadmission | 1 / 0 | 1 / 0 | Match — both pass |
| CMS645FHIRBoneDensityPCADTherapy | 199 / 5 | 204 / 0 | CMS has discrepancies, QICore passes |
| CMS646FHIRIntravesicalBCGTherapy | 182 / 8 | 188 / 2 | Both have discrepancies |
| CMS771FHIRUrinarySymptomScoreBPH | 117 / 7 | 124 / 0 | CMS has discrepancies, QICore passes |
| CMS816FHIRHHHypo | 57 / 27 | 57 / 27 | Both have discrepancies |
| CMS819FHIRHHORAE | 81 / 3 | 81 / 3 | Both have discrepancies |
| CMS826FHIRHHPI | 36 / 0 | 36 / 0 | Match — both pass |
| CMS832FHIRHHAKI | 148 / 0 | 148 / 0 | Match — both pass |
| CMSFHIR844HybridHospitalWideMortality | 8 / 2 | 8 / 2 | Both have discrepancies |
| CMS871FHIRHHHyper | 110 / 20 | 105 / 25 | Both have discrepancies |
| CMS951FHIRKidneyHealthEval | 187 / 33 | 220 / 0 | CMS has discrepancies, QICore passes |
| CMS986FHIRMalnutritionScore | 2622 / 6 | 2628 / 0 | CMS has discrepancies, QICore passes |
| CMS996FHIRAptTxforSTEMI | 563 / 7 | 568 / 2 | Both have discrepancies |
| CMS1017FHIRHHFI | 323 / 2 | 323 / 2 | Both have discrepancies |
| CMS1028FHIRPCSevereOBComps | 1126 / 2 | 1124 / 4 | Both have discrepancies |
| CMS1056FHIRCTClinical | 40 / 0 | 40 / 0 | Match — both pass |
| CMS1074FHIRCTIQR | 40 / 0 | 40 / 0 | Match — both pass |
| CMS1154ScreeningPrediabetesFHIR | 39 / 1 | 39 / 1 | Both have discrepancies |
| CMS1157FHIRHIVRetention | 81 / 0 | 81 / 0 | Match — both pass |
| CMS1173FHIRDiagnosticDelayVTE | 260 / 0 | 260 / 0 | Match — both pass |
| CMS1188FHIRHIVSTITesting | 102 / 0 | 102 / 0 | Match — both pass |
| CMS1206FHIRCTOQR | 40 / 0 | 40 / 0 | Match — both pass |
| CMS1218FHIRHHRF | 274 / 2 | 274 / 2 | Both have discrepancies |
| CMS1244FHIRECATHOQR | 216 / 0 | 216 / 0 | Match — both pass |
| CMS1264FHIRECATREHQR | 22 / 152 | 22 / 152 | Both have discrepancies |
| NHSNAcuteCareHospitalMonthlyInitialPopulation1 | 27 / 0 | 0 / 27 | CMS passes, QICore has discrepancies |
| NHSNGlycemicControlHypoglycemiaInitialPopulation | 80 / 0 | 79 / 1 | CMS passes, QICore has discrepancies |


## Measures with No Discrepancies

### CMS Measures (34)
- CMS50FHIRReceiptofSpecialistReport [ [cql] ](../../input/cql/CMS50FHIRReceiptofSpecialistReport.cql) [ [test results] ](../../input/tests/results/CMS50FHIRReceiptofSpecialistReport.txt) — matches QICore
- CMS56FHIRFuncStatHipReplacement [ [cql] ](../../input/cql/CMS56FHIRFuncStatHipReplacement.cql) [ [test results] ](../../input/tests/results/CMS56FHIRFuncStatHipReplacement.txt) — matches QICore
- CMS74FHIRDentalCariesPrevention [ [cql] ](../../input/cql/CMS74FHIRDentalCariesPrevention.cql) [ [test results] ](../../input/tests/results/CMS74FHIRDentalCariesPrevention.txt) — matches QICore
- CMS75FHIRChildrenDentalDecay [ [cql] ](../../input/cql/CMS75FHIRChildrenDentalDecay.cql) [ [test results] ](../../input/tests/results/CMS75FHIRChildrenDentalDecay.txt) — matches QICore
- CMS90FHIRFSAforHeartFailure [ [cql] ](../../input/cql/CMS90FHIRFSAforHeartFailure.cql) [ [test results] ](../../input/tests/results/CMS90FHIRFSAforHeartFailure.txt) — matches QICore
- CMS117FHIRChildImmunStatus [ [cql] ](../../input/cql/CMS117FHIRChildImmunStatus.cql) [ [test results] ](../../input/tests/results/CMS117FHIRChildImmunStatus.txt) — matches QICore
- CMS128FHIRAntidepressantMgmt [ [cql] ](../../input/cql/CMS128FHIRAntidepressantMgmt.cql) [ [test results] ](../../input/tests/results/CMS128FHIRAntidepressantMgmt.txt) — QICore has discrepancies
- CMS129FHIRProstCaBoneScanUse [ [cql] ](../../input/cql/CMS129FHIRProstCaBoneScanUse.cql) [ [test results] ](../../input/tests/results/CMS129FHIRProstCaBoneScanUse.txt) — matches QICore
- CMS133FHIRCataracts2040BCVA90Days [ [cql] ](../../input/cql/CMS133FHIRCataracts2040BCVA90Days.cql) [ [test results] ](../../input/tests/results/CMS133FHIRCataracts2040BCVA90Days.txt) — matches QICore
- CMS136FHIRChildADHDMedFollowUp [ [cql] ](../../input/cql/CMS136FHIRChildADHDMedFollowUp.cql) [ [test results] ](../../input/tests/results/CMS136FHIRChildADHDMedFollowUp.txt) — QICore has discrepancies
- CMS137FHIRSUDTxInitEngagement [ [cql] ](../../input/cql/CMS137FHIRSUDTxInitEngagement.cql) [ [test results] ](../../input/tests/results/CMS137FHIRSUDTxInitEngagement.txt) — matches QICore
- CMS138FHIRTobaccoScrnCessation [ [cql] ](../../input/cql/CMS138FHIRTobaccoScrnCessation.cql) [ [test results] ](../../input/tests/results/CMS138FHIRTobaccoScrnCessation.txt) — matches QICore
- CMS139FHIRFallRiskScreening [ [cql] ](../../input/cql/CMS139FHIRFallRiskScreening.cql) [ [test results] ](../../input/tests/results/CMS139FHIRFallRiskScreening.txt) — matches QICore
- CMS143FHIRPOAGOpticNerveEval [ [cql] ](../../input/cql/CMS143FHIRPOAGOpticNerveEval.cql) [ [test results] ](../../input/tests/results/CMS143FHIRPOAGOpticNerveEval.txt) — matches QICore
- CMS146FHIRApproTestPharyngitis [ [cql] ](../../input/cql/CMS146FHIRApproTestPharyngitis.cql) [ [test results] ](../../input/tests/results/CMS146FHIRApproTestPharyngitis.txt) — matches QICore
- CMS149FHIRDementiaCognitiveAssess [ [cql] ](../../input/cql/CMS149FHIRDementiaCognitiveAssess.cql) [ [test results] ](../../input/tests/results/CMS149FHIRDementiaCognitiveAssess.txt) — matches QICore
- CMS153FHIRChlamydiaScreening [ [cql] ](../../input/cql/CMS153FHIRChlamydiaScreening.cql) [ [test results] ](../../input/tests/results/CMS153FHIRChlamydiaScreening.txt) — QICore has discrepancies
- CMS154FHIRAppropriateTxforURI [ [cql] ](../../input/cql/CMS154FHIRAppropriateTxforURI.cql) [ [test results] ](../../input/tests/results/CMS154FHIRAppropriateTxforURI.txt) — matches QICore
- CMS155FHIRWgtAssessCounseling [ [cql] ](../../input/cql/CMS155FHIRWgtAssessCounseling.cql) [ [test results] ](../../input/tests/results/CMS155FHIRWgtAssessCounseling.txt) — matches QICore
- CMS314FHIRHIVViralSuppression [ [cql] ](../../input/cql/CMS314FHIRHIVViralSuppression.cql) [ [test results] ](../../input/tests/results/CMS314FHIRHIVViralSuppression.txt) — matches QICore
- CMS349FHIRHIVScreening [ [cql] ](../../input/cql/CMS349FHIRHIVScreening.cql) [ [test results] ](../../input/tests/results/CMS349FHIRHIVScreening.txt) — matches QICore
- CMS506FHIRSafeUseofOpioids [ [cql] ](../../input/cql/CMS506FHIRSafeUseofOpioids.cql) [ [test results] ](../../input/tests/results/CMS506FHIRSafeUseofOpioids.txt) — matches QICore
- CMSFHIR529HybridHospitalWideReadmission [ [cql] ](../../input/cql/CMSFHIR529HybridHospitalWideReadmission.cql) [ [test results] ](../../input/tests/results/CMSFHIR529HybridHospitalWideReadmission.txt) — matches QICore
- CMS826FHIRHHPI [ [cql] ](../../input/cql/CMS826FHIRHHPI.cql) [ [test results] ](../../input/tests/results/CMS826FHIRHHPI.txt) — matches QICore
- CMS832FHIRHHAKI [ [cql] ](../../input/cql/CMS832FHIRHHAKI.cql) [ [test results] ](../../input/tests/results/CMS832FHIRHHAKI.txt) — matches QICore
- CMS1056FHIRCTClinical [ [cql] ](../../input/cql/CMS1056FHIRCTClinical.cql) [ [test results] ](../../input/tests/results/CMS1056FHIRCTClinical.txt) — matches QICore
- CMS1074FHIRCTIQR [ [cql] ](../../input/cql/CMS1074FHIRCTIQR.cql) [ [test results] ](../../input/tests/results/CMS1074FHIRCTIQR.txt) — matches QICore
- CMS1157FHIRHIVRetention [ [cql] ](../../input/cql/CMS1157FHIRHIVRetention.cql) [ [test results] ](../../input/tests/results/CMS1157FHIRHIVRetention.txt) — matches QICore
- CMS1173FHIRDiagnosticDelayVTE [ [cql] ](../../input/cql/CMS1173FHIRDiagnosticDelayVTE.cql) [ [test results] ](../../input/tests/results/CMS1173FHIRDiagnosticDelayVTE.txt) — matches QICore
- CMS1188FHIRHIVSTITesting [ [cql] ](../../input/cql/CMS1188FHIRHIVSTITesting.cql) [ [test results] ](../../input/tests/results/CMS1188FHIRHIVSTITesting.txt) — matches QICore
- CMS1206FHIRCTOQR [ [cql] ](../../input/cql/CMS1206FHIRCTOQR.cql) [ [test results] ](../../input/tests/results/CMS1206FHIRCTOQR.txt) — matches QICore
- CMS1244FHIRECATHOQR [ [cql] ](../../input/cql/CMS1244FHIRECATHOQR.cql) [ [test results] ](../../input/tests/results/CMS1244FHIRECATHOQR.txt) — matches QICore
- NHSNAcuteCareHospitalMonthlyInitialPopulation1 [ [cql] ](../../input/cql/NHSNAcuteCareHospitalMonthlyInitialPopulation1.cql) [ [test results] ](../../input/tests/results/NHSNAcuteCareHospitalMonthlyInitialPopulation1.txt) — QICore has discrepancies
- NHSNGlycemicControlHypoglycemiaInitialPopulation [ [cql] ](../../input/cql/NHSNGlycemicControlHypoglycemiaInitialPopulation.cql) [ [test results] ](../../input/tests/results/NHSNGlycemicControlHypoglycemiaInitialPopulation.txt) — QICore has discrepancies

### QICore Measures (39)
- CMS22FHIRPCSBPScreeningFollowUp [ [cql] ](../../input/cql/CMS22FHIRPCSBPScreeningFollowUp.cql) [ [test results] ](../../input/tests/results/CMS22FHIRPCSBPScreeningFollowUp.txt) — CMS has discrepancies
- CMS50FHIRReceiptofSpecialistReport [ [cql] ](../../input/cql/CMS50FHIRReceiptofSpecialistReport.cql) [ [test results] ](../../input/tests/results/CMS50FHIRReceiptofSpecialistReport.txt) — also passes in CMS
- CMS56FHIRFuncStatHipReplacement [ [cql] ](../../input/cql/CMS56FHIRFuncStatHipReplacement.cql) [ [test results] ](../../input/tests/results/CMS56FHIRFuncStatHipReplacement.txt) — also passes in CMS
- CMS68FHIRDocumentationCurrentMeds [ [cql] ](../../input/cql/CMS68FHIRDocumentationCurrentMeds.cql) [ [test results] ](../../input/tests/results/CMS68FHIRDocumentationCurrentMeds.txt) — CMS has discrepancies
- CMS69FHIRPCSBMIScreenAndFollowUp [ [cql] ](../../input/cql/CMS69FHIRPCSBMIScreenAndFollowUp.cql) [ [test results] ](../../input/tests/results/CMS69FHIRPCSBMIScreenAndFollowUp.txt) — CMS has discrepancies
- CMS74FHIRDentalCariesPrevention [ [cql] ](../../input/cql/CMS74FHIRDentalCariesPrevention.cql) [ [test results] ](../../input/tests/results/CMS74FHIRDentalCariesPrevention.txt) — also passes in CMS
- CMS75FHIRChildrenDentalDecay [ [cql] ](../../input/cql/CMS75FHIRChildrenDentalDecay.cql) [ [test results] ](../../input/tests/results/CMS75FHIRChildrenDentalDecay.txt) — also passes in CMS
- CMS90FHIRFSAforHeartFailure [ [cql] ](../../input/cql/CMS90FHIRFSAforHeartFailure.cql) [ [test results] ](../../input/tests/results/CMS90FHIRFSAforHeartFailure.txt) — also passes in CMS
- CMS117FHIRChildImmunStatus [ [cql] ](../../input/cql/CMS117FHIRChildImmunStatus.cql) [ [test results] ](../../input/tests/results/CMS117FHIRChildImmunStatus.txt) — also passes in CMS
- CMS124FHIRCervicalCancerScreen [ [cql] ](../../input/cql/CMS124FHIRCervicalCancerScreen.cql) [ [test results] ](../../input/tests/results/CMS124FHIRCervicalCancerScreen.txt) — CMS has discrepancies
- CMS129FHIRProstCaBoneScanUse [ [cql] ](../../input/cql/CMS129FHIRProstCaBoneScanUse.cql) [ [test results] ](../../input/tests/results/CMS129FHIRProstCaBoneScanUse.txt) — also passes in CMS
- CMS133FHIRCataracts2040BCVA90Days [ [cql] ](../../input/cql/CMS133FHIRCataracts2040BCVA90Days.cql) [ [test results] ](../../input/tests/results/CMS133FHIRCataracts2040BCVA90Days.txt) — also passes in CMS
- CMS137FHIRSUDTxInitEngagement [ [cql] ](../../input/cql/CMS137FHIRSUDTxInitEngagement.cql) [ [test results] ](../../input/tests/results/CMS137FHIRSUDTxInitEngagement.txt) — also passes in CMS
- CMS138FHIRTobaccoScrnCessation [ [cql] ](../../input/cql/CMS138FHIRTobaccoScrnCessation.cql) [ [test results] ](../../input/tests/results/CMS138FHIRTobaccoScrnCessation.txt) — also passes in CMS
- CMS139FHIRFallRiskScreening [ [cql] ](../../input/cql/CMS139FHIRFallRiskScreening.cql) [ [test results] ](../../input/tests/results/CMS139FHIRFallRiskScreening.txt) — also passes in CMS
- CMS143FHIRPOAGOpticNerveEval [ [cql] ](../../input/cql/CMS143FHIRPOAGOpticNerveEval.cql) [ [test results] ](../../input/tests/results/CMS143FHIRPOAGOpticNerveEval.txt) — also passes in CMS
- CMS144FHIRHFBetaBlockerForLVSD [ [cql] ](../../input/cql/CMS144FHIRHFBetaBlockerForLVSD.cql) [ [test results] ](../../input/tests/results/CMS144FHIRHFBetaBlockerForLVSD.txt) — CMS has discrepancies
- CMS146FHIRApproTestPharyngitis [ [cql] ](../../input/cql/CMS146FHIRApproTestPharyngitis.cql) [ [test results] ](../../input/tests/results/CMS146FHIRApproTestPharyngitis.txt) — also passes in CMS
- CMS149FHIRDementiaCognitiveAssess [ [cql] ](../../input/cql/CMS149FHIRDementiaCognitiveAssess.cql) [ [test results] ](../../input/tests/results/CMS149FHIRDementiaCognitiveAssess.txt) — also passes in CMS
- CMS154FHIRAppropriateTxforURI [ [cql] ](../../input/cql/CMS154FHIRAppropriateTxforURI.cql) [ [test results] ](../../input/tests/results/CMS154FHIRAppropriateTxforURI.txt) — also passes in CMS
- CMS155FHIRWgtAssessCounseling [ [cql] ](../../input/cql/CMS155FHIRWgtAssessCounseling.cql) [ [test results] ](../../input/tests/results/CMS155FHIRWgtAssessCounseling.txt) — also passes in CMS
- CMS177FHIRChildMDDSuicideAssmt [ [cql] ](../../input/cql/CMS177FHIRChildMDDSuicideAssmt.cql) [ [test results] ](../../input/tests/results/CMS177FHIRChildMDDSuicideAssmt.txt) — CMS has discrepancies
- CMS314FHIRHIVViralSuppression [ [cql] ](../../input/cql/CMS314FHIRHIVViralSuppression.cql) [ [test results] ](../../input/tests/results/CMS314FHIRHIVViralSuppression.txt) — also passes in CMS
- CMS349FHIRHIVScreening [ [cql] ](../../input/cql/CMS349FHIRHIVScreening.cql) [ [test results] ](../../input/tests/results/CMS349FHIRHIVScreening.txt) — also passes in CMS
- CMS506FHIRSafeUseofOpioids [ [cql] ](../../input/cql/CMS506FHIRSafeUseofOpioids.cql) [ [test results] ](../../input/tests/results/CMS506FHIRSafeUseofOpioids.txt) — also passes in CMS
- CMSFHIR529HybridHospitalWideReadmission [ [cql] ](../../input/cql/CMSFHIR529HybridHospitalWideReadmission.cql) [ [test results] ](../../input/tests/results/CMSFHIR529HybridHospitalWideReadmission.txt) — also passes in CMS
- CMS645FHIRBoneDensityPCADTherapy [ [cql] ](../../input/cql/CMS645FHIRBoneDensityPCADTherapy.cql) [ [test results] ](../../input/tests/results/CMS645FHIRBoneDensityPCADTherapy.txt) — CMS has discrepancies
- CMS771FHIRUrinarySymptomScoreBPH [ [cql] ](../../input/cql/CMS771FHIRUrinarySymptomScoreBPH.cql) [ [test results] ](../../input/tests/results/CMS771FHIRUrinarySymptomScoreBPH.txt) — CMS has discrepancies
- CMS826FHIRHHPI [ [cql] ](../../input/cql/CMS826FHIRHHPI.cql) [ [test results] ](../../input/tests/results/CMS826FHIRHHPI.txt) — also passes in CMS
- CMS832FHIRHHAKI [ [cql] ](../../input/cql/CMS832FHIRHHAKI.cql) [ [test results] ](../../input/tests/results/CMS832FHIRHHAKI.txt) — also passes in CMS
- CMS951FHIRKidneyHealthEval [ [cql] ](../../input/cql/CMS951FHIRKidneyHealthEval.cql) [ [test results] ](../../input/tests/results/CMS951FHIRKidneyHealthEval.txt) — CMS has discrepancies
- CMS986FHIRMalnutritionScore [ [cql] ](../../input/cql/CMS986FHIRMalnutritionScore.cql) [ [test results] ](../../input/tests/results/CMS986FHIRMalnutritionScore.txt) — CMS has discrepancies
- CMS1056FHIRCTClinical [ [cql] ](../../input/cql/CMS1056FHIRCTClinical.cql) [ [test results] ](../../input/tests/results/CMS1056FHIRCTClinical.txt) — also passes in CMS
- CMS1074FHIRCTIQR [ [cql] ](../../input/cql/CMS1074FHIRCTIQR.cql) [ [test results] ](../../input/tests/results/CMS1074FHIRCTIQR.txt) — also passes in CMS
- CMS1157FHIRHIVRetention [ [cql] ](../../input/cql/CMS1157FHIRHIVRetention.cql) [ [test results] ](../../input/tests/results/CMS1157FHIRHIVRetention.txt) — also passes in CMS
- CMS1173FHIRDiagnosticDelayVTE [ [cql] ](../../input/cql/CMS1173FHIRDiagnosticDelayVTE.cql) [ [test results] ](../../input/tests/results/CMS1173FHIRDiagnosticDelayVTE.txt) — also passes in CMS
- CMS1188FHIRHIVSTITesting [ [cql] ](../../input/cql/CMS1188FHIRHIVSTITesting.cql) [ [test results] ](../../input/tests/results/CMS1188FHIRHIVSTITesting.txt) — also passes in CMS
- CMS1206FHIRCTOQR [ [cql] ](../../input/cql/CMS1206FHIRCTOQR.cql) [ [test results] ](../../input/tests/results/CMS1206FHIRCTOQR.txt) — also passes in CMS
- CMS1244FHIRECATHOQR [ [cql] ](../../input/cql/CMS1244FHIRECATHOQR.cql) [ [test results] ](../../input/tests/results/CMS1244FHIRECATHOQR.txt) — also passes in CMS
## Measures with Discrepancies (40)
| Measure | Total Test Cases | Missing Results | Missing Populations | Mismatched Test Cases | QICore Pass / Fail | QICore Status |
|---|:---:|:---:|:---:|:---:|:---:|---|
| [CMS2FHIRPCSDepScreenAndFollowUp](#cms2fhirpcsdepscreenandfollowup) | 36 | 0 | 0 | 22.22%   (8) | 173 / 7 | has discrepancies (7) |
| [CMS22FHIRPCSBPScreeningFollowUp](#cms22fhirpcsbpscreeningfollowup) | 44 | 0 | 0 | 27.27%   (12) | 220 / 0 | passes |
| [CMS68FHIRDocumentationCurrentMeds](#cms68fhirdocumentationcurrentmeds) | 19 | 1 | 0 | 0.00%   (0) | 76 / 0 | passes |
| [CMS69FHIRPCSBMIScreenAndFollowUp](#cms69fhirpcsbmiscreenandfollowup) | 63 | 0 | 0 | 61.90%   (39) | 315 / 0 | passes |
| [CMS71FHIRSTKAnticoagAFFlutter](#cms71fhirstkanticoagafflutter) | 83 | 0 | 0 | 9.64%   (8) | 411 / 4 | has discrepancies (2) |
| [CMS72FHIRSTKAntithromboticDay2](#cms72fhirstkantithromboticday2) | 158 | 0 | 0 | 8.23%   (13) | 532 / 258 | has discrepancies (98) |
| [CMS104FHIRSTKDCAntithrombotic](#cms104fhirstkdcantithrombotic) | 82 | 0 | 0 | 18.29%   (15) | 225 / 185 | has discrepancies (69) |
| [CMS108FHIRVTEProphylaxis](#cms108fhirvteprophylaxis) | 140 | 0 | 0 | 17.14%   (24) | 548 / 12 | has discrepancies (12) |
| [CMS122FHIRDiabetesAssessGT9Pct](#cms122fhirdiabetesassessgt9pct) | 55 | 0 | 0 | 47.27%   (26) | 208 / 12 | has discrepancies (6) |
| [CMS124FHIRCervicalCancerScreen](#cms124fhircervicalcancerscreen) | 34 | 0 | 0 | 41.18%   (14) | 136 / 0 | passes |
| [CMS125FHIRBreastCancerScreen](#cms125fhirbreastcancerscreen) | 66 | 0 | 0 | 37.88%   (25) | 256 / 8 | has discrepancies (8) |
| [CMS130FHIRColorectalCancerScrn](#cms130fhircolorectalcancerscrn) | 64 | 0 | 0 | 57.81%   (37) | 255 / 1 | has discrepancies (1) |
| [CMS131FHIRDiabetesEyeExam](#cms131fhirdiabeteseyeexam) | 63 | 0 | 0 | 49.21%   (31) | 246 / 6 | has discrepancies (6) |
| [CMS135FHIRACEIorARBorARNIforHF](#cms135fhiraceiorarborarniforhf) | 40 | 3 | 0 | 22.50%   (9) | 185 / 15 | has discrepancies (0) |
| [CMS142FHIRCommWithDrManagingDiab](#cms142fhircommwithdrmanagingdiab) | 32 | 0 | 0 | 15.62%   (5) | 123 / 5 | has discrepancies (5) |
| [CMS144FHIRHFBetaBlockerForLVSD](#cms144fhirhfbetablockerforlvsd) | 48 | 0 | 0 | 6.25%   (3) | 240 / 0 | passes |
| [CMS145FHIRCADBBlockerTPMIorLVSD](#cms145fhircadbblockertpmiorlvsd) | 106 | 0 | 0 | 5.66%   (6) | 422 / 2 | has discrepancies (2) |
| [CMS156FHIRHighRiskMedsElderly](#cms156fhirhighriskmedselderly) | 177 | 0 | 0 | 76.27%   (135) | 702 / 6 | has discrepancies (6) |
| [CMS157FHIRPainIntensityQuantified](#cms157fhirpainintensityquantified) | 126 | 0 | 0 | 15.08%   (19) | 332 / 46 | has discrepancies (19) |
| [CMS159FHIRDepRemissionat12Months](#cms159fhirdepremissionat12months) | 67 | 0 | 0 | 2.99%   (2) | 264 / 4 | has discrepancies (2) |
| [CMS165FHIRControllingHighBP](#cms165fhircontrollinghighbp) | 68 | 1 | 0 | 42.65%   (29) | 259 / 13 | has discrepancies (9) |
| [CMS177FHIRChildMDDSuicideAssmt](#cms177fhirchildmddsuicideassmt) | 41 | 0 | 0 | 2.44%   (1) | 123 / 0 | passes |
| [CMS190FHIRVTEProphylaxisICU](#cms190fhirvteprophylaxisicu) | 125 | 0 | 0 | 19.20%   (24) | 613 / 12 | has discrepancies (11) |
| [CMS0334FHIRPCCesareanBirth](#cms0334fhirpccesareanbirth) | 138 | 0 | 0 | 0.72%   (1) | 550 / 2 | has discrepancies (1) |
| [CMS347FHIRStatinPreventionTxCVD](#cms347fhirstatinpreventiontxcvd) | 752 | 4 | 0 | 30.59%   (230) | 3705 / 55 | has discrepancies (13) |
| [CMS645FHIRBoneDensityPCADTherapy](#cms645fhirbonedensitypcadtherapy) | 51 | 0 | 0 | 5.88%   (3) | 204 / 0 | passes |
| [CMS646FHIRIntravesicalBCGTherapy](#cms646fhirintravesicalbcgtherapy) | 38 | 1 | 0 | 7.89%   (3) | 188 / 2 | has discrepancies (2) |
| [CMS771FHIRUrinarySymptomScoreBPH](#cms771fhirurinarysymptomscorebph) | 31 | 0 | 0 | 22.58%   (7) | 124 / 0 | passes |
| [CMS816FHIRHHHypo](#cms816fhirhhhypo) | 28 | 0 | 0 | 42.86%   (12) | 57 / 27 | has discrepancies (12) |
| [CMS819FHIRHHORAE](#cms819fhirhhorae) | 28 | 0 | 0 | 7.14%   (2) | 81 / 3 | has discrepancies (2) |
| [CMSFHIR844HybridHospitalWideMortality](#cmsfhir844hybridhospitalwidemortality) | 10 | 0 | 0 | 20.00%   (2) | 8 / 2 | has discrepancies (2) |
| [CMS871FHIRHHHyper](#cms871fhirhhhyper) | 26 | 4 | 0 | 0.00%   (0) | 105 / 25 | has discrepancies (0) |
| [CMS951FHIRKidneyHealthEval](#cms951fhirkidneyhealtheval) | 55 | 0 | 0 | 43.64%   (24) | 220 / 0 | passes |
| [CMS986FHIRMalnutritionScore](#cms986fhirmalnutritionscore) | 876 | 0 | 0 | 0.68%   (6) | 2628 / 0 | passes |
| [CMS996FHIRAptTxforSTEMI](#cms996fhirapttxforstemi) | 114 | 0 | 0 | 6.14%   (7) | 568 / 2 | has discrepancies (2) |
| [CMS1017FHIRHHFI](#cms1017fhirhhfi) | 65 | 0 | 0 | 3.08%   (2) | 323 / 2 | has discrepancies (2) |
| [CMS1028FHIRPCSevereOBComps](#cms1028fhirpcsevereobcomps) | 282 | 0 | 0 | 0.71%   (2) | 1124 / 4 | has discrepancies (4) |
| [CMS1154ScreeningPrediabetesFHIR](#cms1154screeningprediabetesfhir) | 10 | 0 | 0 | 10.00%   (1) | 39 / 1 | has discrepancies (1) |
| [CMS1218FHIRHHRF](#cms1218fhirhhrf) | 69 | 0 | 0 | 1.45%   (1) | 274 / 2 | has discrepancies (1) |
| [CMS1264FHIRECATREHQR](#cms1264fhirecatrehqr) | 58 | 0 | 0 | 98.28%   (57) | 22 / 152 | has discrepancies (57) |



#### CMS2FHIRPCSDepScreenAndFollowUp
[ [cql] ](../../input/cql/CMS2FHIRPCSDepScreenAndFollowUp.cql) [ [test results] ](../../input/tests/results/CMS2FHIRPCSDepScreenAndFollowUp.txt)

QICore: 173 / 7 — has discrepancies (7 mismatched, 0 missing)

Mismatched Test Cases (8 of  of 36)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 0e463fc3-d1bf-4e19-882b-fad6342aa668 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/0e463fc3-d1bf-4e19-882b-fad6342aa668/MeasureReport-38443362-8261-414c-80b3-1f719f4ba56e.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 12786a64-c20e-4542-a4c0-bf3129d6a9e0 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/12786a64-c20e-4542-a4c0-bf3129d6a9e0/MeasureReport-d404e2d0-2ded-4329-b254-482be8b54a7c.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 41df0dbe-ae84-4496-b355-320ff8707a85 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/41df0dbe-ae84-4496-b355-320ff8707a85/MeasureReport-922ffb7d-2d13-47b8-ad5d-4f42ff55f77d.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 6078e73e-3265-4022-ae63-216c096b6246 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/6078e73e-3265-4022-ae63-216c096b6246/MeasureReport-dfcfbb31-9da9-4947-8444-53a25c8b8121.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 6aaff09e-4a7b-4efa-93f8-13033e95c230 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/6aaff09e-4a7b-4efa-93f8-13033e95c230/MeasureReport-5981d1e2-7d0b-4887-aed2-884d0e7df4fe.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 86ca7528-efcb-44ed-9203-6f21f37f4332 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/86ca7528-efcb-44ed-9203-6f21f37f4332/MeasureReport-51f60250-c8a8-49d8-81c1-56b58ad0125f.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ d0ba1182-26fa-4cfa-9f91-960503b7fe53 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/d0ba1182-26fa-4cfa-9f91-960503b7fe53/MeasureReport-277359bb-b41c-4dd4-b1af-b3afdb6ee15d.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ f29e2786-fade-4dca-b14d-7037a34ef498 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/f29e2786-fade-4dca-b14d-7037a34ef498/MeasureReport-32baa107-7be1-4a64-a10d-1f25307962e6.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |


#### CMS22FHIRPCSBPScreeningFollowUp
[ [cql] ](../../input/cql/CMS22FHIRPCSBPScreeningFollowUp.cql) [ [test results] ](../../input/tests/results/CMS22FHIRPCSBPScreeningFollowUp.txt)

QICore: 220 / 0 — passes

Mismatched Test Cases (12 of  of 44)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 0278fdf0-f067-46e8-aeb1-fb96dff3c947 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/0278fdf0-f067-46e8-aeb1-fb96dff3c947/MeasureReport-064f5dc2-d804-4a03-a0c8-d0c25ae3b8fb.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 1f16120b-56c9-4d72-8dd4-01d8a0175d77 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/1f16120b-56c9-4d72-8dd4-01d8a0175d77/MeasureReport-b5acac31-18e7-4172-802f-041d29ba3da1.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 695cee04-cf12-411e-a258-99e430093a4e ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/695cee04-cf12-411e-a258-99e430093a4e/MeasureReport-e887022a-7961-4768-9cf3-e48ecfced710.json) | Group_1 | Denominator Exception | 2 | 0 | — | PASS |
| [ 86618b52-e0cc-4e90-b48c-cd64bbae8973 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/86618b52-e0cc-4e90-b48c-cd64bbae8973/MeasureReport-ad10338d-d04c-44de-badb-b69f01b20de5.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 9ed1ecf5-2d93-4bde-a293-5d5fbf209475 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/9ed1ecf5-2d93-4bde-a293-5d5fbf209475/MeasureReport-bd56dca9-e498-4ec5-bf78-c6322930e980.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ a55c6265-a05c-4fad-beb4-c5338420d1b1 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/a55c6265-a05c-4fad-beb4-c5338420d1b1/MeasureReport-a08e2374-4dea-4a09-8163-296239dcd454.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ ad737f80-c9ea-41fd-a142-78d9c80a9c7c ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/ad737f80-c9ea-41fd-a142-78d9c80a9c7c/MeasureReport-29212fe6-6c26-4e87-9711-8b5694567caa.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ afdeaa75-d332-40f2-9b30-0b6ddf7e7c14 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/afdeaa75-d332-40f2-9b30-0b6ddf7e7c14/MeasureReport-fcac6417-0a19-457d-a23b-b55bfb352064.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ c41f9946-cb0f-4489-8367-581a5b876165 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/c41f9946-cb0f-4489-8367-581a5b876165/MeasureReport-f183c739-a20c-4dcd-b12c-5c2cef29eaf5.json) | Group_1 | Denominator Exception<br>Numerator | 2<br>0 | 1<br>1 | — | PASS<br>PASS |
| [ dda022c0-3234-4ad7-ad6e-d696b0b57440 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/dda022c0-3234-4ad7-ad6e-d696b0b57440/MeasureReport-2b4791bc-bde7-4af7-9665-df0a21abc7b0.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ ef9a58ac-e252-480a-bed8-2309c503587d ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/ef9a58ac-e252-480a-bed8-2309c503587d/MeasureReport-292f318b-0b76-4666-9e3e-4b0d8c6924b2.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ f9417a57-54e8-4a0b-a516-ab62b8d4aae0 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/f9417a57-54e8-4a0b-a516-ab62b8d4aae0/MeasureReport-e90efb05-4493-4006-a537-3896b6bf37ba.json) | Group_1 | Denominator Exception<br>Numerator | 2<br>0 | 0<br>1 | — | PASS<br>PASS |


#### CMS68FHIRDocumentationCurrentMeds
[ [cql] ](../../input/cql/CMS68FHIRDocumentationCurrentMeds.cql) [ [test results] ](../../input/tests/results/CMS68FHIRDocumentationCurrentMeds.txt)

QICore: 76 / 0 — passes

Missing Results (1 of 19 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ f2e2e1c0-9e35-4592-9579-72a236cb2f56 ](../.././input/tests/measure/CMS68FHIRDocumentationCurrentMeds/f2e2e1c0-9e35-4592-9579-72a236cb2f56/MeasureReport-7384d607-6a08-487a-9129-d90036bae37e.json) | Group_1 | — |


#### CMS69FHIRPCSBMIScreenAndFollowUp
[ [cql] ](../../input/cql/CMS69FHIRPCSBMIScreenAndFollowUp.cql) [ [test results] ](../../input/tests/results/CMS69FHIRPCSBMIScreenAndFollowUp.txt)

QICore: 315 / 0 — passes

Mismatched Test Cases (39 of  of 63)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 0278fdf0-f067-46e8-aeb1-fb96dff3c947 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/0278fdf0-f067-46e8-aeb1-fb96dff3c947/MeasureReport-d4375950-775b-4267-a1b7-287b130ddba5.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 03f01144-2230-42ab-b81f-594e1c2baa62 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/03f01144-2230-42ab-b81f-594e1c2baa62/MeasureReport-33460c8c-b89d-48c3-9db3-1311fd8ffcfb.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 050201c2-c2c4-46e6-8288-a34f99caebdc ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/050201c2-c2c4-46e6-8288-a34f99caebdc/MeasureReport-9559c66c-9809-48eb-851c-26cc3e45434d.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 1102009b-6f05-4bab-9fd1-191e81cf50e8 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/1102009b-6f05-4bab-9fd1-191e81cf50e8/MeasureReport-74ca5bf1-866c-4f0e-bedf-4f9255ec0318.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 1ba2fc33-1a1b-416b-bb3c-79ba5d0d3359 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/1ba2fc33-1a1b-416b-bb3c-79ba5d0d3359/MeasureReport-adfc850a-59ae-456e-9d12-5e656e6b9296.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 1c607e84-c7c2-4dae-bf63-a75d7a9cfd38 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/1c607e84-c7c2-4dae-bf63-a75d7a9cfd38/MeasureReport-aedcd9ea-26a3-4939-825a-374d08741197.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 1e23fb8f-e27b-4553-a62a-f66edeb4528a ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/1e23fb8f-e27b-4553-a62a-f66edeb4528a/MeasureReport-5cdcf0c7-66f6-4c68-a90c-62ab758aa608.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 1f16120b-56c9-4d72-8dd4-01d8a0175d77 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/1f16120b-56c9-4d72-8dd4-01d8a0175d77/MeasureReport-fada34c0-c489-45ac-a167-b023e4172a30.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 260e1fc8-227f-4c16-bfc6-22625380a12c ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/260e1fc8-227f-4c16-bfc6-22625380a12c/MeasureReport-d350f52b-af0c-476e-bfce-9f21584bb736.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 27849d59-3cef-40bf-8338-a6ec7c0bcf81 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/27849d59-3cef-40bf-8338-a6ec7c0bcf81/MeasureReport-a46fc485-4122-45a5-b342-e0d722d0ab92.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 296d38e4-d69b-481e-a8cf-f7eee8b9e5d7 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/296d38e4-d69b-481e-a8cf-f7eee8b9e5d7/MeasureReport-b87a39fa-4b37-46ea-9fb8-bbcf0e13be3e.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 2a976bc2-493b-421f-842e-36d31463f261 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/2a976bc2-493b-421f-842e-36d31463f261/MeasureReport-220b4e0e-03d1-4e4a-933c-6df80d64f0eb.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 405d4940-7ab2-4d26-b55f-3c27e07eba33 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/405d4940-7ab2-4d26-b55f-3c27e07eba33/MeasureReport-734faae4-3bf4-4920-8d05-32f48d94061f.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 42e6b4d6-defc-4ec5-894f-e3333e3039a3 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/42e6b4d6-defc-4ec5-894f-e3333e3039a3/MeasureReport-35b5dc02-0f37-455c-8e85-6c353fc8f17c.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 461fdfab-fcc1-4630-9dae-2ba3a6ab0c25 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/461fdfab-fcc1-4630-9dae-2ba3a6ab0c25/MeasureReport-ef49c8ea-63d2-4cea-abb9-964d856db616.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 463dd868-997d-472f-962c-96383fd2a5c4 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/463dd868-997d-472f-962c-96383fd2a5c4/MeasureReport-0023b9fa-401a-4e0b-9298-b345b544d9a3.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 6553adbf-2a30-4861-97e6-cca7d2274f01 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/6553adbf-2a30-4861-97e6-cca7d2274f01/MeasureReport-65aeab54-df7f-4629-b35e-df187176b665.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 659f9c7b-5c1c-475f-bfcb-77c246fa7a28 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/659f9c7b-5c1c-475f-bfcb-77c246fa7a28/MeasureReport-3de4937e-ab6f-4569-9e1a-7e08a3cbb3d8.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 6d26d364-a06c-49e6-84df-280ec6b7a8a3 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/6d26d364-a06c-49e6-84df-280ec6b7a8a3/MeasureReport-c8fd1d24-1340-46a7-b8db-95a6ec5339c8.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 7902e3dc-f3da-4cc2-9f2e-4a6c8cd33b88 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/7902e3dc-f3da-4cc2-9f2e-4a6c8cd33b88/MeasureReport-ff7090ac-931d-4cc7-83f7-ee15beec8ed1.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 7ac9722f-8763-4380-a741-53ee4bb98819 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/7ac9722f-8763-4380-a741-53ee4bb98819/MeasureReport-9b0681a1-b58b-43b7-850e-4f12f07d5ca3.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8835a50b-0a0f-4e2f-94fa-7c180cd7f905 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/8835a50b-0a0f-4e2f-94fa-7c180cd7f905/MeasureReport-9219de61-d774-496c-a820-9602e651ce91.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 88a2b45a-7866-445a-8242-91ec0ebb7646 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/88a2b45a-7866-445a-8242-91ec0ebb7646/MeasureReport-1c37e1c3-e40e-4f12-9923-f55a376afd23.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8c89947a-a52b-4a41-86a8-166b0560355b ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/8c89947a-a52b-4a41-86a8-166b0560355b/MeasureReport-74b51720-f88f-4a78-a9c1-2208d37aec2c.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8e130410-9710-45f3-ac56-e69dee0755d9 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/8e130410-9710-45f3-ac56-e69dee0755d9/MeasureReport-bb3c35bb-3dbe-4d18-af54-379925bd9d54.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8e38b797-4dec-437d-8bf0-6f0fc78f8ea7 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/8e38b797-4dec-437d-8bf0-6f0fc78f8ea7/MeasureReport-93a73b49-b742-4d24-9f77-8f72e117110f.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 9d92be1d-6fc8-40f2-99a0-4be9ce1f244b ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/9d92be1d-6fc8-40f2-99a0-4be9ce1f244b/MeasureReport-071ef161-5f61-4057-8d9c-d1c378b1647e.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a0aacdbc-4954-48af-aa88-361ea7e32736 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/a0aacdbc-4954-48af-aa88-361ea7e32736/MeasureReport-16178a04-9fd7-4deb-b228-07bcdf6a4762.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a327cf96-81c4-46ff-9619-6fd9981bb90c ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/a327cf96-81c4-46ff-9619-6fd9981bb90c/MeasureReport-9a20d469-8187-45f2-8df5-7870accd9dae.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a4a1ed63-89ff-4d27-8819-136873e13171 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/a4a1ed63-89ff-4d27-8819-136873e13171/MeasureReport-a104e9cc-b70e-4378-9c2d-68b0ec109e21.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c1df0273-aad8-41a8-859c-edd204bb4f16 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/c1df0273-aad8-41a8-859c-edd204bb4f16/MeasureReport-abbbe154-ab3b-49d5-ad19-34e9c6cec72d.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c3caf126-12a2-473f-8f51-1c7828d63d16 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/c3caf126-12a2-473f-8f51-1c7828d63d16/MeasureReport-efbab239-c362-4ef2-b91b-49e234e8c5c4.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ c84bf29f-80ac-4bf0-beeb-404ba96a3fa8 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/c84bf29f-80ac-4bf0-beeb-404ba96a3fa8/MeasureReport-62e3506b-3f36-48ef-8a9a-69b9b6401c45.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ d3054ffa-e17b-4611-b7e0-4523fb0f9e1d ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/d3054ffa-e17b-4611-b7e0-4523fb0f9e1d/MeasureReport-9d596b56-44ad-48b7-9666-7b91ad3377d7.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d4d064be-d55a-47b5-9bfd-993afebd95a5 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/d4d064be-d55a-47b5-9bfd-993afebd95a5/MeasureReport-3cba3e58-4c3f-4f39-b0af-b52d69bda4b9.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ ddfb765a-a3fb-467f-a9d9-ac6faf4cea9a ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/ddfb765a-a3fb-467f-a9d9-ac6faf4cea9a/MeasureReport-cafadcdb-67de-4c29-b509-53ba98ce19a7.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ e0821eec-ff83-49e9-950d-9219dd3612b9 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/e0821eec-ff83-49e9-950d-9219dd3612b9/MeasureReport-712f56a5-5f65-428c-a73a-cf0d453d1302.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ f5ae6269-d09b-47f8-a519-f1a8a81549fc ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/f5ae6269-d09b-47f8-a519-f1a8a81549fc/MeasureReport-3d833783-caa1-4d2d-ae23-a8f2f6f31cc0.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ ff09cf1e-5b30-45c7-9cc6-d5daf48a3933 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/ff09cf1e-5b30-45c7-9cc6-d5daf48a3933/MeasureReport-81310130-2e1c-4d36-b2f1-d0d26fa6a24e.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |


#### CMS71FHIRSTKAnticoagAFFlutter
[ [cql] ](../../input/cql/CMS71FHIRSTKAnticoagAFFlutter.cql) [ [test results] ](../../input/tests/results/CMS71FHIRSTKAnticoagAFFlutter.txt)

QICore: 411 / 4 — has discrepancies (2 mismatched, 0 missing)

Mismatched Test Cases (8 of  of 83)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 017a2267-f463-47a6-8b8b-dc91465e0869 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/017a2267-f463-47a6-8b8b-dc91465e0869/MeasureReport-3a870421-64af-44eb-8c7a-533079bc2259.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>FAIL |
| [ 0587a75d-0dcc-4c6b-bfc0-f5727342ec1f ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/0587a75d-0dcc-4c6b-bfc0-f5727342ec1f/MeasureReport-c8a99645-6e7a-467b-87aa-456cdc7cafb9.json) | Group_1 | Denominator<br>Numerator | 1<br>1 | 0<br>0 | — | PASS<br>PASS |
| [ 56ae006d-ab1b-428d-8614-2ccd5d962650 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/56ae006d-ab1b-428d-8614-2ccd5d962650/MeasureReport-71b26a14-7533-4479-82e3-7bc54d9ce0db.json) | Group_1 | Denominator<br>Numerator | 1<br>1 | 0<br>0 | — | PASS<br>PASS |
| [ 595ebfd1-fe6a-4b4b-96a1-23a72f6a70da ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/595ebfd1-fe6a-4b4b-96a1-23a72f6a70da/MeasureReport-793a4c67-2bc9-4601-9521-999a2628ffdd.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 9a72ea26-595f-4442-8b00-fc52ed228aa6 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/9a72ea26-595f-4442-8b00-fc52ed228aa6/MeasureReport-47b2254f-ca43-470b-9229-eeb4071ba6e0.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>FAIL |
| [ b29204ac-96ce-4be0-90ad-ae8ecfa4f245 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/b29204ac-96ce-4be0-90ad-ae8ecfa4f245/MeasureReport-e5339c1c-c4cd-497b-97a1-ed9fb1a1bc2e.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ c640ff8f-5b2a-448e-85a2-e739af7a8dc4 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/c640ff8f-5b2a-448e-85a2-e739af7a8dc4/MeasureReport-8b1280e5-8c6d-48b1-ac5a-e4c07e338f56.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ e20b4e76-8523-43ab-abc2-a4f4137a84bb ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/e20b4e76-8523-43ab-abc2-a4f4137a84bb/MeasureReport-ce8fcdb9-f3ff-4f3f-a6cc-114d96185bcb.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |


#### CMS72FHIRSTKAntithromboticDay2
[ [cql] ](../../input/cql/CMS72FHIRSTKAntithromboticDay2.cql) [ [test results] ](../../input/tests/results/CMS72FHIRSTKAntithromboticDay2.txt)

QICore: 532 / 258 — has discrepancies (98 mismatched, 0 missing)

Mismatched Test Cases (13 of  of 158)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 2f7681fa-66b0-4395-aa35-7622e37709ae ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2f7681fa-66b0-4395-aa35-7622e37709ae/MeasureReport-97f5ba10-36d6-4246-b935-fcfc8f4b1061.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ 3432dedb-7130-4614-9283-6c1569fab90f ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3432dedb-7130-4614-9283-6c1569fab90f/MeasureReport-acfc5ee1-09d4-4012-b12a-8487396b9856.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ 5a329008-fcc1-4168-ab9c-89cb5dd6ff32 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5a329008-fcc1-4168-ab9c-89cb5dd6ff32/MeasureReport-dda268cb-4395-4776-acd8-0fee046d392a.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 1<br>1<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8/MeasureReport-bad7b4ba-e916-41e2-a314-11854e1021ff.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ 82399522-ba6c-4997-afc9-23f55bb7da89 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/82399522-ba6c-4997-afc9-23f55bb7da89/MeasureReport-fe335f74-59a9-4afc-ba4c-7a9e003733d6.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ a1a37483-1a67-4dd9-a8ca-b4d49a28a19d ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a1a37483-1a67-4dd9-a8ca-b4d49a28a19d/MeasureReport-e3bfac2a-251a-49fe-9694-6c60803d9ded.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ be5c4068-2639-4b0c-bea3-5b7c80a6fe3b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/be5c4068-2639-4b0c-bea3-5b7c80a6fe3b/MeasureReport-ad329961-b67b-413b-a186-d6b269572c42.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ cb7c95fc-6d6b-4e07-81e8-a79385142b94 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cb7c95fc-6d6b-4e07-81e8-a79385142b94/MeasureReport-6844e7ed-08a4-43d5-be1c-720dc795b3cf.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 3<br>3<br>2 | 1<br>1<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ d496f08e-c55b-44b1-97a7-f86cf9ead1e2 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d496f08e-c55b-44b1-97a7-f86cf9ead1e2/MeasureReport-81e3066d-7dba-46fa-bb3f-2abc24625551.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ dc187313-245c-4ed6-b6bb-fcb94c117fec ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/dc187313-245c-4ed6-b6bb-fcb94c117fec/MeasureReport-d0cc2adb-8b9f-442d-82e2-5ef90a9c30d3.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ e126cdec-dbc8-4ee8-964f-e88e46c04f88 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e126cdec-dbc8-4ee8-964f-e88e46c04f88/MeasureReport-58249af5-0abc-464b-9e0a-456f7c31b4cf.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ ed638412-155e-4349-8461-4550fd4fae3b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ed638412-155e-4349-8461-4550fd4fae3b/MeasureReport-cf1aeb73-d464-4dd9-9f46-38afe84f76ec.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ febd4b3e-99bc-4c55-bba9-3b2136c2160b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/febd4b3e-99bc-4c55-bba9-3b2136c2160b/MeasureReport-4f80f98a-71ab-45d6-bdda-d0875ec02ec9.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 4<br>4<br>2<br>2 | 1<br>1<br>0<br>1 | — | FAIL<br>FAIL<br>FAIL<br>FAIL |


#### CMS104FHIRSTKDCAntithrombotic
[ [cql] ](../../input/cql/CMS104FHIRSTKDCAntithrombotic.cql) [ [test results] ](../../input/tests/results/CMS104FHIRSTKDCAntithrombotic.txt)

QICore: 225 / 185 — has discrepancies (69 mismatched, 0 missing)

Mismatched Test Cases (15 of  of 82)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 0b1aa8ee-e8bf-49f5-b968-48c5a9702843 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0b1aa8ee-e8bf-49f5-b968-48c5a9702843/MeasureReport-38f44642-a505-41c0-b367-013e4bb44d58.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 146a6714-8663-4f45-826a-01110ff34490 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/146a6714-8663-4f45-826a-01110ff34490/MeasureReport-e1b111ec-80f6-4548-b462-dc44dd07fd1e.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ 2d54a94c-edf1-4f92-baf8-3813a8ef452d ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2d54a94c-edf1-4f92-baf8-3813a8ef452d/MeasureReport-023784a8-b40e-491b-850f-0c87cb2e5e03.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ 348471db-5aaa-4bf3-a280-75222f20d599 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/348471db-5aaa-4bf3-a280-75222f20d599/MeasureReport-bf54d81d-f635-45ff-b69b-1580a144d3fb.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 3<br>3<br>1<br>1 | 1<br>1<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL<br>FAIL |
| [ 451b6853-3734-4c1c-b37e-5904629e0350 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/451b6853-3734-4c1c-b37e-5904629e0350/MeasureReport-4eefe8af-efb3-47eb-91df-e2ea877a39e7.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 3<br>3<br>2<br>1 | 1<br>1<br>1<br>0 | — | FAIL<br>FAIL<br>FAIL<br>FAIL |
| [ 48952352-d74c-491c-9420-6e999e60f52a ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/48952352-d74c-491c-9420-6e999e60f52a/MeasureReport-5eeb7443-d897-40c5-8815-c5dead56e05e.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ 591c23ea-1ddd-4800-9203-4b6946979818 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/591c23ea-1ddd-4800-9203-4b6946979818/MeasureReport-a871588f-5c88-44ce-890e-ccac41059f64.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ 593382e8-4ad5-4300-b0ad-26c8954281c6 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/593382e8-4ad5-4300-b0ad-26c8954281c6/MeasureReport-bb6002b4-0bd0-43fa-a7a0-748bd0444688.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ 5adc911a-c2a1-475c-a347-9da4ee98c6df ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/5adc911a-c2a1-475c-a347-9da4ee98c6df/MeasureReport-fbd77dd4-8f40-4bf2-bee9-e1e5ce62d7aa.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ 7b1ac1a8-b7be-41ec-a77f-db545af22263 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7b1ac1a8-b7be-41ec-a77f-db545af22263/MeasureReport-373169e3-3ba1-4ace-bf0c-5c212910cccf.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ a2b8327c-eaf4-4552-863e-851426e729d4 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a2b8327c-eaf4-4552-863e-851426e729d4/MeasureReport-0ced6c1b-75a5-4ee3-a7a0-017818c03e9a.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>2 | 1<br>1<br>1 | — | FAIL<br>FAIL<br>FAIL |
| [ ac56c496-c5d6-4c23-be20-130ee8327fd2 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ac56c496-c5d6-4c23-be20-130ee8327fd2/MeasureReport-34148ef9-fbdd-48ca-ab5d-6a11fd288074.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ c15bee15-84c1-494a-ac82-2159b06da175 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/c15bee15-84c1-494a-ac82-2159b06da175/MeasureReport-bbe28035-6557-410d-964f-21cf38904d0f.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 3<br>3<br>2 | 1<br>1<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ e081bee5-67f8-464f-9356-9b287e32a35a ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e081bee5-67f8-464f-9356-9b287e32a35a/MeasureReport-560b8ee7-5246-423f-8065-7f02c28eb91f.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ e84c89f7-3c9e-4ee9-b71a-5025aadb5990 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e84c89f7-3c9e-4ee9-b71a-5025aadb5990/MeasureReport-51e29a50-abca-429e-95eb-8364998be573.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exception | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |


#### CMS108FHIRVTEProphylaxis
[ [cql] ](../../input/cql/CMS108FHIRVTEProphylaxis.cql) [ [test results] ](../../input/tests/results/CMS108FHIRVTEProphylaxis.txt)

QICore: 548 / 12 — has discrepancies (12 mismatched, 0 missing)

Mismatched Test Cases (24 of  of 140)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 068814f1-4270-4e10-b470-9a5433bceb3e ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/068814f1-4270-4e10-b470-9a5433bceb3e/MeasureReport-22ae9d87-29d1-42c3-9908-93eff318d7b1.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 182103c1-0a38-4d85-819c-148e4e105716 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/182103c1-0a38-4d85-819c-148e4e105716/MeasureReport-ccb6ece2-ea74-4377-b826-2118740d1eee.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 2eff6dbd-f3a2-43ee-9ad3-aab4d3b84812 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/2eff6dbd-f3a2-43ee-9ad3-aab4d3b84812/MeasureReport-735dcbb8-d535-493a-a79c-ff4a9f72ee50.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 33d162ce-3bc7-4b0a-8c04-fec0a42a6263 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/33d162ce-3bc7-4b0a-8c04-fec0a42a6263/MeasureReport-da823951-b92e-4ee9-904f-839f7e8db8df.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 3c854f27-5103-4367-bdef-97c3cde1edb8 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/3c854f27-5103-4367-bdef-97c3cde1edb8/MeasureReport-1c32114e-5b9f-4f01-b021-0b3dd5bd8adf.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 3db5c5a1-2eec-4e01-8e59-ac389a0a2179 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/3db5c5a1-2eec-4e01-8e59-ac389a0a2179/MeasureReport-384a4771-57ba-472a-9ffd-17eeba8f39d7.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 41f2785f-4c4f-4497-a46b-e17fd8b5ee3f ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/41f2785f-4c4f-4497-a46b-e17fd8b5ee3f/MeasureReport-ff4c0b9f-8014-4119-ab3f-78a8e7e8f935.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 525e73f2-77be-49b1-920f-6fc31ef38d22 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/525e73f2-77be-49b1-920f-6fc31ef38d22/MeasureReport-9cb7f213-6011-4f8b-be16-010172559897.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 541ccffb-c1be-4c94-ab24-168d52e3a36b ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/541ccffb-c1be-4c94-ab24-168d52e3a36b/MeasureReport-4b90a8ef-2db7-4e28-aba4-d5404f17eb18.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 5741c41a-04ec-4967-83b2-b0d746bd0ed5 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/5741c41a-04ec-4967-83b2-b0d746bd0ed5/MeasureReport-10dddf5e-f066-457d-b056-01329b17c73e.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 575f2da0-c890-47a3-b17f-f9e134a1096e ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/575f2da0-c890-47a3-b17f-f9e134a1096e/MeasureReport-1f13d7d0-55ce-47e5-8a23-cb74963fc616.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 5f739500-ee12-4662-8980-ef95d8fa74c8 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/5f739500-ee12-4662-8980-ef95d8fa74c8/MeasureReport-5dd7eca4-05b6-49c4-87b7-a7313b46d684.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 8bb999a1-696a-497b-a5f4-aa55e146a16e ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/8bb999a1-696a-497b-a5f4-aa55e146a16e/MeasureReport-f1938984-85bf-4eff-b9b8-e89a556b2f35.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 8e2cfc29-0925-45b9-857f-b9ee9b9fa248 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/8e2cfc29-0925-45b9-857f-b9ee9b9fa248/MeasureReport-b86669af-57ea-48d3-af7b-87c11d0e94b9.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 91ff5f1a-cfdb-472d-b8c3-144f499d1ccc ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/91ff5f1a-cfdb-472d-b8c3-144f499d1ccc/MeasureReport-cee9ae71-29f6-41ee-a479-0fc2d8b338c5.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ b0932ba4-4dfc-43ad-aa67-fbaee9638d3b ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/b0932ba4-4dfc-43ad-aa67-fbaee9638d3b/MeasureReport-980b1611-a5d1-4bab-ae2a-974cdd0b6f75.json) | Group_1 | Denominator Exclusion | 1 | 0 | — | FAIL |
| [ b7783b8c-ba46-4509-a75e-203659abab3d ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/b7783b8c-ba46-4509-a75e-203659abab3d/MeasureReport-097d962a-0304-47fe-9c77-8fd8bd4b48ac.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ ccd7f9d7-35e8-4623-9f2e-f229cf7d829c ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/ccd7f9d7-35e8-4623-9f2e-f229cf7d829c/MeasureReport-c8c8144b-3bac-4663-aac9-9a786e5c1810.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ d205878e-b861-43a8-92e8-47f680987e4d ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/d205878e-b861-43a8-92e8-47f680987e4d/MeasureReport-e96f2279-a61f-40e2-9e19-9137ee4b12e6.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ d9b7ffa9-ed78-484c-8880-b4cbf2b4b6a1 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/d9b7ffa9-ed78-484c-8880-b4cbf2b4b6a1/MeasureReport-43331d8f-cf2d-4a0c-a3a2-e4b8e060a7eb.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ dba7c9af-eb6f-4836-ba24-650a5acc87e7 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/dba7c9af-eb6f-4836-ba24-650a5acc87e7/MeasureReport-7c3e8a2e-61ff-4a73-b3e6-d6b168cb4cc6.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ dc0dcb01-87f0-4e65-9c36-8cf6174abef1 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/dc0dcb01-87f0-4e65-9c36-8cf6174abef1/MeasureReport-7bc64137-ecc6-421a-bb2f-0177667a25b7.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ dd5a1e46-1b99-45a3-b4d3-1fde205d8a11 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/dd5a1e46-1b99-45a3-b4d3-1fde205d8a11/MeasureReport-bc945d90-f897-463b-bbc2-f9b922117784.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ ff814452-be6d-4e4b-905b-c1ae2a551645 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/ff814452-be6d-4e4b-905b-c1ae2a551645/MeasureReport-8f09729a-45b0-45dc-bfdd-047cf0d896ef.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |


#### CMS122FHIRDiabetesAssessGT9Pct
[ [cql] ](../../input/cql/CMS122FHIRDiabetesAssessGT9Pct.cql) [ [test results] ](../../input/tests/results/CMS122FHIRDiabetesAssessGT9Pct.txt)

QICore: 208 / 12 — has discrepancies (6 mismatched, 0 missing)

Mismatched Test Cases (26 of  of 55)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 090ad2fc-274b-4fef-bc5a-2077dbdc28f5 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/090ad2fc-274b-4fef-bc5a-2077dbdc28f5/MeasureReport-ca30e820-61f7-448c-b61b-3fe777fbbb40.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 1e954801-6437-4abc-8fb8-d36b5b5b97d8 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/1e954801-6437-4abc-8fb8-d36b5b5b97d8/MeasureReport-a3aca61d-6c98-479b-b73e-e620f588670a.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 21695544-0997-4b9a-989c-a535da22d033 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/21695544-0997-4b9a-989c-a535da22d033/MeasureReport-0d751987-5b7f-4ace-bb6a-e5ef5d2d036b.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 24fa66c5-52ba-4386-a5e7-7b78002be77a ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/24fa66c5-52ba-4386-a5e7-7b78002be77a/MeasureReport-34318103-069f-4c4d-b3e0-f3aadd9f3e68.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 511548fc-b5c3-4f90-83c6-e04f8e1c98cc ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/511548fc-b5c3-4f90-83c6-e04f8e1c98cc/MeasureReport-2b275c27-4f0c-456c-8a9a-84cc0149a4f9.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 514a74ba-baea-4102-b2e7-050f84c79ef8 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/514a74ba-baea-4102-b2e7-050f84c79ef8/MeasureReport-3e883a8f-e667-4e44-8d5f-81d31e8ce0db.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 5d692a54-a1d5-4a9c-80ba-fb6b20112484 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/5d692a54-a1d5-4a9c-80ba-fb6b20112484/MeasureReport-459468b5-2eae-4096-b992-a16da2e75c21.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 5ed37c9e-85a3-4819-8051-3d960159cae0 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/5ed37c9e-85a3-4819-8051-3d960159cae0/MeasureReport-1e69613d-eda5-4d09-9958-b6a50a70a227.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 61793aba-9080-4521-9083-a23f242b8d0a ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/61793aba-9080-4521-9083-a23f242b8d0a/MeasureReport-551d1108-4981-461f-951e-c7c57bbf401c.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 6630d394-c81d-42f5-a218-40b73a2a4949 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/6630d394-c81d-42f5-a218-40b73a2a4949/MeasureReport-00306284-8ee3-4241-b775-78f424ad4167.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 7706188a-f37c-483d-96c2-4d7eab833605 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/7706188a-f37c-483d-96c2-4d7eab833605/MeasureReport-b63dfb76-16fa-4fc0-97dd-7ad49df6dcfe.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8956ebb5-d3c0-4112-a34a-200961713efd ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/8956ebb5-d3c0-4112-a34a-200961713efd/MeasureReport-008c11a8-06e5-4449-aa99-82f43703ae55.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8b1155b0-ff08-4f28-90e7-ac0e622f840c ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/8b1155b0-ff08-4f28-90e7-ac0e622f840c/MeasureReport-515187cc-05ee-4b19-a8a2-a7ff6773a967.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8fa86a00-fa67-4dd6-b2d8-6fe23edde9c7 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/8fa86a00-fa67-4dd6-b2d8-6fe23edde9c7/MeasureReport-f0e63ca6-25ea-427c-8a8c-ab91aff3b67c.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 981dbc54-03ac-4f2e-a008-dbedfcbd2a7a ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/981dbc54-03ac-4f2e-a008-dbedfcbd2a7a/MeasureReport-c714b823-80e1-44ff-ac07-8db37417b3d8.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 98735c81-5c91-4709-9392-558ac6d40b6c ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/98735c81-5c91-4709-9392-558ac6d40b6c/MeasureReport-49571699-5392-4329-b6e0-e9e045f8a481.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 9da62d36-585d-455e-8cb5-8e5da1f3e476 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/9da62d36-585d-455e-8cb5-8e5da1f3e476/MeasureReport-18b2c127-9975-4447-b5fb-38290111b111.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a6b08556-8019-43ad-8ab0-0c213f3789ca ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/a6b08556-8019-43ad-8ab0-0c213f3789ca/MeasureReport-ecd72e3a-1ba5-4cf9-af09-aeb9fd9187b5.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a7332447-3a23-42b1-bfc2-d93cc5b775af ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/a7332447-3a23-42b1-bfc2-d93cc5b775af/MeasureReport-ece514cf-7490-4d51-a263-2554c01833af.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ ab29ab81-b4fc-4817-bd9c-98d8d4b4a3a3 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/ab29ab81-b4fc-4817-bd9c-98d8d4b4a3a3/MeasureReport-c64e003c-7703-4a6c-85cd-8907e9037fdb.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ abe87c54-c0b1-4f86-94ca-360a228e9aa3 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/abe87c54-c0b1-4f86-94ca-360a228e9aa3/MeasureReport-508b552a-515f-459c-8f62-97e4b6b422e0.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ c66e4e0a-5479-461c-9a39-0298a08f682f ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/c66e4e0a-5479-461c-9a39-0298a08f682f/MeasureReport-9cf87565-7680-4f27-9314-84fb5cefc8fd.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ d3ac0220-8947-489d-b7fe-a199d5365a6f ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/d3ac0220-8947-489d-b7fe-a199d5365a6f/MeasureReport-b77c8374-03bb-4521-9e0f-f57281ca4da8.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ da05305e-9c4c-4b1d-ac55-cab089a11d2b ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/da05305e-9c4c-4b1d-ac55-cab089a11d2b/MeasureReport-42fc86c0-5675-4fdb-af87-cbf167179ebb.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ f64a63d1-cdc9-4486-a4d5-1d140a4f07e1 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/f64a63d1-cdc9-4486-a4d5-1d140a4f07e1/MeasureReport-df25e12a-31cb-47df-b83a-d93ef20c3da0.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ fccb9758-ea26-4a1e-98cf-3942102295b8 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/fccb9758-ea26-4a1e-98cf-3942102295b8/MeasureReport-ab21a11e-4967-43f1-a79e-f2a0cc9a64c0.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |


#### CMS124FHIRCervicalCancerScreen
[ [cql] ](../../input/cql/CMS124FHIRCervicalCancerScreen.cql) [ [test results] ](../../input/tests/results/CMS124FHIRCervicalCancerScreen.txt)

QICore: 136 / 0 — passes

Mismatched Test Cases (14 of  of 34)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 1104f4a8-5328-4629-8b7f-77f7b2e62225 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/1104f4a8-5328-4629-8b7f-77f7b2e62225/MeasureReport-e2b0ff7d-dc13-4d9d-870c-4ae93ac715fb.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 25727adc-4495-4e13-9dfc-8b9cb6bf17b9 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/25727adc-4495-4e13-9dfc-8b9cb6bf17b9/MeasureReport-8e881599-1588-4c22-85d9-dbd25b2b1542.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 321abfa0-2c0e-4885-8b5b-20208512e605 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/321abfa0-2c0e-4885-8b5b-20208512e605/MeasureReport-50497d3b-8459-445c-a273-d7e8e2af3eed.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 3e21058f-64cc-4b0a-8c84-1122df974dae ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/3e21058f-64cc-4b0a-8c84-1122df974dae/MeasureReport-8c76729f-838f-44b7-bcb7-be0f90af32bb.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 4c40d1e6-3943-4a0e-a95c-6e6b845f0851 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/4c40d1e6-3943-4a0e-a95c-6e6b845f0851/MeasureReport-b6f87fcc-5710-46cb-a658-1947bdc82462.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 59ef157d-1417-4a8e-9193-06d9c66ba8e1 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/59ef157d-1417-4a8e-9193-06d9c66ba8e1/MeasureReport-b6c7a212-98ec-40f1-a854-268665d3d873.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 6005d1fd-e9f5-414d-88d6-23087b4f3e94 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/6005d1fd-e9f5-414d-88d6-23087b4f3e94/MeasureReport-1048b712-7b9e-4ed9-aa2f-329074e3482b.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 908f935e-43b9-4666-982a-f211d1cfcd50 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/908f935e-43b9-4666-982a-f211d1cfcd50/MeasureReport-cae95210-ccf2-49df-b0eb-c1bd88af1db9.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ b8c73916-4520-47e1-9456-a36cd1575693 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/b8c73916-4520-47e1-9456-a36cd1575693/MeasureReport-095f1c40-5fe9-4ed6-8f6f-96edfa919522.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c0d1f27d-249b-4d74-a493-a4796fb8e833 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/c0d1f27d-249b-4d74-a493-a4796fb8e833/MeasureReport-6ded9e76-3622-42f2-9024-7aa3ef05417b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c5ea33df-060b-484a-b6c4-17c600559077 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/c5ea33df-060b-484a-b6c4-17c600559077/MeasureReport-7a4a3663-41fa-41a9-9505-bfdf45dc3ca8.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d15cf8c6-5f36-4874-83a5-d726945721c6 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/d15cf8c6-5f36-4874-83a5-d726945721c6/MeasureReport-57656292-7ec2-46df-80ae-a522d1b875f6.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ dc5b8054-7432-4905-aaef-3acd6f3f75b9 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/dc5b8054-7432-4905-aaef-3acd6f3f75b9/MeasureReport-b5c229cd-c9cd-413f-83bb-58dc828538d6.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ e8e5b4c8-0e07-415f-a534-9143ecef5f10 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/e8e5b4c8-0e07-415f-a534-9143ecef5f10/MeasureReport-2979064f-0d99-45aa-b6f4-8784cd786347.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |


#### CMS125FHIRBreastCancerScreen
[ [cql] ](../../input/cql/CMS125FHIRBreastCancerScreen.cql) [ [test results] ](../../input/tests/results/CMS125FHIRBreastCancerScreen.txt)

QICore: 256 / 8 — has discrepancies (8 mismatched, 0 missing)

Mismatched Test Cases (25 of  of 66)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 07fb2077-048c-4cb0-ba3e-6e67ed33133d ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/07fb2077-048c-4cb0-ba3e-6e67ed33133d/MeasureReport-53382a12-55a1-409e-a192-acfa489b42ec.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 33afc6f6-11c8-4d29-9e2d-cdc292565458 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/33afc6f6-11c8-4d29-9e2d-cdc292565458/MeasureReport-98d0ee09-fd9c-465e-82fb-c222ad16dc60.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 46fbbd0e-d175-4203-97bb-fe616cd2ab77 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/46fbbd0e-d175-4203-97bb-fe616cd2ab77/MeasureReport-6cd90376-024c-4aa8-bebd-caf0c2abfd1b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 473f9149-c7f0-4979-8924-9534cabe5117 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/473f9149-c7f0-4979-8924-9534cabe5117/MeasureReport-a2991cc3-1347-4a0b-adc1-f4adad7848bf.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 57d8d494-e828-4edf-8c8b-e27da33ea223 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/57d8d494-e828-4edf-8c8b-e27da33ea223/MeasureReport-1e2e967d-b2e6-4117-9b91-b2509d539bea.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 591e960d-b937-41f3-9817-56cf201a06db ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/591e960d-b937-41f3-9817-56cf201a06db/MeasureReport-80221012-b5dc-46b9-9691-8a0ac2b995eb.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5be43868-ffec-4de5-b99e-185513b74c82 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/5be43868-ffec-4de5-b99e-185513b74c82/MeasureReport-89176983-0efa-41bf-8d68-b545304b362c.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 6226b04f-5e2d-4977-9169-8e9451ffa939 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/6226b04f-5e2d-4977-9169-8e9451ffa939/MeasureReport-489d89a3-983e-4219-892c-b7c702be16d1.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 633c26f2-9c7a-4eaf-b983-83b9e13656ac ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/633c26f2-9c7a-4eaf-b983-83b9e13656ac/MeasureReport-2c3c1dc3-814c-4d77-bb60-7a458bbd63ac.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 68067d39-5287-40dd-ba97-c2aa1bf46d78 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/68067d39-5287-40dd-ba97-c2aa1bf46d78/MeasureReport-748d6371-18c3-4bce-977c-a859a547bde5.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 6b2e313f-6139-45fa-8e18-cc2f0b908981 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/6b2e313f-6139-45fa-8e18-cc2f0b908981/MeasureReport-ec13c21c-dac6-44cb-8c9a-946611b1bf61.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 6fc33313-98bc-460e-9e38-9240dcbd111a ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/6fc33313-98bc-460e-9e38-9240dcbd111a/MeasureReport-85e4e484-36a6-4dda-8823-fa49fa241d44.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 81dce125-8691-4625-ac6b-07fce0a45680 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/81dce125-8691-4625-ac6b-07fce0a45680/MeasureReport-080c3e69-9f37-472f-bd70-34acdc2536ba.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ b528b1a6-cd8d-4f66-83c2-6467e83b6996 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/b528b1a6-cd8d-4f66-83c2-6467e83b6996/MeasureReport-5d118fae-95cc-44c4-84aa-c4b5920d28b4.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ bea75baa-41f5-4755-9986-15c2bba658d5 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/bea75baa-41f5-4755-9986-15c2bba658d5/MeasureReport-99925929-7140-4230-b13a-e26cccb0c5d3.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c32eb7d1-eac5-458e-b965-c717620579a2 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/c32eb7d1-eac5-458e-b965-c717620579a2/MeasureReport-bfbcff07-d5ee-416f-83b8-a662f0f18b56.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ cf727fca-40bc-46ed-b97b-e9021cffb8d3 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/cf727fca-40bc-46ed-b97b-e9021cffb8d3/MeasureReport-0c4d5b0d-470d-443d-9dc9-1dbda3f69d59.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ dd6bd96f-3a4e-4796-bee0-1d31884e96d7 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/dd6bd96f-3a4e-4796-bee0-1d31884e96d7/MeasureReport-6191dfb9-4a22-4606-a938-cb030471ef64.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ deb40976-ede4-4657-8af8-078369fa65f4 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/deb40976-ede4-4657-8af8-078369fa65f4/MeasureReport-abe0121d-fbf8-47c9-979d-a345aea31af9.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ defc50ff-2898-4ab0-ac06-75eae73bc6fa ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/defc50ff-2898-4ab0-ac06-75eae73bc6fa/MeasureReport-fe7f54a2-7f97-4694-819e-b1b649414d03.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ f2f748c2-321f-4c05-896a-2ef9d925eaf9 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/f2f748c2-321f-4c05-896a-2ef9d925eaf9/MeasureReport-28bea5fd-cabe-4b26-8a94-6123fba505c9.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ f4d00e60-e525-4644-a397-4d7d970bcfdb ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/f4d00e60-e525-4644-a397-4d7d970bcfdb/MeasureReport-157d6c28-7d6c-47be-906e-cd622931cf65.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ f7574a1c-122e-45ef-9ab5-cfa35a40d6d6 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/f7574a1c-122e-45ef-9ab5-cfa35a40d6d6/MeasureReport-102319bd-d710-4adf-bee0-40ff88a7b838.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ f9de4c72-b2ed-4c8f-94fe-8c934e42e0a0 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/f9de4c72-b2ed-4c8f-94fe-8c934e42e0a0/MeasureReport-9d2d1444-f7a9-4497-a785-cacfab3639fb.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ ffbb03e1-7188-42ef-8deb-c6cf3f790bfe ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/ffbb03e1-7188-42ef-8deb-c6cf3f790bfe/MeasureReport-c4910aaf-43c1-4f7f-bfaa-4f1ba4385ba3.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |


#### CMS130FHIRColorectalCancerScrn
[ [cql] ](../../input/cql/CMS130FHIRColorectalCancerScrn.cql) [ [test results] ](../../input/tests/results/CMS130FHIRColorectalCancerScrn.txt)

QICore: 255 / 1 — has discrepancies (1 mismatched, 0 missing)

Mismatched Test Cases (37 of  of 64)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 06934496-0ea0-4ccd-af2e-da5b94410b58 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/06934496-0ea0-4ccd-af2e-da5b94410b58/MeasureReport-e98816af-9900-4323-be88-a91f73f59792.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 2292adf2-3232-43f8-9497-8448349c51a9 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/2292adf2-3232-43f8-9497-8448349c51a9/MeasureReport-263140ad-e031-4377-ba90-f5ec426c32f2.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 2847411d-a6c5-4f86-ac1f-d229ffa5a00c ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/2847411d-a6c5-4f86-ac1f-d229ffa5a00c/MeasureReport-649fdce0-61f0-41c1-8c8b-bc79363d03dd.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 2b0d64f9-9f3a-4adf-aadb-c231a8ab98ac ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/2b0d64f9-9f3a-4adf-aadb-c231a8ab98ac/MeasureReport-6dea3ea3-6b0c-4808-ab0e-abae8e81ee20.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 394fbf45-d81c-49d1-be1f-3907227d8940 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/394fbf45-d81c-49d1-be1f-3907227d8940/MeasureReport-ab6d329c-4a71-4c02-8afc-2b179db2ce86.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 39fd4a5e-0db2-478d-ba85-4400a1b7e35e ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/39fd4a5e-0db2-478d-ba85-4400a1b7e35e/MeasureReport-e464de81-75b0-4ce4-9be9-60f6ae9930c9.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 3d75185a-d8e1-4861-9b36-528548e57fc4 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/3d75185a-d8e1-4861-9b36-528548e57fc4/MeasureReport-da0cb1e8-1b6e-4d05-b3fd-1c601686ab4b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 487de25d-a184-42ed-b1c6-389ed217a0a1 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/487de25d-a184-42ed-b1c6-389ed217a0a1/MeasureReport-50422f5a-106f-4d11-8581-f6b87f7beb6c.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 4fc22b6a-0cca-4e61-bedf-2cb73cf66698 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/4fc22b6a-0cca-4e61-bedf-2cb73cf66698/MeasureReport-0edd6f05-3509-4e51-9ea4-03e9aace158c.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5445cc72-68a1-4b73-b06d-4cf52098e0db ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/5445cc72-68a1-4b73-b06d-4cf52098e0db/MeasureReport-3a0446fc-e3dd-4aa8-ba10-fb2043f42bc8.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 54db46c1-fa2a-4e6e-96aa-da6dd67c5f18 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/54db46c1-fa2a-4e6e-96aa-da6dd67c5f18/MeasureReport-12823813-3d41-4881-a248-7a824a7e1410.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 58b6a190-8a9c-4631-a102-6048f3e62a19 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/58b6a190-8a9c-4631-a102-6048f3e62a19/MeasureReport-c1bf067c-1bf9-4c5f-845c-846f1146bad9.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5ebc158d-0736-4467-8bc0-72182bc0f5af ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/5ebc158d-0736-4467-8bc0-72182bc0f5af/MeasureReport-4e09539d-a2fc-4cc9-97e4-1eaaf4ae8131.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 642aafde-fabb-458d-ae4d-5db7343f310c ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/642aafde-fabb-458d-ae4d-5db7343f310c/MeasureReport-9739f023-ced3-438c-953a-c97686e9f634.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 650f4ed7-9418-42ad-a9d7-59fe79e951da ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/650f4ed7-9418-42ad-a9d7-59fe79e951da/MeasureReport-9bc05576-7a01-4d5a-a6d4-00ef26ce88b5.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 683cec0c-5368-467b-85f7-4b70c269e8ea ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/683cec0c-5368-467b-85f7-4b70c269e8ea/MeasureReport-fe3ce79b-2c98-4d8f-8525-1ed26dd33abd.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 7822bd0a-ba96-46f0-8c57-204d37156184 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/7822bd0a-ba96-46f0-8c57-204d37156184/MeasureReport-13aec26c-f40b-461e-b8cb-3f645e8af9a3.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 84ebbde4-0ea8-42ae-908b-ef1721748290 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/84ebbde4-0ea8-42ae-908b-ef1721748290/MeasureReport-f3d72a79-c241-427e-bdf0-f39c75af2ae7.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 95d56325-022c-4bdc-8778-bf02f46139cb ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/95d56325-022c-4bdc-8778-bf02f46139cb/MeasureReport-ec8ad8b0-ca91-4326-a7ac-2b28d3db7306.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 9943e220-d0f1-4718-8377-0d407a529f52 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/9943e220-d0f1-4718-8377-0d407a529f52/MeasureReport-c292eb92-c37a-4b8e-a124-9234237be4c5.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 9c6fd73e-9005-4518-b7f0-5d9db57a7ef5 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/9c6fd73e-9005-4518-b7f0-5d9db57a7ef5/MeasureReport-6c994ed2-5767-4c1e-8271-2f10c95f0036.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ b20cd591-3625-4d95-8081-6f2566c51fa6 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/b20cd591-3625-4d95-8081-6f2566c51fa6/MeasureReport-77482e2e-2dbf-4d37-9eb5-bdd8f5f2eb9f.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ be630df2-cc71-47b9-a600-a715912f90be ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/be630df2-cc71-47b9-a600-a715912f90be/MeasureReport-f42a5f9f-b45e-47dd-98a4-1feb3d0c834f.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ bf3f2c9a-a802-4522-8e38-d1c806e71483 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/bf3f2c9a-a802-4522-8e38-d1c806e71483/MeasureReport-7f292764-9689-4cfc-b8fa-4632e783d532.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ c002ae0a-709f-4a5e-82e3-f0a4d8f3a839 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/c002ae0a-709f-4a5e-82e3-f0a4d8f3a839/MeasureReport-13d8bfbe-4b56-4e49-809f-e45fea6b1c45.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ cdacf996-8b20-49af-8f75-0cfd26fafacb ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/cdacf996-8b20-49af-8f75-0cfd26fafacb/MeasureReport-4770ab12-dcd7-4282-bd79-969abaf565dc.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d0306f4f-06a9-407d-ac0d-e5628fd1cc59 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/d0306f4f-06a9-407d-ac0d-e5628fd1cc59/MeasureReport-8909651a-01b2-436c-8563-a81110d041ef.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ da1e1656-54ae-49f6-ab1b-b8ba9f99b6c2 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/da1e1656-54ae-49f6-ab1b-b8ba9f99b6c2/MeasureReport-c53aa81d-4125-4618-831d-6654068e3306.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ dbead888-2672-453c-8005-d4b9f62b72c9 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/dbead888-2672-453c-8005-d4b9f62b72c9/MeasureReport-20d9fd93-6f99-4c78-85af-74e35a454dee.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ dc337be7-7328-4fce-8f6f-71ee2cb75752 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/dc337be7-7328-4fce-8f6f-71ee2cb75752/MeasureReport-9336e43c-1919-4714-940d-af809a6c713b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ df2c9d36-96e4-4ab6-9a2a-d3b5b0a44328 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/df2c9d36-96e4-4ab6-9a2a-d3b5b0a44328/MeasureReport-2fe698c5-e59f-49b7-9596-3b76a6e4a896.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ e4215f63-f195-48bd-865d-ecb718f742ff ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/e4215f63-f195-48bd-865d-ecb718f742ff/MeasureReport-c2001cbb-6e40-42fe-bd19-b17265d81fd3.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ e904e28b-ec42-4ca5-8dab-f1cf72f705e6 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/e904e28b-ec42-4ca5-8dab-f1cf72f705e6/MeasureReport-2cdc141c-16bb-4303-80e4-309471842edd.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ e9d86ff6-da48-43c9-9e16-dd95d8bc49c3 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/e9d86ff6-da48-43c9-9e16-dd95d8bc49c3/MeasureReport-74afcc62-1ff0-47ca-a90b-0e2fa300dc64.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ ecd9203b-716e-49ee-be53-eecdea8bef86 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/ecd9203b-716e-49ee-be53-eecdea8bef86/MeasureReport-8622a7a1-615e-4c77-a003-dc0af404035e.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ f0dae4e3-d82d-422f-883c-4e5238c14a54 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/f0dae4e3-d82d-422f-883c-4e5238c14a54/MeasureReport-96e56c55-a81a-43fb-b24c-ecf6730a4b48.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ fd8d8328-c766-4c9f-a463-ec53957e0276 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/fd8d8328-c766-4c9f-a463-ec53957e0276/MeasureReport-d70a94e0-3a9b-4526-b25c-1a7b1a956027.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |


#### CMS131FHIRDiabetesEyeExam
[ [cql] ](../../input/cql/CMS131FHIRDiabetesEyeExam.cql) [ [test results] ](../../input/tests/results/CMS131FHIRDiabetesEyeExam.txt)

QICore: 246 / 6 — has discrepancies (6 mismatched, 0 missing)

Mismatched Test Cases (31 of  of 63)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 085b9cf8-58f6-4076-946d-a5206f8de77b ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/085b9cf8-58f6-4076-946d-a5206f8de77b/MeasureReport-34af8a93-d043-4081-8a0f-3a475cd68863.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 0919ba5b-bc08-4660-b8c9-9369b955ffd8 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/0919ba5b-bc08-4660-b8c9-9369b955ffd8/MeasureReport-b281b7a8-9fbb-4bfd-8174-912841dc6185.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 0fa877b4-bbbe-4a5b-814d-57c1472b923b ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/0fa877b4-bbbe-4a5b-814d-57c1472b923b/MeasureReport-fd6f4722-a10b-41e6-a393-e8be4d496592.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 19a6d651-3dd7-45a9-9340-e40e41875a13 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/19a6d651-3dd7-45a9-9340-e40e41875a13/MeasureReport-f4c89d43-4979-47d8-b32e-3f520a8949ef.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 1e8cd1fd-6ba8-48e3-bbdb-d4702c36cf92 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/1e8cd1fd-6ba8-48e3-bbdb-d4702c36cf92/MeasureReport-216fc445-6f5a-4bb3-b155-3939d3f0de89.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 3ff1b618-c425-4d51-9447-d1c4cf048d3c ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/3ff1b618-c425-4d51-9447-d1c4cf048d3c/MeasureReport-f3b0bd1e-f7a6-4cc1-aa61-f55ad882af24.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 51f41079-0dc3-4da2-86e5-d1360f936ca3 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/51f41079-0dc3-4da2-86e5-d1360f936ca3/MeasureReport-6d561ec1-4866-4aee-9308-1bc86e2a08cb.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 52d1f4f3-14a0-4eed-a0c2-334b8146b117 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/52d1f4f3-14a0-4eed-a0c2-334b8146b117/MeasureReport-6c178f11-c343-4b20-a0c2-24ab27e61fed.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 56790710-4864-4665-bf28-0514bdb74f0d ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/56790710-4864-4665-bf28-0514bdb74f0d/MeasureReport-2407fd44-1fb1-4404-8400-0da29defca1f.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5e00bc73-c96c-47c8-99f9-0d857acb3e72 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/5e00bc73-c96c-47c8-99f9-0d857acb3e72/MeasureReport-d7fce42f-ca69-415a-8eb0-1c9c95dbfd5b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 7a38f99c-a713-4631-9a05-13cfe1a21e5a ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/7a38f99c-a713-4631-9a05-13cfe1a21e5a/MeasureReport-b4acc6e6-7c22-408f-a435-f9b76739b51d.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 7c46ee00-603b-4b64-a46b-2cb613f9446d ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/7c46ee00-603b-4b64-a46b-2cb613f9446d/MeasureReport-6e57306b-680d-4f63-bff0-e76f02ba1137.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 7ca93198-2a13-4266-aa39-82003e19b175 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/7ca93198-2a13-4266-aa39-82003e19b175/MeasureReport-ce9ab1a9-0027-4b45-9355-8720d45ec922.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 89073685-3807-41f5-bc32-2cf44c1b8227 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/89073685-3807-41f5-bc32-2cf44c1b8227/MeasureReport-0401a904-6f87-4769-8322-9c7655a68f95.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8cd1152d-fc40-4558-9eb3-547db2e56d7a ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/8cd1152d-fc40-4558-9eb3-547db2e56d7a/MeasureReport-4902b055-3393-43f2-b323-89527e306f91.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8fdd8b35-ce68-452d-a38a-93843c64411e ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/8fdd8b35-ce68-452d-a38a-93843c64411e/MeasureReport-37ad799a-4eea-4aec-8414-e7f1e8cab4dc.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 9177b3ca-1cd7-404c-93f9-5bc782b9963a ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/9177b3ca-1cd7-404c-93f9-5bc782b9963a/MeasureReport-b1807bba-2b1a-43af-a9d9-26536a45d803.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 96729eb4-48b3-44f8-a6e6-eec225648115 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/96729eb4-48b3-44f8-a6e6-eec225648115/MeasureReport-e12eb977-33f9-4851-8818-68d7e2b1f2c6.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 985b5e49-2f5d-4eb3-a33c-2d0eb156bf7b ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/985b5e49-2f5d-4eb3-a33c-2d0eb156bf7b/MeasureReport-8e0e0b26-f0c3-431a-bb63-9bf90b8d406a.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a2c893b1-5727-45ba-9b79-1d9e78697e20 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/a2c893b1-5727-45ba-9b79-1d9e78697e20/MeasureReport-b27b5080-9d3f-455d-966e-040f45c36521.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ b08c80d0-c70e-4653-b5da-e1f8cb858714 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/b08c80d0-c70e-4653-b5da-e1f8cb858714/MeasureReport-de342855-656e-4e20-b7e3-dd56273ef5a7.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ bf5f59ac-661c-4f8c-a4aa-b3c0a66f2a49 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/bf5f59ac-661c-4f8c-a4aa-b3c0a66f2a49/MeasureReport-2da33d95-4d53-46c1-b249-e2de0f581033.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ cd42be5f-e738-465a-aa40-e8cfaa2e82e9 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/cd42be5f-e738-465a-aa40-e8cfaa2e82e9/MeasureReport-6514a69d-8f89-41ba-9947-cdc289fc73ea.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ cfa4b281-a298-4fa9-aac4-5261519a3dd9 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/cfa4b281-a298-4fa9-aac4-5261519a3dd9/MeasureReport-e560170a-f14e-46e3-a49e-39c775219574.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d3b4f0ab-d8d1-4c4c-8763-7a8276e0c3ca ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/d3b4f0ab-d8d1-4c4c-8763-7a8276e0c3ca/MeasureReport-ad3d99ac-3873-41e3-bab7-c55604885e90.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d6fd9369-9e85-415d-a3d1-73747fb30af6 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/d6fd9369-9e85-415d-a3d1-73747fb30af6/MeasureReport-f17a7405-3918-4044-a81b-8b2dd26037e1.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ dcd62616-c203-4ddf-817a-4ce8622e23ca ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/dcd62616-c203-4ddf-817a-4ce8622e23ca/MeasureReport-dfcc36ae-a2ff-40c3-ad6b-cde0edb1a75d.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ eab86b9c-b8e8-4f60-837f-8f9aa6f039ee ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/eab86b9c-b8e8-4f60-837f-8f9aa6f039ee/MeasureReport-5e0d8a67-3a1b-4be7-ab42-74bda40b2261.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ ecc34b3c-1241-4541-a8dd-66183c3d70de ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/ecc34b3c-1241-4541-a8dd-66183c3d70de/MeasureReport-8e8e0a0b-4032-41f7-a897-b9e586f9cd0f.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ ef247fbf-b973-4321-9830-5d184a730a6f ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/ef247fbf-b973-4321-9830-5d184a730a6f/MeasureReport-240567b4-e532-454c-9033-e8ccd02a506e.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ f850c570-3a2b-4b3b-a9f8-f5fc1b03f639 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/f850c570-3a2b-4b3b-a9f8-f5fc1b03f639/MeasureReport-55b562ca-ff3c-47cf-99c9-b6aef6e08cd4.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |


#### CMS135FHIRACEIorARBorARNIforHF
[ [cql] ](../../input/cql/CMS135FHIRACEIorARBorARNIforHF.cql) [ [test results] ](../../input/tests/results/CMS135FHIRACEIorARBorARNIforHF.txt)

QICore: 185 / 15 — has discrepancies (0 mismatched, 3 missing)

Missing Results (3 of 40 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ c095195c-8893-4bf1-aa7d-ad2bfd9bafa5 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/c095195c-8893-4bf1-aa7d-ad2bfd9bafa5/MeasureReport-f2d033da-6f32-46dc-86bc-69fdf82b1cfd.json) | Group_1 | E-11 — resolution pending |
| [ cba5a449-1c45-4e11-ae0b-ba3974b410f7 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/cba5a449-1c45-4e11-ae0b-ba3974b410f7/MeasureReport-ae8c4b99-af76-4577-b66d-b1230ac09aa3.json) | Group_1 | E-11 — resolution pending |
| [ ec508dbb-76f6-4878-b8a2-114ea8e82297 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/ec508dbb-76f6-4878-b8a2-114ea8e82297/MeasureReport-d1b704c8-7e95-4cd9-89e7-a8b90f925ce2.json) | Group_1 | E-11 — resolution pending |


Mismatched Test Cases (9 of  of 40)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 149c3a7c-2b80-47f8-b50d-5c1d233eedb7 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/149c3a7c-2b80-47f8-b50d-5c1d233eedb7/MeasureReport-d8d9ace4-d191-4aff-a0e4-6de581275357.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 1f64a697-a90b-4aaf-a315-fa84168ac2b4 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/1f64a697-a90b-4aaf-a315-fa84168ac2b4/MeasureReport-cf4fe385-8e6f-4642-b1e5-ca08159c0b53.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 298d5342-fa0a-4386-bf48-b9c977a1c367 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/298d5342-fa0a-4386-bf48-b9c977a1c367/MeasureReport-090aa645-1e2b-44df-b6c0-2419bea96186.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 4bc4883f-0770-4a68-824a-5fa4dba72638 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/4bc4883f-0770-4a68-824a-5fa4dba72638/MeasureReport-d4dc5571-57c9-4b1b-95d9-a09ac4c6e34d.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 5b7e720f-e2fc-4779-9b1c-3f34a0241482 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/5b7e720f-e2fc-4779-9b1c-3f34a0241482/MeasureReport-01fb5443-0f43-487e-ac44-f7cc6e163ca0.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 64e76766-9760-4385-a977-cbe8136ce425 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/64e76766-9760-4385-a977-cbe8136ce425/MeasureReport-0488a022-da7e-4dcf-a9af-7e2fbf5e9423.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 6a86918d-3f69-43c8-8863-1d0bf835a2c7 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/6a86918d-3f69-43c8-8863-1d0bf835a2c7/MeasureReport-3decfa0c-9100-4194-9643-c3065c1a253f.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ d18e37a6-7b66-4e7c-b305-692872c13f8d ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/d18e37a6-7b66-4e7c-b305-692872c13f8d/MeasureReport-ecbb5067-dcb1-48ce-8e78-6dfd556ac43d.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ d297e68e-3f02-42a8-a59f-a5a4cecbd47d ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/d297e68e-3f02-42a8-a59f-a5a4cecbd47d/MeasureReport-cc3a4e83-9689-4bb7-83e1-55cb47dc9848.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |


#### CMS142FHIRCommWithDrManagingDiab
[ [cql] ](../../input/cql/CMS142FHIRCommWithDrManagingDiab.cql) [ [test results] ](../../input/tests/results/CMS142FHIRCommWithDrManagingDiab.txt)

QICore: 123 / 5 — has discrepancies (5 mismatched, 0 missing)

Mismatched Test Cases (5 of  of 32)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 05f1e2a6-b317-42bb-827f-993ca3995f5b ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/05f1e2a6-b317-42bb-827f-993ca3995f5b/MeasureReport-84bcf708-71bb-4169-8067-18fd354f3c37.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ 41ae0086-ac99-4a31-9546-21b054bbf7d8 ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/41ae0086-ac99-4a31-9546-21b054bbf7d8/MeasureReport-b77a6309-214c-4fc2-a9bc-18d81c740da6.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ 6aef5a18-59bd-4a47-80bc-2bd44636e41f ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/6aef5a18-59bd-4a47-80bc-2bd44636e41f/MeasureReport-e5735d61-0444-4958-8f47-165a59e91dc0.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ b85440e4-b902-49cd-b3d6-363ba7a99bce ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/b85440e4-b902-49cd-b3d6-363ba7a99bce/MeasureReport-9d61df39-18a0-451f-a795-988388d58778.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |
| [ d9840e8c-3359-42c2-b354-4b236c3c1b15 ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/d9840e8c-3359-42c2-b354-4b236c3c1b15/MeasureReport-1fbf56ab-6e60-4ce6-a1d5-b520382164bd.json) | Group_1 | Denominator Exception | 1 | 0 | — | FAIL |


#### CMS144FHIRHFBetaBlockerForLVSD
[ [cql] ](../../input/cql/CMS144FHIRHFBetaBlockerForLVSD.cql) [ [test results] ](../../input/tests/results/CMS144FHIRHFBetaBlockerForLVSD.txt)

QICore: 240 / 0 — passes

Mismatched Test Cases (3 of  of 48)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 07efd4bb-b45d-4bfd-aeb2-08de49742d91 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/07efd4bb-b45d-4bfd-aeb2-08de49742d91/MeasureReport-ad01867d-c2c7-4317-9925-deb909d156e6.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 67779bc6-07ee-42cf-8ca7-e71302915dba ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/67779bc6-07ee-42cf-8ca7-e71302915dba/MeasureReport-5b182aca-ad2a-4651-ba6b-df02e001ec36.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 7b8885c5-ad14-4361-9755-c76a6e3b8530 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/7b8885c5-ad14-4361-9755-c76a6e3b8530/MeasureReport-7e421d2a-1ee4-4c56-a454-815983c21106.json) | Group_1 | Numerator | 0 | 1 | — | PASS |


#### CMS145FHIRCADBBlockerTPMIorLVSD
[ [cql] ](../../input/cql/CMS145FHIRCADBBlockerTPMIorLVSD.cql) [ [test results] ](../../input/tests/results/CMS145FHIRCADBBlockerTPMIorLVSD.txt)

QICore: 422 / 2 — has discrepancies (2 mismatched, 0 missing)

Mismatched Test Cases (6 of  of 106)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 1f70822b-c513-4c3a-8162-49f0bb9c914b ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/1f70822b-c513-4c3a-8162-49f0bb9c914b/MeasureReport-9b3577fa-355c-409d-8d3f-21e9720fb889.json) | Group_2 | Denominator Exception | 0 | 1 | — | FAIL |
| [ 4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e/MeasureReport-e77c61ff-cc3a-402c-9752-7a97a6727a39.json) | Group_2 | Denominator Exception | 1 | 0 | — | PASS |
| [ 5fd0d626-e9c5-4e6c-a10d-1a1183fa7702 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/5fd0d626-e9c5-4e6c-a10d-1a1183fa7702/MeasureReport-ce1b8712-b9dd-48e2-adf4-554ed641bee5.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 61306767-0e74-44b8-ac06-1339c3783355 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/61306767-0e74-44b8-ac06-1339c3783355/MeasureReport-6ea40199-5a45-4c8d-8a2b-c08bf93ebd8a.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ b65680a0-9768-4ce4-b08d-972fcd84e28e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/b65680a0-9768-4ce4-b08d-972fcd84e28e/MeasureReport-b5ebd0a9-a2de-4b31-b0d9-588888e95872.json) | Group_2 | Denominator Exception | 1 | 0 | — | PASS |
| [ fd5fb311-a466-4c59-966d-48fa7aa88931 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/fd5fb311-a466-4c59-966d-48fa7aa88931/MeasureReport-05ffed3e-5604-40eb-bcf8-99cacecc26c0.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |


#### CMS156FHIRHighRiskMedsElderly
[ [cql] ](../../input/cql/CMS156FHIRHighRiskMedsElderly.cql) [ [test results] ](../../input/tests/results/CMS156FHIRHighRiskMedsElderly.txt)

QICore: 702 / 6 — has discrepancies (6 mismatched, 0 missing)

Mismatched Test Cases (135 of  of 177)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 00da3e6b-9ab1-48b6-8b34-a0f08754fb3c ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/00da3e6b-9ab1-48b6-8b34-a0f08754fb3c/MeasureReport-19caad5b-d3a2-4d95-9330-42178011c446.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 00da3e6b-9ab1-48b6-8b34-a0f08754fb3c ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/00da3e6b-9ab1-48b6-8b34-a0f08754fb3c/MeasureReport-19caad5b-d3a2-4d95-9330-42178011c446.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 00da3e6b-9ab1-48b6-8b34-a0f08754fb3c ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/00da3e6b-9ab1-48b6-8b34-a0f08754fb3c/MeasureReport-19caad5b-d3a2-4d95-9330-42178011c446.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 0440a9c0-f299-43a6-bfef-cb2cf326ee85 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/0440a9c0-f299-43a6-bfef-cb2cf326ee85/MeasureReport-271892a0-66d3-4946-9b36-0f9a51e345e8.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 0440a9c0-f299-43a6-bfef-cb2cf326ee85 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/0440a9c0-f299-43a6-bfef-cb2cf326ee85/MeasureReport-271892a0-66d3-4946-9b36-0f9a51e345e8.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 0440a9c0-f299-43a6-bfef-cb2cf326ee85 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/0440a9c0-f299-43a6-bfef-cb2cf326ee85/MeasureReport-271892a0-66d3-4946-9b36-0f9a51e345e8.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 07f11229-6e8f-42bf-9905-3d319460fb33 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/07f11229-6e8f-42bf-9905-3d319460fb33/MeasureReport-ac117acb-b03e-4be3-b7eb-0e88ef892234.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>FAIL |
| [ 07f11229-6e8f-42bf-9905-3d319460fb33 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/07f11229-6e8f-42bf-9905-3d319460fb33/MeasureReport-ac117acb-b03e-4be3-b7eb-0e88ef892234.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 07f11229-6e8f-42bf-9905-3d319460fb33 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/07f11229-6e8f-42bf-9905-3d319460fb33/MeasureReport-ac117acb-b03e-4be3-b7eb-0e88ef892234.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>FAIL |
| [ 0e31d00f-8b4e-4800-a7f7-ab8b824bf689 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/0e31d00f-8b4e-4800-a7f7-ab8b824bf689/MeasureReport-455174c8-a9fe-46d4-83d3-a16e9aad4056.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 0e31d00f-8b4e-4800-a7f7-ab8b824bf689 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/0e31d00f-8b4e-4800-a7f7-ab8b824bf689/MeasureReport-455174c8-a9fe-46d4-83d3-a16e9aad4056.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 0e31d00f-8b4e-4800-a7f7-ab8b824bf689 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/0e31d00f-8b4e-4800-a7f7-ab8b824bf689/MeasureReport-455174c8-a9fe-46d4-83d3-a16e9aad4056.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 1789d80d-bc5b-4e15-ab64-399d05e55a19 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/1789d80d-bc5b-4e15-ab64-399d05e55a19/MeasureReport-c5842144-5823-4fe2-93dc-a7cc368968fe.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 1789d80d-bc5b-4e15-ab64-399d05e55a19 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/1789d80d-bc5b-4e15-ab64-399d05e55a19/MeasureReport-c5842144-5823-4fe2-93dc-a7cc368968fe.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 1789d80d-bc5b-4e15-ab64-399d05e55a19 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/1789d80d-bc5b-4e15-ab64-399d05e55a19/MeasureReport-c5842144-5823-4fe2-93dc-a7cc368968fe.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 1968ff78-9027-4ea9-99c8-42282743bfc3 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/1968ff78-9027-4ea9-99c8-42282743bfc3/MeasureReport-dd446a06-dac7-4b71-ab9d-ea52a53a7508.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 1968ff78-9027-4ea9-99c8-42282743bfc3 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/1968ff78-9027-4ea9-99c8-42282743bfc3/MeasureReport-dd446a06-dac7-4b71-ab9d-ea52a53a7508.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 1968ff78-9027-4ea9-99c8-42282743bfc3 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/1968ff78-9027-4ea9-99c8-42282743bfc3/MeasureReport-dd446a06-dac7-4b71-ab9d-ea52a53a7508.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 2389a7bb-16a7-4800-ba4a-2585ebd98a0a ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/2389a7bb-16a7-4800-ba4a-2585ebd98a0a/MeasureReport-5d66494f-0140-46ae-9010-59ed11eae359.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 2389a7bb-16a7-4800-ba4a-2585ebd98a0a ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/2389a7bb-16a7-4800-ba4a-2585ebd98a0a/MeasureReport-5d66494f-0140-46ae-9010-59ed11eae359.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 2389a7bb-16a7-4800-ba4a-2585ebd98a0a ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/2389a7bb-16a7-4800-ba4a-2585ebd98a0a/MeasureReport-5d66494f-0140-46ae-9010-59ed11eae359.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 24d82fc3-13b1-4974-9dc1-7771580853df ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/24d82fc3-13b1-4974-9dc1-7771580853df/MeasureReport-bc81d15a-be41-4f94-a855-0717993715b0.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 24d82fc3-13b1-4974-9dc1-7771580853df ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/24d82fc3-13b1-4974-9dc1-7771580853df/MeasureReport-bc81d15a-be41-4f94-a855-0717993715b0.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 24d82fc3-13b1-4974-9dc1-7771580853df ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/24d82fc3-13b1-4974-9dc1-7771580853df/MeasureReport-bc81d15a-be41-4f94-a855-0717993715b0.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 2dfe5252-0eb7-4519-9f4c-d7f95a7acaae ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/2dfe5252-0eb7-4519-9f4c-d7f95a7acaae/MeasureReport-b5094369-1007-4244-b7d9-46734b17b3af.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 2dfe5252-0eb7-4519-9f4c-d7f95a7acaae ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/2dfe5252-0eb7-4519-9f4c-d7f95a7acaae/MeasureReport-b5094369-1007-4244-b7d9-46734b17b3af.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 2dfe5252-0eb7-4519-9f4c-d7f95a7acaae ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/2dfe5252-0eb7-4519-9f4c-d7f95a7acaae/MeasureReport-b5094369-1007-4244-b7d9-46734b17b3af.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 385599b5-a1e9-4b7a-8e9f-281c58fed95e ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/385599b5-a1e9-4b7a-8e9f-281c58fed95e/MeasureReport-e7c00bd2-998d-41ff-8e2d-e7021fbf3134.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 385599b5-a1e9-4b7a-8e9f-281c58fed95e ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/385599b5-a1e9-4b7a-8e9f-281c58fed95e/MeasureReport-e7c00bd2-998d-41ff-8e2d-e7021fbf3134.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 385599b5-a1e9-4b7a-8e9f-281c58fed95e ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/385599b5-a1e9-4b7a-8e9f-281c58fed95e/MeasureReport-e7c00bd2-998d-41ff-8e2d-e7021fbf3134.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 407618d7-e2c7-4aae-9744-b447193c4c15 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/407618d7-e2c7-4aae-9744-b447193c4c15/MeasureReport-d11d837f-0368-4255-bc76-6a4fd80184f6.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 407618d7-e2c7-4aae-9744-b447193c4c15 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/407618d7-e2c7-4aae-9744-b447193c4c15/MeasureReport-d11d837f-0368-4255-bc76-6a4fd80184f6.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 407618d7-e2c7-4aae-9744-b447193c4c15 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/407618d7-e2c7-4aae-9744-b447193c4c15/MeasureReport-d11d837f-0368-4255-bc76-6a4fd80184f6.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 42125b07-9cb2-44df-ba1f-78237b0d3ebc ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/42125b07-9cb2-44df-ba1f-78237b0d3ebc/MeasureReport-218cb83f-e1c9-45de-a46b-caa45ab68688.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 42125b07-9cb2-44df-ba1f-78237b0d3ebc ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/42125b07-9cb2-44df-ba1f-78237b0d3ebc/MeasureReport-218cb83f-e1c9-45de-a46b-caa45ab68688.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 42125b07-9cb2-44df-ba1f-78237b0d3ebc ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/42125b07-9cb2-44df-ba1f-78237b0d3ebc/MeasureReport-218cb83f-e1c9-45de-a46b-caa45ab68688.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 435702f5-68ca-4f81-a7e1-b5060726bb75 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/435702f5-68ca-4f81-a7e1-b5060726bb75/MeasureReport-1e3e3c0b-f7aa-4620-928a-2c4b425f7c89.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 435702f5-68ca-4f81-a7e1-b5060726bb75 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/435702f5-68ca-4f81-a7e1-b5060726bb75/MeasureReport-1e3e3c0b-f7aa-4620-928a-2c4b425f7c89.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 435702f5-68ca-4f81-a7e1-b5060726bb75 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/435702f5-68ca-4f81-a7e1-b5060726bb75/MeasureReport-1e3e3c0b-f7aa-4620-928a-2c4b425f7c89.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 47f69fc0-fac8-4f88-876b-cf415ec0e214 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/47f69fc0-fac8-4f88-876b-cf415ec0e214/MeasureReport-175d70ea-4b34-417c-bf58-6aef6cf08c40.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 47f69fc0-fac8-4f88-876b-cf415ec0e214 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/47f69fc0-fac8-4f88-876b-cf415ec0e214/MeasureReport-175d70ea-4b34-417c-bf58-6aef6cf08c40.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 47f69fc0-fac8-4f88-876b-cf415ec0e214 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/47f69fc0-fac8-4f88-876b-cf415ec0e214/MeasureReport-175d70ea-4b34-417c-bf58-6aef6cf08c40.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 4aa75d19-ac8b-49b0-a686-429fbc033d77 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/MeasureReport-139cc56e-5ffb-46ca-89ce-accd0bb642ab.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>FAIL |
| [ 4aa75d19-ac8b-49b0-a686-429fbc033d77 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/MeasureReport-139cc56e-5ffb-46ca-89ce-accd0bb642ab.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 4aa75d19-ac8b-49b0-a686-429fbc033d77 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/MeasureReport-139cc56e-5ffb-46ca-89ce-accd0bb642ab.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>FAIL |
| [ 52f08670-4df5-4538-b009-eb96e3247618 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/52f08670-4df5-4538-b009-eb96e3247618/MeasureReport-04f4d477-20c4-4d69-a549-e30109dbc937.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 52f08670-4df5-4538-b009-eb96e3247618 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/52f08670-4df5-4538-b009-eb96e3247618/MeasureReport-04f4d477-20c4-4d69-a549-e30109dbc937.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 52f08670-4df5-4538-b009-eb96e3247618 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/52f08670-4df5-4538-b009-eb96e3247618/MeasureReport-04f4d477-20c4-4d69-a549-e30109dbc937.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5326ef57-57d6-49b8-bdc5-b3179cdcb82d ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/5326ef57-57d6-49b8-bdc5-b3179cdcb82d/MeasureReport-13d2c78d-9ea8-43cb-976a-07636ad51575.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5326ef57-57d6-49b8-bdc5-b3179cdcb82d ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/5326ef57-57d6-49b8-bdc5-b3179cdcb82d/MeasureReport-13d2c78d-9ea8-43cb-976a-07636ad51575.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 5326ef57-57d6-49b8-bdc5-b3179cdcb82d ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/5326ef57-57d6-49b8-bdc5-b3179cdcb82d/MeasureReport-13d2c78d-9ea8-43cb-976a-07636ad51575.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 5c33755f-40d8-4409-b699-a3499ddddda0 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/5c33755f-40d8-4409-b699-a3499ddddda0/MeasureReport-a8c9543e-2148-48f9-bd4a-0f5ab6ff7b57.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5c33755f-40d8-4409-b699-a3499ddddda0 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/5c33755f-40d8-4409-b699-a3499ddddda0/MeasureReport-a8c9543e-2148-48f9-bd4a-0f5ab6ff7b57.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5c33755f-40d8-4409-b699-a3499ddddda0 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/5c33755f-40d8-4409-b699-a3499ddddda0/MeasureReport-a8c9543e-2148-48f9-bd4a-0f5ab6ff7b57.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5f200044-e0b1-4e20-8ee7-b9e735d3086c ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/5f200044-e0b1-4e20-8ee7-b9e735d3086c/MeasureReport-9753dfc5-7516-4386-a577-021222f51eed.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5f200044-e0b1-4e20-8ee7-b9e735d3086c ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/5f200044-e0b1-4e20-8ee7-b9e735d3086c/MeasureReport-9753dfc5-7516-4386-a577-021222f51eed.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5f200044-e0b1-4e20-8ee7-b9e735d3086c ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/5f200044-e0b1-4e20-8ee7-b9e735d3086c/MeasureReport-9753dfc5-7516-4386-a577-021222f51eed.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 64c49012-0f98-41da-a00b-9cd673294d16 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/64c49012-0f98-41da-a00b-9cd673294d16/MeasureReport-99a65a2b-e608-4f67-918d-ea3f8ceb03f2.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 64c49012-0f98-41da-a00b-9cd673294d16 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/64c49012-0f98-41da-a00b-9cd673294d16/MeasureReport-99a65a2b-e608-4f67-918d-ea3f8ceb03f2.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 64c49012-0f98-41da-a00b-9cd673294d16 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/64c49012-0f98-41da-a00b-9cd673294d16/MeasureReport-99a65a2b-e608-4f67-918d-ea3f8ceb03f2.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca/MeasureReport-e1513040-9d4e-4b28-9f9d-e6cd97c8e6ec.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca/MeasureReport-e1513040-9d4e-4b28-9f9d-e6cd97c8e6ec.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca/MeasureReport-e1513040-9d4e-4b28-9f9d-e6cd97c8e6ec.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 708b6eaa-5d2e-463b-9d9f-d97b19f4af75 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/708b6eaa-5d2e-463b-9d9f-d97b19f4af75/MeasureReport-327a2ed8-83a9-4d99-8467-5bf3b847c057.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 708b6eaa-5d2e-463b-9d9f-d97b19f4af75 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/708b6eaa-5d2e-463b-9d9f-d97b19f4af75/MeasureReport-327a2ed8-83a9-4d99-8467-5bf3b847c057.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 708b6eaa-5d2e-463b-9d9f-d97b19f4af75 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/708b6eaa-5d2e-463b-9d9f-d97b19f4af75/MeasureReport-327a2ed8-83a9-4d99-8467-5bf3b847c057.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 79c12ad7-f7de-4b87-93c3-ef85e0a644f0 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/79c12ad7-f7de-4b87-93c3-ef85e0a644f0/MeasureReport-317ecbe9-4bf3-45c1-b453-152c7039e46d.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 79c12ad7-f7de-4b87-93c3-ef85e0a644f0 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/79c12ad7-f7de-4b87-93c3-ef85e0a644f0/MeasureReport-317ecbe9-4bf3-45c1-b453-152c7039e46d.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 79c12ad7-f7de-4b87-93c3-ef85e0a644f0 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/79c12ad7-f7de-4b87-93c3-ef85e0a644f0/MeasureReport-317ecbe9-4bf3-45c1-b453-152c7039e46d.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 7f204655-dbf6-47d7-a684-ff1570cf4b05 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/7f204655-dbf6-47d7-a684-ff1570cf4b05/MeasureReport-e3454afe-2c47-48c4-8cfa-8a71eb842a52.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 7f204655-dbf6-47d7-a684-ff1570cf4b05 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/7f204655-dbf6-47d7-a684-ff1570cf4b05/MeasureReport-e3454afe-2c47-48c4-8cfa-8a71eb842a52.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 7f204655-dbf6-47d7-a684-ff1570cf4b05 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/7f204655-dbf6-47d7-a684-ff1570cf4b05/MeasureReport-e3454afe-2c47-48c4-8cfa-8a71eb842a52.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8082ddbf-8d01-4b29-8709-70e70bbc70f9 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8082ddbf-8d01-4b29-8709-70e70bbc70f9/MeasureReport-0e92c959-814d-434f-8b9f-bce7f8644e2b.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8082ddbf-8d01-4b29-8709-70e70bbc70f9 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8082ddbf-8d01-4b29-8709-70e70bbc70f9/MeasureReport-0e92c959-814d-434f-8b9f-bce7f8644e2b.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8082ddbf-8d01-4b29-8709-70e70bbc70f9 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8082ddbf-8d01-4b29-8709-70e70bbc70f9/MeasureReport-0e92c959-814d-434f-8b9f-bce7f8644e2b.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8e648527-5b7e-430c-b5ca-fe70a4133d55 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8e648527-5b7e-430c-b5ca-fe70a4133d55/MeasureReport-0dcd49bb-3eb9-41be-b9ec-2d4cbb9469fb.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8e648527-5b7e-430c-b5ca-fe70a4133d55 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8e648527-5b7e-430c-b5ca-fe70a4133d55/MeasureReport-0dcd49bb-3eb9-41be-b9ec-2d4cbb9469fb.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8e648527-5b7e-430c-b5ca-fe70a4133d55 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8e648527-5b7e-430c-b5ca-fe70a4133d55/MeasureReport-0dcd49bb-3eb9-41be-b9ec-2d4cbb9469fb.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8f713481-66ba-4a58-be92-91b8c7212959 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8f713481-66ba-4a58-be92-91b8c7212959/MeasureReport-60b0bb1f-929e-47cc-a34c-63f8a25b1063.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8f713481-66ba-4a58-be92-91b8c7212959 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8f713481-66ba-4a58-be92-91b8c7212959/MeasureReport-60b0bb1f-929e-47cc-a34c-63f8a25b1063.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8f713481-66ba-4a58-be92-91b8c7212959 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8f713481-66ba-4a58-be92-91b8c7212959/MeasureReport-60b0bb1f-929e-47cc-a34c-63f8a25b1063.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a4ece596-2f97-4fbb-88e6-4418d8a7e713 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a4ece596-2f97-4fbb-88e6-4418d8a7e713/MeasureReport-c77ff600-0052-4993-abf6-acde9d75a623.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a4ece596-2f97-4fbb-88e6-4418d8a7e713 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a4ece596-2f97-4fbb-88e6-4418d8a7e713/MeasureReport-c77ff600-0052-4993-abf6-acde9d75a623.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a4ece596-2f97-4fbb-88e6-4418d8a7e713 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a4ece596-2f97-4fbb-88e6-4418d8a7e713/MeasureReport-c77ff600-0052-4993-abf6-acde9d75a623.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a550fe5a-03ad-4eb3-9157-dcb64f8b13be ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a550fe5a-03ad-4eb3-9157-dcb64f8b13be/MeasureReport-1ae70be3-8690-4902-aca5-e8048a6c2de1.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a550fe5a-03ad-4eb3-9157-dcb64f8b13be ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a550fe5a-03ad-4eb3-9157-dcb64f8b13be/MeasureReport-1ae70be3-8690-4902-aca5-e8048a6c2de1.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a550fe5a-03ad-4eb3-9157-dcb64f8b13be ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a550fe5a-03ad-4eb3-9157-dcb64f8b13be/MeasureReport-1ae70be3-8690-4902-aca5-e8048a6c2de1.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a584af54-f1b9-4abc-b90b-1a2fa3b2016e ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a584af54-f1b9-4abc-b90b-1a2fa3b2016e/MeasureReport-13a90135-8f47-4594-9963-d92d9448031a.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a584af54-f1b9-4abc-b90b-1a2fa3b2016e ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a584af54-f1b9-4abc-b90b-1a2fa3b2016e/MeasureReport-13a90135-8f47-4594-9963-d92d9448031a.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a584af54-f1b9-4abc-b90b-1a2fa3b2016e ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a584af54-f1b9-4abc-b90b-1a2fa3b2016e/MeasureReport-13a90135-8f47-4594-9963-d92d9448031a.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a6b1d740-d580-4e55-970e-3cb4f1e369c2 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a6b1d740-d580-4e55-970e-3cb4f1e369c2/MeasureReport-12fa7bd7-0c1d-4437-8622-2a219942b2e1.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a6b1d740-d580-4e55-970e-3cb4f1e369c2 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a6b1d740-d580-4e55-970e-3cb4f1e369c2/MeasureReport-12fa7bd7-0c1d-4437-8622-2a219942b2e1.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a6b1d740-d580-4e55-970e-3cb4f1e369c2 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a6b1d740-d580-4e55-970e-3cb4f1e369c2/MeasureReport-12fa7bd7-0c1d-4437-8622-2a219942b2e1.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a7b09e2e-cdb0-4206-986a-45bb70f9d49f ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a7b09e2e-cdb0-4206-986a-45bb70f9d49f/MeasureReport-1bea13f9-069e-4f0a-90c8-4f298ffefae2.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a7b09e2e-cdb0-4206-986a-45bb70f9d49f ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a7b09e2e-cdb0-4206-986a-45bb70f9d49f/MeasureReport-1bea13f9-069e-4f0a-90c8-4f298ffefae2.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a7b09e2e-cdb0-4206-986a-45bb70f9d49f ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/a7b09e2e-cdb0-4206-986a-45bb70f9d49f/MeasureReport-1bea13f9-069e-4f0a-90c8-4f298ffefae2.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ aeef1eb1-86fa-4af0-b24d-fc7ad8398191 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/aeef1eb1-86fa-4af0-b24d-fc7ad8398191/MeasureReport-28a51e65-13c2-4fde-b103-93fcb80f0903.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ aeef1eb1-86fa-4af0-b24d-fc7ad8398191 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/aeef1eb1-86fa-4af0-b24d-fc7ad8398191/MeasureReport-28a51e65-13c2-4fde-b103-93fcb80f0903.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ aeef1eb1-86fa-4af0-b24d-fc7ad8398191 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/aeef1eb1-86fa-4af0-b24d-fc7ad8398191/MeasureReport-28a51e65-13c2-4fde-b103-93fcb80f0903.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ bc0146d2-5deb-46bc-b7a8-657d4f3ed031 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/bc0146d2-5deb-46bc-b7a8-657d4f3ed031/MeasureReport-ddd926ab-7a9f-48e5-b916-78cf3b9b8920.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ bc0146d2-5deb-46bc-b7a8-657d4f3ed031 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/bc0146d2-5deb-46bc-b7a8-657d4f3ed031/MeasureReport-ddd926ab-7a9f-48e5-b916-78cf3b9b8920.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ bc0146d2-5deb-46bc-b7a8-657d4f3ed031 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/bc0146d2-5deb-46bc-b7a8-657d4f3ed031/MeasureReport-ddd926ab-7a9f-48e5-b916-78cf3b9b8920.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ c0af145d-bf0c-4b3d-8f65-d446c9f93b15 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c0af145d-bf0c-4b3d-8f65-d446c9f93b15/MeasureReport-8117fc2d-ec6c-4fdb-8486-d872c18a4b0c.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ c0af145d-bf0c-4b3d-8f65-d446c9f93b15 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c0af145d-bf0c-4b3d-8f65-d446c9f93b15/MeasureReport-8117fc2d-ec6c-4fdb-8486-d872c18a4b0c.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c0af145d-bf0c-4b3d-8f65-d446c9f93b15 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c0af145d-bf0c-4b3d-8f65-d446c9f93b15/MeasureReport-8117fc2d-ec6c-4fdb-8486-d872c18a4b0c.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ c409fbc9-a31f-4d53-9aa7-9e443e87812a ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c409fbc9-a31f-4d53-9aa7-9e443e87812a/MeasureReport-51d79f15-0540-456f-858f-e6ad2c96e95a.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>FAIL |
| [ c409fbc9-a31f-4d53-9aa7-9e443e87812a ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c409fbc9-a31f-4d53-9aa7-9e443e87812a/MeasureReport-51d79f15-0540-456f-858f-e6ad2c96e95a.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c409fbc9-a31f-4d53-9aa7-9e443e87812a ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c409fbc9-a31f-4d53-9aa7-9e443e87812a/MeasureReport-51d79f15-0540-456f-858f-e6ad2c96e95a.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>FAIL |
| [ c5c6788b-16f3-4c11-badf-5739989be2f6 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c5c6788b-16f3-4c11-badf-5739989be2f6/MeasureReport-a8e6a568-6c12-407e-92c9-c2a7a78632ad.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c5c6788b-16f3-4c11-badf-5739989be2f6 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c5c6788b-16f3-4c11-badf-5739989be2f6/MeasureReport-a8e6a568-6c12-407e-92c9-c2a7a78632ad.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c5c6788b-16f3-4c11-badf-5739989be2f6 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c5c6788b-16f3-4c11-badf-5739989be2f6/MeasureReport-a8e6a568-6c12-407e-92c9-c2a7a78632ad.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c9aa1676-c1cd-4d98-aa1d-fe66762d4c73 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c9aa1676-c1cd-4d98-aa1d-fe66762d4c73/MeasureReport-ef9fd8cc-cf6a-4909-a2fa-9d1e196dc1d5.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c9aa1676-c1cd-4d98-aa1d-fe66762d4c73 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c9aa1676-c1cd-4d98-aa1d-fe66762d4c73/MeasureReport-ef9fd8cc-cf6a-4909-a2fa-9d1e196dc1d5.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c9aa1676-c1cd-4d98-aa1d-fe66762d4c73 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/c9aa1676-c1cd-4d98-aa1d-fe66762d4c73/MeasureReport-ef9fd8cc-cf6a-4909-a2fa-9d1e196dc1d5.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ cb01ddd0-a804-4bbe-8544-d4c753898eca ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/cb01ddd0-a804-4bbe-8544-d4c753898eca/MeasureReport-2efd499c-6b01-4fa8-b4e1-80c5e09571f8.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ cb01ddd0-a804-4bbe-8544-d4c753898eca ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/cb01ddd0-a804-4bbe-8544-d4c753898eca/MeasureReport-2efd499c-6b01-4fa8-b4e1-80c5e09571f8.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ cb01ddd0-a804-4bbe-8544-d4c753898eca ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/cb01ddd0-a804-4bbe-8544-d4c753898eca/MeasureReport-2efd499c-6b01-4fa8-b4e1-80c5e09571f8.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ d0e744f6-9951-4a29-99d9-8052efcde892 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/d0e744f6-9951-4a29-99d9-8052efcde892/MeasureReport-efa74566-4f92-4c71-b7d9-38764d7bed10.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d0e744f6-9951-4a29-99d9-8052efcde892 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/d0e744f6-9951-4a29-99d9-8052efcde892/MeasureReport-efa74566-4f92-4c71-b7d9-38764d7bed10.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d0e744f6-9951-4a29-99d9-8052efcde892 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/d0e744f6-9951-4a29-99d9-8052efcde892/MeasureReport-efa74566-4f92-4c71-b7d9-38764d7bed10.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d641333e-031e-40e1-9552-11d4bbe7cd33 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/d641333e-031e-40e1-9552-11d4bbe7cd33/MeasureReport-3cb861c7-8942-42a3-9079-debd1f13d094.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ d641333e-031e-40e1-9552-11d4bbe7cd33 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/d641333e-031e-40e1-9552-11d4bbe7cd33/MeasureReport-3cb861c7-8942-42a3-9079-debd1f13d094.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d641333e-031e-40e1-9552-11d4bbe7cd33 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/d641333e-031e-40e1-9552-11d4bbe7cd33/MeasureReport-3cb861c7-8942-42a3-9079-debd1f13d094.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ e00d1066-19b2-4d59-8829-d90f1e7a1233 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/e00d1066-19b2-4d59-8829-d90f1e7a1233/MeasureReport-5b50ece9-6516-40c2-9c49-d261d06b80d5.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ e00d1066-19b2-4d59-8829-d90f1e7a1233 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/e00d1066-19b2-4d59-8829-d90f1e7a1233/MeasureReport-5b50ece9-6516-40c2-9c49-d261d06b80d5.json) | Group_2 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ e00d1066-19b2-4d59-8829-d90f1e7a1233 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/e00d1066-19b2-4d59-8829-d90f1e7a1233/MeasureReport-5b50ece9-6516-40c2-9c49-d261d06b80d5.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ e4cdfed0-16f0-46cd-a45c-95714744758b ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/e4cdfed0-16f0-46cd-a45c-95714744758b/MeasureReport-1f8de45d-01a9-4150-b45f-259ba6d3a429.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ e4cdfed0-16f0-46cd-a45c-95714744758b ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/e4cdfed0-16f0-46cd-a45c-95714744758b/MeasureReport-1f8de45d-01a9-4150-b45f-259ba6d3a429.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ e4cdfed0-16f0-46cd-a45c-95714744758b ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/e4cdfed0-16f0-46cd-a45c-95714744758b/MeasureReport-1f8de45d-01a9-4150-b45f-259ba6d3a429.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ ea9af1dc-c26e-4bc3-947b-6c4bbd65523c ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/ea9af1dc-c26e-4bc3-947b-6c4bbd65523c/MeasureReport-95607d58-bfa1-43fb-8fe9-8a0e479b8885.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ ea9af1dc-c26e-4bc3-947b-6c4bbd65523c ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/ea9af1dc-c26e-4bc3-947b-6c4bbd65523c/MeasureReport-95607d58-bfa1-43fb-8fe9-8a0e479b8885.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ ea9af1dc-c26e-4bc3-947b-6c4bbd65523c ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/ea9af1dc-c26e-4bc3-947b-6c4bbd65523c/MeasureReport-95607d58-bfa1-43fb-8fe9-8a0e479b8885.json) | Group_3 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4/MeasureReport-d77fc8e8-cc89-4885-abd7-e6ccce9c9e53.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4/MeasureReport-d77fc8e8-cc89-4885-abd7-e6ccce9c9e53.json) | Group_2 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4/MeasureReport-d77fc8e8-cc89-4885-abd7-e6ccce9c9e53.json) | Group_3 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |


#### CMS157FHIRPainIntensityQuantified
[ [cql] ](../../input/cql/CMS157FHIRPainIntensityQuantified.cql) [ [test results] ](../../input/tests/results/CMS157FHIRPainIntensityQuantified.txt)

QICore: 332 / 46 — has discrepancies (19 mismatched, 0 missing)

Mismatched Test Cases (19 of  of 126)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 055640ae-dc71-4e1d-918b-e367013de209 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/055640ae-dc71-4e1d-918b-e367013de209/MeasureReport-1bbaa68f-b303-4828-aa6b-c3f5d25b9246.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 233d84af-d725-4682-8253-d6c4e02da0d5 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/233d84af-d725-4682-8253-d6c4e02da0d5/MeasureReport-8ebccd0b-cee9-43d9-b663-9d228417615d.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 2c3f5ac5-6b7f-4bb8-a4fe-8faf0553b21e ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/2c3f5ac5-6b7f-4bb8-a4fe-8faf0553b21e/MeasureReport-c0205a42-bb91-4962-a72f-4df278aae5b7.json) | Group_2 | Initial Population<br>Denominator | 2<br>2 | 0<br>0 | — | FAIL<br>FAIL |
| [ 51d8547c-f07f-4441-b616-f458f38e4506 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/51d8547c-f07f-4441-b616-f458f38e4506/MeasureReport-54825fed-8c96-4302-90ae-f0b99310d3dd.json) | Group_2 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 5cca62ff-f856-4b8f-9902-6a018a4599cb ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/5cca62ff-f856-4b8f-9902-6a018a4599cb/MeasureReport-c03b4642-f99f-40d7-ae8f-37795a5caf5f.json) | Group_2 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 66c60f6c-2a7b-4868-b9bd-5ede60b61463 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/66c60f6c-2a7b-4868-b9bd-5ede60b61463/MeasureReport-e916d4be-b50b-4fec-92aa-9b8307a9d3ed.json) | Group_2 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 719a6ae4-ac86-406f-a762-380383e4a74d ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/719a6ae4-ac86-406f-a762-380383e4a74d/MeasureReport-84729f91-b0f3-4571-80b0-40bfa0dd05ee.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 757c5855-602e-4c25-8783-c22afccc1618 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/757c5855-602e-4c25-8783-c22afccc1618/MeasureReport-64d75922-fcb8-4e74-b5e0-c399e8920b43.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 7cedf97f-741c-4c37-9ae9-40e0b8c64576 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/7cedf97f-741c-4c37-9ae9-40e0b8c64576/MeasureReport-32f463b3-7147-4a6c-aaf5-05478cb060da.json) | Group_2 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 837cc0e4-cc26-48cd-9d34-232d7fbcd056 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/837cc0e4-cc26-48cd-9d34-232d7fbcd056/MeasureReport-8156684d-e121-4d37-81b6-58a35429e39e.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 8e23417a-471a-45bb-b936-57466dc6592c ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/8e23417a-471a-45bb-b936-57466dc6592c/MeasureReport-c828863c-4c72-4cc4-8156-ede8adc10db1.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 90d3454a-ca4b-4035-a524-255a2f03bef7 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/90d3454a-ca4b-4035-a524-255a2f03bef7/MeasureReport-a518ac8d-270d-4777-b241-d68e6d89d348.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>2 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 9972f780-aa2f-40e0-ba7d-133d7fe38bc9 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/9972f780-aa2f-40e0-ba7d-133d7fe38bc9/MeasureReport-17ffaaff-f814-456d-a5b2-9481b621a657.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ aa355e31-8d29-4b06-8d13-7d00a2c817da ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/aa355e31-8d29-4b06-8d13-7d00a2c817da/MeasureReport-cd826ca2-6155-4ae2-884d-6fa9c5343198.json) | Group_2 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ c97c9ecf-6c31-4868-bbd3-7a5509bb3882 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/c97c9ecf-6c31-4868-bbd3-7a5509bb3882/MeasureReport-f718a369-2b4b-430a-9d24-9a4f06a7b002.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ d4b441fb-5b3a-40f7-ada1-ecf06376f4fb ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/d4b441fb-5b3a-40f7-ada1-ecf06376f4fb/MeasureReport-72e35d1c-2e54-4a52-ac2e-430785c31ee5.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ e085c0d1-a736-4596-a5cd-7de785d0d144 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/e085c0d1-a736-4596-a5cd-7de785d0d144/MeasureReport-dfa6cb5c-77dd-47e1-968c-8b280300f2d0.json) | Group_2 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ ede0d103-285f-42f0-807e-ff272f1ae70e ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/ede0d103-285f-42f0-807e-ff272f1ae70e/MeasureReport-db410136-ae00-4328-941e-366a83436c05.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ fe6ef07d-bff1-4e0e-9bf4-b0424a1d0ab4 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/fe6ef07d-bff1-4e0e-9bf4-b0424a1d0ab4/MeasureReport-0648e2db-7eb4-422a-b7f2-b920be7285f2.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |


#### CMS159FHIRDepRemissionat12Months
[ [cql] ](../../input/cql/CMS159FHIRDepRemissionat12Months.cql) [ [test results] ](../../input/tests/results/CMS159FHIRDepRemissionat12Months.txt)

QICore: 264 / 4 — has discrepancies (2 mismatched, 0 missing)

Mismatched Test Cases (2 of  of 67)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 491f554e-e897-40c5-ad2b-0983923df4e8 ](../.././input/tests/measure/CMS159FHIRDepRemissionat12Months/491f554e-e897-40c5-ad2b-0983923df4e8/MeasureReport-580087e1-b59e-43eb-b110-692c35a82dca.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>FAIL |
| [ 96b6579c-1cee-423f-9433-a72db6fb8a0a ](../.././input/tests/measure/CMS159FHIRDepRemissionat12Months/96b6579c-1cee-423f-9433-a72db6fb8a0a/MeasureReport-e3ec1311-05ed-4a6f-b13f-a4d290865bb3.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>FAIL |


#### CMS165FHIRControllingHighBP
[ [cql] ](../../input/cql/CMS165FHIRControllingHighBP.cql) [ [test results] ](../../input/tests/results/CMS165FHIRControllingHighBP.txt)

QICore: 259 / 13 — has discrepancies (9 mismatched, 1 missing)

Missing Results (1 of 68 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 45e01fed-56bb-483d-a860-af3d566bda11 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/45e01fed-56bb-483d-a860-af3d566bda11/MeasureReport-02991ca7-859d-422d-8849-655760f8e10a.json) | Group_1 | E-11 — resolution pending |


Mismatched Test Cases (29 of  of 68)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 045f7e0b-bfb3-4ee0-a06d-83c853f6a81e ](../.././input/tests/measure/CMS165FHIRControllingHighBP/045f7e0b-bfb3-4ee0-a06d-83c853f6a81e/MeasureReport-331c5841-65f1-4d32-9bb6-36cf2716fe15.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 048a7212-c19c-4f9d-89e2-13727b23e585 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/048a7212-c19c-4f9d-89e2-13727b23e585/MeasureReport-6c48d533-a267-4186-be87-1c5b0fc04064.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 23004f44-4848-4e62-8813-2a56d900613c ](../.././input/tests/measure/CMS165FHIRControllingHighBP/23004f44-4848-4e62-8813-2a56d900613c/MeasureReport-aec86d39-b601-48bf-b9fb-b40a0aea1847.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 32edbb16-2029-425a-85e0-6ea9182d1d91 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/32edbb16-2029-425a-85e0-6ea9182d1d91/MeasureReport-ab84003e-9b93-4eeb-aa31-e13c43b5c32d.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 43efb820-9e6e-4180-9a4d-2d7459896e5f ](../.././input/tests/measure/CMS165FHIRControllingHighBP/43efb820-9e6e-4180-9a4d-2d7459896e5f/MeasureReport-2bc454a5-ec02-45c8-b84c-a0a772bcba9f.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 4c814ca9-da50-43e3-9e31-dbe755ee5c5e ](../.././input/tests/measure/CMS165FHIRControllingHighBP/4c814ca9-da50-43e3-9e31-dbe755ee5c5e/MeasureReport-be1e7096-9087-4965-b04a-6e2804339203.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 4d50f3eb-f56f-4f13-8fcf-4d26e05b9a6a ](../.././input/tests/measure/CMS165FHIRControllingHighBP/4d50f3eb-f56f-4f13-8fcf-4d26e05b9a6a/MeasureReport-5c4d32a9-5bfa-4894-96b5-b382c7a5931b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 546de5d8-f614-41c7-938f-671d14e4f540 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/546de5d8-f614-41c7-938f-671d14e4f540/MeasureReport-3fe308d7-0a5e-4a53-aa92-181144441d82.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 598f05e7-83b4-4609-9795-e9ac75f57f36 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/598f05e7-83b4-4609-9795-e9ac75f57f36/MeasureReport-d0e6d92d-0c97-4597-a0e5-79d962308e3f.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 6769ebe0-1b45-472a-ba7b-8f9a014d94a6 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/6769ebe0-1b45-472a-ba7b-8f9a014d94a6/MeasureReport-b6cfed0d-872c-483f-be0a-9282541d740b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 6795a52e-1f83-480b-a2a7-b0d0922c0e5b ](../.././input/tests/measure/CMS165FHIRControllingHighBP/6795a52e-1f83-480b-a2a7-b0d0922c0e5b/MeasureReport-f8c5fcff-e9b8-4192-ab40-27059a5b5767.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 67ee2f03-89c1-4edb-b0fa-7e07effb4477 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/67ee2f03-89c1-4edb-b0fa-7e07effb4477/MeasureReport-85796632-f2d0-4cca-a88a-f3dbacbb3715.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 6885264d-efbf-4e48-99a2-2e8ce29d61ba ](../.././input/tests/measure/CMS165FHIRControllingHighBP/6885264d-efbf-4e48-99a2-2e8ce29d61ba/MeasureReport-1f5b7da1-80c7-4104-85c6-a27c6e8e399b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 698b2574-1170-4438-8400-f3e1992a4807 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/698b2574-1170-4438-8400-f3e1992a4807/MeasureReport-62a56341-c08b-425f-8b54-86b56388cffa.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 6d97c086-8776-45f4-898f-cece9e80990a ](../.././input/tests/measure/CMS165FHIRControllingHighBP/6d97c086-8776-45f4-898f-cece9e80990a/MeasureReport-71955a26-8189-435a-8ae7-bafae74877d8.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 6f37e357-7575-4b40-a63e-4b882532250f ](../.././input/tests/measure/CMS165FHIRControllingHighBP/6f37e357-7575-4b40-a63e-4b882532250f/MeasureReport-fba2c617-1601-4900-a65c-6e1e1f7adcbc.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 75d880c8-4220-4907-b29a-f595dc0df2fb ](../.././input/tests/measure/CMS165FHIRControllingHighBP/75d880c8-4220-4907-b29a-f595dc0df2fb/MeasureReport-5c02c21a-feac-484c-b0fb-66926ea4a688.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8e477157-81e8-4b7b-ba79-4a441a2a1109 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/8e477157-81e8-4b7b-ba79-4a441a2a1109/MeasureReport-c97552ce-def4-4566-ad61-e13c9f1aed8a.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 94d2a25e-9eec-44ce-bc34-711452549be8 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/94d2a25e-9eec-44ce-bc34-711452549be8/MeasureReport-fc4417c5-d5ce-47d1-b934-185fb732e88d.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a3deee90-5966-4309-b52f-c0a76046f680 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/a3deee90-5966-4309-b52f-c0a76046f680/MeasureReport-e3867723-a042-49e7-a98f-a71e13a7d8cf.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ b378c30b-ebc2-4378-9a75-8a97711cac81 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/b378c30b-ebc2-4378-9a75-8a97711cac81/MeasureReport-928d91b8-7482-496d-a809-641afc75f39c.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ b84bdc08-62ae-4bce-857d-d2492e0c82fd ](../.././input/tests/measure/CMS165FHIRControllingHighBP/b84bdc08-62ae-4bce-857d-d2492e0c82fd/MeasureReport-7ca59b81-c61b-47e9-b900-5f1e1d6fc310.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ bff7264b-35fc-402b-8a15-22c78e227064 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/bff7264b-35fc-402b-8a15-22c78e227064/MeasureReport-70a6dfe9-450a-471c-a8a8-73e14443e71b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c57b8e40-b3be-484f-8874-8ccafa3d5a38 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/c57b8e40-b3be-484f-8874-8ccafa3d5a38/MeasureReport-b973a4a8-e57b-4cdd-8be0-ba9af8505c76.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d150409f-0616-4565-ba60-7ca732a87288 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/d150409f-0616-4565-ba60-7ca732a87288/MeasureReport-c99c233b-98f9-44bd-a0ba-137b6a9a916b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d513ed00-6ea1-4522-ae7c-c3bc29082e92 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/d513ed00-6ea1-4522-ae7c-c3bc29082e92/MeasureReport-2685f647-34dd-40e1-ab8e-0dfa70d30388.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ dbc8c8f1-3f10-4352-adbe-e0d4c12ade72 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/dbc8c8f1-3f10-4352-adbe-e0d4c12ade72/MeasureReport-7b4148a0-65ef-40c4-bccc-6efa603e461a.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ e94daaa3-ffff-4ca5-b971-7fd4407c3580 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/e94daaa3-ffff-4ca5-b971-7fd4407c3580/MeasureReport-eb1a3ad2-ed14-4225-88f4-523fe5edfa5e.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ f2d1fd7e-35ae-45cd-86e6-8b874c3e3fb9 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/f2d1fd7e-35ae-45cd-86e6-8b874c3e3fb9/MeasureReport-657791f1-242d-40ee-8b6a-1fdb4d85c849.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |


#### CMS177FHIRChildMDDSuicideAssmt
[ [cql] ](../../input/cql/CMS177FHIRChildMDDSuicideAssmt.cql) [ [test results] ](../../input/tests/results/CMS177FHIRChildMDDSuicideAssmt.txt)

QICore: 123 / 0 — passes

Mismatched Test Cases (1 of  of 41)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 85e6225c-a9bb-4338-a228-297564e38c4d ](../.././input/tests/measure/CMS177FHIRChildMDDSuicideAssmt/85e6225c-a9bb-4338-a228-297564e38c4d/MeasureReport-89005c1a-09a3-421d-aa89-d44837ae5904.json) | Group_1 | Initial Population<br>Denominator | 0<br>0 | 1<br>1 | — | PASS<br>PASS |


#### CMS190FHIRVTEProphylaxisICU
[ [cql] ](../../input/cql/CMS190FHIRVTEProphylaxisICU.cql) [ [test results] ](../../input/tests/results/CMS190FHIRVTEProphylaxisICU.txt)

QICore: 613 / 12 — has discrepancies (11 mismatched, 0 missing)

Mismatched Test Cases (24 of  of 125)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 208cb0f9-a6e9-4207-b6a4-3325fb463099 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/208cb0f9-a6e9-4207-b6a4-3325fb463099/MeasureReport-3cb6a3ba-7c97-47c9-9ac7-cd39959ecc39.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 282ae3a0-a4fd-4fed-8ce9-bff3840c7ca9 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/282ae3a0-a4fd-4fed-8ce9-bff3840c7ca9/MeasureReport-bb0ca899-9892-4d53-a171-fa41dc45d404.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 2bcbe960-db7d-4088-a574-d771baf0f9c7 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/2bcbe960-db7d-4088-a574-d771baf0f9c7/MeasureReport-cfb7bc83-85fe-45b7-b133-a2b1429e1e31.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 39215b49-af59-45a7-a773-65e8353dfafd ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/39215b49-af59-45a7-a773-65e8353dfafd/MeasureReport-4358ad9b-1c93-4569-9985-0f388fe56ebe.json) | Group_1 | Initial Population<br>Denominator | 0<br>0 | 1<br>1 | — | FAIL<br>FAIL |
| [ 4724cb2f-b5bd-4c50-85cc-4a5ba25f04ca ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/4724cb2f-b5bd-4c50-85cc-4a5ba25f04ca/MeasureReport-4ca4bed8-36fa-40a9-a273-ce3f8e9f377e.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 4c32b73b-abba-431b-a352-f0f454e7c9dd ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/4c32b73b-abba-431b-a352-f0f454e7c9dd/MeasureReport-e9ac894c-9f4c-47d8-8325-7750b25036e0.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 4fc421c7-e490-4d4e-a326-53d08635efb9 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/4fc421c7-e490-4d4e-a326-53d08635efb9/MeasureReport-c206bcec-44ba-493e-8114-8ae57bf6b7e6.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 632831b0-1ebf-47b5-b439-3a124cd77c37 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/632831b0-1ebf-47b5-b439-3a124cd77c37/MeasureReport-dff9d9bd-b0cc-400f-815b-9255b426e828.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 7e7f4563-a628-40ab-990b-ca0837313759 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/7e7f4563-a628-40ab-990b-ca0837313759/MeasureReport-6b131b52-199b-46ac-b099-fad21dbda4ad.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 8ec9cf6a-2dcd-4c2e-9e2e-1ba237b66808 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/8ec9cf6a-2dcd-4c2e-9e2e-1ba237b66808/MeasureReport-53445771-3d55-46d3-8091-a92e9f7a0915.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 95a54d01-197e-48ef-bb48-d3d398aecbe8 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/95a54d01-197e-48ef-bb48-d3d398aecbe8/MeasureReport-89a6d854-e283-4df7-bd78-60dfa86483cf.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 98d6da30-f55a-411d-94b4-359b204bcb5a ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/98d6da30-f55a-411d-94b4-359b204bcb5a/MeasureReport-6e63dc69-1e82-44f5-bccb-e417baa090e5.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ 9ddea16c-55d3-4dda-a1d8-a256fbff0b64 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/9ddea16c-55d3-4dda-a1d8-a256fbff0b64/MeasureReport-90c1518e-8e3a-4f2a-b266-9210baffdcbf.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ a30e5588-0e2a-487c-b4d3-15d9e0006741 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/a30e5588-0e2a-487c-b4d3-15d9e0006741/MeasureReport-bdba93da-ab6a-4f3b-b72e-86f0168f9b43.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ a82cd0c1-900e-4ab3-a498-840ac1608486 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/a82cd0c1-900e-4ab3-a498-840ac1608486/MeasureReport-94a26fc6-de93-43a2-9be0-2ca52b24d988.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a9c75661-be1c-41b2-aa15-222cc7d2ca81 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/a9c75661-be1c-41b2-aa15-222cc7d2ca81/MeasureReport-21816bad-859d-416f-883b-24246a1db64c.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ c0481b47-738b-4a09-8901-915ece2beb7e ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/c0481b47-738b-4a09-8901-915ece2beb7e/MeasureReport-a28ce7c4-934f-4fac-a002-aee0c87b7cb9.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ dbfc823e-0e2f-409d-a409-2d9399db1118 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/dbfc823e-0e2f-409d-a409-2d9399db1118/MeasureReport-e7db6f05-3243-4d94-bf90-1b5c6cff7c10.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |
| [ e8931859-4ad8-49c8-9cdd-8697293456a2 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/e8931859-4ad8-49c8-9cdd-8697293456a2/MeasureReport-cfc06289-ff74-4caa-ba81-3647f98e3646.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ f00f3778-6ad1-466d-a3bd-bcbc63d62b55 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f00f3778-6ad1-466d-a3bd-bcbc63d62b55/MeasureReport-d3f2a4f2-6c34-484a-b29b-b2d34f1d8334.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ f035a977-30d0-487c-b542-a596e718420c ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f035a977-30d0-487c-b542-a596e718420c/MeasureReport-2318030c-b923-45ed-988f-5925f46200e9.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ f82746cf-f6cd-4fcc-bc9e-7e569ae26211 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f82746cf-f6cd-4fcc-bc9e-7e569ae26211/MeasureReport-ecd1d81f-c8df-4d19-b85f-5bb0d5c9f771.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ f859dd94-f201-4517-a368-32b98dd486c9 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f859dd94-f201-4517-a368-32b98dd486c9/MeasureReport-da236e59-3d0a-46c4-a352-3eec5846dbe6.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ f981eba4-4aac-45ce-8c52-f0bc02c9a0dc ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f981eba4-4aac-45ce-8c52-f0bc02c9a0dc/MeasureReport-01143c30-f69f-464f-99fd-405617644ce8.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |


#### CMS0334FHIRPCCesareanBirth
[ [cql] ](../../input/cql/CMS0334FHIRPCCesareanBirth.cql) [ [test results] ](../../input/tests/results/CMS0334FHIRPCCesareanBirth.txt)

QICore: 550 / 2 — has discrepancies (1 mismatched, 0 missing)

Mismatched Test Cases (1 of  of 138)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ c58acff5-248b-49c9-b18d-69e4a84a08d9 ](../.././input/tests/measure/CMS0334FHIRPCCesareanBirth/c58acff5-248b-49c9-b18d-69e4a84a08d9/MeasureReport-920b0c2e-1f1f-42d3-ab1f-1d7b12fa4bd0.json) | Group_1 | Denominator<br>Denominator Exclusion | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |


#### CMS347FHIRStatinPreventionTxCVD
[ [cql] ](../../input/cql/CMS347FHIRStatinPreventionTxCVD.cql) [ [test results] ](../../input/tests/results/CMS347FHIRStatinPreventionTxCVD.txt)

QICore: 3705 / 55 — has discrepancies (13 mismatched, 4 missing)

Missing Results (4 of 752 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 6da189af-7eb0-47b0-8c77-905944706aa1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-7eb0-47b0-8c77-905944706aa1/MeasureReport-503d8574-49d9-4cc6-809a-16a00b4ddc0a.json) | Group_1 | — |
| [ 6da189af-7eb0-47b0-8c77-905944706aa1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-7eb0-47b0-8c77-905944706aa1/MeasureReport-503d8574-49d9-4cc6-809a-16a00b4ddc0a.json) | Group_2 | — |
| [ 6da189af-7eb0-47b0-8c77-905944706aa1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-7eb0-47b0-8c77-905944706aa1/MeasureReport-503d8574-49d9-4cc6-809a-16a00b4ddc0a.json) | Group_3 | — |
| [ 6da189af-7eb0-47b0-8c77-905944706aa1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-7eb0-47b0-8c77-905944706aa1/MeasureReport-503d8574-49d9-4cc6-809a-16a00b4ddc0a.json) | Group_4 | — |


Mismatched Test Cases (230 of  of 752)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 019bf4e8-68b3-476d-ac64-a4ba0aa368c5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/019bf4e8-68b3-476d-ac64-a4ba0aa368c5/MeasureReport-d9888d12-e605-4a60-ae98-969613c9f70c.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 019bf4e8-68b3-476d-ac64-a4ba0aa368c5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/019bf4e8-68b3-476d-ac64-a4ba0aa368c5/MeasureReport-d9888d12-e605-4a60-ae98-969613c9f70c.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 019bf4e8-68b3-476d-ac64-a4ba0aa368c5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/019bf4e8-68b3-476d-ac64-a4ba0aa368c5/MeasureReport-d9888d12-e605-4a60-ae98-969613c9f70c.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 019bf4e8-68b3-476d-ac64-a4ba0aa368c5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/019bf4e8-68b3-476d-ac64-a4ba0aa368c5/MeasureReport-d9888d12-e605-4a60-ae98-969613c9f70c.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 022c05d8-8337-4f1a-9d69-abb6500b1be5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/022c05d8-8337-4f1a-9d69-abb6500b1be5/MeasureReport-5aa9c748-278d-45d3-9f2d-e3159a8fea67.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 031e746c-9c2c-4eea-acca-a26c8862c9d5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/031e746c-9c2c-4eea-acca-a26c8862c9d5/MeasureReport-70adac1d-ba41-4a52-90b6-f4e0367749f8.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0438e6ec-b6c0-422d-b8c9-074e5f8d9af5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0438e6ec-b6c0-422d-b8c9-074e5f8d9af5/MeasureReport-8ee44bab-5427-4547-92ec-f3eb32c298e0.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0774a58a-2910-4da7-a48a-6613d418b5d1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0774a58a-2910-4da7-a48a-6613d418b5d1/MeasureReport-1ad0809b-509e-4408-a359-dc5030c14287.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0774a58a-2910-4da7-a48a-6613d418b5d1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0774a58a-2910-4da7-a48a-6613d418b5d1/MeasureReport-1ad0809b-509e-4408-a359-dc5030c14287.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0774a58a-2910-4da7-a48a-6613d418b5d1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0774a58a-2910-4da7-a48a-6613d418b5d1/MeasureReport-1ad0809b-509e-4408-a359-dc5030c14287.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0774a58a-2910-4da7-a48a-6613d418b5d1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0774a58a-2910-4da7-a48a-6613d418b5d1/MeasureReport-1ad0809b-509e-4408-a359-dc5030c14287.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 078ef6a8-509f-4f36-98f3-977174636356 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/078ef6a8-509f-4f36-98f3-977174636356/MeasureReport-07129034-b09f-453a-8ba9-85bcfcb7405f.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 078ef6a8-509f-4f36-98f3-977174636356 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/078ef6a8-509f-4f36-98f3-977174636356/MeasureReport-07129034-b09f-453a-8ba9-85bcfcb7405f.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 078ef6a8-509f-4f36-98f3-977174636356 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/078ef6a8-509f-4f36-98f3-977174636356/MeasureReport-07129034-b09f-453a-8ba9-85bcfcb7405f.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 08a2c605-1316-4d3c-b26e-2b40a28a2e44 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08a2c605-1316-4d3c-b26e-2b40a28a2e44/MeasureReport-02728b90-76ad-45c8-b67d-d7cd49b8098b.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 08a2c605-1316-4d3c-b26e-2b40a28a2e44 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08a2c605-1316-4d3c-b26e-2b40a28a2e44/MeasureReport-02728b90-76ad-45c8-b67d-d7cd49b8098b.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 08a2c605-1316-4d3c-b26e-2b40a28a2e44 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08a2c605-1316-4d3c-b26e-2b40a28a2e44/MeasureReport-02728b90-76ad-45c8-b67d-d7cd49b8098b.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a/MeasureReport-deb5d37f-7ec1-44fd-9568-c94cd0c3e378.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a/MeasureReport-deb5d37f-7ec1-44fd-9568-c94cd0c3e378.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a/MeasureReport-deb5d37f-7ec1-44fd-9568-c94cd0c3e378.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_2 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0e334f85-c298-401d-95ab-bad7ae13ced8 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0e334f85-c298-401d-95ab-bad7ae13ced8/MeasureReport-3d256ed0-b009-4cf1-be98-f3301f3715ac.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0e334f85-c298-401d-95ab-bad7ae13ced8 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0e334f85-c298-401d-95ab-bad7ae13ced8/MeasureReport-3d256ed0-b009-4cf1-be98-f3301f3715ac.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0e334f85-c298-401d-95ab-bad7ae13ced8 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0e334f85-c298-401d-95ab-bad7ae13ced8/MeasureReport-3d256ed0-b009-4cf1-be98-f3301f3715ac.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 0fdfb3c8-c32a-48d7-877c-f5d8b6687d44 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0fdfb3c8-c32a-48d7-877c-f5d8b6687d44/MeasureReport-19e6b20f-7817-4078-a2dc-fd3df1fd09fd.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1051c571-b7e4-48d1-8e77-02b1da164b73 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1051c571-b7e4-48d1-8e77-02b1da164b73/MeasureReport-42968df2-485d-4859-aef4-76118b8627b5.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 117785fd-791b-4d9b-a5e7-436e39a62a6b ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/117785fd-791b-4d9b-a5e7-436e39a62a6b/MeasureReport-32779e39-b993-442c-8ceb-797df3d0754d.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 13d790be-84c6-438c-b571-842698654db7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/13d790be-84c6-438c-b571-842698654db7/MeasureReport-9c579c8b-08aa-41dc-af98-f7b8ed9ccf47.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 13d790be-84c6-438c-b571-842698654db7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/13d790be-84c6-438c-b571-842698654db7/MeasureReport-9c579c8b-08aa-41dc-af98-f7b8ed9ccf47.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 13d790be-84c6-438c-b571-842698654db7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/13d790be-84c6-438c-b571-842698654db7/MeasureReport-9c579c8b-08aa-41dc-af98-f7b8ed9ccf47.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1ba7b147-b701-424c-bade-4e8270547030 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1ba7b147-b701-424c-bade-4e8270547030/MeasureReport-2278c703-994b-4b13-8e3b-c726ba6b8530.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1ccceb3f-9a44-4dd3-88c6-f492965b87d5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1ccceb3f-9a44-4dd3-88c6-f492965b87d5/MeasureReport-05d842c6-2b9e-4086-a865-2765fc22f903.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1ccceb3f-9a44-4dd3-88c6-f492965b87d5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1ccceb3f-9a44-4dd3-88c6-f492965b87d5/MeasureReport-05d842c6-2b9e-4086-a865-2765fc22f903.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1ccceb3f-9a44-4dd3-88c6-f492965b87d5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1ccceb3f-9a44-4dd3-88c6-f492965b87d5/MeasureReport-05d842c6-2b9e-4086-a865-2765fc22f903.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1d3021bb-b593-4efc-af5b-320243bbe9b7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1d3021bb-b593-4efc-af5b-320243bbe9b7/MeasureReport-13a9bd51-27c8-4992-bbb0-c491175b6a6e.json) | Group_3 | Initial Population<br>Denominator<br>Denominator Exception | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 1d8a33ba-baaa-4b30-8c1a-ebace8d2d64e ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1d8a33ba-baaa-4b30-8c1a-ebace8d2d64e/MeasureReport-3f61d57d-d254-44e8-b024-8e70cb516372.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1d8a33ba-baaa-4b30-8c1a-ebace8d2d64e ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1d8a33ba-baaa-4b30-8c1a-ebace8d2d64e/MeasureReport-3f61d57d-d254-44e8-b024-8e70cb516372.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 1d8a33ba-baaa-4b30-8c1a-ebace8d2d64e ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1d8a33ba-baaa-4b30-8c1a-ebace8d2d64e/MeasureReport-3f61d57d-d254-44e8-b024-8e70cb516372.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 20922873-db29-4914-a413-eed415e4504b ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/20922873-db29-4914-a413-eed415e4504b/MeasureReport-32f3dcc4-b147-4ee2-9dcd-3d8a921c895a.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 20922873-db29-4914-a413-eed415e4504b ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/20922873-db29-4914-a413-eed415e4504b/MeasureReport-32f3dcc4-b147-4ee2-9dcd-3d8a921c895a.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 20922873-db29-4914-a413-eed415e4504b ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/20922873-db29-4914-a413-eed415e4504b/MeasureReport-32f3dcc4-b147-4ee2-9dcd-3d8a921c895a.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 231a16e4-7d60-4e2c-943b-2f4c98994808 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/231a16e4-7d60-4e2c-943b-2f4c98994808/MeasureReport-ac6f51ee-41d4-4194-a069-f9be88028718.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 231a16e4-7d60-4e2c-943b-2f4c98994808 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/231a16e4-7d60-4e2c-943b-2f4c98994808/MeasureReport-ac6f51ee-41d4-4194-a069-f9be88028718.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 231a16e4-7d60-4e2c-943b-2f4c98994808 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/231a16e4-7d60-4e2c-943b-2f4c98994808/MeasureReport-ac6f51ee-41d4-4194-a069-f9be88028718.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 24acf4a1-d67f-4584-9b6b-6c8025ffcc0a ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/24acf4a1-d67f-4584-9b6b-6c8025ffcc0a/MeasureReport-5aba9b11-5a24-443d-abc6-41a3a5ff7762.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 24acf4a1-d67f-4584-9b6b-6c8025ffcc0a ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/24acf4a1-d67f-4584-9b6b-6c8025ffcc0a/MeasureReport-5aba9b11-5a24-443d-abc6-41a3a5ff7762.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 24acf4a1-d67f-4584-9b6b-6c8025ffcc0a ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/24acf4a1-d67f-4584-9b6b-6c8025ffcc0a/MeasureReport-5aba9b11-5a24-443d-abc6-41a3a5ff7762.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 2727681a-5857-4de1-a892-0cd4e531541c ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/2727681a-5857-4de1-a892-0cd4e531541c/MeasureReport-eaee6dcf-60c0-42c0-bd77-a542b1023c29.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 285c85db-f879-4938-867f-daba78f08494 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/285c85db-f879-4938-867f-daba78f08494/MeasureReport-a4359f49-e2d5-40cd-a5f6-c8b0f33ae909.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 285c85db-f879-4938-867f-daba78f08494 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/285c85db-f879-4938-867f-daba78f08494/MeasureReport-a4359f49-e2d5-40cd-a5f6-c8b0f33ae909.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 285c85db-f879-4938-867f-daba78f08494 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/285c85db-f879-4938-867f-daba78f08494/MeasureReport-a4359f49-e2d5-40cd-a5f6-c8b0f33ae909.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 2cff757c-4470-46a2-a685-6e23cf82c045 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/2cff757c-4470-46a2-a685-6e23cf82c045/MeasureReport-4cae9687-fba5-4a5d-af12-fe19c1ef3760.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 2cff757c-4470-46a2-a685-6e23cf82c045 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/2cff757c-4470-46a2-a685-6e23cf82c045/MeasureReport-4cae9687-fba5-4a5d-af12-fe19c1ef3760.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 2cff757c-4470-46a2-a685-6e23cf82c045 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/2cff757c-4470-46a2-a685-6e23cf82c045/MeasureReport-4cae9687-fba5-4a5d-af12-fe19c1ef3760.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 2cff757c-4470-46a2-a685-6e23cf82c045 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/2cff757c-4470-46a2-a685-6e23cf82c045/MeasureReport-4cae9687-fba5-4a5d-af12-fe19c1ef3760.json) | Group_4 | Numerator | 0 | 1 | — | PASS |
| [ 30b8f03a-668f-400e-b824-a74e6b6dd1dc ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/30b8f03a-668f-400e-b824-a74e6b6dd1dc/MeasureReport-c705230a-9e09-44e5-b7b8-4613e6e8b831.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 30b8f03a-668f-400e-b824-a74e6b6dd1dc ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/30b8f03a-668f-400e-b824-a74e6b6dd1dc/MeasureReport-c705230a-9e09-44e5-b7b8-4613e6e8b831.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 30b8f03a-668f-400e-b824-a74e6b6dd1dc ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/30b8f03a-668f-400e-b824-a74e6b6dd1dc/MeasureReport-c705230a-9e09-44e5-b7b8-4613e6e8b831.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 31841a30-decc-4b6b-80a8-1cb18275cb6b ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/31841a30-decc-4b6b-80a8-1cb18275cb6b/MeasureReport-19a297ac-bb20-4e81-8303-c50068590e25.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 31841a30-decc-4b6b-80a8-1cb18275cb6b ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/31841a30-decc-4b6b-80a8-1cb18275cb6b/MeasureReport-19a297ac-bb20-4e81-8303-c50068590e25.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 31841a30-decc-4b6b-80a8-1cb18275cb6b ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/31841a30-decc-4b6b-80a8-1cb18275cb6b/MeasureReport-19a297ac-bb20-4e81-8303-c50068590e25.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 35999af4-f52b-4e73-8f05-4bfca8dee7ec ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/35999af4-f52b-4e73-8f05-4bfca8dee7ec/MeasureReport-15841cbc-4e69-4607-896d-c83a345d7deb.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 35d9e119-50ef-4df1-b303-f348596657ad ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/35d9e119-50ef-4df1-b303-f348596657ad/MeasureReport-e4a91bc9-f8bc-48f6-8a99-f0a6f03acc02.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 35d9e119-50ef-4df1-b303-f348596657ad ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/35d9e119-50ef-4df1-b303-f348596657ad/MeasureReport-e4a91bc9-f8bc-48f6-8a99-f0a6f03acc02.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 35d9e119-50ef-4df1-b303-f348596657ad ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/35d9e119-50ef-4df1-b303-f348596657ad/MeasureReport-e4a91bc9-f8bc-48f6-8a99-f0a6f03acc02.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 36408f0f-58eb-47fe-8e64-1b98e47e5c36 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/36408f0f-58eb-47fe-8e64-1b98e47e5c36/MeasureReport-0ef30d6d-b807-423c-98a3-328028c61a3d.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f/MeasureReport-875f07e5-2f83-465d-b102-3860b9ea20df.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f/MeasureReport-875f07e5-2f83-465d-b102-3860b9ea20df.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f/MeasureReport-875f07e5-2f83-465d-b102-3860b9ea20df.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_3 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 3dd27b30-058d-409a-84eb-252d40470597 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3dd27b30-058d-409a-84eb-252d40470597/MeasureReport-c6b168bd-67cd-4635-8cc4-2f250f6321d1.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 409116c1-3cd5-4f1f-8dd5-6b5646bbaff3 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/409116c1-3cd5-4f1f-8dd5-6b5646bbaff3/MeasureReport-b75e6323-2270-46cc-9b57-fc7b967e1e50.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 40aa228f-ff55-4653-8bbe-125dc0fb5983 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/40aa228f-ff55-4653-8bbe-125dc0fb5983/MeasureReport-f50e5947-872c-4d62-ac9a-8b9e62a8dc06.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_2 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 4ea5e47c-48de-4f1f-a7bb-499753983f9b ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4ea5e47c-48de-4f1f-a7bb-499753983f9b/MeasureReport-6fce629d-eb5b-40f1-9514-70d76cdb3525.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 4fe9e695-6348-44e7-af08-0e326c1420b7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4fe9e695-6348-44e7-af08-0e326c1420b7/MeasureReport-f5094548-f386-4ac6-8592-f63b3dc500a0.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_2 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | FAIL<br>PASS |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 5355d1bc-f8b4-4063-945a-0717e9530281 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5355d1bc-f8b4-4063-945a-0717e9530281/MeasureReport-ef1b7f33-9f01-4c91-87ac-9bcf656e1a0f.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 5355d1bc-f8b4-4063-945a-0717e9530281 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5355d1bc-f8b4-4063-945a-0717e9530281/MeasureReport-ef1b7f33-9f01-4c91-87ac-9bcf656e1a0f.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 5355d1bc-f8b4-4063-945a-0717e9530281 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5355d1bc-f8b4-4063-945a-0717e9530281/MeasureReport-ef1b7f33-9f01-4c91-87ac-9bcf656e1a0f.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 59715b85-2d66-4627-ad73-d91e5862cb5b ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/59715b85-2d66-4627-ad73-d91e5862cb5b/MeasureReport-df5cc6ad-153c-4947-929a-348e8a84415d.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 5976248c-c671-41e4-90df-b3367b1faefd ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5976248c-c671-41e4-90df-b3367b1faefd/MeasureReport-00d2ed08-c849-4e1a-b0ef-2e550cdf1e35.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 5a086712-eccf-4041-9eb7-b25c0dcf2317 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5a086712-eccf-4041-9eb7-b25c0dcf2317/MeasureReport-5cc29966-0e47-43f6-b308-5faf7e7cc710.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 5a086712-eccf-4041-9eb7-b25c0dcf2317 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5a086712-eccf-4041-9eb7-b25c0dcf2317/MeasureReport-5cc29966-0e47-43f6-b308-5faf7e7cc710.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 5a086712-eccf-4041-9eb7-b25c0dcf2317 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5a086712-eccf-4041-9eb7-b25c0dcf2317/MeasureReport-5cc29966-0e47-43f6-b308-5faf7e7cc710.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 5bbad8cc-56b9-4802-a5da-7de376a461f0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5bbad8cc-56b9-4802-a5da-7de376a461f0/MeasureReport-627541a3-ec37-470e-b0ab-04a8f80a7da7.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 5c70a969-ae6d-46ca-9a71-92e15292804d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5c70a969-ae6d-46ca-9a71-92e15292804d/MeasureReport-9da93dd1-07ec-47d7-899d-010097955b1f.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 5e65bf6d-6518-44d7-a827-821b59b00cc0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5e65bf6d-6518-44d7-a827-821b59b00cc0/MeasureReport-767cbd60-c073-4d9e-befd-d6052110b1f6.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 6840a0da-456f-40f7-b939-aac2cdf5620d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6840a0da-456f-40f7-b939-aac2cdf5620d/MeasureReport-e4bf2679-eb9e-45ca-8837-033709d13854.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 6840a0da-456f-40f7-b939-aac2cdf5620d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6840a0da-456f-40f7-b939-aac2cdf5620d/MeasureReport-e4bf2679-eb9e-45ca-8837-033709d13854.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 6840a0da-456f-40f7-b939-aac2cdf5620d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6840a0da-456f-40f7-b939-aac2cdf5620d/MeasureReport-e4bf2679-eb9e-45ca-8837-033709d13854.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 694248de-4f73-4557-816b-f6a932f15793 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/694248de-4f73-4557-816b-f6a932f15793/MeasureReport-e9d1f6f5-6a41-438e-866c-7bb115df9bb2.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 694248de-4f73-4557-816b-f6a932f15793 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/694248de-4f73-4557-816b-f6a932f15793/MeasureReport-e9d1f6f5-6a41-438e-866c-7bb115df9bb2.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 694248de-4f73-4557-816b-f6a932f15793 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/694248de-4f73-4557-816b-f6a932f15793/MeasureReport-e9d1f6f5-6a41-438e-866c-7bb115df9bb2.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 695b64d8-8102-4109-89c2-9ca128d43f4d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/695b64d8-8102-4109-89c2-9ca128d43f4d/MeasureReport-24ede31e-7a68-4dc2-90d6-acd7360ee071.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 72194a73-a0fe-4d50-8f07-0ad92320a467 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/72194a73-a0fe-4d50-8f07-0ad92320a467/MeasureReport-d6425543-eab6-4fcb-9b2f-363e7c91c48e.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 76ccc2ea-1cb2-4151-80bf-be8b14b1a074 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/76ccc2ea-1cb2-4151-80bf-be8b14b1a074/MeasureReport-1b0e12dd-220e-4757-898f-a09b2533414f.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 76ccc2ea-1cb2-4151-80bf-be8b14b1a074 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/76ccc2ea-1cb2-4151-80bf-be8b14b1a074/MeasureReport-1b0e12dd-220e-4757-898f-a09b2533414f.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 76ccc2ea-1cb2-4151-80bf-be8b14b1a074 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/76ccc2ea-1cb2-4151-80bf-be8b14b1a074/MeasureReport-1b0e12dd-220e-4757-898f-a09b2533414f.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 7b9268b7-2d3d-4a2b-822a-e1f470593fdf ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b9268b7-2d3d-4a2b-822a-e1f470593fdf/MeasureReport-b6f5f038-6129-4547-a34d-e37fd6cdeb97.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 7b9268b7-2d3d-4a2b-822a-e1f470593fdf ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b9268b7-2d3d-4a2b-822a-e1f470593fdf/MeasureReport-b6f5f038-6129-4547-a34d-e37fd6cdeb97.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 7b9268b7-2d3d-4a2b-822a-e1f470593fdf ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b9268b7-2d3d-4a2b-822a-e1f470593fdf/MeasureReport-b6f5f038-6129-4547-a34d-e37fd6cdeb97.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 7bc28f33-e1e6-4122-8a38-e9c36685a6ba ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7bc28f33-e1e6-4122-8a38-e9c36685a6ba/MeasureReport-a2b410a9-629f-484e-8918-64308678a396.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 87b32275-37d7-4adf-afa4-8a4518964de0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/87b32275-37d7-4adf-afa4-8a4518964de0/MeasureReport-42eb6783-cd1f-435b-b341-0ff5d7a8d4b9.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 88dc444e-3a42-4d5b-a757-62a5013cd131 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/88dc444e-3a42-4d5b-a757-62a5013cd131/MeasureReport-ec6a14d4-4d11-4e8c-9d24-72c9c3bb96c9.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_3 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_3 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ a0202aaf-756f-4d08-8329-8fd585ddda63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/a0202aaf-756f-4d08-8329-8fd585ddda63/MeasureReport-942c1538-4562-4011-8e6f-7df4c4d1b62c.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ a03e2988-3bed-4fc5-b1e7-70eac99f0612 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/a03e2988-3bed-4fc5-b1e7-70eac99f0612/MeasureReport-68c477e8-bc91-4774-b6c3-7da427e8d04b.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ b2705cfc-a0d5-4fb4-908b-89d00a51cc06 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/b2705cfc-a0d5-4fb4-908b-89d00a51cc06/MeasureReport-6f4eddf4-9c82-4072-9e8b-c5aace814460.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ b2705cfc-a0d5-4fb4-908b-89d00a51cc06 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/b2705cfc-a0d5-4fb4-908b-89d00a51cc06/MeasureReport-6f4eddf4-9c82-4072-9e8b-c5aace814460.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ b2705cfc-a0d5-4fb4-908b-89d00a51cc06 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/b2705cfc-a0d5-4fb4-908b-89d00a51cc06/MeasureReport-6f4eddf4-9c82-4072-9e8b-c5aace814460.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ b708e603-c09f-4798-9631-4603653c1380 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/b708e603-c09f-4798-9631-4603653c1380/MeasureReport-a8a77909-e33f-456a-9d69-3caaf6a4f7b8.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ b8893156-afda-4685-9d5e-06d2113f1409 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/b8893156-afda-4685-9d5e-06d2113f1409/MeasureReport-517320af-73b6-4168-9ec8-cbc54fe19718.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ bb80a309-08ab-4d5d-b863-111ae594d65d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/bb80a309-08ab-4d5d-b863-111ae594d65d/MeasureReport-20a44ba1-230f-4fb3-beb5-54e90fdd9f0d.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ bfb8c317-cc95-41cc-9d3d-e1e66dd5b168 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/bfb8c317-cc95-41cc-9d3d-e1e66dd5b168/MeasureReport-38dc6598-68c1-4938-ab30-6687b6b509fa.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ c00d7354-2160-48f4-a251-1fcf892d1b42 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/c00d7354-2160-48f4-a251-1fcf892d1b42/MeasureReport-29c874a9-691f-4ad7-8187-9a970aa1a920.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ c00d7354-2160-48f4-a251-1fcf892d1b42 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/c00d7354-2160-48f4-a251-1fcf892d1b42/MeasureReport-29c874a9-691f-4ad7-8187-9a970aa1a920.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ c00d7354-2160-48f4-a251-1fcf892d1b42 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/c00d7354-2160-48f4-a251-1fcf892d1b42/MeasureReport-29c874a9-691f-4ad7-8187-9a970aa1a920.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ c686053c-d4b7-45b7-9ebb-19080a24f031 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/c686053c-d4b7-45b7-9ebb-19080a24f031/MeasureReport-29337290-624b-4143-b38b-a890a07484bc.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ c77c84ce-f0a9-4949-a8d7-4413565db083 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/c77c84ce-f0a9-4949-a8d7-4413565db083/MeasureReport-40da76bf-01f0-4fdb-8e43-0aca227f4004.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ ca949c24-f283-493e-a697-426eaec3e9f1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/ca949c24-f283-493e-a697-426eaec3e9f1/MeasureReport-3ba4f928-e401-4977-bafd-519e85cf4b4f.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ d5c55655-2c12-4300-9ee1-31044497d665 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d5c55655-2c12-4300-9ee1-31044497d665/MeasureReport-7f9aeb46-1747-4d45-8c09-6eb935fda0dc.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ d9d151d1-9bd3-40ce-a2c1-cb8a985328fc ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d9d151d1-9bd3-40ce-a2c1-cb8a985328fc/MeasureReport-578ea6af-0a9a-4131-867d-a4daca7999dd.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ d9f94b3d-5bba-4965-8364-1d7c87957c3e ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d9f94b3d-5bba-4965-8364-1d7c87957c3e/MeasureReport-bcc58c25-307d-4ce2-88b1-618061f34605.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ da5f94c9-9d0c-42ea-ab7f-dd3a92a04ceb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/da5f94c9-9d0c-42ea-ab7f-dd3a92a04ceb/MeasureReport-8d5b6e09-01a8-4dce-b133-299dff0f601e.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ e0813324-b2e0-4138-99f4-696f03c3db30 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e0813324-b2e0-4138-99f4-696f03c3db30/MeasureReport-b0c87e9b-3cc9-462c-92a5-8e707e8f441e.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ e0813324-b2e0-4138-99f4-696f03c3db30 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e0813324-b2e0-4138-99f4-696f03c3db30/MeasureReport-b0c87e9b-3cc9-462c-92a5-8e707e8f441e.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ e0813324-b2e0-4138-99f4-696f03c3db30 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e0813324-b2e0-4138-99f4-696f03c3db30/MeasureReport-b0c87e9b-3cc9-462c-92a5-8e707e8f441e.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ e20a62fd-329e-44d7-8767-1951f9392396 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e20a62fd-329e-44d7-8767-1951f9392396/MeasureReport-75467789-8be3-4e69-8a6c-068c0fb269f5.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ e2edb18a-fb70-43cc-b680-6f933af7d182 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e2edb18a-fb70-43cc-b680-6f933af7d182/MeasureReport-155370b0-9120-424f-a125-a410fb05a018.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ e55d9fc4-44e6-4f00-bf53-b82a5b646222 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e55d9fc4-44e6-4f00-bf53-b82a5b646222/MeasureReport-efee9875-e635-4970-abfe-f6f5dbb8dc68.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ e55d9fc4-44e6-4f00-bf53-b82a5b646222 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e55d9fc4-44e6-4f00-bf53-b82a5b646222/MeasureReport-efee9875-e635-4970-abfe-f6f5dbb8dc68.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ e55d9fc4-44e6-4f00-bf53-b82a5b646222 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e55d9fc4-44e6-4f00-bf53-b82a5b646222/MeasureReport-efee9875-e635-4970-abfe-f6f5dbb8dc68.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ e8e584cf-df78-4932-bc9a-66ac5af10a47 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8e584cf-df78-4932-bc9a-66ac5af10a47/MeasureReport-9b4de60e-86cb-4c14-9cff-6c758fda083d.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ ef3f90d1-4954-40bd-b230-e44ffa98ed29 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/ef3f90d1-4954-40bd-b230-e44ffa98ed29/MeasureReport-c5498326-8b33-4fdb-be8b-faa79199495d.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ ef3f90d1-4954-40bd-b230-e44ffa98ed29 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/ef3f90d1-4954-40bd-b230-e44ffa98ed29/MeasureReport-c5498326-8b33-4fdb-be8b-faa79199495d.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ ef3f90d1-4954-40bd-b230-e44ffa98ed29 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/ef3f90d1-4954-40bd-b230-e44ffa98ed29/MeasureReport-c5498326-8b33-4fdb-be8b-faa79199495d.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ f101bf69-38b2-4c86-9978-727c665dfb31 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f101bf69-38b2-4c86-9978-727c665dfb31/MeasureReport-d789130f-bed1-4094-abaf-c7ade9aace54.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ f3b17514-f40d-43f9-baa9-a0418142ca98 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f3b17514-f40d-43f9-baa9-a0418142ca98/MeasureReport-3861c471-f858-4479-8185-1b673d30948b.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ f72b9ae4-40e0-4f28-a5bd-14f09ed84e75 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f72b9ae4-40e0-4f28-a5bd-14f09ed84e75/MeasureReport-d4cc33e9-917d-436b-bacd-20e7cd3c0c7e.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ f925afe3-4a77-404d-ba92-e78740f37d15 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f925afe3-4a77-404d-ba92-e78740f37d15/MeasureReport-6121c222-1d1a-4a39-8693-742a0f388014.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ f925afe3-4a77-404d-ba92-e78740f37d15 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f925afe3-4a77-404d-ba92-e78740f37d15/MeasureReport-6121c222-1d1a-4a39-8693-742a0f388014.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ f925afe3-4a77-404d-ba92-e78740f37d15 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f925afe3-4a77-404d-ba92-e78740f37d15/MeasureReport-6121c222-1d1a-4a39-8693-742a0f388014.json) | Group_4 | Denominator Exception | 0 | 1 | — | PASS |
| [ f9a03175-0a16-4c4a-97d5-f7b38e359526 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f9a03175-0a16-4c4a-97d5-f7b38e359526/MeasureReport-ddadb133-b74c-40fa-b094-871c925d01bf.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ f9a03175-0a16-4c4a-97d5-f7b38e359526 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f9a03175-0a16-4c4a-97d5-f7b38e359526/MeasureReport-ddadb133-b74c-40fa-b094-871c925d01bf.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ f9a03175-0a16-4c4a-97d5-f7b38e359526 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f9a03175-0a16-4c4a-97d5-f7b38e359526/MeasureReport-ddadb133-b74c-40fa-b094-871c925d01bf.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_1 | Denominator Exception | 0 | 1 | — | PASS |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_2 | Denominator Exception | 0 | 1 | — | PASS |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ fc82f4cb-7c62-41bd-9779-dd0f2e6e437f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/fc82f4cb-7c62-41bd-9779-dd0f2e6e437f/MeasureReport-7810b010-2aca-4459-9147-b60351425809.json) | Group_3 | Denominator Exception | 0 | 1 | — | PASS |


#### CMS645FHIRBoneDensityPCADTherapy
[ [cql] ](../../input/cql/CMS645FHIRBoneDensityPCADTherapy.cql) [ [test results] ](../../input/tests/results/CMS645FHIRBoneDensityPCADTherapy.txt)

QICore: 204 / 0 — passes

Mismatched Test Cases (3 of  of 51)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 8c41481d-f89e-4113-ba12-df7c53e93d80 ](../.././input/tests/measure/CMS645FHIRBoneDensityPCADTherapy/8c41481d-f89e-4113-ba12-df7c53e93d80/MeasureReport-5199a981-c1fd-4530-bd20-438541e8993f.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6 ](../.././input/tests/measure/CMS645FHIRBoneDensityPCADTherapy/c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6/MeasureReport-ff0dae36-899e-426e-9f9d-0b7270a49bfb.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — | PASS<br>PASS |
| [ d07cf359-d46c-4adf-b2d4-e02a2f43b78e ](../.././input/tests/measure/CMS645FHIRBoneDensityPCADTherapy/d07cf359-d46c-4adf-b2d4-e02a2f43b78e/MeasureReport-2e25820a-ce7b-4c83-b5b6-56eeec0f5577.json) | Group_1 | Numerator | 0 | 1 | — | PASS |


#### CMS646FHIRIntravesicalBCGTherapy
[ [cql] ](../../input/cql/CMS646FHIRIntravesicalBCGTherapy.cql) [ [test results] ](../../input/tests/results/CMS646FHIRIntravesicalBCGTherapy.txt)

QICore: 188 / 2 — has discrepancies (2 mismatched, 0 missing)

Missing Results (1 of 38 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/342d2bec-0acc-43e5-aaf7-3c9a65b09f91/MeasureReport-12cd358b-deb0-4130-a045-4c6b61e110c0.json) | Group_1 | — |


Mismatched Test Cases (3 of  of 38)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 10cec7db-41ae-49ad-b883-022f19d92a8b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/10cec7db-41ae-49ad-b883-022f19d92a8b/MeasureReport-b8b4961d-450b-4980-ac8f-95500c6393d4.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | FAIL |
| [ ab48e0c0-6543-4537-8f00-bfcdcba7a81b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/ab48e0c0-6543-4537-8f00-bfcdcba7a81b/MeasureReport-ea6cfef5-54d2-4d6d-a7aa-48cf8e749eaf.json) | Group_1 | Numerator | 0 | 1 | — | FAIL |
| [ e648fa70-0532-49b0-92f6-dfb5a6d28d94 ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/e648fa70-0532-49b0-92f6-dfb5a6d28d94/MeasureReport-57107c42-23df-40d4-92fe-5f7fdd475629.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |


#### CMS771FHIRUrinarySymptomScoreBPH
[ [cql] ](../../input/cql/CMS771FHIRUrinarySymptomScoreBPH.cql) [ [test results] ](../../input/tests/results/CMS771FHIRUrinarySymptomScoreBPH.txt)

QICore: 124 / 0 — passes

Mismatched Test Cases (7 of  of 31)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 051c5977-9f2c-4e8b-8e02-ac3ec0c718d6 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/051c5977-9f2c-4e8b-8e02-ac3ec0c718d6/MeasureReport-13a299d2-1f32-41d7-b226-7380902e41b7.json) | Group_1 | Denominator | 1 | 0 | — | PASS |
| [ 3ab3ac1d-9b5e-4087-8862-dcb2562fb90f ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/3ab3ac1d-9b5e-4087-8862-dcb2562fb90f/MeasureReport-47dae27e-89cf-4ee5-8c8b-bf1e44997d07.json) | Group_1 | Denominator | 1 | 0 | — | PASS |
| [ 4c234ec0-3f89-4d55-b767-219d1130f634 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/4c234ec0-3f89-4d55-b767-219d1130f634/MeasureReport-47a91ced-cb5f-44c0-9417-e8efa33a4b08.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ 9be591a0-517b-4be2-b652-a29be0c75c15 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/9be591a0-517b-4be2-b652-a29be0c75c15/MeasureReport-004d2ae6-6c2e-49f8-bf07-26cada3bbaf3.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ bc79e5bc-237e-44be-b5fc-c5c4efb50286 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/bc79e5bc-237e-44be-b5fc-c5c4efb50286/MeasureReport-621196a7-ca5f-4408-8508-851332413956.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ bf0f8968-c2c0-4416-88db-11ea3e3da968 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/bf0f8968-c2c0-4416-88db-11ea3e3da968/MeasureReport-bcce208a-3ff4-4c82-9d49-c0b64ccb9138.json) | Group_1 | Numerator | 1 | 0 | — | PASS |
| [ e90d90a7-3071-44de-8089-ad7b6f5f3e5d ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/e90d90a7-3071-44de-8089-ad7b6f5f3e5d/MeasureReport-9ef2db11-d78a-49af-a2ac-6536fac264a1.json) | Group_1 | Numerator | 1 | 0 | — | PASS |


#### CMS816FHIRHHHypo
[ [cql] ](../../input/cql/CMS816FHIRHHHypo.cql) [ [test results] ](../../input/tests/results/CMS816FHIRHHHypo.txt)

QICore: 57 / 27 — has discrepancies (12 mismatched, 0 missing)

Mismatched Test Cases (12 of  of 28)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 05c8cd12-addd-4b94-8f92-da093c556a84 ](../.././input/tests/measure/CMS816FHIRHHHypo/05c8cd12-addd-4b94-8f92-da093c556a84/MeasureReport-e66fcfe4-57f5-4259-bb05-540d4f6a864c.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 1d2bb25a-21a7-4529-9486-a320d4864719 ](../.././input/tests/measure/CMS816FHIRHHHypo/1d2bb25a-21a7-4529-9486-a320d4864719/MeasureReport-b0513b24-8789-4c07-a13d-322d9defbeb8.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 2adf5469-46a1-4020-be3b-01f91f8acc9d ](../.././input/tests/measure/CMS816FHIRHHHypo/2adf5469-46a1-4020-be3b-01f91f8acc9d/MeasureReport-af8c832f-f1ad-407a-9751-575339d08367.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 304052f7-e416-4da4-87ae-488e6589cab3 ](../.././input/tests/measure/CMS816FHIRHHHypo/304052f7-e416-4da4-87ae-488e6589cab3/MeasureReport-a754b13e-2ef7-4c69-a205-f9af9a9a089e.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 339a989b-722c-4452-9d25-454e2d53eea8 ](../.././input/tests/measure/CMS816FHIRHHHypo/339a989b-722c-4452-9d25-454e2d53eea8/MeasureReport-1f48c160-8aba-4e86-bd5d-c5c4bdef1afd.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 37fd9c7e-bf9e-4769-b448-094ed97bd3e8 ](../.././input/tests/measure/CMS816FHIRHHHypo/37fd9c7e-bf9e-4769-b448-094ed97bd3e8/MeasureReport-6c210a7d-98b1-4d37-a268-45d14a7e7b1d.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 5bfa3b7e-2b6f-4eb5-b09b-7c6f1145780b ](../.././input/tests/measure/CMS816FHIRHHHypo/5bfa3b7e-2b6f-4eb5-b09b-7c6f1145780b/MeasureReport-0fb98a8a-a7ac-49a3-a1bd-e042373dc1c6.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 6bc18290-1925-4239-81d7-0118bd062225 ](../.././input/tests/measure/CMS816FHIRHHHypo/6bc18290-1925-4239-81d7-0118bd062225/MeasureReport-1e896d30-3808-482a-b8a3-51198a58d4a6.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 8301c6c8-e50c-4457-add0-1ebd909c8ca7 ](../.././input/tests/measure/CMS816FHIRHHHypo/8301c6c8-e50c-4457-add0-1ebd909c8ca7/MeasureReport-a821b7fb-7913-45e4-82e2-cf232818d643.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 974284eb-fc89-452a-9b38-a884c0e0477e ](../.././input/tests/measure/CMS816FHIRHHHypo/974284eb-fc89-452a-9b38-a884c0e0477e/MeasureReport-6244d8f6-995c-4a0e-9d86-9c3abfc3fcb7.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ aa5f21cc-2d56-4749-a190-2828d579f790 ](../.././input/tests/measure/CMS816FHIRHHHypo/aa5f21cc-2d56-4749-a190-2828d579f790/MeasureReport-9eeadd82-4599-4b8b-95a5-f1d59697b451.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ ecde4132-9028-420a-aa7c-d1d14e5c1ab0 ](../.././input/tests/measure/CMS816FHIRHHHypo/ecde4132-9028-420a-aa7c-d1d14e5c1ab0/MeasureReport-b8bedfa5-6f9c-4727-be26-8b53d9a13a5b.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |


#### CMS819FHIRHHORAE
[ [cql] ](../../input/cql/CMS819FHIRHHORAE.cql) [ [test results] ](../../input/tests/results/CMS819FHIRHHORAE.txt)

QICore: 81 / 3 — has discrepancies (2 mismatched, 0 missing)

Mismatched Test Cases (2 of  of 28)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 31b40acc-ca5f-4d1d-bd83-4b1a14eb822e ](../.././input/tests/measure/CMS819FHIRHHORAE/31b40acc-ca5f-4d1d-bd83-4b1a14eb822e/MeasureReport-c93e2b69-18fd-425e-8c71-b52eb967eda0.json) | Group_1 | Initial Population<br>Denominator | 2<br>2 | 1<br>1 | — | FAIL<br>FAIL |
| [ 73b0c1fe-874b-4982-8cb2-3c30520441de ](../.././input/tests/measure/CMS819FHIRHHORAE/73b0c1fe-874b-4982-8cb2-3c30520441de/MeasureReport-15d9e04f-4116-4856-b61a-f7c7b38e3325.json) | Group_1 | Numerator | 1 | 0 | — | FAIL |


#### CMSFHIR844HybridHospitalWideMortality
[ [cql] ](../../input/cql/CMSFHIR844HybridHospitalWideMortality.cql) [ [test results] ](../../input/tests/results/CMSFHIR844HybridHospitalWideMortality.txt)

QICore: 8 / 2 — has discrepancies (2 mismatched, 0 missing)

Mismatched Test Cases (2 of  of 10)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 6f22a06f-7186-4db1-9310-4f907dc49ff3 ](../.././input/tests/measure/CMSFHIR844HybridHospitalWideMortality/6f22a06f-7186-4db1-9310-4f907dc49ff3/MeasureReport-a02a261f-1274-4f8b-b1f3-5496f7885cbe.json) | Group_1 | Initial Population | 1 | 0 | — | FAIL |
| [ af1b9448-3e7a-4b7f-8934-15bb63258b75 ](../.././input/tests/measure/CMSFHIR844HybridHospitalWideMortality/af1b9448-3e7a-4b7f-8934-15bb63258b75/MeasureReport-7afefb0f-3075-4fb8-8d56-474ba1112c38.json) | Group_1 | Initial Population | 2 | 1 | — | FAIL |


#### CMS871FHIRHHHyper
[ [cql] ](../../input/cql/CMS871FHIRHHHyper.cql) [ [test results] ](../../input/tests/results/CMS871FHIRHHHyper.txt)

QICore: 105 / 25 — has discrepancies (0 mismatched, 5 missing)

Missing Results (4 of 26 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 35719b1a-85bd-4072-b8d5-7218309358c6 ](../.././input/tests/measure/CMS871FHIRHHHyper/35719b1a-85bd-4072-b8d5-7218309358c6/MeasureReport-d5793b30-25e6-4cd6-8f7e-619b1c1802e5.json) | Group_1 | — |
| [ 7507debb-a991-4de0-bd71-634a684ddcd7 ](../.././input/tests/measure/CMS871FHIRHHHyper/7507debb-a991-4de0-bd71-634a684ddcd7/MeasureReport-6b01e3f8-ef51-41c3-8a23-b2868877df06.json) | Group_1 | — |
| [ 98533ccd-24ee-41b3-aab2-ef6cbf89e00d ](../.././input/tests/measure/CMS871FHIRHHHyper/98533ccd-24ee-41b3-aab2-ef6cbf89e00d/MeasureReport-82c8805c-b129-4009-8533-1ed12cf5d18f.json) | Group_1 | — |
| [ fd579f44-757b-4c98-9b09-27b17b935650 ](../.././input/tests/measure/CMS871FHIRHHHyper/fd579f44-757b-4c98-9b09-27b17b935650/MeasureReport-22df2e2a-404d-4ab0-831a-e2ab043197a2.json) | Group_1 | — |


#### CMS951FHIRKidneyHealthEval
[ [cql] ](../../input/cql/CMS951FHIRKidneyHealthEval.cql) [ [test results] ](../../input/tests/results/CMS951FHIRKidneyHealthEval.txt)

QICore: 220 / 0 — passes

Mismatched Test Cases (24 of  of 55)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 0ce4362d-60f0-41af-8d47-c61f76d025a4 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/0ce4362d-60f0-41af-8d47-c61f76d025a4/MeasureReport-1200ee11-135e-4eae-9442-22d04ab45096.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 1d012d11-4b38-4bdc-bd27-e7d8bcc88c89 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/1d012d11-4b38-4bdc-bd27-e7d8bcc88c89/MeasureReport-c8bfacf0-8fc5-4fda-a3f5-f50c328dd33c.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 51e9e9aa-edcc-46f4-8472-24f377014ad4 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/51e9e9aa-edcc-46f4-8472-24f377014ad4/MeasureReport-a6485f82-0333-461b-9920-4cdfef80f5e7.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 52988c36-5e85-4818-9baa-983a3e27281a ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/52988c36-5e85-4818-9baa-983a3e27281a/MeasureReport-f1ed60b0-465f-45a1-9ebd-b0847b7463b0.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 56063388-7942-4a1d-8568-2d805d31ad30 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/56063388-7942-4a1d-8568-2d805d31ad30/MeasureReport-45207130-0a0c-4bb7-ac92-390de10c9638.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 5aa9e5eb-adeb-4779-a4d3-5b731411e141 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/5aa9e5eb-adeb-4779-a4d3-5b731411e141/MeasureReport-b3c2feda-b53b-4e80-8ef8-67a9b6d53613.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 7e7c41ee-7704-419c-937b-72d10c76f99a ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/7e7c41ee-7704-419c-937b-72d10c76f99a/MeasureReport-ec04c274-90dc-4c27-b4ca-e879f1a3a9ea.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 8ca88661-f12a-4b24-98e8-93183e8e2472 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/8ca88661-f12a-4b24-98e8-93183e8e2472/MeasureReport-3a5be592-629d-40dd-b45b-c11b47942cf2.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8cfb2747-a46d-4348-9e21-5ef3417e524a ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/8cfb2747-a46d-4348-9e21-5ef3417e524a/MeasureReport-a8870c7b-9907-4383-9197-962e7ea65483.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ 8e10675e-b991-4327-9514-6feb9d385b7f ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/8e10675e-b991-4327-9514-6feb9d385b7f/MeasureReport-938f2b1f-4f22-4f5a-adea-12254625d58d.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 94f26954-f280-4596-8bd3-e77ca79c1f41 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/94f26954-f280-4596-8bd3-e77ca79c1f41/MeasureReport-302d827a-e064-4c1a-96b0-12583cefaf21.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 95ee3081-b973-4bd2-8b86-5b46bd664905 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/95ee3081-b973-4bd2-8b86-5b46bd664905/MeasureReport-a24076c4-8b40-4a88-a1fc-55551e5616c0.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 9821f4e3-39db-4f45-8da3-eed161841bd2 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/9821f4e3-39db-4f45-8da3-eed161841bd2/MeasureReport-7df3cedf-b2f7-46b9-bfbe-2ee6c143d0b9.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ 9f3b1077-d99c-4714-a88d-8aecc667fe57 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/9f3b1077-d99c-4714-a88d-8aecc667fe57/MeasureReport-f5c94639-261d-4552-8e3e-136a849dbef3.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ a7284289-8784-48d9-a342-7d851085efb7 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/a7284289-8784-48d9-a342-7d851085efb7/MeasureReport-cff70463-6350-4b4a-8bd9-53fe6cc6a35b.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ a9536c98-3157-4443-bfe1-ef4e585360be ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/a9536c98-3157-4443-bfe1-ef4e585360be/MeasureReport-981cc37e-91b0-4c2a-b935-96c844a1b213.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ ac7a62b6-a440-4d4c-849d-0ce05743109c ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/ac7a62b6-a440-4d4c-849d-0ce05743109c/MeasureReport-f79ffa99-1097-443f-acc3-fd7d06ce5e4b.json) | Group_1 | Denominator Exclusion<br>Numerator | 0<br>1 | 1<br>0 | — | PASS<br>PASS |
| [ ae52c591-1a71-4090-aeeb-2dd758f63ce4 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/ae52c591-1a71-4090-aeeb-2dd758f63ce4/MeasureReport-9fca972d-a76d-4d44-a58e-5e10bceb6aa2.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ c13a82b6-fb44-4fc7-befd-d762b9fafa97 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/c13a82b6-fb44-4fc7-befd-d762b9fafa97/MeasureReport-5f4e99f0-106c-4878-b3ce-e0862c5d5b11.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d4340928-bbc6-4c24-8888-9f12e5cbefad ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/d4340928-bbc6-4c24-8888-9f12e5cbefad/MeasureReport-11f81dac-dd61-420f-820c-f52e719e30a5.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ d4a593b2-d485-4bfa-a8b1-a401bdbf8d23 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/d4a593b2-d485-4bfa-a8b1-a401bdbf8d23/MeasureReport-60086e16-1f50-4623-b189-15f81e0f8db5.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ ed17f9e5-1200-49e3-a4fc-1c188d8932dc ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/ed17f9e5-1200-49e3-a4fc-1c188d8932dc/MeasureReport-e769e58e-6958-4e2e-abb0-d414f74a0115.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ f4d1182a-1c06-4c62-a0be-1f994c4343b3 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/f4d1182a-1c06-4c62-a0be-1f994c4343b3/MeasureReport-093f7e5c-36d7-4d4d-903a-dc44236897b2.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ f8c48a84-406c-44b7-b79e-b7a5f9d15b31 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/f8c48a84-406c-44b7-b79e-b7a5f9d15b31/MeasureReport-5488874f-31ea-4ede-a255-43e110dba2fa.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |


#### CMS986FHIRMalnutritionScore
[ [cql] ](../../input/cql/CMS986FHIRMalnutritionScore.cql) [ [test results] ](../../input/tests/results/CMS986FHIRMalnutritionScore.txt)

QICore: 2628 / 0 — passes

Mismatched Test Cases (6 of  of 876)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_1 | Measure Population Exclusion | 1 | 0 | — | PASS |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_2 | Measure Population Exclusion | 1 | 0 | — | PASS |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_3 | Measure Population Exclusion | 1 | 0 | — | PASS |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_4 | Measure Population Exclusion | 1 | 0 | — | PASS |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_5 | Measure Population Exclusion | 1 | 0 | — | PASS |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_6 | Measure Population Exclusion | 1 | 0 | — | PASS |


#### CMS996FHIRAptTxforSTEMI
[ [cql] ](../../input/cql/CMS996FHIRAptTxforSTEMI.cql) [ [test results] ](../../input/tests/results/CMS996FHIRAptTxforSTEMI.txt)

QICore: 568 / 2 — has discrepancies (2 mismatched, 0 missing)

Mismatched Test Cases (7 of  of 114)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 60823d79-b37f-4358-819f-f39b4e885c6d ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/60823d79-b37f-4358-819f-f39b4e885c6d/MeasureReport-96a1323f-d99d-4b31-aace-c90b90f8af7a.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 7edab122-3af3-4172-9231-7c1470ecc1e0 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/7edab122-3af3-4172-9231-7c1470ecc1e0/MeasureReport-9d0666d5-6e19-4f7f-b284-1af640b254f3.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ 88d99809-90d6-4cbc-a4bb-d5d73375fc81 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/88d99809-90d6-4cbc-a4bb-d5d73375fc81/MeasureReport-8f114534-ca1f-4d09-bdf1-c683d7a680a7.json) | Group_1 | Denominator Exclusion | 1 | 0 | — | FAIL |
| [ 8bb7c40b-7447-42ca-b662-161a7026ed8f ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/8bb7c40b-7447-42ca-b662-161a7026ed8f/MeasureReport-bb15a071-2c69-428e-ac66-6405f7d75d07.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ ccc7deaf-98b7-4dad-b190-8fee10f2cf77 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/ccc7deaf-98b7-4dad-b190-8fee10f2cf77/MeasureReport-9d6a333f-3243-42df-9063-031aa80e74ff.json) | Group_1 | Denominator Exception | 1 | 0 | — | PASS |
| [ f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847/MeasureReport-f2a63299-25e1-4d91-8e5c-1bdf3b60e9cb.json) | Group_1 | Denominator Exclusion | 0 | 1 | — | PASS |
| [ f71b56bb-42fc-4db0-aa60-6b7b91333295 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/f71b56bb-42fc-4db0-aa60-6b7b91333295/MeasureReport-261ec6b2-42f5-46c2-906d-12fe22084f4c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — | FAIL |


#### CMS1017FHIRHHFI
[ [cql] ](../../input/cql/CMS1017FHIRHHFI.cql) [ [test results] ](../../input/tests/results/CMS1017FHIRHHFI.txt)

QICore: 323 / 2 — has discrepancies (2 mismatched, 0 missing)

Mismatched Test Cases (2 of  of 65)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 0dfafc1a-cf94-4ca1-becf-c1b843896810 ](../.././input/tests/measure/CMS1017FHIRHHFI/0dfafc1a-cf94-4ca1-becf-c1b843896810/MeasureReport-cd491c44-6ed1-483f-8775-516f92b9c16d.json) | Group_1 | Numerator Exclusion | 0 | 1 | — | FAIL |
| [ 5ff2713d-ca89-42ae-91bb-cba3e1d9a487 ](../.././input/tests/measure/CMS1017FHIRHHFI/5ff2713d-ca89-42ae-91bb-cba3e1d9a487/MeasureReport-74f8c3e3-881b-4ba8-bfdb-ceef555ed020.json) | Group_1 | Numerator Exclusion | 0 | 1 | — | FAIL |


#### CMS1028FHIRPCSevereOBComps
[ [cql] ](../../input/cql/CMS1028FHIRPCSevereOBComps.cql) [ [test results] ](../../input/tests/results/CMS1028FHIRPCSevereOBComps.txt)

QICore: 1124 / 4 — has discrepancies (4 mismatched, 0 missing)

Mismatched Test Cases (2 of  of 282)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 763d86f9-d93f-4873-8b64-8439566b242e ](../.././input/tests/measure/CMS1028FHIRPCSevereOBComps/763d86f9-d93f-4873-8b64-8439566b242e/MeasureReport-7ca90ad8-935e-4d56-80d9-5470c8a98481.json) | Group_1 | Numerator | 2 | 1 | — | FAIL |
| [ 763d86f9-d93f-4873-8b64-8439566b242e ](../.././input/tests/measure/CMS1028FHIRPCSevereOBComps/763d86f9-d93f-4873-8b64-8439566b242e/MeasureReport-7ca90ad8-935e-4d56-80d9-5470c8a98481.json) | Group_2 | Numerator | 2 | 1 | — | FAIL |


#### CMS1154ScreeningPrediabetesFHIR
[ [cql] ](../../input/cql/CMS1154ScreeningPrediabetesFHIR.cql) [ [test results] ](../../input/tests/results/CMS1154ScreeningPrediabetesFHIR.txt)

QICore: 39 / 1 — has discrepancies (1 mismatched, 0 missing)

Mismatched Test Cases (1 of  of 10)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ bc9c82ca-72b5-41c4-a9a3-7e3860a9ac2d ](../.././input/tests/measure/CMS1154ScreeningPrediabetesFHIR/bc9c82ca-72b5-41c4-a9a3-7e3860a9ac2d/MeasureReport-466dec57-6ceb-4f37-8daa-40f26f14a191.json) | Group_1 | Denominator Exclusion | 1 | 0 | — | FAIL |


#### CMS1218FHIRHHRF
[ [cql] ](../../input/cql/CMS1218FHIRHHRF.cql) [ [test results] ](../../input/tests/results/CMS1218FHIRHHRF.txt)

QICore: 274 / 2 — has discrepancies (1 mismatched, 0 missing)

Mismatched Test Cases (1 of  of 69)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ ea9c34ee-b50e-4d13-bd9c-ab2033d15717 ](../.././input/tests/measure/CMS1218FHIRHHRF/ea9c34ee-b50e-4d13-bd9c-ab2033d15717/MeasureReport-97044259-fd76-403c-a40f-1177631abe4f.json) | Group_1 | Initial Population<br>Denominator | 0<br>0 | 1<br>1 | — | FAIL<br>FAIL |


#### CMS1264FHIRECATREHQR
[ [cql] ](../../input/cql/CMS1264FHIRECATREHQR.cql) [ [test results] ](../../input/tests/results/CMS1264FHIRECATREHQR.txt)

QICore: 22 / 152 — has discrepancies (57 mismatched, 0 missing)

Mismatched Test Cases (57 of  of 58)
| Test Case | Group | Population | Expected | Actual | Known Issue | QICore |
|---|---|---|:---:|:---:|---|:---:|
| [ 01959faf-5ea5-41cb-b960-b74da18cca85 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/01959faf-5ea5-41cb-b960-b74da18cca85/MeasureReport-2f4760e8-af42-4b7d-8a46-4feb91442b90.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 040dc7b1-27f9-43a3-82c9-b1a514db3071 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/040dc7b1-27f9-43a3-82c9-b1a514db3071/MeasureReport-dd896932-8400-44a0-8bbd-f40ebcc7ac0a.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 048b1f27-6343-4bcd-950d-e228de06aa9c ](../.././input/tests/measure/CMS1264FHIRECATREHQR/048b1f27-6343-4bcd-950d-e228de06aa9c/MeasureReport-3c7751e4-7b1b-47ce-a993-0818e4729316.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>2 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 11703274-1218-440d-bb98-08502a794179 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/11703274-1218-440d-bb98-08502a794179/MeasureReport-1b1e8699-5d88-492e-80fa-4d25037c7e02.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 16cffb87-15ea-48b7-bd68-f211f48d6f19 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/16cffb87-15ea-48b7-bd68-f211f48d6f19/MeasureReport-0005e228-bb68-4881-af9c-240e46283d0a.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 1f8035de-4255-434e-a32f-b97039ec57ff ](../.././input/tests/measure/CMS1264FHIRECATREHQR/1f8035de-4255-434e-a32f-b97039ec57ff/MeasureReport-c3f2487b-ee18-4b0d-8edd-c845ae784a25.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 21b841f6-b863-4c1d-8798-41c527b04a92 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/21b841f6-b863-4c1d-8798-41c527b04a92/MeasureReport-8c5aea70-bcfc-4f43-8a03-02148f1c58f2.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 221f787f-b5b1-4e16-ab64-6ab9d3e8744f ](../.././input/tests/measure/CMS1264FHIRECATREHQR/221f787f-b5b1-4e16-ab64-6ab9d3e8744f/MeasureReport-a6415772-907e-43a9-adc2-b78338487eb4.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 2c2a7958-4d1a-4142-9360-8045067a1c5b ](../.././input/tests/measure/CMS1264FHIRECATREHQR/2c2a7958-4d1a-4142-9360-8045067a1c5b/MeasureReport-aaf7a0ea-063d-4416-b3f2-2fc6a66165f1.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 2fc54731-4fd9-4884-aba5-9a8385111375 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/2fc54731-4fd9-4884-aba5-9a8385111375/MeasureReport-b256cd82-9be1-4a6e-ad60-0749478fd31f.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 3302c6ff-8767-4be7-9c81-f1d98351b247 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/3302c6ff-8767-4be7-9c81-f1d98351b247/MeasureReport-744fb66d-cf11-4a6f-ad15-0923d3f4c86e.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 35fd427f-1233-4f3c-b8b3-9e400755da8f ](../.././input/tests/measure/CMS1264FHIRECATREHQR/35fd427f-1233-4f3c-b8b3-9e400755da8f/MeasureReport-f40f11bd-98c0-448b-a847-f3cab9795ceb.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 404c928b-a752-4792-91c4-8a1fd0656759 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/404c928b-a752-4792-91c4-8a1fd0656759/MeasureReport-45bbb41b-8faf-4133-b9c1-c808f0dd760a.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 42be9d46-4c2f-4493-8299-d33dcbb7170e ](../.././input/tests/measure/CMS1264FHIRECATREHQR/42be9d46-4c2f-4493-8299-d33dcbb7170e/MeasureReport-5bf93706-504a-4b52-a41b-a2da5590d734.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 4c95d881-2e7e-4e81-bb4c-b1ae680ff286 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/4c95d881-2e7e-4e81-bb4c-b1ae680ff286/MeasureReport-8fd2f7d7-b39b-4678-9405-a6f4e41253b6.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 50270eff-f1ed-4cb3-b22b-467d89937c3a ](../.././input/tests/measure/CMS1264FHIRECATREHQR/50270eff-f1ed-4cb3-b22b-467d89937c3a/MeasureReport-27647613-f529-437e-8e23-d49adf62610c.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 540b665b-e89c-466a-9ef8-758b3883a37c ](../.././input/tests/measure/CMS1264FHIRECATREHQR/540b665b-e89c-466a-9ef8-758b3883a37c/MeasureReport-7fe41d15-f8e9-4884-9143-2bb4a3893d42.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 5ae9589c-1301-45a0-af30-ac7b679b649f ](../.././input/tests/measure/CMS1264FHIRECATREHQR/5ae9589c-1301-45a0-af30-ac7b679b649f/MeasureReport-9f7e1750-ebca-4be4-baed-625c1edae5b9.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 5fb0b78c-ffd3-47c3-91a3-252bc4a70177 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/5fb0b78c-ffd3-47c3-91a3-252bc4a70177/MeasureReport-8759064a-9ff7-4b89-b6f7-6849c0f027e9.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 6252a858-2362-4c63-8d7d-6db0b7ac9299 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/6252a858-2362-4c63-8d7d-6db0b7ac9299/MeasureReport-93955381-b5e5-4b38-b998-96c2d5d84925.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 63cea3d6-d2e0-4736-a035-87633ca960bd ](../.././input/tests/measure/CMS1264FHIRECATREHQR/63cea3d6-d2e0-4736-a035-87633ca960bd/MeasureReport-4fcd9a1b-054e-449c-a9b0-82241166fb79.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 666528ac-0d94-4b09-8e6c-c5930b7dd17c ](../.././input/tests/measure/CMS1264FHIRECATREHQR/666528ac-0d94-4b09-8e6c-c5930b7dd17c/MeasureReport-2bdf64db-100e-4cc7-832e-c8e7a6ed11e7.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 66803f75-5dc5-43fb-9844-f18d765a64ec ](../.././input/tests/measure/CMS1264FHIRECATREHQR/66803f75-5dc5-43fb-9844-f18d765a64ec/MeasureReport-63143fc7-e06a-496c-8783-ed0c3a27bcfd.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 74855a5c-bb3b-438a-9eb9-7fdc1994d06d ](../.././input/tests/measure/CMS1264FHIRECATREHQR/74855a5c-bb3b-438a-9eb9-7fdc1994d06d/MeasureReport-2667286a-1f17-4235-9149-6d106ebed3f4.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 78cbc6ac-f30d-404b-b539-6b903c7cfeba ](../.././input/tests/measure/CMS1264FHIRECATREHQR/78cbc6ac-f30d-404b-b539-6b903c7cfeba/MeasureReport-e78fb83c-b07d-4135-a58d-2c52732af4ff.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 7bcd79b7-7898-437d-b563-cfb9068df210 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/7bcd79b7-7898-437d-b563-cfb9068df210/MeasureReport-443dba04-97ce-4512-8ff7-44cf3b1ee268.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 7bee402e-2687-4813-9b39-37d723663d18 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/7bee402e-2687-4813-9b39-37d723663d18/MeasureReport-74472d36-5e68-48e1-a83d-bf876766f3c5.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 7dd19e80-23c6-4e31-86a9-bb833cfc676b ](../.././input/tests/measure/CMS1264FHIRECATREHQR/7dd19e80-23c6-4e31-86a9-bb833cfc676b/MeasureReport-3e9f0319-d5b8-4b5d-95d1-ac37ab1386f3.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 7fbb7e37-228b-4b3b-8974-871a3e798720 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/7fbb7e37-228b-4b3b-8974-871a3e798720/MeasureReport-ab112d95-d899-48a7-b5dd-cf7687760b02.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 7fd4f9cd-8fbb-4935-9bfd-959c538166b2 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/7fd4f9cd-8fbb-4935-9bfd-959c538166b2/MeasureReport-f980a8f2-68ec-4fd6-87a4-825841eb7244.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 8e43bc64-4242-494d-b47f-fdbbd3372bbe ](../.././input/tests/measure/CMS1264FHIRECATREHQR/8e43bc64-4242-494d-b47f-fdbbd3372bbe/MeasureReport-1fefe64b-f677-4b5a-90d1-e759d70a1b15.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 9098f676-4f4e-402c-80e3-331aabb6d414 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/9098f676-4f4e-402c-80e3-331aabb6d414/MeasureReport-d0bb06a8-7d89-4dcd-b053-c05ce8ec9dff.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 91d5385d-09ac-4206-b009-0c7feffc22ff ](../.././input/tests/measure/CMS1264FHIRECATREHQR/91d5385d-09ac-4206-b009-0c7feffc22ff/MeasureReport-b476d02c-6da7-4bb7-ad2f-169d32483880.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 9b5e4d84-366b-4082-8409-b7e18e0a3c45 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/9b5e4d84-366b-4082-8409-b7e18e0a3c45/MeasureReport-9ef1d49f-3c33-4cba-af3d-810699715f9f.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ 9bac5045-01af-4350-b54f-63ab17f3ba9f ](../.././input/tests/measure/CMS1264FHIRECATREHQR/9bac5045-01af-4350-b54f-63ab17f3ba9f/MeasureReport-cf089dfc-546b-4a56-becc-d8bd41ccd1ee.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 0<br>0<br>0 | 1<br>1<br>1 | — | FAIL<br>FAIL<br>FAIL |
| [ 9ec1a135-fb47-4c1c-8f6b-98afab15274e ](../.././input/tests/measure/CMS1264FHIRECATREHQR/9ec1a135-fb47-4c1c-8f6b-98afab15274e/MeasureReport-157ca8e1-8d77-42c6-96aa-a820025cb208.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ 9f77830b-ff7c-4060-bf38-295b215ab56d ](../.././input/tests/measure/CMS1264FHIRECATREHQR/9f77830b-ff7c-4060-bf38-295b215ab56d/MeasureReport-9d516dbf-c0fd-4789-b886-1b654a12f14c.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ a11dce52-c6b3-46e5-bc01-8994b0c8f471 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/a11dce52-c6b3-46e5-bc01-8994b0c8f471/MeasureReport-8fcf6211-b9db-4479-b8a6-297349f52858.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ a3dd602c-cd84-4e7a-aa37-eae4b15fdf4e ](../.././input/tests/measure/CMS1264FHIRECATREHQR/a3dd602c-cd84-4e7a-aa37-eae4b15fdf4e/MeasureReport-cbe16456-557c-4446-8d00-b88231aa00d0.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ a42d4cc2-24ca-4637-889f-276bcdd1e7cf ](../.././input/tests/measure/CMS1264FHIRECATREHQR/a42d4cc2-24ca-4637-889f-276bcdd1e7cf/MeasureReport-5b2dd2e3-bc1a-491d-bbb5-16d02f4d3165.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ b312fbc9-083f-4832-8d7c-d3e64df4145b ](../.././input/tests/measure/CMS1264FHIRECATREHQR/b312fbc9-083f-4832-8d7c-d3e64df4145b/MeasureReport-e34ea624-916b-461c-9a1b-78f28ee3f661.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ bfc497aa-308c-4113-9a36-21c6e17c3802 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/bfc497aa-308c-4113-9a36-21c6e17c3802/MeasureReport-144788e3-0a90-4dfe-b90e-1fb369101f36.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>2 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ c3284314-fe9b-408a-9b26-a21830f84432 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/c3284314-fe9b-408a-9b26-a21830f84432/MeasureReport-ecd56688-5c4f-4cba-a64e-acc9a9f82787.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ cc00e728-de5f-4df8-abcb-1e610496be66 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/cc00e728-de5f-4df8-abcb-1e610496be66/MeasureReport-5fd6e45e-8014-4f99-9491-6586df43c60e.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ cc01e29c-7ebb-4876-b63a-29de550c62f9 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/cc01e29c-7ebb-4876-b63a-29de550c62f9/MeasureReport-8d1aca19-bdf2-4cab-bc04-9f90063907ab.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ cee26b56-54cf-444e-8944-6edfbd6d2b93 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/cee26b56-54cf-444e-8944-6edfbd6d2b93/MeasureReport-c2d1fa86-d291-4821-a4c3-22c0afb4aa12.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ d1b64acd-58bc-4831-b150-a80b4240d6b1 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/d1b64acd-58bc-4831-b150-a80b4240d6b1/MeasureReport-aa17b20f-d8aa-4f07-bd9b-1e2634e8087f.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ d3a7a6b7-bbbc-4c08-bd8c-ce1e1cbdc8a8 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/d3a7a6b7-bbbc-4c08-bd8c-ce1e1cbdc8a8/MeasureReport-832050a0-5484-4e2c-b016-ebafcacb11b1.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ d5fe6f9c-6036-4004-9993-290f3a2be34a ](../.././input/tests/measure/CMS1264FHIRECATREHQR/d5fe6f9c-6036-4004-9993-290f3a2be34a/MeasureReport-202e7d04-8f7f-4da9-8718-49f3b74f63ff.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ d8832769-c838-4f1b-9c1e-fa4ed3a3efb9 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/d8832769-c838-4f1b-9c1e-fa4ed3a3efb9/MeasureReport-c2411650-c758-421a-bc62-2bc7e0a72104.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ dac89c3d-536e-4dca-9871-570a0bcd8d16 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/dac89c3d-536e-4dca-9871-570a0bcd8d16/MeasureReport-47a23458-9f77-4925-bcfa-0c123309bfb0.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>2 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ dad5b672-1e5b-437c-91fe-1f69b5d58c70 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/dad5b672-1e5b-437c-91fe-1f69b5d58c70/MeasureReport-387b7766-106c-4d36-acd8-c4d850dfec7d.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ dfd5dc6b-3299-4e4f-ae02-45f251e1f75b ](../.././input/tests/measure/CMS1264FHIRECATREHQR/dfd5dc6b-3299-4e4f-ae02-45f251e1f75b/MeasureReport-58ac98ca-0786-4bef-994d-2ee921ed228a.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ e982ec87-76b0-4fe2-b437-ac0503cf2159 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/e982ec87-76b0-4fe2-b437-ac0503cf2159/MeasureReport-d23defeb-bd48-4a58-87f0-381be384d6b2.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ eabe386d-5bca-4fdd-acb0-8228b4df83c0 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/eabe386d-5bca-4fdd-acb0-8228b4df83c0/MeasureReport-056a1f62-729c-4eaf-845e-379f89e90b26.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |
| [ ed5fa616-8b70-4016-b40d-6f87983e2776 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/ed5fa616-8b70-4016-b40d-6f87983e2776/MeasureReport-76c00443-8243-4565-b1be-5b0daffb5ded.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — | FAIL<br>FAIL<br>FAIL |
| [ ee13a2d8-61d9-4d2f-8f13-1423bd271950 ](../.././input/tests/measure/CMS1264FHIRECATREHQR/ee13a2d8-61d9-4d2f-8f13-1423bd271950/MeasureReport-709420f2-5a51-4704-9588-4483aa8c2ccc.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — | FAIL<br>FAIL |


## Engine Diff: CMS vs QI-Core (qicore-2025)

_Where the CMS engine's actual results differ from the QI-Core engine's (source of truth) on the same test case and population. QI-Core-only rows are populations the QI-Core engine produced that are absent from CMS._

| Measure | Mismatch | CMS-Only | QI-Core-Only |
| --- | ---: | ---: | ---: |
| CMS2FHIRPCSDepScreenAndFollowUp | 15 | 0 | 0 |
| CMS22FHIRPCSBPScreeningFollowUp | 14 | 0 | 0 |
| CMS68FHIRDocumentationCurrentMeds | 0 | 0 | 4 |
| CMS69FHIRPCSBMIScreenAndFollowUp | 55 | 0 | 0 |
| CMS71FHIRSTKAnticoagAFFlutter | 12 | 0 | 0 |
| CMS72FHIRSTKAntithromboticDay2 | 243 | 0 | 0 |
| CMS104FHIRSTKDCAntithrombotic | 175 | 0 | 0 |
| CMS108FHIRVTEProphylaxis | 16 | 0 | 0 |
| CMS122FHIRDiabetesAssessGT9Pct | 63 | 0 | 0 |
| CMS124FHIRCervicalCancerScreen | 18 | 0 | 0 |
| CMS125FHIRBreastCancerScreen | 35 | 0 | 0 |
| CMS128FHIRAntidepressantMgmt | 134 | 0 | 0 |
| CMS130FHIRColorectalCancerScrn | 46 | 0 | 0 |
| CMS131FHIRDiabetesEyeExam | 47 | 0 | 0 |
| CMS135FHIRACEIorARBorARNIforHF | 15 | 0 | 0 |
| CMS136FHIRChildADHDMedFollowUp | 5 | 0 | 0 |
| CMS144FHIRHFBetaBlockerForLVSD | 5 | 0 | 0 |
| CMS145FHIRCADBBlockerTPMIorLVSD | 6 | 0 | 0 |
| CMS153FHIRChlamydiaScreening | 2 | 0 | 0 |
| CMS156FHIRHighRiskMedsElderly | 193 | 0 | 0 |
| CMS165FHIRControllingHighBP | 40 | 0 | 0 |
| CMS177FHIRChildMDDSuicideAssmt | 2 | 0 | 0 |
| CMS190FHIRVTEProphylaxisICU | 17 | 0 | 0 |
| CMS347FHIRStatinPreventionTxCVD | 279 | 0 | 0 |
| CMS645FHIRBoneDensityPCADTherapy | 5 | 0 | 0 |
| CMS646FHIRIntravesicalBCGTherapy | 1 | 0 | 5 |
| CMS771FHIRUrinarySymptomScoreBPH | 7 | 0 | 0 |
| CMS871FHIRHHHyper | 0 | 7 | 0 |
| CMS951FHIRKidneyHealthEval | 33 | 0 | 0 |
| CMS986FHIRMalnutritionScore | 6 | 0 | 0 |
| CMS996FHIRAptTxforSTEMI | 5 | 0 | 0 |
| CMS1028FHIRPCSevereOBComps | 2 | 0 | 0 |
| NHSNAcuteCareHospitalMonthlyInitialPopulation1 | 27 | 0 | 0 |
| NHSNGlycemicControlHypoglycemiaInitialPopulation | 1 | 0 | 0 |

| **Total** | **1524** | **7** | **9** |

### CMS2FHIRPCSDepScreenAndFollowUp

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0e463fc3-d1bf-4e19-882b-fad6342aa668 | Denominator Exception | 0 | 1 | mismatch |
| 12786a64-c20e-4542-a4c0-bf3129d6a9e0 | Denominator Exception | 0 | 1 | mismatch |
| 28bf1260-965f-4682-b427-2c2a2084312a | Numerator | 1 | 0 | mismatch |
| 328248a7-33d6-4923-a99a-a56ec88c515e | Numerator | 1 | 0 | mismatch |
| 4149c02e-ee5c-4b8d-a4ee-425dfa2460e7 | Numerator | 1 | 0 | mismatch |
| 41df0dbe-ae84-4496-b355-320ff8707a85 | Denominator Exception | 0 | 1 | mismatch |
| 6078e73e-3265-4022-ae63-216c096b6246 | Denominator Exception | 0 | 1 | mismatch |
| 6aaff09e-4a7b-4efa-93f8-13033e95c230 | Denominator Exception | 0 | 1 | mismatch |
| 75a5223a-3a62-418a-bcc6-4522cfe71726 | Numerator | 1 | 0 | mismatch |
| 86ca7528-efcb-44ed-9203-6f21f37f4332 | Denominator Exception | 0 | 1 | mismatch |
| d0ba1182-26fa-4cfa-9f91-960503b7fe53 | Denominator Exception | 0 | 1 | mismatch |
| d2cde80b-5a6c-48e9-b38a-de938f019096 | Numerator | 1 | 0 | mismatch |
| d5f7630a-6fcf-4cfc-ba20-dfd5ee88af9a | Numerator | 1 | 0 | mismatch |
| f29e2786-fade-4dca-b14d-7037a34ef498 | Denominator Exception | 0 | 1 | mismatch |
| ff6f7416-7e1d-4712-b4f5-aab79b2a7c01 | Numerator | 1 | 0 | mismatch |

### CMS22FHIRPCSBPScreeningFollowUp

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0278fdf0-f067-46e8-aeb1-fb96dff3c947 | Denominator Exception | 0 | 1 | mismatch |
| 1f16120b-56c9-4d72-8dd4-01d8a0175d77 | Denominator Exception | 0 | 1 | mismatch |
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
| f2e2e1c0-9e35-4592-9579-72a236cb2f56 | Denominator | — | — | qicore-only |
| f2e2e1c0-9e35-4592-9579-72a236cb2f56 | Denominator Exception | — | — | qicore-only |
| f2e2e1c0-9e35-4592-9579-72a236cb2f56 | Initial Population | — | — | qicore-only |
| f2e2e1c0-9e35-4592-9579-72a236cb2f56 | Numerator | — | — | qicore-only |

### CMS69FHIRPCSBMIScreenAndFollowUp

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0278fdf0-f067-46e8-aeb1-fb96dff3c947 | Denominator Exclusion | 1 | 0 | mismatch |
| 03f01144-2230-42ab-b81f-594e1c2baa62 | Denominator Exclusion | 1 | 0 | mismatch |
| 050201c2-c2c4-46e6-8288-a34f99caebdc | Denominator Exclusion | 1 | 0 | mismatch |
| 050201c2-c2c4-46e6-8288-a34f99caebdc | Numerator | 0 | 1 | mismatch |
| 1102009b-6f05-4bab-9fd1-191e81cf50e8 | Denominator Exclusion | 1 | 0 | mismatch |
| 1102009b-6f05-4bab-9fd1-191e81cf50e8 | Numerator | 0 | 1 | mismatch |
| 1ba2fc33-1a1b-416b-bb3c-79ba5d0d3359 | Denominator Exclusion | 1 | 0 | mismatch |
| 1c607e84-c7c2-4dae-bf63-a75d7a9cfd38 | Denominator Exclusion | 1 | 0 | mismatch |
| 1e23fb8f-e27b-4553-a62a-f66edeb4528a | Denominator Exclusion | 1 | 0 | mismatch |
| 1e23fb8f-e27b-4553-a62a-f66edeb4528a | Numerator | 0 | 1 | mismatch |
| 1f16120b-56c9-4d72-8dd4-01d8a0175d77 | Denominator Exclusion | 1 | 0 | mismatch |
| 260e1fc8-227f-4c16-bfc6-22625380a12c | Denominator Exclusion | 1 | 0 | mismatch |
| 27849d59-3cef-40bf-8338-a6ec7c0bcf81 | Denominator Exclusion | 1 | 0 | mismatch |
| 27849d59-3cef-40bf-8338-a6ec7c0bcf81 | Numerator | 0 | 1 | mismatch |
| 296d38e4-d69b-481e-a8cf-f7eee8b9e5d7 | Denominator Exclusion | 1 | 0 | mismatch |
| 2a976bc2-493b-421f-842e-36d31463f261 | Denominator Exclusion | 1 | 0 | mismatch |
| 405d4940-7ab2-4d26-b55f-3c27e07eba33 | Denominator Exclusion | 1 | 0 | mismatch |
| 42e6b4d6-defc-4ec5-894f-e3333e3039a3 | Denominator Exclusion | 1 | 0 | mismatch |
| 42e6b4d6-defc-4ec5-894f-e3333e3039a3 | Numerator | 0 | 1 | mismatch |
| 461fdfab-fcc1-4630-9dae-2ba3a6ab0c25 | Denominator Exclusion | 1 | 0 | mismatch |
| 461fdfab-fcc1-4630-9dae-2ba3a6ab0c25 | Numerator | 0 | 1 | mismatch |
| 463dd868-997d-472f-962c-96383fd2a5c4 | Denominator Exclusion | 1 | 0 | mismatch |
| 463dd868-997d-472f-962c-96383fd2a5c4 | Numerator | 0 | 1 | mismatch |
| 6553adbf-2a30-4861-97e6-cca7d2274f01 | Denominator Exclusion | 1 | 0 | mismatch |
| 659f9c7b-5c1c-475f-bfcb-77c246fa7a28 | Denominator Exclusion | 1 | 0 | mismatch |
| 6d26d364-a06c-49e6-84df-280ec6b7a8a3 | Denominator Exclusion | 1 | 0 | mismatch |
| 7902e3dc-f3da-4cc2-9f2e-4a6c8cd33b88 | Denominator Exclusion | 1 | 0 | mismatch |
| 7902e3dc-f3da-4cc2-9f2e-4a6c8cd33b88 | Numerator | 0 | 1 | mismatch |
| 7ac9722f-8763-4380-a741-53ee4bb98819 | Denominator Exclusion | 1 | 0 | mismatch |
| 8835a50b-0a0f-4e2f-94fa-7c180cd7f905 | Denominator Exclusion | 1 | 0 | mismatch |
| 8835a50b-0a0f-4e2f-94fa-7c180cd7f905 | Numerator | 0 | 1 | mismatch |
| 88a2b45a-7866-445a-8242-91ec0ebb7646 | Denominator Exclusion | 1 | 0 | mismatch |
| 8c89947a-a52b-4a41-86a8-166b0560355b | Denominator Exclusion | 1 | 0 | mismatch |
| 8e130410-9710-45f3-ac56-e69dee0755d9 | Denominator Exclusion | 1 | 0 | mismatch |
| 8e38b797-4dec-437d-8bf0-6f0fc78f8ea7 | Denominator Exclusion | 1 | 0 | mismatch |
| 8e38b797-4dec-437d-8bf0-6f0fc78f8ea7 | Numerator | 0 | 1 | mismatch |
| 9d92be1d-6fc8-40f2-99a0-4be9ce1f244b | Denominator Exclusion | 1 | 0 | mismatch |
| 9d92be1d-6fc8-40f2-99a0-4be9ce1f244b | Numerator | 0 | 1 | mismatch |
| a0aacdbc-4954-48af-aa88-361ea7e32736 | Denominator Exclusion | 1 | 0 | mismatch |
| a327cf96-81c4-46ff-9619-6fd9981bb90c | Denominator Exclusion | 1 | 0 | mismatch |
| a4a1ed63-89ff-4d27-8819-136873e13171 | Denominator Exclusion | 1 | 0 | mismatch |
| c1df0273-aad8-41a8-859c-edd204bb4f16 | Denominator Exclusion | 1 | 0 | mismatch |
| c3caf126-12a2-473f-8f51-1c7828d63d16 | Denominator Exclusion | 1 | 0 | mismatch |
| c3caf126-12a2-473f-8f51-1c7828d63d16 | Numerator | 0 | 1 | mismatch |
| c84bf29f-80ac-4bf0-beeb-404ba96a3fa8 | Denominator Exclusion | 1 | 0 | mismatch |
| c84bf29f-80ac-4bf0-beeb-404ba96a3fa8 | Numerator | 0 | 1 | mismatch |
| d3054ffa-e17b-4611-b7e0-4523fb0f9e1d | Denominator Exclusion | 1 | 0 | mismatch |
| d4d064be-d55a-47b5-9bfd-993afebd95a5 | Denominator Exclusion | 1 | 0 | mismatch |
| d4d064be-d55a-47b5-9bfd-993afebd95a5 | Numerator | 0 | 1 | mismatch |
| ddfb765a-a3fb-467f-a9d9-ac6faf4cea9a | Denominator Exclusion | 1 | 0 | mismatch |
| e0821eec-ff83-49e9-950d-9219dd3612b9 | Denominator Exclusion | 1 | 0 | mismatch |
| e0821eec-ff83-49e9-950d-9219dd3612b9 | Numerator | 0 | 1 | mismatch |
| f5ae6269-d09b-47f8-a519-f1a8a81549fc | Denominator Exclusion | 1 | 0 | mismatch |
| f5ae6269-d09b-47f8-a519-f1a8a81549fc | Numerator | 0 | 1 | mismatch |
| ff09cf1e-5b30-45c7-9cc6-d5daf48a3933 | Denominator Exclusion | 1 | 0 | mismatch |

### CMS71FHIRSTKAnticoagAFFlutter

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0587a75d-0dcc-4c6b-bfc0-f5727342ec1f | Denominator | 0 | 1 | mismatch |
| 0587a75d-0dcc-4c6b-bfc0-f5727342ec1f | Numerator | 0 | 1 | mismatch |
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
| f0d37c4e-7377-4876-8533-f955963f96f9 | Denominator | 1 | 0 | mismatch |
| f0d37c4e-7377-4876-8533-f955963f96f9 | Initial Population | 1 | 0 | mismatch |
| f25baf5f-2980-416c-a8ef-3b9e42d751c3 | Denominator | 1 | 0 | mismatch |
| f25baf5f-2980-416c-a8ef-3b9e42d751c3 | Initial Population | 1 | 0 | mismatch |
| f5f317c7-69f1-4a89-850a-8a58789c80f2 | Denominator | 1 | 0 | mismatch |
| f5f317c7-69f1-4a89-850a-8a58789c80f2 | Initial Population | 1 | 0 | mismatch |
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
| 33d162ce-3bc7-4b0a-8c04-fec0a42a6263 | Numerator | 0 | 1 | mismatch |
| 3db5c5a1-2eec-4e01-8e59-ac389a0a2179 | Numerator | 0 | 1 | mismatch |
| 41f2785f-4c4f-4497-a46b-e17fd8b5ee3f | Denominator Exclusion | 1 | 0 | mismatch |
| 541ccffb-c1be-4c94-ab24-168d52e3a36b | Numerator | 0 | 1 | mismatch |
| 5741c41a-04ec-4967-83b2-b0d746bd0ed5 | Numerator | 0 | 1 | mismatch |
| 575f2da0-c890-47a3-b17f-f9e134a1096e | Numerator | 0 | 1 | mismatch |
| 70a5b41a-14ac-4e08-b661-d5523ad80fbf | Denominator Exclusion | 1 | 0 | mismatch |
| 8bb999a1-696a-497b-a5f4-aa55e146a16e | Numerator | 0 | 1 | mismatch |
| 8e2cfc29-0925-45b9-857f-b9ee9b9fa248 | Numerator | 0 | 1 | mismatch |
| b7783b8c-ba46-4509-a75e-203659abab3d | Numerator | 0 | 1 | mismatch |
| ccd7f9d7-35e8-4623-9f2e-f229cf7d829c | Numerator | 0 | 1 | mismatch |
| d9b7ffa9-ed78-484c-8880-b4cbf2b4b6a1 | Numerator | 0 | 1 | mismatch |
| dba7c9af-eb6f-4836-ba24-650a5acc87e7 | Numerator | 0 | 1 | mismatch |
| dc0dcb01-87f0-4e65-9c36-8cf6174abef1 | Numerator | 0 | 1 | mismatch |
| dd5a1e46-1b99-45a3-b4d3-1fde205d8a11 | Numerator | 0 | 1 | mismatch |
| eb754c68-82c7-48cd-a2f0-26ee1cd92544 | Denominator Exclusion | 1 | 0 | mismatch |

### CMS122FHIRDiabetesAssessGT9Pct

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 090ad2fc-274b-4fef-bc5a-2077dbdc28f5 | Denominator Exclusion | 1 | 0 | mismatch |
| 090ad2fc-274b-4fef-bc5a-2077dbdc28f5 | Numerator | 0 | 1 | mismatch |
| 1e954801-6437-4abc-8fb8-d36b5b5b97d8 | Denominator Exclusion | 1 | 0 | mismatch |
| 1e954801-6437-4abc-8fb8-d36b5b5b97d8 | Numerator | 0 | 1 | mismatch |
| 21695544-0997-4b9a-989c-a535da22d033 | Denominator Exclusion | 1 | 0 | mismatch |
| 21695544-0997-4b9a-989c-a535da22d033 | Numerator | 0 | 1 | mismatch |
| 24fa66c5-52ba-4386-a5e7-7b78002be77a | Denominator Exclusion | 1 | 0 | mismatch |
| 24fa66c5-52ba-4386-a5e7-7b78002be77a | Numerator | 0 | 1 | mismatch |
| 3b62b0a8-44f2-4365-bcb9-7cadef5bab2e | Denominator Exclusion | 1 | 0 | mismatch |
| 3b62b0a8-44f2-4365-bcb9-7cadef5bab2e | Numerator | 0 | 1 | mismatch |
| 511548fc-b5c3-4f90-83c6-e04f8e1c98cc | Denominator Exclusion | 1 | 0 | mismatch |
| 511548fc-b5c3-4f90-83c6-e04f8e1c98cc | Numerator | 0 | 1 | mismatch |
| 514a74ba-baea-4102-b2e7-050f84c79ef8 | Denominator Exclusion | 1 | 0 | mismatch |
| 514a74ba-baea-4102-b2e7-050f84c79ef8 | Numerator | 0 | 1 | mismatch |
| 5d692a54-a1d5-4a9c-80ba-fb6b20112484 | Denominator Exclusion | 1 | 0 | mismatch |
| 5d692a54-a1d5-4a9c-80ba-fb6b20112484 | Numerator | 0 | 1 | mismatch |
| 5ed37c9e-85a3-4819-8051-3d960159cae0 | Denominator Exclusion | 1 | 0 | mismatch |
| 61793aba-9080-4521-9083-a23f242b8d0a | Denominator Exclusion | 1 | 0 | mismatch |
| 61793aba-9080-4521-9083-a23f242b8d0a | Numerator | 0 | 1 | mismatch |
| 6630d394-c81d-42f5-a218-40b73a2a4949 | Denominator Exclusion | 1 | 0 | mismatch |
| 6630d394-c81d-42f5-a218-40b73a2a4949 | Numerator | 0 | 1 | mismatch |
| 7706188a-f37c-483d-96c2-4d7eab833605 | Denominator Exclusion | 1 | 0 | mismatch |
| 7706188a-f37c-483d-96c2-4d7eab833605 | Numerator | 0 | 1 | mismatch |
| 8956ebb5-d3c0-4112-a34a-200961713efd | Denominator Exclusion | 1 | 0 | mismatch |
| 8956ebb5-d3c0-4112-a34a-200961713efd | Numerator | 0 | 1 | mismatch |
| 8b1155b0-ff08-4f28-90e7-ac0e622f840c | Denominator Exclusion | 1 | 0 | mismatch |
| 8b1155b0-ff08-4f28-90e7-ac0e622f840c | Numerator | 0 | 1 | mismatch |
| 8fa86a00-fa67-4dd6-b2d8-6fe23edde9c7 | Denominator Exclusion | 1 | 0 | mismatch |
| 8fa86a00-fa67-4dd6-b2d8-6fe23edde9c7 | Numerator | 0 | 1 | mismatch |
| 981dbc54-03ac-4f2e-a008-dbedfcbd2a7a | Denominator Exclusion | 1 | 0 | mismatch |
| 981dbc54-03ac-4f2e-a008-dbedfcbd2a7a | Numerator | 0 | 1 | mismatch |
| 98735c81-5c91-4709-9392-558ac6d40b6c | Denominator Exclusion | 1 | 0 | mismatch |
| 98735c81-5c91-4709-9392-558ac6d40b6c | Numerator | 0 | 1 | mismatch |
| 9cba6cfa-9671-4850-803d-e286c7d59ee7 | Denominator Exclusion | 1 | 0 | mismatch |
| 9cba6cfa-9671-4850-803d-e286c7d59ee7 | Numerator | 0 | 1 | mismatch |
| 9da62d36-585d-455e-8cb5-8e5da1f3e476 | Denominator Exclusion | 1 | 0 | mismatch |
| 9da62d36-585d-455e-8cb5-8e5da1f3e476 | Numerator | 0 | 1 | mismatch |
| a6b08556-8019-43ad-8ab0-0c213f3789ca | Denominator Exclusion | 1 | 0 | mismatch |
| a6b08556-8019-43ad-8ab0-0c213f3789ca | Numerator | 0 | 1 | mismatch |
| a7332447-3a23-42b1-bfc2-d93cc5b775af | Denominator Exclusion | 1 | 0 | mismatch |
| a7332447-3a23-42b1-bfc2-d93cc5b775af | Numerator | 0 | 1 | mismatch |
| ab29ab81-b4fc-4817-bd9c-98d8d4b4a3a3 | Denominator Exclusion | 1 | 0 | mismatch |
| ab29ab81-b4fc-4817-bd9c-98d8d4b4a3a3 | Numerator | 0 | 1 | mismatch |
| abe87c54-c0b1-4f86-94ca-360a228e9aa3 | Denominator Exclusion | 1 | 0 | mismatch |
| abe87c54-c0b1-4f86-94ca-360a228e9aa3 | Numerator | 0 | 1 | mismatch |
| c66e4e0a-5479-461c-9a39-0298a08f682f | Denominator Exclusion | 1 | 0 | mismatch |
| c66e4e0a-5479-461c-9a39-0298a08f682f | Numerator | 0 | 1 | mismatch |
| cade5021-b1bf-43e9-a0a4-659c05b386d0 | Denominator Exclusion | 1 | 0 | mismatch |
| cade5021-b1bf-43e9-a0a4-659c05b386d0 | Numerator | 0 | 1 | mismatch |
| d3ac0220-8947-489d-b7fe-a199d5365a6f | Denominator Exclusion | 1 | 0 | mismatch |
| d3ac0220-8947-489d-b7fe-a199d5365a6f | Numerator | 0 | 1 | mismatch |
| da05305e-9c4c-4b1d-ac55-cab089a11d2b | Denominator Exclusion | 1 | 0 | mismatch |
| da05305e-9c4c-4b1d-ac55-cab089a11d2b | Numerator | 0 | 1 | mismatch |
| e61be907-af68-493f-a6bc-3d93ef8b6c6e | Denominator Exclusion | 1 | 0 | mismatch |
| e61be907-af68-493f-a6bc-3d93ef8b6c6e | Numerator | 0 | 1 | mismatch |
| ede0ee7a-18ab-4ba7-934c-23618f1270ea | Denominator Exclusion | 1 | 0 | mismatch |
| ede0ee7a-18ab-4ba7-934c-23618f1270ea | Numerator | 0 | 1 | mismatch |
| f5771b74-a7de-439a-a51f-49a3863e086b | Denominator Exclusion | 1 | 0 | mismatch |
| f5771b74-a7de-439a-a51f-49a3863e086b | Numerator | 0 | 1 | mismatch |
| f64a63d1-cdc9-4486-a4d5-1d140a4f07e1 | Denominator Exclusion | 1 | 0 | mismatch |
| f64a63d1-cdc9-4486-a4d5-1d140a4f07e1 | Numerator | 0 | 1 | mismatch |
| fccb9758-ea26-4a1e-98cf-3942102295b8 | Denominator Exclusion | 1 | 0 | mismatch |
| fccb9758-ea26-4a1e-98cf-3942102295b8 | Numerator | 0 | 1 | mismatch |

### CMS124FHIRCervicalCancerScreen

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 1104f4a8-5328-4629-8b7f-77f7b2e62225 | Denominator Exclusion | 1 | 0 | mismatch |
| 25727adc-4495-4e13-9dfc-8b9cb6bf17b9 | Denominator Exclusion | 1 | 0 | mismatch |
| 25727adc-4495-4e13-9dfc-8b9cb6bf17b9 | Numerator | 0 | 1 | mismatch |
| 321abfa0-2c0e-4885-8b5b-20208512e605 | Denominator Exclusion | 1 | 0 | mismatch |
| 321abfa0-2c0e-4885-8b5b-20208512e605 | Numerator | 0 | 1 | mismatch |
| 3e21058f-64cc-4b0a-8c84-1122df974dae | Denominator Exclusion | 1 | 0 | mismatch |
| 4c40d1e6-3943-4a0e-a95c-6e6b845f0851 | Denominator Exclusion | 1 | 0 | mismatch |
| 4c40d1e6-3943-4a0e-a95c-6e6b845f0851 | Numerator | 0 | 1 | mismatch |
| 59ef157d-1417-4a8e-9193-06d9c66ba8e1 | Denominator Exclusion | 1 | 0 | mismatch |
| 6005d1fd-e9f5-414d-88d6-23087b4f3e94 | Denominator Exclusion | 1 | 0 | mismatch |
| 6005d1fd-e9f5-414d-88d6-23087b4f3e94 | Numerator | 0 | 1 | mismatch |
| 908f935e-43b9-4666-982a-f211d1cfcd50 | Denominator Exclusion | 1 | 0 | mismatch |
| b8c73916-4520-47e1-9456-a36cd1575693 | Denominator Exclusion | 1 | 0 | mismatch |
| c0d1f27d-249b-4d74-a493-a4796fb8e833 | Denominator Exclusion | 1 | 0 | mismatch |
| c5ea33df-060b-484a-b6c4-17c600559077 | Denominator Exclusion | 1 | 0 | mismatch |
| d15cf8c6-5f36-4874-83a5-d726945721c6 | Denominator Exclusion | 1 | 0 | mismatch |
| dc5b8054-7432-4905-aaef-3acd6f3f75b9 | Denominator Exclusion | 1 | 0 | mismatch |
| e8e5b4c8-0e07-415f-a534-9143ecef5f10 | Denominator Exclusion | 1 | 0 | mismatch |

### CMS125FHIRBreastCancerScreen

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 07fb2077-048c-4cb0-ba3e-6e67ed33133d | Denominator Exclusion | 1 | 0 | mismatch |
| 0ced1e0c-9c92-4582-a4b1-e44f130e436f | Denominator Exclusion | 1 | 0 | mismatch |
| 14b87edd-7f1e-4f6a-9910-f905966ec904 | Denominator Exclusion | 1 | 0 | mismatch |
| 24557438-17c9-405c-88dc-0c0bfda17d27 | Denominator Exclusion | 1 | 0 | mismatch |
| 33afc6f6-11c8-4d29-9e2d-cdc292565458 | Denominator Exclusion | 1 | 0 | mismatch |
| 46fbbd0e-d175-4203-97bb-fe616cd2ab77 | Denominator Exclusion | 1 | 0 | mismatch |
| 473f9149-c7f0-4979-8924-9534cabe5117 | Denominator Exclusion | 1 | 0 | mismatch |
| 57d8d494-e828-4edf-8c8b-e27da33ea223 | Denominator Exclusion | 1 | 0 | mismatch |
| 591e960d-b937-41f3-9817-56cf201a06db | Denominator Exclusion | 1 | 0 | mismatch |
| 5be43868-ffec-4de5-b99e-185513b74c82 | Denominator Exclusion | 1 | 0 | mismatch |
| 5e3f01ad-1eda-4cb7-8d37-1146beae59e9 | Denominator Exclusion | 1 | 0 | mismatch |
| 6226b04f-5e2d-4977-9169-8e9451ffa939 | Denominator Exclusion | 1 | 0 | mismatch |
| 6226b04f-5e2d-4977-9169-8e9451ffa939 | Numerator | 0 | 1 | mismatch |
| 633c26f2-9c7a-4eaf-b983-83b9e13656ac | Denominator Exclusion | 1 | 0 | mismatch |
| 68067d39-5287-40dd-ba97-c2aa1bf46d78 | Denominator Exclusion | 1 | 0 | mismatch |
| 6b2e313f-6139-45fa-8e18-cc2f0b908981 | Denominator Exclusion | 1 | 0 | mismatch |
| 6fc33313-98bc-460e-9e38-9240dcbd111a | Denominator Exclusion | 1 | 0 | mismatch |
| 81dce125-8691-4625-ac6b-07fce0a45680 | Denominator Exclusion | 1 | 0 | mismatch |
| 81dce125-8691-4625-ac6b-07fce0a45680 | Numerator | 0 | 1 | mismatch |
| 8278ae07-69ec-469c-ae01-e933d051f764 | Denominator Exclusion | 1 | 0 | mismatch |
| b528b1a6-cd8d-4f66-83c2-6467e83b6996 | Denominator Exclusion | 1 | 0 | mismatch |
| bea75baa-41f5-4755-9986-15c2bba658d5 | Denominator Exclusion | 1 | 0 | mismatch |
| c32eb7d1-eac5-458e-b965-c717620579a2 | Denominator Exclusion | 1 | 0 | mismatch |
| cf727fca-40bc-46ed-b97b-e9021cffb8d3 | Denominator Exclusion | 1 | 0 | mismatch |
| d4540640-2561-4ebd-b7c6-15878a4dc582 | Denominator Exclusion | 1 | 0 | mismatch |
| da85601e-ce6f-4351-b639-1e58c725bf2f | Denominator Exclusion | 1 | 0 | mismatch |
| dd6bd96f-3a4e-4796-bee0-1d31884e96d7 | Denominator Exclusion | 1 | 0 | mismatch |
| deb40976-ede4-4657-8af8-078369fa65f4 | Denominator Exclusion | 1 | 0 | mismatch |
| defc50ff-2898-4ab0-ac06-75eae73bc6fa | Denominator Exclusion | 1 | 0 | mismatch |
| f2f748c2-321f-4c05-896a-2ef9d925eaf9 | Denominator Exclusion | 1 | 0 | mismatch |
| f38ce16a-658f-4aa0-b4a6-fac61d2e58a8 | Denominator Exclusion | 1 | 0 | mismatch |
| f4d00e60-e525-4644-a397-4d7d970bcfdb | Denominator Exclusion | 1 | 0 | mismatch |
| f7574a1c-122e-45ef-9ab5-cfa35a40d6d6 | Denominator Exclusion | 1 | 0 | mismatch |
| f9de4c72-b2ed-4c8f-94fe-8c934e42e0a0 | Denominator Exclusion | 1 | 0 | mismatch |
| ffbb03e1-7188-42ef-8deb-c6cf3f790bfe | Denominator Exclusion | 1 | 0 | mismatch |

### CMS128FHIRAntidepressantMgmt

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 006165b0-ab24-4823-bcee-61d64ae5f581 | Denominator | 1 | 0 | mismatch |
| 006165b0-ab24-4823-bcee-61d64ae5f581 | Initial Population | 1 | 0 | mismatch |
| 006165b0-ab24-4823-bcee-61d64ae5f581 | Numerator | 1 | 0 | mismatch |
| 006165b0-ab24-4823-bcee-61d64ae5f581 | Denominator | 1 | 0 | mismatch |
| 006165b0-ab24-4823-bcee-61d64ae5f581 | Initial Population | 1 | 0 | mismatch |
| 0b61ffb2-9d2d-4eb4-a208-f34f74824543 | Denominator | 1 | 0 | mismatch |
| 0b61ffb2-9d2d-4eb4-a208-f34f74824543 | Initial Population | 1 | 0 | mismatch |
| 0b61ffb2-9d2d-4eb4-a208-f34f74824543 | Denominator | 1 | 0 | mismatch |
| 0b61ffb2-9d2d-4eb4-a208-f34f74824543 | Initial Population | 1 | 0 | mismatch |
| 0c8ea277-b375-40a1-84b5-d05bfbaa5657 | Denominator | 1 | 0 | mismatch |
| 0c8ea277-b375-40a1-84b5-d05bfbaa5657 | Initial Population | 1 | 0 | mismatch |
| 0c8ea277-b375-40a1-84b5-d05bfbaa5657 | Denominator | 1 | 0 | mismatch |
| 0c8ea277-b375-40a1-84b5-d05bfbaa5657 | Initial Population | 1 | 0 | mismatch |
| 3207b0d6-43cb-4dd7-a71f-db8ad4b9e07a | Denominator | 1 | 0 | mismatch |
| 3207b0d6-43cb-4dd7-a71f-db8ad4b9e07a | Initial Population | 1 | 0 | mismatch |
| 3207b0d6-43cb-4dd7-a71f-db8ad4b9e07a | Denominator | 1 | 0 | mismatch |
| 3207b0d6-43cb-4dd7-a71f-db8ad4b9e07a | Initial Population | 1 | 0 | mismatch |
| 35317aef-07fd-4c19-aa61-01a0f61dfe4c | Denominator | 1 | 0 | mismatch |
| 35317aef-07fd-4c19-aa61-01a0f61dfe4c | Initial Population | 1 | 0 | mismatch |
| 35317aef-07fd-4c19-aa61-01a0f61dfe4c | Denominator | 1 | 0 | mismatch |
| 35317aef-07fd-4c19-aa61-01a0f61dfe4c | Initial Population | 1 | 0 | mismatch |
| 40ed567d-9ecf-4bf8-b552-be9b87a6834d | Denominator | 1 | 0 | mismatch |
| 40ed567d-9ecf-4bf8-b552-be9b87a6834d | Initial Population | 1 | 0 | mismatch |
| 40ed567d-9ecf-4bf8-b552-be9b87a6834d | Denominator | 1 | 0 | mismatch |
| 40ed567d-9ecf-4bf8-b552-be9b87a6834d | Initial Population | 1 | 0 | mismatch |
| 4365633e-3edf-4bcf-a30e-33efb41fd496 | Denominator | 1 | 0 | mismatch |
| 4365633e-3edf-4bcf-a30e-33efb41fd496 | Initial Population | 1 | 0 | mismatch |
| 4365633e-3edf-4bcf-a30e-33efb41fd496 | Denominator | 1 | 0 | mismatch |
| 4365633e-3edf-4bcf-a30e-33efb41fd496 | Initial Population | 1 | 0 | mismatch |
| 4c2caf57-7168-4149-a596-d0914d7e3fe8 | Denominator | 1 | 0 | mismatch |
| 4c2caf57-7168-4149-a596-d0914d7e3fe8 | Denominator Exclusion | 1 | 0 | mismatch |
| 4c2caf57-7168-4149-a596-d0914d7e3fe8 | Initial Population | 1 | 0 | mismatch |
| 4c2caf57-7168-4149-a596-d0914d7e3fe8 | Denominator | 1 | 0 | mismatch |
| 4c2caf57-7168-4149-a596-d0914d7e3fe8 | Denominator Exclusion | 1 | 0 | mismatch |
| 4c2caf57-7168-4149-a596-d0914d7e3fe8 | Initial Population | 1 | 0 | mismatch |
| 62ea0c3d-46da-48a1-87dd-d1927ed2df75 | Denominator | 1 | 0 | mismatch |
| 62ea0c3d-46da-48a1-87dd-d1927ed2df75 | Denominator Exclusion | 1 | 0 | mismatch |
| 62ea0c3d-46da-48a1-87dd-d1927ed2df75 | Initial Population | 1 | 0 | mismatch |
| 62ea0c3d-46da-48a1-87dd-d1927ed2df75 | Denominator | 1 | 0 | mismatch |
| 62ea0c3d-46da-48a1-87dd-d1927ed2df75 | Denominator Exclusion | 1 | 0 | mismatch |
| 62ea0c3d-46da-48a1-87dd-d1927ed2df75 | Initial Population | 1 | 0 | mismatch |
| 71cc96f3-e525-4e60-b6ad-1037d16a3c17 | Denominator | 1 | 0 | mismatch |
| 71cc96f3-e525-4e60-b6ad-1037d16a3c17 | Denominator Exclusion | 1 | 0 | mismatch |
| 71cc96f3-e525-4e60-b6ad-1037d16a3c17 | Initial Population | 1 | 0 | mismatch |
| 71cc96f3-e525-4e60-b6ad-1037d16a3c17 | Denominator | 1 | 0 | mismatch |
| 71cc96f3-e525-4e60-b6ad-1037d16a3c17 | Denominator Exclusion | 1 | 0 | mismatch |
| 71cc96f3-e525-4e60-b6ad-1037d16a3c17 | Initial Population | 1 | 0 | mismatch |
| 76e30d44-a803-4b4b-a6ba-f11de6fa6329 | Denominator | 1 | 0 | mismatch |
| 76e30d44-a803-4b4b-a6ba-f11de6fa6329 | Denominator Exclusion | 1 | 0 | mismatch |
| 76e30d44-a803-4b4b-a6ba-f11de6fa6329 | Initial Population | 1 | 0 | mismatch |
| 76e30d44-a803-4b4b-a6ba-f11de6fa6329 | Denominator | 1 | 0 | mismatch |
| 76e30d44-a803-4b4b-a6ba-f11de6fa6329 | Denominator Exclusion | 1 | 0 | mismatch |
| 76e30d44-a803-4b4b-a6ba-f11de6fa6329 | Initial Population | 1 | 0 | mismatch |
| 778e804e-7356-400f-bc36-8d202d775509 | Denominator | 1 | 0 | mismatch |
| 778e804e-7356-400f-bc36-8d202d775509 | Initial Population | 1 | 0 | mismatch |
| 778e804e-7356-400f-bc36-8d202d775509 | Denominator | 1 | 0 | mismatch |
| 778e804e-7356-400f-bc36-8d202d775509 | Initial Population | 1 | 0 | mismatch |
| 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 | Denominator | 1 | 0 | mismatch |
| 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 | Denominator Exclusion | 1 | 0 | mismatch |
| 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 | Initial Population | 1 | 0 | mismatch |
| 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 | Denominator | 1 | 0 | mismatch |
| 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 | Denominator Exclusion | 1 | 0 | mismatch |
| 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 | Initial Population | 1 | 0 | mismatch |
| 84a1aec5-0730-446f-bd5c-328938534e5e | Denominator | 1 | 0 | mismatch |
| 84a1aec5-0730-446f-bd5c-328938534e5e | Initial Population | 1 | 0 | mismatch |
| 84a1aec5-0730-446f-bd5c-328938534e5e | Denominator | 1 | 0 | mismatch |
| 84a1aec5-0730-446f-bd5c-328938534e5e | Initial Population | 1 | 0 | mismatch |
| 925b4f30-ea5c-47c3-a24f-8bfcec5dcdf6 | Denominator | 1 | 0 | mismatch |
| 925b4f30-ea5c-47c3-a24f-8bfcec5dcdf6 | Initial Population | 1 | 0 | mismatch |
| 925b4f30-ea5c-47c3-a24f-8bfcec5dcdf6 | Denominator | 1 | 0 | mismatch |
| 925b4f30-ea5c-47c3-a24f-8bfcec5dcdf6 | Initial Population | 1 | 0 | mismatch |
| 925ef058-b2e2-489e-8d5e-1a33299efa30 | Denominator | 1 | 0 | mismatch |
| 925ef058-b2e2-489e-8d5e-1a33299efa30 | Denominator Exclusion | 1 | 0 | mismatch |
| 925ef058-b2e2-489e-8d5e-1a33299efa30 | Initial Population | 1 | 0 | mismatch |
| 925ef058-b2e2-489e-8d5e-1a33299efa30 | Denominator | 1 | 0 | mismatch |
| 925ef058-b2e2-489e-8d5e-1a33299efa30 | Denominator Exclusion | 1 | 0 | mismatch |
| 925ef058-b2e2-489e-8d5e-1a33299efa30 | Initial Population | 1 | 0 | mismatch |
| 9ab27fb9-1253-4b89-b88c-693d5f8ae65d | Denominator | 1 | 0 | mismatch |
| 9ab27fb9-1253-4b89-b88c-693d5f8ae65d | Denominator Exclusion | 1 | 0 | mismatch |
| 9ab27fb9-1253-4b89-b88c-693d5f8ae65d | Initial Population | 1 | 0 | mismatch |
| 9ab27fb9-1253-4b89-b88c-693d5f8ae65d | Denominator | 1 | 0 | mismatch |
| 9ab27fb9-1253-4b89-b88c-693d5f8ae65d | Denominator Exclusion | 1 | 0 | mismatch |
| 9ab27fb9-1253-4b89-b88c-693d5f8ae65d | Initial Population | 1 | 0 | mismatch |
| 9c3b43d2-cbfd-4877-8b0e-e9e8cd9c6312 | Denominator | 1 | 0 | mismatch |
| 9c3b43d2-cbfd-4877-8b0e-e9e8cd9c6312 | Initial Population | 1 | 0 | mismatch |
| 9c3b43d2-cbfd-4877-8b0e-e9e8cd9c6312 | Denominator | 1 | 0 | mismatch |
| 9c3b43d2-cbfd-4877-8b0e-e9e8cd9c6312 | Initial Population | 1 | 0 | mismatch |
| a3733b4f-0049-45cf-8b30-3e56ec3d5301 | Denominator | 1 | 0 | mismatch |
| a3733b4f-0049-45cf-8b30-3e56ec3d5301 | Initial Population | 1 | 0 | mismatch |
| a3733b4f-0049-45cf-8b30-3e56ec3d5301 | Denominator | 1 | 0 | mismatch |
| a3733b4f-0049-45cf-8b30-3e56ec3d5301 | Initial Population | 1 | 0 | mismatch |
| aca49569-f2da-4181-b7a3-4037b715f7dd | Denominator | 1 | 0 | mismatch |
| aca49569-f2da-4181-b7a3-4037b715f7dd | Initial Population | 1 | 0 | mismatch |
| aca49569-f2da-4181-b7a3-4037b715f7dd | Denominator | 1 | 0 | mismatch |
| aca49569-f2da-4181-b7a3-4037b715f7dd | Initial Population | 1 | 0 | mismatch |
| b371fd28-5026-43db-840e-21466bde11c9 | Denominator | 1 | 0 | mismatch |
| b371fd28-5026-43db-840e-21466bde11c9 | Denominator Exclusion | 1 | 0 | mismatch |
| b371fd28-5026-43db-840e-21466bde11c9 | Initial Population | 1 | 0 | mismatch |
| b371fd28-5026-43db-840e-21466bde11c9 | Denominator | 1 | 0 | mismatch |
| b371fd28-5026-43db-840e-21466bde11c9 | Denominator Exclusion | 1 | 0 | mismatch |
| b371fd28-5026-43db-840e-21466bde11c9 | Initial Population | 1 | 0 | mismatch |
| b4cd38ff-6828-49bc-b8e6-d4a24b9624b1 | Denominator | 1 | 0 | mismatch |
| b4cd38ff-6828-49bc-b8e6-d4a24b9624b1 | Initial Population | 1 | 0 | mismatch |
| b4cd38ff-6828-49bc-b8e6-d4a24b9624b1 | Denominator | 1 | 0 | mismatch |
| b4cd38ff-6828-49bc-b8e6-d4a24b9624b1 | Initial Population | 1 | 0 | mismatch |
| bdbc73af-00ed-4eca-b5a4-dfaab4fc2c8c | Denominator | 1 | 0 | mismatch |
| bdbc73af-00ed-4eca-b5a4-dfaab4fc2c8c | Initial Population | 1 | 0 | mismatch |
| bdbc73af-00ed-4eca-b5a4-dfaab4fc2c8c | Denominator | 1 | 0 | mismatch |
| bdbc73af-00ed-4eca-b5a4-dfaab4fc2c8c | Initial Population | 1 | 0 | mismatch |
| bff2a70b-b2df-4c6b-9d98-be4edde798e0 | Denominator | 1 | 0 | mismatch |
| bff2a70b-b2df-4c6b-9d98-be4edde798e0 | Initial Population | 1 | 0 | mismatch |
| bff2a70b-b2df-4c6b-9d98-be4edde798e0 | Numerator | 1 | 0 | mismatch |
| bff2a70b-b2df-4c6b-9d98-be4edde798e0 | Denominator | 1 | 0 | mismatch |
| bff2a70b-b2df-4c6b-9d98-be4edde798e0 | Initial Population | 1 | 0 | mismatch |
| bff2a70b-b2df-4c6b-9d98-be4edde798e0 | Numerator | 1 | 0 | mismatch |
| ce747de2-3f8f-4ad8-8370-3ed53b990094 | Denominator | 1 | 0 | mismatch |
| ce747de2-3f8f-4ad8-8370-3ed53b990094 | Initial Population | 1 | 0 | mismatch |
| ce747de2-3f8f-4ad8-8370-3ed53b990094 | Denominator | 1 | 0 | mismatch |
| ce747de2-3f8f-4ad8-8370-3ed53b990094 | Initial Population | 1 | 0 | mismatch |
| dc4c8b59-2a44-4a74-9983-48baabe5679f | Denominator | 1 | 0 | mismatch |
| dc4c8b59-2a44-4a74-9983-48baabe5679f | Initial Population | 1 | 0 | mismatch |
| dc4c8b59-2a44-4a74-9983-48baabe5679f | Denominator | 1 | 0 | mismatch |
| dc4c8b59-2a44-4a74-9983-48baabe5679f | Initial Population | 1 | 0 | mismatch |
| ee6d52b0-149c-4ffe-b260-bb214151652c | Denominator | 1 | 0 | mismatch |
| ee6d52b0-149c-4ffe-b260-bb214151652c | Denominator Exclusion | 1 | 0 | mismatch |
| ee6d52b0-149c-4ffe-b260-bb214151652c | Initial Population | 1 | 0 | mismatch |
| ee6d52b0-149c-4ffe-b260-bb214151652c | Denominator | 1 | 0 | mismatch |
| ee6d52b0-149c-4ffe-b260-bb214151652c | Denominator Exclusion | 1 | 0 | mismatch |
| ee6d52b0-149c-4ffe-b260-bb214151652c | Initial Population | 1 | 0 | mismatch |
| fcfaba77-8917-48de-993e-438eb8d5b77b | Denominator | 1 | 0 | mismatch |
| fcfaba77-8917-48de-993e-438eb8d5b77b | Initial Population | 1 | 0 | mismatch |
| fcfaba77-8917-48de-993e-438eb8d5b77b | Numerator | 1 | 0 | mismatch |
| fcfaba77-8917-48de-993e-438eb8d5b77b | Denominator | 1 | 0 | mismatch |
| fcfaba77-8917-48de-993e-438eb8d5b77b | Initial Population | 1 | 0 | mismatch |

### CMS130FHIRColorectalCancerScrn

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 06934496-0ea0-4ccd-af2e-da5b94410b58 | Denominator Exclusion | 1 | 0 | mismatch |
| 2292adf2-3232-43f8-9497-8448349c51a9 | Denominator Exclusion | 1 | 0 | mismatch |
| 2292adf2-3232-43f8-9497-8448349c51a9 | Numerator | 0 | 1 | mismatch |
| 2847411d-a6c5-4f86-ac1f-d229ffa5a00c | Denominator Exclusion | 1 | 0 | mismatch |
| 2b0d64f9-9f3a-4adf-aadb-c231a8ab98ac | Denominator Exclusion | 1 | 0 | mismatch |
| 2b0d64f9-9f3a-4adf-aadb-c231a8ab98ac | Numerator | 0 | 1 | mismatch |
| 394fbf45-d81c-49d1-be1f-3907227d8940 | Denominator Exclusion | 1 | 0 | mismatch |
| 39fd4a5e-0db2-478d-ba85-4400a1b7e35e | Denominator Exclusion | 1 | 0 | mismatch |
| 3d75185a-d8e1-4861-9b36-528548e57fc4 | Denominator Exclusion | 1 | 0 | mismatch |
| 487de25d-a184-42ed-b1c6-389ed217a0a1 | Denominator Exclusion | 1 | 0 | mismatch |
| 4fc22b6a-0cca-4e61-bedf-2cb73cf66698 | Denominator Exclusion | 1 | 0 | mismatch |
| 5445cc72-68a1-4b73-b06d-4cf52098e0db | Denominator Exclusion | 1 | 0 | mismatch |
| 54db46c1-fa2a-4e6e-96aa-da6dd67c5f18 | Denominator Exclusion | 1 | 0 | mismatch |
| 58b6a190-8a9c-4631-a102-6048f3e62a19 | Denominator Exclusion | 1 | 0 | mismatch |
| 5ebc158d-0736-4467-8bc0-72182bc0f5af | Denominator Exclusion | 1 | 0 | mismatch |
| 642aafde-fabb-458d-ae4d-5db7343f310c | Denominator Exclusion | 1 | 0 | mismatch |
| 650f4ed7-9418-42ad-a9d7-59fe79e951da | Denominator Exclusion | 1 | 0 | mismatch |
| 683cec0c-5368-467b-85f7-4b70c269e8ea | Denominator Exclusion | 1 | 0 | mismatch |
| 7822bd0a-ba96-46f0-8c57-204d37156184 | Denominator Exclusion | 1 | 0 | mismatch |
| 84ebbde4-0ea8-42ae-908b-ef1721748290 | Denominator Exclusion | 1 | 0 | mismatch |
| 95d56325-022c-4bdc-8778-bf02f46139cb | Denominator Exclusion | 1 | 0 | mismatch |
| 95d56325-022c-4bdc-8778-bf02f46139cb | Numerator | 0 | 1 | mismatch |
| 9943e220-d0f1-4718-8377-0d407a529f52 | Denominator Exclusion | 1 | 0 | mismatch |
| 9c6fd73e-9005-4518-b7f0-5d9db57a7ef5 | Denominator Exclusion | 1 | 0 | mismatch |
| b20cd591-3625-4d95-8081-6f2566c51fa6 | Denominator Exclusion | 1 | 0 | mismatch |
| b20cd591-3625-4d95-8081-6f2566c51fa6 | Numerator | 0 | 1 | mismatch |
| be630df2-cc71-47b9-a600-a715912f90be | Denominator Exclusion | 1 | 0 | mismatch |
| be630df2-cc71-47b9-a600-a715912f90be | Numerator | 0 | 1 | mismatch |
| bf3f2c9a-a802-4522-8e38-d1c806e71483 | Denominator Exclusion | 1 | 0 | mismatch |
| bf3f2c9a-a802-4522-8e38-d1c806e71483 | Numerator | 0 | 1 | mismatch |
| c002ae0a-709f-4a5e-82e3-f0a4d8f3a839 | Denominator Exclusion | 1 | 0 | mismatch |
| cdacf996-8b20-49af-8f75-0cfd26fafacb | Denominator Exclusion | 1 | 0 | mismatch |
| d0306f4f-06a9-407d-ac0d-e5628fd1cc59 | Denominator Exclusion | 1 | 0 | mismatch |
| da1e1656-54ae-49f6-ab1b-b8ba9f99b6c2 | Denominator Exclusion | 1 | 0 | mismatch |
| dbead888-2672-453c-8005-d4b9f62b72c9 | Denominator Exclusion | 1 | 0 | mismatch |
| dc337be7-7328-4fce-8f6f-71ee2cb75752 | Denominator Exclusion | 1 | 0 | mismatch |
| df2c9d36-96e4-4ab6-9a2a-d3b5b0a44328 | Denominator Exclusion | 1 | 0 | mismatch |
| e4215f63-f195-48bd-865d-ecb718f742ff | Denominator Exclusion | 1 | 0 | mismatch |
| e904e28b-ec42-4ca5-8dab-f1cf72f705e6 | Denominator Exclusion | 1 | 0 | mismatch |
| e904e28b-ec42-4ca5-8dab-f1cf72f705e6 | Numerator | 0 | 1 | mismatch |
| e9d86ff6-da48-43c9-9e16-dd95d8bc49c3 | Denominator Exclusion | 1 | 0 | mismatch |
| ecd9203b-716e-49ee-be53-eecdea8bef86 | Denominator Exclusion | 1 | 0 | mismatch |
| f0dae4e3-d82d-422f-883c-4e5238c14a54 | Denominator Exclusion | 1 | 0 | mismatch |
| f0dae4e3-d82d-422f-883c-4e5238c14a54 | Numerator | 0 | 1 | mismatch |
| f9ef1fd1-cced-47ad-a47b-d9c20254511c | Denominator Exclusion | 1 | 0 | mismatch |
| fd8d8328-c766-4c9f-a463-ec53957e0276 | Denominator Exclusion | 1 | 0 | mismatch |

### CMS131FHIRDiabetesEyeExam

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 01a1241d-fd97-4c72-b288-fd31c4c7ae80 | Denominator Exclusion | 1 | 0 | mismatch |
| 085b9cf8-58f6-4076-946d-a5206f8de77b | Denominator Exclusion | 1 | 0 | mismatch |
| 0919ba5b-bc08-4660-b8c9-9369b955ffd8 | Denominator Exclusion | 1 | 0 | mismatch |
| 0fa877b4-bbbe-4a5b-814d-57c1472b923b | Denominator Exclusion | 1 | 0 | mismatch |
| 0fa877b4-bbbe-4a5b-814d-57c1472b923b | Numerator | 0 | 1 | mismatch |
| 19a6d651-3dd7-45a9-9340-e40e41875a13 | Denominator Exclusion | 1 | 0 | mismatch |
| 1e8cd1fd-6ba8-48e3-bbdb-d4702c36cf92 | Denominator Exclusion | 1 | 0 | mismatch |
| 3ff1b618-c425-4d51-9447-d1c4cf048d3c | Denominator Exclusion | 1 | 0 | mismatch |
| 4eaa0238-d22c-44c2-a91e-81239a497359 | Denominator Exclusion | 1 | 0 | mismatch |
| 51f41079-0dc3-4da2-86e5-d1360f936ca3 | Denominator Exclusion | 1 | 0 | mismatch |
| 51f41079-0dc3-4da2-86e5-d1360f936ca3 | Numerator | 0 | 1 | mismatch |
| 52d1f4f3-14a0-4eed-a0c2-334b8146b117 | Denominator Exclusion | 1 | 0 | mismatch |
| 56790710-4864-4665-bf28-0514bdb74f0d | Denominator Exclusion | 1 | 0 | mismatch |
| 5e00bc73-c96c-47c8-99f9-0d857acb3e72 | Denominator Exclusion | 1 | 0 | mismatch |
| 61dfb0bd-8fe0-4e30-a911-fa07c782afd9 | Denominator Exclusion | 1 | 0 | mismatch |
| 7a38f99c-a713-4631-9a05-13cfe1a21e5a | Denominator Exclusion | 1 | 0 | mismatch |
| 7c46ee00-603b-4b64-a46b-2cb613f9446d | Denominator Exclusion | 1 | 0 | mismatch |
| 7c46ee00-603b-4b64-a46b-2cb613f9446d | Numerator | 0 | 1 | mismatch |
| 7ca93198-2a13-4266-aa39-82003e19b175 | Denominator Exclusion | 1 | 0 | mismatch |
| 7ca93198-2a13-4266-aa39-82003e19b175 | Numerator | 0 | 1 | mismatch |
| 89073685-3807-41f5-bc32-2cf44c1b8227 | Denominator Exclusion | 1 | 0 | mismatch |
| 8cd1152d-fc40-4558-9eb3-547db2e56d7a | Denominator Exclusion | 1 | 0 | mismatch |
| 8cd1152d-fc40-4558-9eb3-547db2e56d7a | Numerator | 0 | 1 | mismatch |
| 8fdd8b35-ce68-452d-a38a-93843c64411e | Denominator Exclusion | 1 | 0 | mismatch |
| 8ffd1c24-67a9-4991-86cb-3378a45ffd6e | Denominator Exclusion | 1 | 0 | mismatch |
| 9177b3ca-1cd7-404c-93f9-5bc782b9963a | Denominator Exclusion | 1 | 0 | mismatch |
| 96729eb4-48b3-44f8-a6e6-eec225648115 | Denominator Exclusion | 1 | 0 | mismatch |
| 985b5e49-2f5d-4eb3-a33c-2d0eb156bf7b | Denominator Exclusion | 1 | 0 | mismatch |
| 985b5e49-2f5d-4eb3-a33c-2d0eb156bf7b | Numerator | 0 | 1 | mismatch |
| a2c893b1-5727-45ba-9b79-1d9e78697e20 | Denominator Exclusion | 1 | 0 | mismatch |
| b08c80d0-c70e-4653-b5da-e1f8cb858714 | Denominator Exclusion | 1 | 0 | mismatch |
| bf5f59ac-661c-4f8c-a4aa-b3c0a66f2a49 | Denominator Exclusion | 1 | 0 | mismatch |
| bf5f59ac-661c-4f8c-a4aa-b3c0a66f2a49 | Numerator | 0 | 1 | mismatch |
| cd42be5f-e738-465a-aa40-e8cfaa2e82e9 | Denominator Exclusion | 1 | 0 | mismatch |
| cd42be5f-e738-465a-aa40-e8cfaa2e82e9 | Numerator | 0 | 1 | mismatch |
| cfa4b281-a298-4fa9-aac4-5261519a3dd9 | Denominator Exclusion | 1 | 0 | mismatch |
| d3b4f0ab-d8d1-4c4c-8763-7a8276e0c3ca | Denominator Exclusion | 1 | 0 | mismatch |
| d4091ecf-638c-41ae-bae9-2b0c3bea864e | Denominator Exclusion | 1 | 0 | mismatch |
| d6fd9369-9e85-415d-a3d1-73747fb30af6 | Denominator Exclusion | 1 | 0 | mismatch |
| dcd62616-c203-4ddf-817a-4ce8622e23ca | Denominator Exclusion | 1 | 0 | mismatch |
| dcd62616-c203-4ddf-817a-4ce8622e23ca | Numerator | 0 | 1 | mismatch |
| eab86b9c-b8e8-4f60-837f-8f9aa6f039ee | Denominator Exclusion | 1 | 0 | mismatch |
| ecc34b3c-1241-4541-a8dd-66183c3d70de | Denominator Exclusion | 1 | 0 | mismatch |
| ef247fbf-b973-4321-9830-5d184a730a6f | Denominator Exclusion | 1 | 0 | mismatch |
| f45a1cb0-d1a7-42cf-9cae-6ea6c7799085 | Denominator Exclusion | 1 | 0 | mismatch |
| f850c570-3a2b-4b3b-a9f8-f5fc1b03f639 | Denominator Exclusion | 1 | 0 | mismatch |
| f850c570-3a2b-4b3b-a9f8-f5fc1b03f639 | Numerator | 0 | 1 | mismatch |

### CMS135FHIRACEIorARBorARNIforHF

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 149c3a7c-2b80-47f8-b50d-5c1d233eedb7 | Denominator Exception | 0 | 1 | mismatch |
| 149c3a7c-2b80-47f8-b50d-5c1d233eedb7 | Numerator | 1 | 0 | mismatch |
| 1f64a697-a90b-4aaf-a315-fa84168ac2b4 | Denominator Exception | 0 | 1 | mismatch |
| 1f64a697-a90b-4aaf-a315-fa84168ac2b4 | Numerator | 1 | 0 | mismatch |
| 298d5342-fa0a-4386-bf48-b9c977a1c367 | Denominator Exception | 0 | 1 | mismatch |
| 298d5342-fa0a-4386-bf48-b9c977a1c367 | Numerator | 1 | 0 | mismatch |
| 4bc4883f-0770-4a68-824a-5fa4dba72638 | Denominator Exception | 0 | 1 | mismatch |
| 4bc4883f-0770-4a68-824a-5fa4dba72638 | Numerator | 1 | 0 | mismatch |
| 5b7e720f-e2fc-4779-9b1c-3f34a0241482 | Denominator Exception | 0 | 1 | mismatch |
| 64e76766-9760-4385-a977-cbe8136ce425 | Denominator Exception | 0 | 1 | mismatch |
| 64e76766-9760-4385-a977-cbe8136ce425 | Numerator | 1 | 0 | mismatch |
| 6a86918d-3f69-43c8-8863-1d0bf835a2c7 | Denominator Exception | 0 | 1 | mismatch |
| d18e37a6-7b66-4e7c-b305-692872c13f8d | Denominator Exception | 0 | 1 | mismatch |
| d297e68e-3f02-42a8-a59f-a5a4cecbd47d | Denominator Exception | 0 | 1 | mismatch |
| d297e68e-3f02-42a8-a59f-a5a4cecbd47d | Numerator | 1 | 0 | mismatch |

### CMS136FHIRChildADHDMedFollowUp

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| a46db6aa-5016-4111-bc4e-a31156c87ec6 | Denominator | 1 | 0 | mismatch |
| a46db6aa-5016-4111-bc4e-a31156c87ec6 | Initial Population | 1 | 0 | mismatch |
| a46db6aa-5016-4111-bc4e-a31156c87ec6 | Numerator | 1 | 0 | mismatch |
| a46db6aa-5016-4111-bc4e-a31156c87ec6 | Denominator | 1 | 0 | mismatch |
| a46db6aa-5016-4111-bc4e-a31156c87ec6 | Initial Population | 1 | 0 | mismatch |

### CMS144FHIRHFBetaBlockerForLVSD

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 07efd4bb-b45d-4bfd-aeb2-08de49742d91 | Denominator Exception | 0 | 1 | mismatch |
| 07efd4bb-b45d-4bfd-aeb2-08de49742d91 | Numerator | 1 | 0 | mismatch |
| 67779bc6-07ee-42cf-8ca7-e71302915dba | Denominator Exception | 0 | 1 | mismatch |
| 67779bc6-07ee-42cf-8ca7-e71302915dba | Numerator | 1 | 0 | mismatch |
| 7b8885c5-ad14-4361-9755-c76a6e3b8530 | Numerator | 1 | 0 | mismatch |

### CMS145FHIRCADBBlockerTPMIorLVSD

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 4a3086cd-63f3-41c3-8ce9-f75b4b18b85c | Denominator Exception | 0 | 1 | mismatch |
| 4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e | Denominator Exception | 0 | 1 | mismatch |
| 5fd0d626-e9c5-4e6c-a10d-1a1183fa7702 | Denominator Exception | 0 | 1 | mismatch |
| 61306767-0e74-44b8-ac06-1339c3783355 | Denominator Exception | 0 | 1 | mismatch |
| b65680a0-9768-4ce4-b08d-972fcd84e28e | Denominator Exception | 0 | 1 | mismatch |
| fd5fb311-a466-4c59-966d-48fa7aa88931 | Denominator Exception | 0 | 1 | mismatch |

### CMS153FHIRChlamydiaScreening

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 6e31a1eb-0d32-4a9b-aa86-ee34436f99c1 | Denominator | 1 | 0 | mismatch |
| 6e31a1eb-0d32-4a9b-aa86-ee34436f99c1 | Initial Population | 1 | 0 | mismatch |

### CMS156FHIRHighRiskMedsElderly

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 00da3e6b-9ab1-48b6-8b34-a0f08754fb3c | Denominator Exclusion | 1 | 0 | mismatch |
| 00da3e6b-9ab1-48b6-8b34-a0f08754fb3c | Numerator | 0 | 1 | mismatch |
| 00da3e6b-9ab1-48b6-8b34-a0f08754fb3c | Denominator Exclusion | 1 | 0 | mismatch |
| 00da3e6b-9ab1-48b6-8b34-a0f08754fb3c | Denominator Exclusion | 1 | 0 | mismatch |
| 00da3e6b-9ab1-48b6-8b34-a0f08754fb3c | Numerator | 0 | 1 | mismatch |
| 0440a9c0-f299-43a6-bfef-cb2cf326ee85 | Denominator Exclusion | 1 | 0 | mismatch |
| 0440a9c0-f299-43a6-bfef-cb2cf326ee85 | Numerator | 0 | 1 | mismatch |
| 0440a9c0-f299-43a6-bfef-cb2cf326ee85 | Denominator Exclusion | 1 | 0 | mismatch |
| 0440a9c0-f299-43a6-bfef-cb2cf326ee85 | Denominator Exclusion | 1 | 0 | mismatch |
| 0440a9c0-f299-43a6-bfef-cb2cf326ee85 | Numerator | 0 | 1 | mismatch |
| 07f11229-6e8f-42bf-9905-3d319460fb33 | Denominator Exclusion | 1 | 0 | mismatch |
| 07f11229-6e8f-42bf-9905-3d319460fb33 | Denominator Exclusion | 1 | 0 | mismatch |
| 07f11229-6e8f-42bf-9905-3d319460fb33 | Denominator Exclusion | 1 | 0 | mismatch |
| 0e31d00f-8b4e-4800-a7f7-ab8b824bf689 | Denominator Exclusion | 1 | 0 | mismatch |
| 0e31d00f-8b4e-4800-a7f7-ab8b824bf689 | Numerator | 0 | 1 | mismatch |
| 0e31d00f-8b4e-4800-a7f7-ab8b824bf689 | Denominator Exclusion | 1 | 0 | mismatch |
| 0e31d00f-8b4e-4800-a7f7-ab8b824bf689 | Denominator Exclusion | 1 | 0 | mismatch |
| 0e31d00f-8b4e-4800-a7f7-ab8b824bf689 | Numerator | 0 | 1 | mismatch |
| 1789d80d-bc5b-4e15-ab64-399d05e55a19 | Denominator Exclusion | 1 | 0 | mismatch |
| 1789d80d-bc5b-4e15-ab64-399d05e55a19 | Denominator Exclusion | 1 | 0 | mismatch |
| 1789d80d-bc5b-4e15-ab64-399d05e55a19 | Denominator Exclusion | 1 | 0 | mismatch |
| 1968ff78-9027-4ea9-99c8-42282743bfc3 | Denominator Exclusion | 1 | 0 | mismatch |
| 1968ff78-9027-4ea9-99c8-42282743bfc3 | Numerator | 0 | 1 | mismatch |
| 1968ff78-9027-4ea9-99c8-42282743bfc3 | Denominator Exclusion | 1 | 0 | mismatch |
| 1968ff78-9027-4ea9-99c8-42282743bfc3 | Denominator Exclusion | 1 | 0 | mismatch |
| 1968ff78-9027-4ea9-99c8-42282743bfc3 | Numerator | 0 | 1 | mismatch |
| 2389a7bb-16a7-4800-ba4a-2585ebd98a0a | Denominator Exclusion | 1 | 0 | mismatch |
| 2389a7bb-16a7-4800-ba4a-2585ebd98a0a | Denominator Exclusion | 1 | 0 | mismatch |
| 2389a7bb-16a7-4800-ba4a-2585ebd98a0a | Denominator Exclusion | 1 | 0 | mismatch |
| 24d82fc3-13b1-4974-9dc1-7771580853df | Denominator Exclusion | 1 | 0 | mismatch |
| 24d82fc3-13b1-4974-9dc1-7771580853df | Numerator | 0 | 1 | mismatch |
| 24d82fc3-13b1-4974-9dc1-7771580853df | Denominator Exclusion | 1 | 0 | mismatch |
| 24d82fc3-13b1-4974-9dc1-7771580853df | Denominator Exclusion | 1 | 0 | mismatch |
| 24d82fc3-13b1-4974-9dc1-7771580853df | Numerator | 0 | 1 | mismatch |
| 2dfe5252-0eb7-4519-9f4c-d7f95a7acaae | Denominator Exclusion | 1 | 0 | mismatch |
| 2dfe5252-0eb7-4519-9f4c-d7f95a7acaae | Numerator | 0 | 1 | mismatch |
| 2dfe5252-0eb7-4519-9f4c-d7f95a7acaae | Denominator Exclusion | 1 | 0 | mismatch |
| 2dfe5252-0eb7-4519-9f4c-d7f95a7acaae | Denominator Exclusion | 1 | 0 | mismatch |
| 2dfe5252-0eb7-4519-9f4c-d7f95a7acaae | Numerator | 0 | 1 | mismatch |
| 385599b5-a1e9-4b7a-8e9f-281c58fed95e | Denominator Exclusion | 1 | 0 | mismatch |
| 385599b5-a1e9-4b7a-8e9f-281c58fed95e | Denominator Exclusion | 1 | 0 | mismatch |
| 385599b5-a1e9-4b7a-8e9f-281c58fed95e | Denominator Exclusion | 1 | 0 | mismatch |
| 407618d7-e2c7-4aae-9744-b447193c4c15 | Denominator Exclusion | 1 | 0 | mismatch |
| 407618d7-e2c7-4aae-9744-b447193c4c15 | Numerator | 0 | 1 | mismatch |
| 407618d7-e2c7-4aae-9744-b447193c4c15 | Denominator Exclusion | 1 | 0 | mismatch |
| 407618d7-e2c7-4aae-9744-b447193c4c15 | Denominator Exclusion | 1 | 0 | mismatch |
| 407618d7-e2c7-4aae-9744-b447193c4c15 | Numerator | 0 | 1 | mismatch |
| 42125b07-9cb2-44df-ba1f-78237b0d3ebc | Denominator Exclusion | 1 | 0 | mismatch |
| 42125b07-9cb2-44df-ba1f-78237b0d3ebc | Denominator Exclusion | 1 | 0 | mismatch |
| 42125b07-9cb2-44df-ba1f-78237b0d3ebc | Numerator | 0 | 1 | mismatch |
| 42125b07-9cb2-44df-ba1f-78237b0d3ebc | Denominator Exclusion | 1 | 0 | mismatch |
| 42125b07-9cb2-44df-ba1f-78237b0d3ebc | Numerator | 0 | 1 | mismatch |
| 435702f5-68ca-4f81-a7e1-b5060726bb75 | Denominator Exclusion | 1 | 0 | mismatch |
| 435702f5-68ca-4f81-a7e1-b5060726bb75 | Denominator Exclusion | 1 | 0 | mismatch |
| 435702f5-68ca-4f81-a7e1-b5060726bb75 | Denominator Exclusion | 1 | 0 | mismatch |
| 47f69fc0-fac8-4f88-876b-cf415ec0e214 | Denominator Exclusion | 1 | 0 | mismatch |
| 47f69fc0-fac8-4f88-876b-cf415ec0e214 | Numerator | 0 | 1 | mismatch |
| 47f69fc0-fac8-4f88-876b-cf415ec0e214 | Denominator Exclusion | 1 | 0 | mismatch |
| 47f69fc0-fac8-4f88-876b-cf415ec0e214 | Denominator Exclusion | 1 | 0 | mismatch |
| 47f69fc0-fac8-4f88-876b-cf415ec0e214 | Numerator | 0 | 1 | mismatch |
| 4aa75d19-ac8b-49b0-a686-429fbc033d77 | Denominator Exclusion | 1 | 0 | mismatch |
| 4aa75d19-ac8b-49b0-a686-429fbc033d77 | Denominator Exclusion | 1 | 0 | mismatch |
| 4aa75d19-ac8b-49b0-a686-429fbc033d77 | Denominator Exclusion | 1 | 0 | mismatch |
| 52f08670-4df5-4538-b009-eb96e3247618 | Denominator Exclusion | 1 | 0 | mismatch |
| 52f08670-4df5-4538-b009-eb96e3247618 | Denominator Exclusion | 1 | 0 | mismatch |
| 52f08670-4df5-4538-b009-eb96e3247618 | Denominator Exclusion | 1 | 0 | mismatch |
| 5326ef57-57d6-49b8-bdc5-b3179cdcb82d | Denominator Exclusion | 1 | 0 | mismatch |
| 5326ef57-57d6-49b8-bdc5-b3179cdcb82d | Denominator Exclusion | 1 | 0 | mismatch |
| 5326ef57-57d6-49b8-bdc5-b3179cdcb82d | Numerator | 0 | 1 | mismatch |
| 5326ef57-57d6-49b8-bdc5-b3179cdcb82d | Denominator Exclusion | 1 | 0 | mismatch |
| 5326ef57-57d6-49b8-bdc5-b3179cdcb82d | Numerator | 0 | 1 | mismatch |
| 5c33755f-40d8-4409-b699-a3499ddddda0 | Denominator Exclusion | 1 | 0 | mismatch |
| 5c33755f-40d8-4409-b699-a3499ddddda0 | Denominator Exclusion | 1 | 0 | mismatch |
| 5c33755f-40d8-4409-b699-a3499ddddda0 | Denominator Exclusion | 1 | 0 | mismatch |
| 5f200044-e0b1-4e20-8ee7-b9e735d3086c | Denominator Exclusion | 1 | 0 | mismatch |
| 5f200044-e0b1-4e20-8ee7-b9e735d3086c | Denominator Exclusion | 1 | 0 | mismatch |
| 5f200044-e0b1-4e20-8ee7-b9e735d3086c | Denominator Exclusion | 1 | 0 | mismatch |
| 64c49012-0f98-41da-a00b-9cd673294d16 | Denominator Exclusion | 1 | 0 | mismatch |
| 64c49012-0f98-41da-a00b-9cd673294d16 | Denominator Exclusion | 1 | 0 | mismatch |
| 64c49012-0f98-41da-a00b-9cd673294d16 | Numerator | 0 | 1 | mismatch |
| 64c49012-0f98-41da-a00b-9cd673294d16 | Denominator Exclusion | 1 | 0 | mismatch |
| 64c49012-0f98-41da-a00b-9cd673294d16 | Numerator | 0 | 1 | mismatch |
| 68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca | Denominator Exclusion | 1 | 0 | mismatch |
| 68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca | Denominator Exclusion | 1 | 0 | mismatch |
| 68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca | Numerator | 0 | 1 | mismatch |
| 68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca | Denominator Exclusion | 1 | 0 | mismatch |
| 68ccf96b-cdd0-4bd7-b0d5-9ed33ec895ca | Numerator | 0 | 1 | mismatch |
| 708b6eaa-5d2e-463b-9d9f-d97b19f4af75 | Denominator Exclusion | 1 | 0 | mismatch |
| 708b6eaa-5d2e-463b-9d9f-d97b19f4af75 | Numerator | 0 | 1 | mismatch |
| 708b6eaa-5d2e-463b-9d9f-d97b19f4af75 | Denominator Exclusion | 1 | 0 | mismatch |
| 708b6eaa-5d2e-463b-9d9f-d97b19f4af75 | Denominator Exclusion | 1 | 0 | mismatch |
| 708b6eaa-5d2e-463b-9d9f-d97b19f4af75 | Numerator | 0 | 1 | mismatch |
| 79c12ad7-f7de-4b87-93c3-ef85e0a644f0 | Denominator Exclusion | 1 | 0 | mismatch |
| 79c12ad7-f7de-4b87-93c3-ef85e0a644f0 | Denominator Exclusion | 1 | 0 | mismatch |
| 79c12ad7-f7de-4b87-93c3-ef85e0a644f0 | Denominator Exclusion | 1 | 0 | mismatch |
| 7f204655-dbf6-47d7-a684-ff1570cf4b05 | Denominator Exclusion | 1 | 0 | mismatch |
| 7f204655-dbf6-47d7-a684-ff1570cf4b05 | Denominator Exclusion | 1 | 0 | mismatch |
| 7f204655-dbf6-47d7-a684-ff1570cf4b05 | Numerator | 0 | 1 | mismatch |
| 7f204655-dbf6-47d7-a684-ff1570cf4b05 | Denominator Exclusion | 1 | 0 | mismatch |
| 7f204655-dbf6-47d7-a684-ff1570cf4b05 | Numerator | 0 | 1 | mismatch |
| 8082ddbf-8d01-4b29-8709-70e70bbc70f9 | Denominator Exclusion | 1 | 0 | mismatch |
| 8082ddbf-8d01-4b29-8709-70e70bbc70f9 | Numerator | 0 | 1 | mismatch |
| 8082ddbf-8d01-4b29-8709-70e70bbc70f9 | Denominator Exclusion | 1 | 0 | mismatch |
| 8082ddbf-8d01-4b29-8709-70e70bbc70f9 | Denominator Exclusion | 1 | 0 | mismatch |
| 8082ddbf-8d01-4b29-8709-70e70bbc70f9 | Numerator | 0 | 1 | mismatch |
| 8e648527-5b7e-430c-b5ca-fe70a4133d55 | Denominator Exclusion | 1 | 0 | mismatch |
| 8e648527-5b7e-430c-b5ca-fe70a4133d55 | Denominator Exclusion | 1 | 0 | mismatch |
| 8e648527-5b7e-430c-b5ca-fe70a4133d55 | Numerator | 0 | 1 | mismatch |
| 8e648527-5b7e-430c-b5ca-fe70a4133d55 | Denominator Exclusion | 1 | 0 | mismatch |
| 8e648527-5b7e-430c-b5ca-fe70a4133d55 | Numerator | 0 | 1 | mismatch |
| 8f713481-66ba-4a58-be92-91b8c7212959 | Denominator Exclusion | 1 | 0 | mismatch |
| 8f713481-66ba-4a58-be92-91b8c7212959 | Numerator | 0 | 1 | mismatch |
| 8f713481-66ba-4a58-be92-91b8c7212959 | Denominator Exclusion | 1 | 0 | mismatch |
| 8f713481-66ba-4a58-be92-91b8c7212959 | Denominator Exclusion | 1 | 0 | mismatch |
| 8f713481-66ba-4a58-be92-91b8c7212959 | Numerator | 0 | 1 | mismatch |
| a4ece596-2f97-4fbb-88e6-4418d8a7e713 | Denominator Exclusion | 1 | 0 | mismatch |
| a4ece596-2f97-4fbb-88e6-4418d8a7e713 | Numerator | 0 | 1 | mismatch |
| a4ece596-2f97-4fbb-88e6-4418d8a7e713 | Denominator Exclusion | 1 | 0 | mismatch |
| a4ece596-2f97-4fbb-88e6-4418d8a7e713 | Denominator Exclusion | 1 | 0 | mismatch |
| a4ece596-2f97-4fbb-88e6-4418d8a7e713 | Numerator | 0 | 1 | mismatch |
| a550fe5a-03ad-4eb3-9157-dcb64f8b13be | Denominator Exclusion | 1 | 0 | mismatch |
| a550fe5a-03ad-4eb3-9157-dcb64f8b13be | Denominator Exclusion | 1 | 0 | mismatch |
| a550fe5a-03ad-4eb3-9157-dcb64f8b13be | Numerator | 0 | 1 | mismatch |
| a550fe5a-03ad-4eb3-9157-dcb64f8b13be | Denominator Exclusion | 1 | 0 | mismatch |
| a550fe5a-03ad-4eb3-9157-dcb64f8b13be | Numerator | 0 | 1 | mismatch |
| a584af54-f1b9-4abc-b90b-1a2fa3b2016e | Denominator Exclusion | 1 | 0 | mismatch |
| a584af54-f1b9-4abc-b90b-1a2fa3b2016e | Numerator | 0 | 1 | mismatch |
| a584af54-f1b9-4abc-b90b-1a2fa3b2016e | Denominator Exclusion | 1 | 0 | mismatch |
| a584af54-f1b9-4abc-b90b-1a2fa3b2016e | Denominator Exclusion | 1 | 0 | mismatch |
| a584af54-f1b9-4abc-b90b-1a2fa3b2016e | Numerator | 0 | 1 | mismatch |
| a6b1d740-d580-4e55-970e-3cb4f1e369c2 | Denominator Exclusion | 1 | 0 | mismatch |
| a6b1d740-d580-4e55-970e-3cb4f1e369c2 | Denominator Exclusion | 1 | 0 | mismatch |
| a6b1d740-d580-4e55-970e-3cb4f1e369c2 | Numerator | 0 | 1 | mismatch |
| a6b1d740-d580-4e55-970e-3cb4f1e369c2 | Denominator Exclusion | 1 | 0 | mismatch |
| a6b1d740-d580-4e55-970e-3cb4f1e369c2 | Numerator | 0 | 1 | mismatch |
| a7b09e2e-cdb0-4206-986a-45bb70f9d49f | Denominator Exclusion | 1 | 0 | mismatch |
| a7b09e2e-cdb0-4206-986a-45bb70f9d49f | Denominator Exclusion | 1 | 0 | mismatch |
| a7b09e2e-cdb0-4206-986a-45bb70f9d49f | Denominator Exclusion | 1 | 0 | mismatch |
| aeef1eb1-86fa-4af0-b24d-fc7ad8398191 | Denominator Exclusion | 1 | 0 | mismatch |
| aeef1eb1-86fa-4af0-b24d-fc7ad8398191 | Denominator Exclusion | 1 | 0 | mismatch |
| aeef1eb1-86fa-4af0-b24d-fc7ad8398191 | Numerator | 0 | 1 | mismatch |
| aeef1eb1-86fa-4af0-b24d-fc7ad8398191 | Denominator Exclusion | 1 | 0 | mismatch |
| aeef1eb1-86fa-4af0-b24d-fc7ad8398191 | Numerator | 0 | 1 | mismatch |
| bc0146d2-5deb-46bc-b7a8-657d4f3ed031 | Denominator Exclusion | 1 | 0 | mismatch |
| bc0146d2-5deb-46bc-b7a8-657d4f3ed031 | Numerator | 0 | 1 | mismatch |
| bc0146d2-5deb-46bc-b7a8-657d4f3ed031 | Denominator Exclusion | 1 | 0 | mismatch |
| bc0146d2-5deb-46bc-b7a8-657d4f3ed031 | Denominator Exclusion | 1 | 0 | mismatch |
| bc0146d2-5deb-46bc-b7a8-657d4f3ed031 | Numerator | 0 | 1 | mismatch |
| c0af145d-bf0c-4b3d-8f65-d446c9f93b15 | Denominator Exclusion | 1 | 0 | mismatch |
| c0af145d-bf0c-4b3d-8f65-d446c9f93b15 | Numerator | 0 | 1 | mismatch |
| c0af145d-bf0c-4b3d-8f65-d446c9f93b15 | Denominator Exclusion | 1 | 0 | mismatch |
| c0af145d-bf0c-4b3d-8f65-d446c9f93b15 | Denominator Exclusion | 1 | 0 | mismatch |
| c0af145d-bf0c-4b3d-8f65-d446c9f93b15 | Numerator | 0 | 1 | mismatch |
| c409fbc9-a31f-4d53-9aa7-9e443e87812a | Denominator Exclusion | 1 | 0 | mismatch |
| c409fbc9-a31f-4d53-9aa7-9e443e87812a | Denominator Exclusion | 1 | 0 | mismatch |
| c409fbc9-a31f-4d53-9aa7-9e443e87812a | Denominator Exclusion | 1 | 0 | mismatch |
| c5c6788b-16f3-4c11-badf-5739989be2f6 | Denominator Exclusion | 1 | 0 | mismatch |
| c5c6788b-16f3-4c11-badf-5739989be2f6 | Denominator Exclusion | 1 | 0 | mismatch |
| c5c6788b-16f3-4c11-badf-5739989be2f6 | Denominator Exclusion | 1 | 0 | mismatch |
| c9aa1676-c1cd-4d98-aa1d-fe66762d4c73 | Denominator Exclusion | 1 | 0 | mismatch |
| c9aa1676-c1cd-4d98-aa1d-fe66762d4c73 | Denominator Exclusion | 1 | 0 | mismatch |
| c9aa1676-c1cd-4d98-aa1d-fe66762d4c73 | Denominator Exclusion | 1 | 0 | mismatch |
| cb01ddd0-a804-4bbe-8544-d4c753898eca | Denominator Exclusion | 1 | 0 | mismatch |
| cb01ddd0-a804-4bbe-8544-d4c753898eca | Denominator Exclusion | 1 | 0 | mismatch |
| cb01ddd0-a804-4bbe-8544-d4c753898eca | Numerator | 0 | 1 | mismatch |
| cb01ddd0-a804-4bbe-8544-d4c753898eca | Denominator Exclusion | 1 | 0 | mismatch |
| cb01ddd0-a804-4bbe-8544-d4c753898eca | Numerator | 0 | 1 | mismatch |
| d0e744f6-9951-4a29-99d9-8052efcde892 | Denominator Exclusion | 1 | 0 | mismatch |
| d0e744f6-9951-4a29-99d9-8052efcde892 | Denominator Exclusion | 1 | 0 | mismatch |
| d0e744f6-9951-4a29-99d9-8052efcde892 | Denominator Exclusion | 1 | 0 | mismatch |
| d641333e-031e-40e1-9552-11d4bbe7cd33 | Denominator Exclusion | 1 | 0 | mismatch |
| d641333e-031e-40e1-9552-11d4bbe7cd33 | Numerator | 0 | 1 | mismatch |
| d641333e-031e-40e1-9552-11d4bbe7cd33 | Denominator Exclusion | 1 | 0 | mismatch |
| d641333e-031e-40e1-9552-11d4bbe7cd33 | Denominator Exclusion | 1 | 0 | mismatch |
| d641333e-031e-40e1-9552-11d4bbe7cd33 | Numerator | 0 | 1 | mismatch |
| e00d1066-19b2-4d59-8829-d90f1e7a1233 | Denominator Exclusion | 1 | 0 | mismatch |
| e00d1066-19b2-4d59-8829-d90f1e7a1233 | Denominator Exclusion | 1 | 0 | mismatch |
| e00d1066-19b2-4d59-8829-d90f1e7a1233 | Numerator | 0 | 1 | mismatch |
| e00d1066-19b2-4d59-8829-d90f1e7a1233 | Denominator Exclusion | 1 | 0 | mismatch |
| e00d1066-19b2-4d59-8829-d90f1e7a1233 | Numerator | 0 | 1 | mismatch |
| e4cdfed0-16f0-46cd-a45c-95714744758b | Denominator Exclusion | 1 | 0 | mismatch |
| e4cdfed0-16f0-46cd-a45c-95714744758b | Numerator | 0 | 1 | mismatch |
| e4cdfed0-16f0-46cd-a45c-95714744758b | Denominator Exclusion | 1 | 0 | mismatch |
| e4cdfed0-16f0-46cd-a45c-95714744758b | Denominator Exclusion | 1 | 0 | mismatch |
| e4cdfed0-16f0-46cd-a45c-95714744758b | Numerator | 0 | 1 | mismatch |
| ea9af1dc-c26e-4bc3-947b-6c4bbd65523c | Denominator Exclusion | 1 | 0 | mismatch |
| ea9af1dc-c26e-4bc3-947b-6c4bbd65523c | Denominator Exclusion | 1 | 0 | mismatch |
| ea9af1dc-c26e-4bc3-947b-6c4bbd65523c | Denominator Exclusion | 1 | 0 | mismatch |
| edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4 | Denominator Exclusion | 1 | 0 | mismatch |
| edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4 | Numerator | 0 | 1 | mismatch |
| edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4 | Denominator Exclusion | 1 | 0 | mismatch |
| edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4 | Denominator Exclusion | 1 | 0 | mismatch |
| edf5b2e0-54b2-4ec6-a58d-0a2dcef456c4 | Numerator | 0 | 1 | mismatch |

### CMS165FHIRControllingHighBP

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 045f7e0b-bfb3-4ee0-a06d-83c853f6a81e | Denominator Exclusion | 1 | 0 | mismatch |
| 048a7212-c19c-4f9d-89e2-13727b23e585 | Denominator Exclusion | 1 | 0 | mismatch |
| 1905549a-1783-4195-95b9-b0879cb81d96 | Denominator Exclusion | 1 | 0 | mismatch |
| 23004f44-4848-4e62-8813-2a56d900613c | Denominator Exclusion | 1 | 0 | mismatch |
| 2c55811b-1571-43e5-919c-f90bf763b3d4 | Denominator Exclusion | 1 | 0 | mismatch |
| 32edbb16-2029-425a-85e0-6ea9182d1d91 | Denominator Exclusion | 1 | 0 | mismatch |
| 43efb820-9e6e-4180-9a4d-2d7459896e5f | Denominator Exclusion | 1 | 0 | mismatch |
| 474b2964-23a1-4c77-ad16-8a21543b2ed3 | Denominator Exclusion | 1 | 0 | mismatch |
| 4c814ca9-da50-43e3-9e31-dbe755ee5c5e | Denominator Exclusion | 1 | 0 | mismatch |
| 4d50f3eb-f56f-4f13-8fcf-4d26e05b9a6a | Denominator Exclusion | 1 | 0 | mismatch |
| 5421e420-8d42-4628-ba47-9abaf9ebfaa8 | Denominator Exclusion | 1 | 0 | mismatch |
| 546de5d8-f614-41c7-938f-671d14e4f540 | Denominator Exclusion | 1 | 0 | mismatch |
| 598f05e7-83b4-4609-9795-e9ac75f57f36 | Denominator Exclusion | 1 | 0 | mismatch |
| 59d7f239-7614-4e6e-a973-fe107aee5749 | Denominator Exclusion | 1 | 0 | mismatch |
| 6769ebe0-1b45-472a-ba7b-8f9a014d94a6 | Denominator Exclusion | 1 | 0 | mismatch |
| 6795a52e-1f83-480b-a2a7-b0d0922c0e5b | Denominator Exclusion | 1 | 0 | mismatch |
| 67ee2f03-89c1-4edb-b0fa-7e07effb4477 | Denominator Exclusion | 1 | 0 | mismatch |
| 686e2c47-b08f-465c-ab31-1712dd72028b | Denominator Exclusion | 1 | 0 | mismatch |
| 6885264d-efbf-4e48-99a2-2e8ce29d61ba | Denominator Exclusion | 1 | 0 | mismatch |
| 698b2574-1170-4438-8400-f3e1992a4807 | Denominator Exclusion | 1 | 0 | mismatch |
| 6d97c086-8776-45f4-898f-cece9e80990a | Denominator Exclusion | 1 | 0 | mismatch |
| 6f37e357-7575-4b40-a63e-4b882532250f | Denominator Exclusion | 1 | 0 | mismatch |
| 6f37e357-7575-4b40-a63e-4b882532250f | Numerator | 0 | 1 | mismatch |
| 75d880c8-4220-4907-b29a-f595dc0df2fb | Denominator Exclusion | 1 | 0 | mismatch |
| 7c59efb5-56ab-4a25-af83-bd81daeee026 | Denominator Exclusion | 1 | 0 | mismatch |
| 8e477157-81e8-4b7b-ba79-4a441a2a1109 | Denominator Exclusion | 1 | 0 | mismatch |
| 94d2a25e-9eec-44ce-bc34-711452549be8 | Denominator Exclusion | 1 | 0 | mismatch |
| 9f063f76-a97a-4bba-9f6a-35e7a429a72c | Denominator Exclusion | 1 | 0 | mismatch |
| a3deee90-5966-4309-b52f-c0a76046f680 | Denominator Exclusion | 1 | 0 | mismatch |
| a7ec972f-f0c1-428d-aba5-ba76cba5cd73 | Denominator Exclusion | 1 | 0 | mismatch |
| b378c30b-ebc2-4378-9a75-8a97711cac81 | Denominator Exclusion | 1 | 0 | mismatch |
| b84bdc08-62ae-4bce-857d-d2492e0c82fd | Denominator Exclusion | 1 | 0 | mismatch |
| bff7264b-35fc-402b-8a15-22c78e227064 | Denominator Exclusion | 1 | 0 | mismatch |
| c57b8e40-b3be-484f-8874-8ccafa3d5a38 | Denominator Exclusion | 1 | 0 | mismatch |
| d150409f-0616-4565-ba60-7ca732a87288 | Denominator Exclusion | 1 | 0 | mismatch |
| d513ed00-6ea1-4522-ae7c-c3bc29082e92 | Denominator Exclusion | 1 | 0 | mismatch |
| dbc8c8f1-3f10-4352-adbe-e0d4c12ade72 | Denominator Exclusion | 1 | 0 | mismatch |
| e94daaa3-ffff-4ca5-b971-7fd4407c3580 | Denominator Exclusion | 1 | 0 | mismatch |
| f2d1fd7e-35ae-45cd-86e6-8b874c3e3fb9 | Denominator Exclusion | 1 | 0 | mismatch |
| f2d1fd7e-35ae-45cd-86e6-8b874c3e3fb9 | Numerator | 0 | 1 | mismatch |

### CMS177FHIRChildMDDSuicideAssmt

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 85e6225c-a9bb-4338-a228-297564e38c4d | Denominator | 1 | 0 | mismatch |
| 85e6225c-a9bb-4338-a228-297564e38c4d | Initial Population | 1 | 0 | mismatch |

### CMS190FHIRVTEProphylaxisICU

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 208cb0f9-a6e9-4207-b6a4-3325fb463099 | Numerator | 0 | 1 | mismatch |
| 2e1ee160-9c41-4c6f-b368-56c074cfb592 | Denominator Exclusion | 1 | 0 | mismatch |
| 4c32b73b-abba-431b-a352-f0f454e7c9dd | Numerator | 0 | 1 | mismatch |
| 4fc421c7-e490-4d4e-a326-53d08635efb9 | Numerator | 0 | 1 | mismatch |
| 632831b0-1ebf-47b5-b439-3a124cd77c37 | Numerator | 0 | 1 | mismatch |
| 7e7f4563-a628-40ab-990b-ca0837313759 | Numerator | 0 | 1 | mismatch |
| 95a54d01-197e-48ef-bb48-d3d398aecbe8 | Numerator | 0 | 1 | mismatch |
| 9ddea16c-55d3-4dda-a1d8-a256fbff0b64 | Numerator | 0 | 1 | mismatch |
| a82cd0c1-900e-4ab3-a498-840ac1608486 | Denominator Exclusion | 1 | 0 | mismatch |
| a9c75661-be1c-41b2-aa15-222cc7d2ca81 | Numerator | 0 | 1 | mismatch |
| c0481b47-738b-4a09-8901-915ece2beb7e | Numerator | 0 | 1 | mismatch |
| d665c40d-2323-471f-9642-983472d2be7b | Denominator Exclusion | 1 | 0 | mismatch |
| e8931859-4ad8-49c8-9cdd-8697293456a2 | Numerator | 0 | 1 | mismatch |
| f00f3778-6ad1-466d-a3bd-bcbc63d62b55 | Numerator | 0 | 1 | mismatch |
| f035a977-30d0-487c-b542-a596e718420c | Numerator | 0 | 1 | mismatch |
| f82746cf-f6cd-4fcc-bc9e-7e569ae26211 | Numerator | 0 | 1 | mismatch |
| f859dd94-f201-4517-a368-32b98dd486c9 | Numerator | 0 | 1 | mismatch |

### CMS347FHIRStatinPreventionTxCVD

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 019bf4e8-68b3-476d-ac64-a4ba0aa368c5 | Denominator Exception | 1 | 0 | mismatch |
| 019bf4e8-68b3-476d-ac64-a4ba0aa368c5 | Denominator Exception | 1 | 0 | mismatch |
| 019bf4e8-68b3-476d-ac64-a4ba0aa368c5 | Denominator Exception | 1 | 0 | mismatch |
| 019bf4e8-68b3-476d-ac64-a4ba0aa368c5 | Denominator Exception | 1 | 0 | mismatch |
| 022c05d8-8337-4f1a-9d69-abb6500b1be5 | Denominator Exception | 1 | 0 | mismatch |
| 031e746c-9c2c-4eea-acca-a26c8862c9d5 | Denominator Exception | 1 | 0 | mismatch |
| 0438e6ec-b6c0-422d-b8c9-074e5f8d9af5 | Denominator Exception | 1 | 0 | mismatch |
| 0774a58a-2910-4da7-a48a-6613d418b5d1 | Denominator Exception | 1 | 0 | mismatch |
| 0774a58a-2910-4da7-a48a-6613d418b5d1 | Denominator Exception | 1 | 0 | mismatch |
| 0774a58a-2910-4da7-a48a-6613d418b5d1 | Denominator Exception | 1 | 0 | mismatch |
| 0774a58a-2910-4da7-a48a-6613d418b5d1 | Denominator Exception | 1 | 0 | mismatch |
| 078ef6a8-509f-4f36-98f3-977174636356 | Denominator Exception | 1 | 0 | mismatch |
| 078ef6a8-509f-4f36-98f3-977174636356 | Denominator Exception | 1 | 0 | mismatch |
| 078ef6a8-509f-4f36-98f3-977174636356 | Denominator Exception | 1 | 0 | mismatch |
| 08a2c605-1316-4d3c-b26e-2b40a28a2e44 | Denominator Exception | 1 | 0 | mismatch |
| 08a2c605-1316-4d3c-b26e-2b40a28a2e44 | Denominator Exception | 1 | 0 | mismatch |
| 08a2c605-1316-4d3c-b26e-2b40a28a2e44 | Denominator Exception | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 0 | 1 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Numerator | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 1 | 0 | mismatch |
| 08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a | Denominator Exception | 1 | 0 | mismatch |
| 08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a | Denominator Exception | 1 | 0 | mismatch |
| 08f8343e-9a9e-4ff5-b21c-1fb44e29aa9a | Denominator Exception | 1 | 0 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Denominator Exception | 0 | 1 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Numerator | 1 | 0 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Denominator Exception | 1 | 0 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Denominator Exception | 1 | 0 | mismatch |
| 0ba942ff-50d6-4123-ab21-adcf5fdff0df | Denominator Exception | 1 | 0 | mismatch |
| 0ce81150-5908-49a1-bef9-21406359af63 | Denominator Exception | 1 | 0 | mismatch |
| 0ce81150-5908-49a1-bef9-21406359af63 | Denominator Exception | 0 | 1 | mismatch |
| 0ce81150-5908-49a1-bef9-21406359af63 | Numerator | 1 | 0 | mismatch |
| 0ce81150-5908-49a1-bef9-21406359af63 | Denominator Exception | 1 | 0 | mismatch |
| 0ce81150-5908-49a1-bef9-21406359af63 | Denominator Exception | 1 | 0 | mismatch |
| 0e334f85-c298-401d-95ab-bad7ae13ced8 | Denominator Exception | 1 | 0 | mismatch |
| 0e334f85-c298-401d-95ab-bad7ae13ced8 | Denominator Exception | 1 | 0 | mismatch |
| 0e334f85-c298-401d-95ab-bad7ae13ced8 | Denominator Exception | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Initial Population | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Numerator | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator Exception | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator Exception | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator Exception | 1 | 0 | mismatch |
| 0fdfb3c8-c32a-48d7-877c-f5d8b6687d44 | Denominator Exception | 1 | 0 | mismatch |
| 1051c571-b7e4-48d1-8e77-02b1da164b73 | Denominator Exception | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 0 | 1 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Numerator | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 1 | 0 | mismatch |
| 117785fd-791b-4d9b-a5e7-436e39a62a6b | Denominator Exception | 1 | 0 | mismatch |
| 13d790be-84c6-438c-b571-842698654db7 | Denominator Exception | 1 | 0 | mismatch |
| 13d790be-84c6-438c-b571-842698654db7 | Denominator Exception | 1 | 0 | mismatch |
| 13d790be-84c6-438c-b571-842698654db7 | Denominator Exception | 1 | 0 | mismatch |
| 1ba7b147-b701-424c-bade-4e8270547030 | Denominator Exception | 1 | 0 | mismatch |
| 1ccceb3f-9a44-4dd3-88c6-f492965b87d5 | Denominator Exception | 1 | 0 | mismatch |
| 1ccceb3f-9a44-4dd3-88c6-f492965b87d5 | Denominator Exception | 1 | 0 | mismatch |
| 1ccceb3f-9a44-4dd3-88c6-f492965b87d5 | Denominator Exception | 1 | 0 | mismatch |
| 1d8a33ba-baaa-4b30-8c1a-ebace8d2d64e | Denominator Exception | 1 | 0 | mismatch |
| 1d8a33ba-baaa-4b30-8c1a-ebace8d2d64e | Denominator Exception | 1 | 0 | mismatch |
| 1d8a33ba-baaa-4b30-8c1a-ebace8d2d64e | Denominator Exception | 1 | 0 | mismatch |
| 20922873-db29-4914-a413-eed415e4504b | Denominator Exception | 1 | 0 | mismatch |
| 20922873-db29-4914-a413-eed415e4504b | Denominator Exception | 1 | 0 | mismatch |
| 20922873-db29-4914-a413-eed415e4504b | Denominator Exception | 1 | 0 | mismatch |
| 231a16e4-7d60-4e2c-943b-2f4c98994808 | Denominator Exception | 1 | 0 | mismatch |
| 231a16e4-7d60-4e2c-943b-2f4c98994808 | Denominator Exception | 1 | 0 | mismatch |
| 231a16e4-7d60-4e2c-943b-2f4c98994808 | Denominator Exception | 1 | 0 | mismatch |
| 24acf4a1-d67f-4584-9b6b-6c8025ffcc0a | Denominator Exception | 1 | 0 | mismatch |
| 24acf4a1-d67f-4584-9b6b-6c8025ffcc0a | Denominator Exception | 1 | 0 | mismatch |
| 24acf4a1-d67f-4584-9b6b-6c8025ffcc0a | Denominator Exception | 1 | 0 | mismatch |
| 2727681a-5857-4de1-a892-0cd4e531541c | Denominator Exception | 1 | 0 | mismatch |
| 285c85db-f879-4938-867f-daba78f08494 | Denominator Exception | 1 | 0 | mismatch |
| 285c85db-f879-4938-867f-daba78f08494 | Denominator Exception | 1 | 0 | mismatch |
| 285c85db-f879-4938-867f-daba78f08494 | Denominator Exception | 1 | 0 | mismatch |
| 2cff757c-4470-46a2-a685-6e23cf82c045 | Denominator Exception | 1 | 0 | mismatch |
| 2cff757c-4470-46a2-a685-6e23cf82c045 | Denominator Exception | 1 | 0 | mismatch |
| 2cff757c-4470-46a2-a685-6e23cf82c045 | Denominator Exception | 1 | 0 | mismatch |
| 2cff757c-4470-46a2-a685-6e23cf82c045 | Numerator | 1 | 0 | mismatch |
| 30b8f03a-668f-400e-b824-a74e6b6dd1dc | Denominator Exception | 1 | 0 | mismatch |
| 30b8f03a-668f-400e-b824-a74e6b6dd1dc | Denominator Exception | 1 | 0 | mismatch |
| 30b8f03a-668f-400e-b824-a74e6b6dd1dc | Denominator Exception | 1 | 0 | mismatch |
| 31841a30-decc-4b6b-80a8-1cb18275cb6b | Denominator Exception | 1 | 0 | mismatch |
| 31841a30-decc-4b6b-80a8-1cb18275cb6b | Denominator Exception | 1 | 0 | mismatch |
| 31841a30-decc-4b6b-80a8-1cb18275cb6b | Denominator Exception | 1 | 0 | mismatch |
| 35999af4-f52b-4e73-8f05-4bfca8dee7ec | Denominator Exception | 1 | 0 | mismatch |
| 35d9e119-50ef-4df1-b303-f348596657ad | Denominator Exception | 1 | 0 | mismatch |
| 35d9e119-50ef-4df1-b303-f348596657ad | Denominator Exception | 1 | 0 | mismatch |
| 35d9e119-50ef-4df1-b303-f348596657ad | Denominator Exception | 1 | 0 | mismatch |
| 36408f0f-58eb-47fe-8e64-1b98e47e5c36 | Denominator Exception | 1 | 0 | mismatch |
| 39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f | Denominator Exception | 1 | 0 | mismatch |
| 39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f | Denominator Exception | 1 | 0 | mismatch |
| 39d8c5d6-d3ed-4cfe-b62c-c8e57a45508f | Denominator Exception | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 0 | 1 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Numerator | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 1 | 0 | mismatch |
| 3dd27b30-058d-409a-84eb-252d40470597 | Denominator Exception | 1 | 0 | mismatch |
| 3e09af44-0445-4077-b73c-6896fdbe49c5 | Denominator | 1 | 0 | mismatch |
| 3e09af44-0445-4077-b73c-6896fdbe49c5 | Denominator Exception | 1 | 0 | mismatch |
| 3e09af44-0445-4077-b73c-6896fdbe49c5 | Initial Population | 1 | 0 | mismatch |
| 409116c1-3cd5-4f1f-8dd5-6b5646bbaff3 | Denominator Exception | 1 | 0 | mismatch |
| 40aa228f-ff55-4653-8bbe-125dc0fb5983 | Denominator Exception | 1 | 0 | mismatch |
| 4120512a-d0f4-4ffa-acd9-0191db3b7f46 | Denominator | 1 | 0 | mismatch |
| 4120512a-d0f4-4ffa-acd9-0191db3b7f46 | Denominator Exception | 1 | 0 | mismatch |
| 4120512a-d0f4-4ffa-acd9-0191db3b7f46 | Initial Population | 1 | 0 | mismatch |
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
| 4ea5e47c-48de-4f1f-a7bb-499753983f9b | Denominator Exception | 1 | 0 | mismatch |
| 4fe9e695-6348-44e7-af08-0e326c1420b7 | Denominator Exception | 1 | 0 | mismatch |
| 50c7b2fc-879b-4088-88bf-36a9f8c0baf0 | Denominator Exception | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator Exception | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Initial Population | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Numerator | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator Exception | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator Exception | 1 | 0 | mismatch |
| 5355d1bc-f8b4-4063-945a-0717e9530281 | Denominator Exception | 1 | 0 | mismatch |
| 5355d1bc-f8b4-4063-945a-0717e9530281 | Denominator Exception | 1 | 0 | mismatch |
| 5355d1bc-f8b4-4063-945a-0717e9530281 | Denominator Exception | 1 | 0 | mismatch |
| 537d14db-6ced-4cd2-9553-e88bd6551771 | Denominator | 1 | 0 | mismatch |
| 537d14db-6ced-4cd2-9553-e88bd6551771 | Denominator Exception | 1 | 0 | mismatch |
| 537d14db-6ced-4cd2-9553-e88bd6551771 | Initial Population | 1 | 0 | mismatch |
| 59715b85-2d66-4627-ad73-d91e5862cb5b | Denominator Exception | 1 | 0 | mismatch |
| 5976248c-c671-41e4-90df-b3367b1faefd | Denominator Exception | 1 | 0 | mismatch |
| 59d6bb14-b82e-4295-baf1-d96be73e1e38 | Denominator | 1 | 0 | mismatch |
| 59d6bb14-b82e-4295-baf1-d96be73e1e38 | Denominator Exception | 1 | 0 | mismatch |
| 59d6bb14-b82e-4295-baf1-d96be73e1e38 | Initial Population | 1 | 0 | mismatch |
| 5a086712-eccf-4041-9eb7-b25c0dcf2317 | Denominator Exception | 1 | 0 | mismatch |
| 5a086712-eccf-4041-9eb7-b25c0dcf2317 | Denominator Exception | 1 | 0 | mismatch |
| 5a086712-eccf-4041-9eb7-b25c0dcf2317 | Denominator Exception | 1 | 0 | mismatch |
| 5bbad8cc-56b9-4802-a5da-7de376a461f0 | Denominator Exception | 1 | 0 | mismatch |
| 5c70a969-ae6d-46ca-9a71-92e15292804d | Denominator Exception | 1 | 0 | mismatch |
| 5e65bf6d-6518-44d7-a827-821b59b00cc0 | Denominator Exception | 1 | 0 | mismatch |
| 6840a0da-456f-40f7-b939-aac2cdf5620d | Denominator Exception | 1 | 0 | mismatch |
| 6840a0da-456f-40f7-b939-aac2cdf5620d | Denominator Exception | 1 | 0 | mismatch |
| 6840a0da-456f-40f7-b939-aac2cdf5620d | Denominator Exception | 1 | 0 | mismatch |
| 694248de-4f73-4557-816b-f6a932f15793 | Denominator Exception | 1 | 0 | mismatch |
| 694248de-4f73-4557-816b-f6a932f15793 | Denominator Exception | 1 | 0 | mismatch |
| 694248de-4f73-4557-816b-f6a932f15793 | Denominator Exception | 1 | 0 | mismatch |
| 695b64d8-8102-4109-89c2-9ca128d43f4d | Denominator Exception | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 0 | 1 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Numerator | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 1 | 0 | mismatch |
| 72194a73-a0fe-4d50-8f07-0ad92320a467 | Denominator Exception | 1 | 0 | mismatch |
| 759a89b4-51ed-4622-adae-6b0930701ebb | Denominator | 1 | 0 | mismatch |
| 759a89b4-51ed-4622-adae-6b0930701ebb | Denominator Exception | 1 | 0 | mismatch |
| 759a89b4-51ed-4622-adae-6b0930701ebb | Initial Population | 1 | 0 | mismatch |
| 76ccc2ea-1cb2-4151-80bf-be8b14b1a074 | Denominator Exception | 1 | 0 | mismatch |
| 76ccc2ea-1cb2-4151-80bf-be8b14b1a074 | Denominator Exception | 1 | 0 | mismatch |
| 76ccc2ea-1cb2-4151-80bf-be8b14b1a074 | Denominator Exception | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 0 | 1 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Numerator | 1 | 0 | mismatch |
| 7b9268b7-2d3d-4a2b-822a-e1f470593fdf | Denominator Exception | 1 | 0 | mismatch |
| 7b9268b7-2d3d-4a2b-822a-e1f470593fdf | Denominator Exception | 1 | 0 | mismatch |
| 7b9268b7-2d3d-4a2b-822a-e1f470593fdf | Denominator Exception | 1 | 0 | mismatch |
| 7bc28f33-e1e6-4122-8a38-e9c36685a6ba | Denominator Exception | 1 | 0 | mismatch |
| 87b32275-37d7-4adf-afa4-8a4518964de0 | Denominator Exception | 1 | 0 | mismatch |
| 88dc444e-3a42-4d5b-a757-62a5013cd131 | Denominator Exception | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 0 | 1 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Numerator | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 1 | 0 | mismatch |
| 93aea3e2-4736-4be0-830f-54c1ef6df6d5 | Denominator | 1 | 0 | mismatch |
| 93aea3e2-4736-4be0-830f-54c1ef6df6d5 | Denominator Exception | 1 | 0 | mismatch |
| 93aea3e2-4736-4be0-830f-54c1ef6df6d5 | Initial Population | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 0 | 1 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Numerator | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 0 | 1 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Numerator | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 0 | 1 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Numerator | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 1 | 0 | mismatch |
| a0202aaf-756f-4d08-8329-8fd585ddda63 | Denominator | 1 | 0 | mismatch |
| a0202aaf-756f-4d08-8329-8fd585ddda63 | Denominator Exception | 1 | 0 | mismatch |
| a0202aaf-756f-4d08-8329-8fd585ddda63 | Initial Population | 1 | 0 | mismatch |
| a03e2988-3bed-4fc5-b1e7-70eac99f0612 | Denominator | 1 | 0 | mismatch |
| a03e2988-3bed-4fc5-b1e7-70eac99f0612 | Denominator Exception | 1 | 0 | mismatch |
| a03e2988-3bed-4fc5-b1e7-70eac99f0612 | Initial Population | 1 | 0 | mismatch |
| b2705cfc-a0d5-4fb4-908b-89d00a51cc06 | Denominator Exception | 1 | 0 | mismatch |
| b2705cfc-a0d5-4fb4-908b-89d00a51cc06 | Denominator Exception | 1 | 0 | mismatch |
| b2705cfc-a0d5-4fb4-908b-89d00a51cc06 | Denominator Exception | 1 | 0 | mismatch |
| b708e603-c09f-4798-9631-4603653c1380 | Denominator Exception | 1 | 0 | mismatch |
| b8893156-afda-4685-9d5e-06d2113f1409 | Denominator Exception | 1 | 0 | mismatch |
| bb80a309-08ab-4d5d-b863-111ae594d65d | Denominator Exception | 1 | 0 | mismatch |
| bfb8c317-cc95-41cc-9d3d-e1e66dd5b168 | Denominator Exception | 1 | 0 | mismatch |
| c00d7354-2160-48f4-a251-1fcf892d1b42 | Denominator Exception | 1 | 0 | mismatch |
| c00d7354-2160-48f4-a251-1fcf892d1b42 | Denominator Exception | 1 | 0 | mismatch |
| c00d7354-2160-48f4-a251-1fcf892d1b42 | Denominator Exception | 1 | 0 | mismatch |
| c686053c-d4b7-45b7-9ebb-19080a24f031 | Denominator Exception | 1 | 0 | mismatch |
| c77c84ce-f0a9-4949-a8d7-4413565db083 | Denominator Exception | 1 | 0 | mismatch |
| ca949c24-f283-493e-a697-426eaec3e9f1 | Denominator Exception | 1 | 0 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Denominator Exception | 0 | 1 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Numerator | 1 | 0 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Denominator Exception | 1 | 0 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Denominator Exception | 1 | 0 | mismatch |
| cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 | Denominator Exception | 1 | 0 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Denominator Exception | 0 | 1 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Numerator | 1 | 0 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Denominator Exception | 1 | 0 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Denominator Exception | 1 | 0 | mismatch |
| d06256e5-091f-445e-898f-b8c31d8d3772 | Denominator Exception | 1 | 0 | mismatch |
| d2c7d463-775a-4c8d-bcb0-35ea689b2d20 | Denominator Exception | 1 | 0 | mismatch |
| d2c7d463-775a-4c8d-bcb0-35ea689b2d20 | Denominator Exception | 1 | 0 | mismatch |
| d2c7d463-775a-4c8d-bcb0-35ea689b2d20 | Denominator Exception | 1 | 0 | mismatch |
| d2c7d463-775a-4c8d-bcb0-35ea689b2d20 | Denominator Exception | 0 | 1 | mismatch |
| d2c7d463-775a-4c8d-bcb0-35ea689b2d20 | Numerator | 1 | 0 | mismatch |
| d5c55655-2c12-4300-9ee1-31044497d665 | Denominator Exception | 1 | 0 | mismatch |
| d9d151d1-9bd3-40ce-a2c1-cb8a985328fc | Denominator Exception | 1 | 0 | mismatch |
| d9f94b3d-5bba-4965-8364-1d7c87957c3e | Denominator Exception | 1 | 0 | mismatch |
| da5f94c9-9d0c-42ea-ab7f-dd3a92a04ceb | Denominator Exception | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 0 | 1 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Numerator | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 1 | 0 | mismatch |
| df05b853-3e6d-4a12-b1db-fd9d0ec790a2 | Denominator | 1 | 0 | mismatch |
| df05b853-3e6d-4a12-b1db-fd9d0ec790a2 | Denominator Exception | 1 | 0 | mismatch |
| df05b853-3e6d-4a12-b1db-fd9d0ec790a2 | Initial Population | 1 | 0 | mismatch |
| e0813324-b2e0-4138-99f4-696f03c3db30 | Denominator Exception | 1 | 0 | mismatch |
| e0813324-b2e0-4138-99f4-696f03c3db30 | Denominator Exception | 1 | 0 | mismatch |
| e0813324-b2e0-4138-99f4-696f03c3db30 | Denominator Exception | 1 | 0 | mismatch |
| e20a62fd-329e-44d7-8767-1951f9392396 | Denominator Exception | 1 | 0 | mismatch |
| e2edb18a-fb70-43cc-b680-6f933af7d182 | Denominator Exception | 1 | 0 | mismatch |
| e55d9fc4-44e6-4f00-bf53-b82a5b646222 | Denominator Exception | 1 | 0 | mismatch |
| e55d9fc4-44e6-4f00-bf53-b82a5b646222 | Denominator Exception | 1 | 0 | mismatch |
| e55d9fc4-44e6-4f00-bf53-b82a5b646222 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 0 | 1 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Numerator | 1 | 0 | mismatch |
| e8e584cf-df78-4932-bc9a-66ac5af10a47 | Denominator Exception | 1 | 0 | mismatch |
| ef3f90d1-4954-40bd-b230-e44ffa98ed29 | Denominator Exception | 1 | 0 | mismatch |
| ef3f90d1-4954-40bd-b230-e44ffa98ed29 | Denominator Exception | 1 | 0 | mismatch |
| ef3f90d1-4954-40bd-b230-e44ffa98ed29 | Denominator Exception | 1 | 0 | mismatch |
| f101bf69-38b2-4c86-9978-727c665dfb31 | Denominator Exception | 1 | 0 | mismatch |
| f3b17514-f40d-43f9-baa9-a0418142ca98 | Denominator Exception | 1 | 0 | mismatch |
| f72b9ae4-40e0-4f28-a5bd-14f09ed84e75 | Denominator Exception | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 0 | 1 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Numerator | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 1 | 0 | mismatch |
| f925afe3-4a77-404d-ba92-e78740f37d15 | Denominator Exception | 1 | 0 | mismatch |
| f925afe3-4a77-404d-ba92-e78740f37d15 | Denominator Exception | 1 | 0 | mismatch |
| f925afe3-4a77-404d-ba92-e78740f37d15 | Denominator Exception | 1 | 0 | mismatch |
| f9a03175-0a16-4c4a-97d5-f7b38e359526 | Denominator Exception | 1 | 0 | mismatch |
| f9a03175-0a16-4c4a-97d5-f7b38e359526 | Denominator Exception | 1 | 0 | mismatch |
| f9a03175-0a16-4c4a-97d5-f7b38e359526 | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 0 | 1 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Numerator | 1 | 0 | mismatch |
| fc82f4cb-7c62-41bd-9779-dd0f2e6e437f | Denominator Exception | 1 | 0 | mismatch |

### CMS645FHIRBoneDensityPCADTherapy

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 8c41481d-f89e-4113-ba12-df7c53e93d80 | Denominator Exception | 0 | 1 | mismatch |
| 8c41481d-f89e-4113-ba12-df7c53e93d80 | Numerator | 1 | 0 | mismatch |
| c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6 | Denominator Exception | 0 | 1 | mismatch |
| c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6 | Numerator | 1 | 0 | mismatch |
| d07cf359-d46c-4adf-b2d4-e02a2f43b78e | Numerator | 1 | 0 | mismatch |

### CMS646FHIRIntravesicalBCGTherapy

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| e648fa70-0532-49b0-92f6-dfb5a6d28d94 | Denominator Exception | 0 | 1 | mismatch |
| 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 | Denominator | — | — | qicore-only |
| 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 | Denominator Exception | — | — | qicore-only |
| 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 | Denominator Exclusion | — | — | qicore-only |
| 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 | Initial Population | — | — | qicore-only |
| 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 | Numerator | — | — | qicore-only |

### CMS771FHIRUrinarySymptomScoreBPH

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 051c5977-9f2c-4e8b-8e02-ac3ec0c718d6 | Denominator | 0 | 1 | mismatch |
| 3ab3ac1d-9b5e-4087-8862-dcb2562fb90f | Denominator | 0 | 1 | mismatch |
| 4c234ec0-3f89-4d55-b767-219d1130f634 | Numerator | 0 | 1 | mismatch |
| 9be591a0-517b-4be2-b652-a29be0c75c15 | Numerator | 0 | 1 | mismatch |
| bc79e5bc-237e-44be-b5fc-c5c4efb50286 | Numerator | 0 | 1 | mismatch |
| bf0f8968-c2c0-4416-88db-11ea3e3da968 | Numerator | 0 | 1 | mismatch |
| e90d90a7-3071-44de-8089-ad7b6f5f3e5d | Numerator | 0 | 1 | mismatch |

### CMS871FHIRHHHyper

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 113a6e72-7049-4a7f-90cf-5ec3435b0dee | Denominator | — | — | cms-only |
| 113a6e72-7049-4a7f-90cf-5ec3435b0dee | Denominator Exclusion | — | — | cms-only |
| 113a6e72-7049-4a7f-90cf-5ec3435b0dee | Denominator Observation | — | — | cms-only |
| 113a6e72-7049-4a7f-90cf-5ec3435b0dee | Initial Population | — | — | cms-only |
| 113a6e72-7049-4a7f-90cf-5ec3435b0dee | Numerator | — | — | cms-only |
| 113a6e72-7049-4a7f-90cf-5ec3435b0dee | Numerator Exclusion | — | — | cms-only |
| 113a6e72-7049-4a7f-90cf-5ec3435b0dee | Numerator Observation | — | — | cms-only |

### CMS951FHIRKidneyHealthEval

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0ce4362d-60f0-41af-8d47-c61f76d025a4 | Denominator Exclusion | 1 | 0 | mismatch |
| 1d012d11-4b38-4bdc-bd27-e7d8bcc88c89 | Denominator Exclusion | 1 | 0 | mismatch |
| 1d012d11-4b38-4bdc-bd27-e7d8bcc88c89 | Numerator | 0 | 1 | mismatch |
| 51e9e9aa-edcc-46f4-8472-24f377014ad4 | Denominator Exclusion | 1 | 0 | mismatch |
| 52988c36-5e85-4818-9baa-983a3e27281a | Denominator Exclusion | 1 | 0 | mismatch |
| 52988c36-5e85-4818-9baa-983a3e27281a | Numerator | 0 | 1 | mismatch |
| 56063388-7942-4a1d-8568-2d805d31ad30 | Denominator Exclusion | 1 | 0 | mismatch |
| 5aa9e5eb-adeb-4779-a4d3-5b731411e141 | Denominator Exclusion | 1 | 0 | mismatch |
| 7e7c41ee-7704-419c-937b-72d10c76f99a | Denominator Exclusion | 1 | 0 | mismatch |
| 7e7c41ee-7704-419c-937b-72d10c76f99a | Numerator | 0 | 1 | mismatch |
| 8ca88661-f12a-4b24-98e8-93183e8e2472 | Denominator Exclusion | 1 | 0 | mismatch |
| 8cfb2747-a46d-4348-9e21-5ef3417e524a | Denominator Exclusion | 1 | 0 | mismatch |
| 8e10675e-b991-4327-9514-6feb9d385b7f | Denominator Exclusion | 1 | 0 | mismatch |
| 8e10675e-b991-4327-9514-6feb9d385b7f | Numerator | 0 | 1 | mismatch |
| 94f26954-f280-4596-8bd3-e77ca79c1f41 | Denominator Exclusion | 1 | 0 | mismatch |
| 94f26954-f280-4596-8bd3-e77ca79c1f41 | Numerator | 0 | 1 | mismatch |
| 95ee3081-b973-4bd2-8b86-5b46bd664905 | Denominator Exclusion | 1 | 0 | mismatch |
| 95ee3081-b973-4bd2-8b86-5b46bd664905 | Numerator | 0 | 1 | mismatch |
| 9821f4e3-39db-4f45-8da3-eed161841bd2 | Denominator Exclusion | 1 | 0 | mismatch |
| 9821f4e3-39db-4f45-8da3-eed161841bd2 | Numerator | 0 | 1 | mismatch |
| 9f3b1077-d99c-4714-a88d-8aecc667fe57 | Denominator Exclusion | 1 | 0 | mismatch |
| 9f3b1077-d99c-4714-a88d-8aecc667fe57 | Numerator | 0 | 1 | mismatch |
| a7284289-8784-48d9-a342-7d851085efb7 | Denominator Exclusion | 1 | 0 | mismatch |
| a9536c98-3157-4443-bfe1-ef4e585360be | Denominator Exclusion | 1 | 0 | mismatch |
| ac7a62b6-a440-4d4c-849d-0ce05743109c | Denominator Exclusion | 1 | 0 | mismatch |
| ac7a62b6-a440-4d4c-849d-0ce05743109c | Numerator | 0 | 1 | mismatch |
| ae52c591-1a71-4090-aeeb-2dd758f63ce4 | Denominator Exclusion | 1 | 0 | mismatch |
| c13a82b6-fb44-4fc7-befd-d762b9fafa97 | Denominator Exclusion | 1 | 0 | mismatch |
| d4340928-bbc6-4c24-8888-9f12e5cbefad | Denominator Exclusion | 1 | 0 | mismatch |
| d4a593b2-d485-4bfa-a8b1-a401bdbf8d23 | Denominator Exclusion | 1 | 0 | mismatch |
| ed17f9e5-1200-49e3-a4fc-1c188d8932dc | Denominator Exclusion | 1 | 0 | mismatch |
| f4d1182a-1c06-4c62-a0be-1f994c4343b3 | Denominator Exclusion | 1 | 0 | mismatch |
| f8c48a84-406c-44b7-b79e-b7a5f9d15b31 | Denominator Exclusion | 1 | 0 | mismatch |

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
| 8bb7c40b-7447-42ca-b662-161a7026ed8f | Denominator Exception | 0 | 1 | mismatch |
| ccc7deaf-98b7-4dad-b190-8fee10f2cf77 | Denominator Exception | 0 | 1 | mismatch |
| f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847 | Denominator Exclusion | 1 | 0 | mismatch |

### CMS1028FHIRPCSevereOBComps

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 4911c0c6-22e1-45ad-b39d-7e4d88c200d8 | Numerator | 1 | 0 | mismatch |
| 4911c0c6-22e1-45ad-b39d-7e4d88c200d8 | Numerator | 1 | 0 | mismatch |

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

