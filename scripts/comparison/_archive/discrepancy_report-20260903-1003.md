# Discrepancy Report
| Details | Value |
| --- | --- |
| Generated | 2026-09-03 10:03:45.851487 |
| Total Measures | 74 |
| Total Test Cases | 3964 |
| Measures with Discrepancies | 53 |
| Known Issues (resolution pending) | 11 issues / 4 test cases |
| Pass Count (all) | 22717 (95.76%) |
| Fail Count (all) | 1005 (4.24%) |
| Pass Count (excl. resolution-pending) | 22717 (95.84%) |
| Fail Count (excl. resolution-pending) | 986 (4.16%) |
| QICore Pass Count | 0 (0.00%) |
| QICore Fail Count | 23722 (100.00%) |
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
| Mismatched Test Cases | 51 | 750 |



_Note: Measures can have multiple discrepancies, so the Measures with Discrepancies count may not match the summary counts._
## CMS vs QICore Comparison

| Measure | CMS Pass / Fail | QICore Pass / Fail | Notes |
|---|:---:|:---:|---|
| CMS2FHIRPCSDepScreenAndFollowUp | 172 / 8 | 0 / 180 | Both have discrepancies |
| CMS22FHIRPCSBPScreeningFollowUp | 206 / 14 | 0 / 220 | CMS has discrepancies, QICore passes |
| CMS50FHIRReceiptofSpecialistReport | 99 / 0 | 0 / 99 | Match — both pass |
| CMS56FHIRFuncStatHipReplacement | 222 / 10 | 0 / 232 | CMS has discrepancies, QICore passes |
| CMS68FHIRDocumentationCurrentMeds | 72 / 4 | 0 / 76 | CMS has discrepancies, QICore passes |
| CMS69FHIRPCSBMIScreenAndFollowUp | 282 / 33 | 0 / 315 | CMS has discrepancies, QICore passes |
| CMS71FHIRSTKAnticoagAFFlutter | 399 / 16 | 0 / 415 | Both have discrepancies |
| CMS72FHIRSTKAntithromboticDay2 | 768 / 22 | 0 / 790 | Both have discrepancies |
| CMS74FHIRDentalCariesPrevention | 73 / 7 | 0 / 80 | CMS has discrepancies, QICore passes |
| CMS75FHIRChildrenDentalDecay | 73 / 7 | 0 / 80 | CMS has discrepancies, QICore passes |
| CMS90FHIRFSAforHeartFailure | 140 / 8 | 0 / 148 | CMS has discrepancies, QICore passes |
| CMS104FHIRSTKDCAntithrombotic | 372 / 38 | 0 / 410 | Both have discrepancies |
| CMS108FHIRVTEProphylaxis | 536 / 24 | 0 / 560 | Both have discrepancies |
| CMS117FHIRChildImmunStatus | 172 / 8 | 0 / 180 | CMS has discrepancies, QICore passes |
| CMS122FHIRDiabetesAssessGT9Pct | 170 / 50 | 0 / 220 | Both have discrepancies |
| CMS124FHIRCervicalCancerScreen | 123 / 13 | 0 / 136 | CMS has discrepancies, QICore passes |
| CMS125FHIRBreastCancerScreen | 238 / 26 | 0 / 264 | Both have discrepancies |
| CMS128FHIRAntidepressantMgmt | 216 / 16 | 0 / 232 | Both have discrepancies |
| CMS129FHIRProstCaBoneScanUse | 204 / 0 | 0 / 204 | Match — both pass |
| CMS130FHIRColorectalCancerScrn | 239 / 17 | 0 / 256 | Both have discrepancies |
| CMS131FHIRDiabetesEyeExam | 228 / 24 | 0 / 252 | Both have discrepancies |
| CMS133FHIRCataracts2040BCVA90Days | 292 / 0 | 0 / 292 | Match — both pass |
| CMS135FHIRACEIorARBorARNIforHF | 170 / 30 | 0 / 200 | Both have discrepancies |
| CMS136FHIRChildADHDMedFollowUp | 474 / 38 | 0 / 512 | Both have discrepancies |
| CMS137FHIRSUDTxInitEngagement | 342 / 18 | 0 / 360 | CMS has discrepancies, QICore passes |
| CMS138FHIRTobaccoScrnCessation | 512 / 52 | 0 / 564 | CMS has discrepancies, QICore passes |
| CMS139FHIRFallRiskScreening | 108 / 8 | 0 / 116 | CMS has discrepancies, QICore passes |
| CMS142FHIRCommWithDrManagingDiab | 123 / 5 | 0 / 128 | Both have discrepancies |
| CMS143FHIRPOAGOpticNerveEval | 128 / 0 | 0 / 128 | Match — both pass |
| CMS144FHIRHFBetaBlockerForLVSD | 235 / 5 | 0 / 240 | CMS has discrepancies, QICore passes |
| CMS145FHIRCADBBlockerTPMIorLVSD | 418 / 6 | 0 / 424 | Both have discrepancies |
| CMS146FHIRApproTestPharyngitis | 142 / 10 | 0 / 152 | CMS has discrepancies, QICore passes |
| CMS149FHIRDementiaCognitiveAssess | 132 / 0 | 0 / 132 | Match — both pass |
| CMS153FHIRChlamydiaScreening | 120 / 8 | 0 / 128 | Both have discrepancies |
| CMS154FHIRAppropriateTxforURI | 116 / 16 | 0 / 132 | CMS has discrepancies, QICore passes |
| CMS155FHIRWgtAssessCounseling | 381 / 27 | 0 / 408 | CMS has discrepancies, QICore passes |
| CMS156FHIRHighRiskMedsElderly | 667 / 41 | 0 / 708 | Both have discrepancies |
| CMS157FHIRPainIntensityQuantified | 332 / 46 | 0 / 378 | Both have discrepancies |
| CMS159FHIRDepRemissionat12Months | 264 / 4 | 0 / 268 | Both have discrepancies |
| CMS165FHIRControllingHighBP | 239 / 33 | 0 / 272 | Both have discrepancies |
| CMS177FHIRChildMDDSuicideAssmt | 121 / 2 | 0 / 123 | CMS has discrepancies, QICore passes |
| CMS190FHIRVTEProphylaxisICU | 600 / 25 | 0 / 625 | Both have discrepancies |
| CMS314FHIRHIVViralSuppression | 129 / 0 | 0 / 129 | Match — both pass |
| CMS0334FHIRPCCesareanBirth | 550 / 2 | 0 / 552 | Both have discrepancies |
| CMS347FHIRStatinPreventionTxCVD | 3581 / 179 | 0 / 3760 | Both have discrepancies |
| CMS349FHIRHIVScreening | 180 / 0 | 0 / 180 | Match — both pass |
| CMS506FHIRSafeUseofOpioids | 204 / 0 | 0 / 204 | Match — both pass |
| CMSFHIR529HybridHospitalWideReadmission | 1 / 0 | 0 / 1 | Match — both pass |
| CMS645FHIRBoneDensityPCADTherapy | 199 / 5 | 0 / 204 | CMS has discrepancies, QICore passes |
| CMS646FHIRIntravesicalBCGTherapy | 182 / 8 | 0 / 190 | Both have discrepancies |
| CMS771FHIRUrinarySymptomScoreBPH | 117 / 7 | 0 / 124 | CMS has discrepancies, QICore passes |
| CMS816FHIRHHHypo | 57 / 27 | 0 / 84 | Both have discrepancies |
| CMS819FHIRHHORAE | 81 / 3 | 0 / 84 | Both have discrepancies |
| CMS826FHIRHHPI | 36 / 0 | 0 / 36 | Match — both pass |
| CMS832FHIRHHAKI | 148 / 0 | 0 / 148 | Match — both pass |
| CMSFHIR844HybridHospitalWideMortality | 8 / 2 | 0 / 10 | Both have discrepancies |
| CMS871FHIRHHHyper | 110 / 20 | 0 / 130 | Both have discrepancies |
| CMS951FHIRKidneyHealthEval | 207 / 13 | 0 / 220 | CMS has discrepancies, QICore passes |
| CMS986FHIRMalnutritionScore | 2622 / 6 | 0 / 2628 | CMS has discrepancies, QICore passes |
| CMS996FHIRAptTxforSTEMI | 563 / 7 | 0 / 570 | Both have discrepancies |
| CMS1017FHIRHHFI | 323 / 2 | 0 / 325 | Both have discrepancies |
| CMS1028FHIRPCSevereOBComps | 1126 / 2 | 0 / 1128 | Both have discrepancies |
| CMS1056FHIRCTClinical | 40 / 0 | 0 / 40 | Match — both pass |
| CMS1074FHIRCTIQR | 40 / 0 | 0 / 40 | Match — both pass |
| CMS1154ScreeningPrediabetesFHIR | 39 / 1 | 0 / 40 | Both have discrepancies |
| CMS1157FHIRHIVRetention | 81 / 0 | 0 / 81 | Match — both pass |
| CMS1173FHIRDiagnosticDelayVTE | 260 / 0 | 0 / 260 | Match — both pass |
| CMS1188FHIRHIVSTITesting | 102 / 0 | 0 / 102 | Match — both pass |
| CMS1206FHIRCTOQR | 40 / 0 | 0 / 40 | Match — both pass |
| CMS1218FHIRHHRF | 274 / 2 | 0 / 276 | Both have discrepancies |
| CMS1244FHIRECATHOQR | 216 / 0 | 0 / 216 | Match — both pass |
| CMS1264FHIRECATREHQR | 174 / 0 | 0 / 174 | CMS passes, QICore has discrepancies |
| NHSNAcuteCareHospitalMonthlyInitialPopulation1 | 27 / 0 | 0 / 27 | CMS passes, QICore has discrepancies |
| NHSNGlycemicControlHypoglycemiaInitialPopulation | 80 / 0 | 0 / 80 | CMS passes, QICore has discrepancies |


## Measures with No Discrepancies

### CMS Measures (21)
- CMS50FHIRReceiptofSpecialistReport [ [cql] ](../../input/cql/CMS50FHIRReceiptofSpecialistReport.cql) [ [test results] ](../../input/tests/results/CMS50FHIRReceiptofSpecialistReport.txt) — matches QICore
- CMS129FHIRProstCaBoneScanUse [ [cql] ](../../input/cql/CMS129FHIRProstCaBoneScanUse.cql) [ [test results] ](../../input/tests/results/CMS129FHIRProstCaBoneScanUse.txt) — matches QICore
- CMS133FHIRCataracts2040BCVA90Days [ [cql] ](../../input/cql/CMS133FHIRCataracts2040BCVA90Days.cql) [ [test results] ](../../input/tests/results/CMS133FHIRCataracts2040BCVA90Days.txt) — matches QICore
- CMS143FHIRPOAGOpticNerveEval [ [cql] ](../../input/cql/CMS143FHIRPOAGOpticNerveEval.cql) [ [test results] ](../../input/tests/results/CMS143FHIRPOAGOpticNerveEval.txt) — matches QICore
- CMS149FHIRDementiaCognitiveAssess [ [cql] ](../../input/cql/CMS149FHIRDementiaCognitiveAssess.cql) [ [test results] ](../../input/tests/results/CMS149FHIRDementiaCognitiveAssess.txt) — matches QICore
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
- CMS1264FHIRECATREHQR [ [cql] ](../../input/cql/CMS1264FHIRECATREHQR.cql) [ [test results] ](../../input/tests/results/CMS1264FHIRECATREHQR.txt) — QICore has discrepancies
- NHSNAcuteCareHospitalMonthlyInitialPopulation1 [ [cql] ](../../input/cql/NHSNAcuteCareHospitalMonthlyInitialPopulation1.cql) [ [test results] ](../../input/tests/results/NHSNAcuteCareHospitalMonthlyInitialPopulation1.txt) — QICore has discrepancies
- NHSNGlycemicControlHypoglycemiaInitialPopulation [ [cql] ](../../input/cql/NHSNGlycemicControlHypoglycemiaInitialPopulation.cql) [ [test results] ](../../input/tests/results/NHSNGlycemicControlHypoglycemiaInitialPopulation.txt) — QICore has discrepancies

### QICore Measures (39)
- CMS22FHIRPCSBPScreeningFollowUp [ [cql] ](../../input/cql/CMS22FHIRPCSBPScreeningFollowUp.cql) [ [test results] ](../../input/tests/results/CMS22FHIRPCSBPScreeningFollowUp.txt) — CMS has discrepancies
- CMS50FHIRReceiptofSpecialistReport [ [cql] ](../../input/cql/CMS50FHIRReceiptofSpecialistReport.cql) [ [test results] ](../../input/tests/results/CMS50FHIRReceiptofSpecialistReport.txt) — also passes in CMS
- CMS56FHIRFuncStatHipReplacement [ [cql] ](../../input/cql/CMS56FHIRFuncStatHipReplacement.cql) [ [test results] ](../../input/tests/results/CMS56FHIRFuncStatHipReplacement.txt) — CMS has discrepancies
- CMS68FHIRDocumentationCurrentMeds [ [cql] ](../../input/cql/CMS68FHIRDocumentationCurrentMeds.cql) [ [test results] ](../../input/tests/results/CMS68FHIRDocumentationCurrentMeds.txt) — CMS has discrepancies
- CMS69FHIRPCSBMIScreenAndFollowUp [ [cql] ](../../input/cql/CMS69FHIRPCSBMIScreenAndFollowUp.cql) [ [test results] ](../../input/tests/results/CMS69FHIRPCSBMIScreenAndFollowUp.txt) — CMS has discrepancies
- CMS74FHIRDentalCariesPrevention [ [cql] ](../../input/cql/CMS74FHIRDentalCariesPrevention.cql) [ [test results] ](../../input/tests/results/CMS74FHIRDentalCariesPrevention.txt) — CMS has discrepancies
- CMS75FHIRChildrenDentalDecay [ [cql] ](../../input/cql/CMS75FHIRChildrenDentalDecay.cql) [ [test results] ](../../input/tests/results/CMS75FHIRChildrenDentalDecay.txt) — CMS has discrepancies
- CMS90FHIRFSAforHeartFailure [ [cql] ](../../input/cql/CMS90FHIRFSAforHeartFailure.cql) [ [test results] ](../../input/tests/results/CMS90FHIRFSAforHeartFailure.txt) — CMS has discrepancies
- CMS117FHIRChildImmunStatus [ [cql] ](../../input/cql/CMS117FHIRChildImmunStatus.cql) [ [test results] ](../../input/tests/results/CMS117FHIRChildImmunStatus.txt) — CMS has discrepancies
- CMS124FHIRCervicalCancerScreen [ [cql] ](../../input/cql/CMS124FHIRCervicalCancerScreen.cql) [ [test results] ](../../input/tests/results/CMS124FHIRCervicalCancerScreen.txt) — CMS has discrepancies
- CMS129FHIRProstCaBoneScanUse [ [cql] ](../../input/cql/CMS129FHIRProstCaBoneScanUse.cql) [ [test results] ](../../input/tests/results/CMS129FHIRProstCaBoneScanUse.txt) — also passes in CMS
- CMS133FHIRCataracts2040BCVA90Days [ [cql] ](../../input/cql/CMS133FHIRCataracts2040BCVA90Days.cql) [ [test results] ](../../input/tests/results/CMS133FHIRCataracts2040BCVA90Days.txt) — also passes in CMS
- CMS137FHIRSUDTxInitEngagement [ [cql] ](../../input/cql/CMS137FHIRSUDTxInitEngagement.cql) [ [test results] ](../../input/tests/results/CMS137FHIRSUDTxInitEngagement.txt) — CMS has discrepancies
- CMS138FHIRTobaccoScrnCessation [ [cql] ](../../input/cql/CMS138FHIRTobaccoScrnCessation.cql) [ [test results] ](../../input/tests/results/CMS138FHIRTobaccoScrnCessation.txt) — CMS has discrepancies
- CMS139FHIRFallRiskScreening [ [cql] ](../../input/cql/CMS139FHIRFallRiskScreening.cql) [ [test results] ](../../input/tests/results/CMS139FHIRFallRiskScreening.txt) — CMS has discrepancies
- CMS143FHIRPOAGOpticNerveEval [ [cql] ](../../input/cql/CMS143FHIRPOAGOpticNerveEval.cql) [ [test results] ](../../input/tests/results/CMS143FHIRPOAGOpticNerveEval.txt) — also passes in CMS
- CMS144FHIRHFBetaBlockerForLVSD [ [cql] ](../../input/cql/CMS144FHIRHFBetaBlockerForLVSD.cql) [ [test results] ](../../input/tests/results/CMS144FHIRHFBetaBlockerForLVSD.txt) — CMS has discrepancies
- CMS146FHIRApproTestPharyngitis [ [cql] ](../../input/cql/CMS146FHIRApproTestPharyngitis.cql) [ [test results] ](../../input/tests/results/CMS146FHIRApproTestPharyngitis.txt) — CMS has discrepancies
- CMS149FHIRDementiaCognitiveAssess [ [cql] ](../../input/cql/CMS149FHIRDementiaCognitiveAssess.cql) [ [test results] ](../../input/tests/results/CMS149FHIRDementiaCognitiveAssess.txt) — also passes in CMS
- CMS154FHIRAppropriateTxforURI [ [cql] ](../../input/cql/CMS154FHIRAppropriateTxforURI.cql) [ [test results] ](../../input/tests/results/CMS154FHIRAppropriateTxforURI.txt) — CMS has discrepancies
- CMS155FHIRWgtAssessCounseling [ [cql] ](../../input/cql/CMS155FHIRWgtAssessCounseling.cql) [ [test results] ](../../input/tests/results/CMS155FHIRWgtAssessCounseling.txt) — CMS has discrepancies
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
## Measures with Discrepancies (53)
| Measure | Total Test Cases | Missing Results | Missing Populations | Mismatched Test Cases |
|---|:---:|:---:|:---:|:---:|
| [CMS2FHIRPCSDepScreenAndFollowUp](#cms2fhirpcsdepscreenandfollowup) | 36 | 0 | 0 | 22.22%   (8) |
| [CMS22FHIRPCSBPScreeningFollowUp](#cms22fhirpcsbpscreeningfollowup) | 44 | 0 | 0 | 27.27%   (12) |
| [CMS56FHIRFuncStatHipReplacement](#cms56fhirfuncstathipreplacement) | 58 | 0 | 0 | 13.79%   (8) |
| [CMS68FHIRDocumentationCurrentMeds](#cms68fhirdocumentationcurrentmeds) | 19 | 1 | 0 | 0.00%   (0) |
| [CMS69FHIRPCSBMIScreenAndFollowUp](#cms69fhirpcsbmiscreenandfollowup) | 63 | 0 | 0 | 52.38%   (33) |
| [CMS71FHIRSTKAnticoagAFFlutter](#cms71fhirstkanticoagafflutter) | 83 | 0 | 0 | 9.64%   (8) |
| [CMS72FHIRSTKAntithromboticDay2](#cms72fhirstkantithromboticday2) | 158 | 0 | 0 | 8.23%   (13) |
| [CMS74FHIRDentalCariesPrevention](#cms74fhirdentalcariesprevention) | 20 | 0 | 0 | 35.00%   (7) |
| [CMS75FHIRChildrenDentalDecay](#cms75fhirchildrendentaldecay) | 20 | 0 | 0 | 35.00%   (7) |
| [CMS90FHIRFSAforHeartFailure](#cms90fhirfsaforheartfailure) | 37 | 0 | 0 | 21.62%   (8) |
| [CMS104FHIRSTKDCAntithrombotic](#cms104fhirstkdcantithrombotic) | 82 | 0 | 0 | 18.29%   (15) |
| [CMS108FHIRVTEProphylaxis](#cms108fhirvteprophylaxis) | 140 | 0 | 0 | 17.14%   (24) |
| [CMS117FHIRChildImmunStatus](#cms117fhirchildimmunstatus) | 45 | 0 | 0 | 17.78%   (8) |
| [CMS122FHIRDiabetesAssessGT9Pct](#cms122fhirdiabetesassessgt9pct) | 55 | 0 | 0 | 45.45%   (25) |
| [CMS124FHIRCervicalCancerScreen](#cms124fhircervicalcancerscreen) | 34 | 0 | 0 | 38.24%   (13) |
| [CMS125FHIRBreastCancerScreen](#cms125fhirbreastcancerscreen) | 66 | 0 | 0 | 39.39%   (26) |
| [CMS128FHIRAntidepressantMgmt](#cms128fhirantidepressantmgmt) | 58 | 0 | 0 | 27.59%   (16) |
| [CMS130FHIRColorectalCancerScrn](#cms130fhircolorectalcancerscrn) | 64 | 0 | 0 | 26.56%   (17) |
| [CMS131FHIRDiabetesEyeExam](#cms131fhirdiabeteseyeexam) | 63 | 0 | 0 | 38.10%   (24) |
| [CMS135FHIRACEIorARBorARNIforHF](#cms135fhiraceiorarborarniforhf) | 40 | 3 | 0 | 22.50%   (9) |
| [CMS136FHIRChildADHDMedFollowUp](#cms136fhirchildadhdmedfollowup) | 128 | 0 | 0 | 17.97%   (23) |
| [CMS137FHIRSUDTxInitEngagement](#cms137fhirsudtxinitengagement) | 90 | 0 | 0 | 20.00%   (18) |
| [CMS138FHIRTobaccoScrnCessation](#cms138fhirtobaccoscrncessation) | 141 | 0 | 0 | 28.37%   (40) |
| [CMS139FHIRFallRiskScreening](#cms139fhirfallriskscreening) | 29 | 0 | 0 | 27.59%   (8) |
| [CMS142FHIRCommWithDrManagingDiab](#cms142fhircommwithdrmanagingdiab) | 32 | 0 | 0 | 15.62%   (5) |
| [CMS144FHIRHFBetaBlockerForLVSD](#cms144fhirhfbetablockerforlvsd) | 48 | 0 | 0 | 6.25%   (3) |
| [CMS145FHIRCADBBlockerTPMIorLVSD](#cms145fhircadbblockertpmiorlvsd) | 106 | 0 | 0 | 5.66%   (6) |
| [CMS146FHIRApproTestPharyngitis](#cms146fhirapprotestpharyngitis) | 38 | 0 | 0 | 26.32%   (10) |
| [CMS153FHIRChlamydiaScreening](#cms153fhirchlamydiascreening) | 32 | 0 | 0 | 25.00%   (8) |
| [CMS154FHIRAppropriateTxforURI](#cms154fhirappropriatetxforuri) | 33 | 0 | 0 | 24.24%   (8) |
| [CMS155FHIRWgtAssessCounseling](#cms155fhirwgtassesscounseling) | 102 | 0 | 0 | 26.47%   (27) |
| [CMS156FHIRHighRiskMedsElderly](#cms156fhirhighriskmedselderly) | 177 | 0 | 0 | 23.16%   (41) |
| [CMS157FHIRPainIntensityQuantified](#cms157fhirpainintensityquantified) | 126 | 0 | 0 | 15.08%   (19) |
| [CMS159FHIRDepRemissionat12Months](#cms159fhirdepremissionat12months) | 67 | 0 | 0 | 2.99%   (2) |
| [CMS165FHIRControllingHighBP](#cms165fhircontrollinghighbp) | 68 | 1 | 0 | 42.65%   (29) |
| [CMS177FHIRChildMDDSuicideAssmt](#cms177fhirchildmddsuicideassmt) | 41 | 0 | 0 | 2.44%   (1) |
| [CMS190FHIRVTEProphylaxisICU](#cms190fhirvteprophylaxisicu) | 125 | 0 | 0 | 19.20%   (24) |
| [CMS0334FHIRPCCesareanBirth](#cms0334fhirpccesareanbirth) | 138 | 0 | 0 | 0.72%   (1) |
| [CMS347FHIRStatinPreventionTxCVD](#cms347fhirstatinpreventiontxcvd) | 752 | 4 | 0 | 17.95%   (135) |
| [CMS645FHIRBoneDensityPCADTherapy](#cms645fhirbonedensitypcadtherapy) | 51 | 0 | 0 | 5.88%   (3) |
| [CMS646FHIRIntravesicalBCGTherapy](#cms646fhirintravesicalbcgtherapy) | 38 | 1 | 0 | 7.89%   (3) |
| [CMS771FHIRUrinarySymptomScoreBPH](#cms771fhirurinarysymptomscorebph) | 31 | 0 | 0 | 22.58%   (7) |
| [CMS816FHIRHHHypo](#cms816fhirhhhypo) | 28 | 0 | 0 | 42.86%   (12) |
| [CMS819FHIRHHORAE](#cms819fhirhhorae) | 28 | 0 | 0 | 7.14%   (2) |
| [CMSFHIR844HybridHospitalWideMortality](#cmsfhir844hybridhospitalwidemortality) | 10 | 0 | 0 | 20.00%   (2) |
| [CMS871FHIRHHHyper](#cms871fhirhhhyper) | 26 | 4 | 0 | 0.00%   (0) |
| [CMS951FHIRKidneyHealthEval](#cms951fhirkidneyhealtheval) | 55 | 0 | 0 | 23.64%   (13) |
| [CMS986FHIRMalnutritionScore](#cms986fhirmalnutritionscore) | 876 | 0 | 0 | 0.68%   (6) |
| [CMS996FHIRAptTxforSTEMI](#cms996fhirapttxforstemi) | 114 | 0 | 0 | 6.14%   (7) |
| [CMS1017FHIRHHFI](#cms1017fhirhhfi) | 65 | 0 | 0 | 3.08%   (2) |
| [CMS1028FHIRPCSevereOBComps](#cms1028fhirpcsevereobcomps) | 282 | 0 | 0 | 0.71%   (2) |
| [CMS1154ScreeningPrediabetesFHIR](#cms1154screeningprediabetesfhir) | 10 | 0 | 0 | 10.00%   (1) |
| [CMS1218FHIRHHRF](#cms1218fhirhhrf) | 69 | 0 | 0 | 1.45%   (1) |



#### CMS2FHIRPCSDepScreenAndFollowUp
[ [cql] ](../../input/cql/CMS2FHIRPCSDepScreenAndFollowUp.cql) [ [test results] ](../../input/tests/results/CMS2FHIRPCSDepScreenAndFollowUp.txt)

Mismatched Test Cases (8 of  of 36)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 0e463fc3-d1bf-4e19-882b-fad6342aa668 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/0e463fc3-d1bf-4e19-882b-fad6342aa668/MeasureReport-38443362-8261-414c-80b3-1f719f4ba56e.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 12786a64-c20e-4542-a4c0-bf3129d6a9e0 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/12786a64-c20e-4542-a4c0-bf3129d6a9e0/MeasureReport-d404e2d0-2ded-4329-b254-482be8b54a7c.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 41df0dbe-ae84-4496-b355-320ff8707a85 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/41df0dbe-ae84-4496-b355-320ff8707a85/MeasureReport-922ffb7d-2d13-47b8-ad5d-4f42ff55f77d.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 6078e73e-3265-4022-ae63-216c096b6246 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/6078e73e-3265-4022-ae63-216c096b6246/MeasureReport-dfcfbb31-9da9-4947-8444-53a25c8b8121.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 6aaff09e-4a7b-4efa-93f8-13033e95c230 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/6aaff09e-4a7b-4efa-93f8-13033e95c230/MeasureReport-5981d1e2-7d0b-4887-aed2-884d0e7df4fe.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 86ca7528-efcb-44ed-9203-6f21f37f4332 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/86ca7528-efcb-44ed-9203-6f21f37f4332/MeasureReport-51f60250-c8a8-49d8-81c1-56b58ad0125f.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ d0ba1182-26fa-4cfa-9f91-960503b7fe53 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/d0ba1182-26fa-4cfa-9f91-960503b7fe53/MeasureReport-277359bb-b41c-4dd4-b1af-b3afdb6ee15d.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ f29e2786-fade-4dca-b14d-7037a34ef498 ](../.././input/tests/measure/CMS2FHIRPCSDepScreenAndFollowUp/f29e2786-fade-4dca-b14d-7037a34ef498/MeasureReport-32baa107-7be1-4a64-a10d-1f25307962e6.json) | Group_1 | Denominator Exception | 1 | 0 | — |


#### CMS22FHIRPCSBPScreeningFollowUp
[ [cql] ](../../input/cql/CMS22FHIRPCSBPScreeningFollowUp.cql) [ [test results] ](../../input/tests/results/CMS22FHIRPCSBPScreeningFollowUp.txt)

Mismatched Test Cases (12 of  of 44)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 0278fdf0-f067-46e8-aeb1-fb96dff3c947 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/0278fdf0-f067-46e8-aeb1-fb96dff3c947/MeasureReport-064f5dc2-d804-4a03-a0c8-d0c25ae3b8fb.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 1f16120b-56c9-4d72-8dd4-01d8a0175d77 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/1f16120b-56c9-4d72-8dd4-01d8a0175d77/MeasureReport-b5acac31-18e7-4172-802f-041d29ba3da1.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 695cee04-cf12-411e-a258-99e430093a4e ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/695cee04-cf12-411e-a258-99e430093a4e/MeasureReport-e887022a-7961-4768-9cf3-e48ecfced710.json) | Group_1 | Denominator Exception | 2 | 0 | — |
| [ 86618b52-e0cc-4e90-b48c-cd64bbae8973 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/86618b52-e0cc-4e90-b48c-cd64bbae8973/MeasureReport-ad10338d-d04c-44de-badb-b69f01b20de5.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 9ed1ecf5-2d93-4bde-a293-5d5fbf209475 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/9ed1ecf5-2d93-4bde-a293-5d5fbf209475/MeasureReport-bd56dca9-e498-4ec5-bf78-c6322930e980.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ a55c6265-a05c-4fad-beb4-c5338420d1b1 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/a55c6265-a05c-4fad-beb4-c5338420d1b1/MeasureReport-a08e2374-4dea-4a09-8163-296239dcd454.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ ad737f80-c9ea-41fd-a142-78d9c80a9c7c ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/ad737f80-c9ea-41fd-a142-78d9c80a9c7c/MeasureReport-29212fe6-6c26-4e87-9711-8b5694567caa.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ afdeaa75-d332-40f2-9b30-0b6ddf7e7c14 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/afdeaa75-d332-40f2-9b30-0b6ddf7e7c14/MeasureReport-fcac6417-0a19-457d-a23b-b55bfb352064.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ c41f9946-cb0f-4489-8367-581a5b876165 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/c41f9946-cb0f-4489-8367-581a5b876165/MeasureReport-f183c739-a20c-4dcd-b12c-5c2cef29eaf5.json) | Group_1 | Denominator Exception<br>Numerator | 2<br>0 | 1<br>1 | — |
| [ dda022c0-3234-4ad7-ad6e-d696b0b57440 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/dda022c0-3234-4ad7-ad6e-d696b0b57440/MeasureReport-2b4791bc-bde7-4af7-9665-df0a21abc7b0.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ ef9a58ac-e252-480a-bed8-2309c503587d ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/ef9a58ac-e252-480a-bed8-2309c503587d/MeasureReport-292f318b-0b76-4666-9e3e-4b0d8c6924b2.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ f9417a57-54e8-4a0b-a516-ab62b8d4aae0 ](../.././input/tests/measure/CMS22FHIRPCSBPScreeningFollowUp/f9417a57-54e8-4a0b-a516-ab62b8d4aae0/MeasureReport-e90efb05-4493-4006-a537-3896b6bf37ba.json) | Group_1 | Denominator Exception<br>Numerator | 2<br>0 | 0<br>1 | — |


#### CMS56FHIRFuncStatHipReplacement
[ [cql] ](../../input/cql/CMS56FHIRFuncStatHipReplacement.cql) [ [test results] ](../../input/tests/results/CMS56FHIRFuncStatHipReplacement.txt)

Mismatched Test Cases (8 of  of 58)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 10e6851a-0db4-4706-8a6e-7fbbb27c588e ](../.././input/tests/measure/CMS56FHIRFuncStatHipReplacement/10e6851a-0db4-4706-8a6e-7fbbb27c588e/MeasureReport-51a08c7d-df82-4af1-9b3a-30a16405fe0a.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ 289b7214-0496-425b-8ffa-14b2aaa9f771 ](../.././input/tests/measure/CMS56FHIRFuncStatHipReplacement/289b7214-0496-425b-8ffa-14b2aaa9f771/MeasureReport-fcb7591b-7fe2-4a0c-b626-68caba5b6568.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 34fa486b-b691-4760-9acc-1e5c0fc8a4dc ](../.././input/tests/measure/CMS56FHIRFuncStatHipReplacement/34fa486b-b691-4760-9acc-1e5c0fc8a4dc/MeasureReport-20f3f4ae-7b38-4a41-8e8c-4982ee82f6e2.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 3574f4b8-cbdc-410b-8b6a-7f0737546e56 ](../.././input/tests/measure/CMS56FHIRFuncStatHipReplacement/3574f4b8-cbdc-410b-8b6a-7f0737546e56/MeasureReport-ac3dfe55-8975-49b5-9fd4-8db0c01ae667.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 97ec6179-f96b-4d88-a042-c482f8fe525a ](../.././input/tests/measure/CMS56FHIRFuncStatHipReplacement/97ec6179-f96b-4d88-a042-c482f8fe525a/MeasureReport-6dc1210e-32b2-4fbc-9b1b-db104f90624f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 9e3e68df-73f6-4a91-9bef-b4fb94c11756 ](../.././input/tests/measure/CMS56FHIRFuncStatHipReplacement/9e3e68df-73f6-4a91-9bef-b4fb94c11756/MeasureReport-24a64324-ec16-454e-8ff4-f6f4e5c56d91.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d1746049-b5df-4a21-a0ea-2b1709c0c502 ](../.././input/tests/measure/CMS56FHIRFuncStatHipReplacement/d1746049-b5df-4a21-a0ea-2b1709c0c502/MeasureReport-eb66366d-b383-4534-9d49-cb53bfaf97f7.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d2682114-7f8e-41a4-88b1-e96a670e964a ](../.././input/tests/measure/CMS56FHIRFuncStatHipReplacement/d2682114-7f8e-41a4-88b1-e96a670e964a/MeasureReport-25a568dd-3b19-40b0-96d9-ac5f575d6463.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS68FHIRDocumentationCurrentMeds
[ [cql] ](../../input/cql/CMS68FHIRDocumentationCurrentMeds.cql) [ [test results] ](../../input/tests/results/CMS68FHIRDocumentationCurrentMeds.txt)

Missing Results (1 of 19 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ f2e2e1c0-9e35-4592-9579-72a236cb2f56 ](../.././input/tests/measure/CMS68FHIRDocumentationCurrentMeds/f2e2e1c0-9e35-4592-9579-72a236cb2f56/MeasureReport-7384d607-6a08-487a-9129-d90036bae37e.json) | Group_1 | — |


#### CMS69FHIRPCSBMIScreenAndFollowUp
[ [cql] ](../../input/cql/CMS69FHIRPCSBMIScreenAndFollowUp.cql) [ [test results] ](../../input/tests/results/CMS69FHIRPCSBMIScreenAndFollowUp.txt)

Mismatched Test Cases (33 of  of 63)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 050201c2-c2c4-46e6-8288-a34f99caebdc ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/050201c2-c2c4-46e6-8288-a34f99caebdc/MeasureReport-9559c66c-9809-48eb-851c-26cc3e45434d.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 097cbc7a-d22e-4395-9fcf-fd1f904f7c92 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/097cbc7a-d22e-4395-9fcf-fd1f904f7c92/MeasureReport-47e5ceae-cb93-44a3-847c-aeab934dea06.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 09e4ff5a-fe3b-4c89-a36e-68f64c7e489c ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/09e4ff5a-fe3b-4c89-a36e-68f64c7e489c/MeasureReport-2170ac3f-1253-4fe4-b62e-a859b14250bb.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 1102009b-6f05-4bab-9fd1-191e81cf50e8 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/1102009b-6f05-4bab-9fd1-191e81cf50e8/MeasureReport-74ca5bf1-866c-4f0e-bedf-4f9255ec0318.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 1b102c21-830a-41a5-ac27-9aa77ea5adfe ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/1b102c21-830a-41a5-ac27-9aa77ea5adfe/MeasureReport-3ad40e5e-bf9c-4875-9440-95cfa52942fa.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 1e23fb8f-e27b-4553-a62a-f66edeb4528a ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/1e23fb8f-e27b-4553-a62a-f66edeb4528a/MeasureReport-5cdcf0c7-66f6-4c68-a90c-62ab758aa608.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 27849d59-3cef-40bf-8338-a6ec7c0bcf81 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/27849d59-3cef-40bf-8338-a6ec7c0bcf81/MeasureReport-a46fc485-4122-45a5-b342-e0d722d0ab92.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 353cb8b7-96ac-4b51-9a0d-60cd64e6d854 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/353cb8b7-96ac-4b51-9a0d-60cd64e6d854/MeasureReport-b7de60a9-4dc4-4042-a003-b663bbfb48ee.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 3ecce155-635d-47ec-b35d-d53126423a81 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/3ecce155-635d-47ec-b35d-d53126423a81/MeasureReport-97382c07-ee89-4833-a30b-4f1a60e4414f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 42e6b4d6-defc-4ec5-894f-e3333e3039a3 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/42e6b4d6-defc-4ec5-894f-e3333e3039a3/MeasureReport-35b5dc02-0f37-455c-8e85-6c353fc8f17c.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 45b1ce40-0f49-4559-8c3b-5c2a8070b0a7 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/45b1ce40-0f49-4559-8c3b-5c2a8070b0a7/MeasureReport-157b505d-30c5-4f3f-aeb2-b7de8f06a79c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 461fdfab-fcc1-4630-9dae-2ba3a6ab0c25 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/461fdfab-fcc1-4630-9dae-2ba3a6ab0c25/MeasureReport-ef49c8ea-63d2-4cea-abb9-964d856db616.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 463dd868-997d-472f-962c-96383fd2a5c4 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/463dd868-997d-472f-962c-96383fd2a5c4/MeasureReport-0023b9fa-401a-4e0b-9298-b345b544d9a3.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 57858042-c2aa-49f4-b401-1f1fd9ab289a ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/57858042-c2aa-49f4-b401-1f1fd9ab289a/MeasureReport-f2536a94-89c0-4b41-9366-1851f9e5244f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 5d34e56e-f4f1-4817-b7e4-e4c57f811300 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/5d34e56e-f4f1-4817-b7e4-e4c57f811300/MeasureReport-005250b3-0d49-48cf-ae6f-17c039265358.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 5d48c3b8-93e9-4e29-8c20-a002761d9e24 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/5d48c3b8-93e9-4e29-8c20-a002761d9e24/MeasureReport-59169730-a1eb-40d1-9b71-d84981ad8e3e.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 5ef4acf3-4b42-41fd-8793-7d1a9342865a ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/5ef4acf3-4b42-41fd-8793-7d1a9342865a/MeasureReport-b251f176-9318-47a3-87f2-fea12f92e3c4.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 6092a810-f9e0-4975-9582-37bbb06e8e56 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/6092a810-f9e0-4975-9582-37bbb06e8e56/MeasureReport-44c748c1-2037-4d8a-a875-2736c4a18d16.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 6f0c3642-5efc-4923-ac24-9f5e9d1831d6 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/6f0c3642-5efc-4923-ac24-9f5e9d1831d6/MeasureReport-14fc8964-a0c4-4ddf-bcaa-4300c26eb986.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 736b5472-4a6f-4278-80d3-373d1c78c4c5 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/736b5472-4a6f-4278-80d3-373d1c78c4c5/MeasureReport-e5e922b8-7613-4b10-8821-dfc20202743e.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 7902e3dc-f3da-4cc2-9f2e-4a6c8cd33b88 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/7902e3dc-f3da-4cc2-9f2e-4a6c8cd33b88/MeasureReport-ff7090ac-931d-4cc7-83f7-ee15beec8ed1.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 7b34e64e-e7fe-402c-9a26-12da90662897 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/7b34e64e-e7fe-402c-9a26-12da90662897/MeasureReport-76dee5fb-41e8-4b52-a8eb-9e8a22d7aa01.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 8835a50b-0a0f-4e2f-94fa-7c180cd7f905 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/8835a50b-0a0f-4e2f-94fa-7c180cd7f905/MeasureReport-9219de61-d774-496c-a820-9602e651ce91.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 8e38b797-4dec-437d-8bf0-6f0fc78f8ea7 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/8e38b797-4dec-437d-8bf0-6f0fc78f8ea7/MeasureReport-93a73b49-b742-4d24-9f77-8f72e117110f.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 953ef59d-4c39-40ef-8067-87b5ecf84727 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/953ef59d-4c39-40ef-8067-87b5ecf84727/MeasureReport-70849e89-3eeb-47cb-932a-413e6967a1cd.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 9d92be1d-6fc8-40f2-99a0-4be9ce1f244b ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/9d92be1d-6fc8-40f2-99a0-4be9ce1f244b/MeasureReport-071ef161-5f61-4057-8d9c-d1c378b1647e.json) | Group_1 | Numerator | 1 | 0 | — |
| [ c3caf126-12a2-473f-8f51-1c7828d63d16 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/c3caf126-12a2-473f-8f51-1c7828d63d16/MeasureReport-efbab239-c362-4ef2-b91b-49e234e8c5c4.json) | Group_1 | Numerator | 1 | 0 | — |
| [ c84bf29f-80ac-4bf0-beeb-404ba96a3fa8 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/c84bf29f-80ac-4bf0-beeb-404ba96a3fa8/MeasureReport-62e3506b-3f36-48ef-8a9a-69b9b6401c45.json) | Group_1 | Numerator | 1 | 0 | — |
| [ ca6deaeb-459d-4d1a-9daf-e454ff76a6f0 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/ca6deaeb-459d-4d1a-9daf-e454ff76a6f0/MeasureReport-728faa06-3efd-4d80-bcbe-f4c7217e36fb.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d4d064be-d55a-47b5-9bfd-993afebd95a5 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/d4d064be-d55a-47b5-9bfd-993afebd95a5/MeasureReport-3cba3e58-4c3f-4f39-b0af-b52d69bda4b9.json) | Group_1 | Numerator | 1 | 0 | — |
| [ e0821eec-ff83-49e9-950d-9219dd3612b9 ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/e0821eec-ff83-49e9-950d-9219dd3612b9/MeasureReport-712f56a5-5f65-428c-a73a-cf0d453d1302.json) | Group_1 | Numerator | 1 | 0 | — |
| [ e25fc2f1-0083-4375-8fc3-9164a5aee53d ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/e25fc2f1-0083-4375-8fc3-9164a5aee53d/MeasureReport-132b5702-5c1a-47fc-8326-cb020958dff5.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ f5ae6269-d09b-47f8-a519-f1a8a81549fc ](../.././input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/f5ae6269-d09b-47f8-a519-f1a8a81549fc/MeasureReport-3d833783-caa1-4d2d-ae23-a8f2f6f31cc0.json) | Group_1 | Numerator | 1 | 0 | — |


#### CMS71FHIRSTKAnticoagAFFlutter
[ [cql] ](../../input/cql/CMS71FHIRSTKAnticoagAFFlutter.cql) [ [test results] ](../../input/tests/results/CMS71FHIRSTKAnticoagAFFlutter.txt)

Mismatched Test Cases (8 of  of 83)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 017a2267-f463-47a6-8b8b-dc91465e0869 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/017a2267-f463-47a6-8b8b-dc91465e0869/MeasureReport-3a870421-64af-44eb-8c7a-533079bc2259.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 0587a75d-0dcc-4c6b-bfc0-f5727342ec1f ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/0587a75d-0dcc-4c6b-bfc0-f5727342ec1f/MeasureReport-c8a99645-6e7a-467b-87aa-456cdc7cafb9.json) | Group_1 | Denominator<br>Numerator | 1<br>1 | 0<br>0 | — |
| [ 56ae006d-ab1b-428d-8614-2ccd5d962650 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/56ae006d-ab1b-428d-8614-2ccd5d962650/MeasureReport-71b26a14-7533-4479-82e3-7bc54d9ce0db.json) | Group_1 | Denominator<br>Numerator | 1<br>1 | 0<br>0 | — |
| [ 595ebfd1-fe6a-4b4b-96a1-23a72f6a70da ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/595ebfd1-fe6a-4b4b-96a1-23a72f6a70da/MeasureReport-793a4c67-2bc9-4601-9521-999a2628ffdd.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 9a72ea26-595f-4442-8b00-fc52ed228aa6 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/9a72ea26-595f-4442-8b00-fc52ed228aa6/MeasureReport-47b2254f-ca43-470b-9229-eeb4071ba6e0.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ b29204ac-96ce-4be0-90ad-ae8ecfa4f245 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/b29204ac-96ce-4be0-90ad-ae8ecfa4f245/MeasureReport-e5339c1c-c4cd-497b-97a1-ed9fb1a1bc2e.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ c640ff8f-5b2a-448e-85a2-e739af7a8dc4 ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/c640ff8f-5b2a-448e-85a2-e739af7a8dc4/MeasureReport-8b1280e5-8c6d-48b1-ac5a-e4c07e338f56.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ e20b4e76-8523-43ab-abc2-a4f4137a84bb ](../.././input/tests/measure/CMS71FHIRSTKAnticoagAFFlutter/e20b4e76-8523-43ab-abc2-a4f4137a84bb/MeasureReport-ce8fcdb9-f3ff-4f3f-a6cc-114d96185bcb.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |


#### CMS72FHIRSTKAntithromboticDay2
[ [cql] ](../../input/cql/CMS72FHIRSTKAntithromboticDay2.cql) [ [test results] ](../../input/tests/results/CMS72FHIRSTKAntithromboticDay2.txt)

Mismatched Test Cases (13 of  of 158)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 2f7681fa-66b0-4395-aa35-7622e37709ae ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2f7681fa-66b0-4395-aa35-7622e37709ae/MeasureReport-97f5ba10-36d6-4246-b935-fcfc8f4b1061.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 3432dedb-7130-4614-9283-6c1569fab90f ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3432dedb-7130-4614-9283-6c1569fab90f/MeasureReport-acfc5ee1-09d4-4012-b12a-8487396b9856.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 5a329008-fcc1-4168-ab9c-89cb5dd6ff32 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5a329008-fcc1-4168-ab9c-89cb5dd6ff32/MeasureReport-dda268cb-4395-4776-acd8-0fee046d392a.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 1<br>1<br>0 | — |
| [ 7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8/MeasureReport-bad7b4ba-e916-41e2-a314-11854e1021ff.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 82399522-ba6c-4997-afc9-23f55bb7da89 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/82399522-ba6c-4997-afc9-23f55bb7da89/MeasureReport-fe335f74-59a9-4afc-ba4c-7a9e003733d6.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ a1a37483-1a67-4dd9-a8ca-b4d49a28a19d ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a1a37483-1a67-4dd9-a8ca-b4d49a28a19d/MeasureReport-e3bfac2a-251a-49fe-9694-6c60803d9ded.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ be5c4068-2639-4b0c-bea3-5b7c80a6fe3b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/be5c4068-2639-4b0c-bea3-5b7c80a6fe3b/MeasureReport-ad329961-b67b-413b-a186-d6b269572c42.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ cb7c95fc-6d6b-4e07-81e8-a79385142b94 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cb7c95fc-6d6b-4e07-81e8-a79385142b94/MeasureReport-6844e7ed-08a4-43d5-be1c-720dc795b3cf.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 3<br>3<br>2 | 1<br>1<br>0 | — |
| [ d496f08e-c55b-44b1-97a7-f86cf9ead1e2 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d496f08e-c55b-44b1-97a7-f86cf9ead1e2/MeasureReport-81e3066d-7dba-46fa-bb3f-2abc24625551.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ dc187313-245c-4ed6-b6bb-fcb94c117fec ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/dc187313-245c-4ed6-b6bb-fcb94c117fec/MeasureReport-d0cc2adb-8b9f-442d-82e2-5ef90a9c30d3.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ e126cdec-dbc8-4ee8-964f-e88e46c04f88 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e126cdec-dbc8-4ee8-964f-e88e46c04f88/MeasureReport-58249af5-0abc-464b-9e0a-456f7c31b4cf.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ ed638412-155e-4349-8461-4550fd4fae3b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ed638412-155e-4349-8461-4550fd4fae3b/MeasureReport-cf1aeb73-d464-4dd9-9f46-38afe84f76ec.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ febd4b3e-99bc-4c55-bba9-3b2136c2160b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/febd4b3e-99bc-4c55-bba9-3b2136c2160b/MeasureReport-4f80f98a-71ab-45d6-bdda-d0875ec02ec9.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 4<br>4<br>2<br>2 | 1<br>1<br>0<br>1 | — |


#### CMS74FHIRDentalCariesPrevention
[ [cql] ](../../input/cql/CMS74FHIRDentalCariesPrevention.cql) [ [test results] ](../../input/tests/results/CMS74FHIRDentalCariesPrevention.txt)

Mismatched Test Cases (7 of  of 20)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 31bee4bc-9ca4-4d84-9f1a-a6a6d2d3fac0 ](../.././input/tests/measure/CMS74FHIRDentalCariesPrevention/31bee4bc-9ca4-4d84-9f1a-a6a6d2d3fac0/MeasureReport-527e90a7-da52-4aeb-bde0-0bab30030567.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 499fd8d2-0a68-4d27-a194-c61aae97e492 ](../.././input/tests/measure/CMS74FHIRDentalCariesPrevention/499fd8d2-0a68-4d27-a194-c61aae97e492/MeasureReport-956a77dc-86f3-4b55-aba7-d42bd5eb121f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4fc1e663-46e6-4159-853d-b2dbb146b2ac ](../.././input/tests/measure/CMS74FHIRDentalCariesPrevention/4fc1e663-46e6-4159-853d-b2dbb146b2ac/MeasureReport-4222e706-7c21-4356-b467-7a81ade0a0d3.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 70208367-16df-46d6-b49c-c1e31b7e1d5f ](../.././input/tests/measure/CMS74FHIRDentalCariesPrevention/70208367-16df-46d6-b49c-c1e31b7e1d5f/MeasureReport-1afefa48-4ea8-462c-9d65-e113dbafea42.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 890dbdad-7466-494d-966b-a20515508db5 ](../.././input/tests/measure/CMS74FHIRDentalCariesPrevention/890dbdad-7466-494d-966b-a20515508db5/MeasureReport-d3aa9228-b953-4f41-9715-9a4e2bdab41b.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 96c38952-91cc-468c-b16b-32386bb312ec ](../.././input/tests/measure/CMS74FHIRDentalCariesPrevention/96c38952-91cc-468c-b16b-32386bb312ec/MeasureReport-a63cb2f7-9022-41e0-968b-a8d1393dbf8b.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ fe5f3172-5263-4498-b1ba-0d62de7455ef ](../.././input/tests/measure/CMS74FHIRDentalCariesPrevention/fe5f3172-5263-4498-b1ba-0d62de7455ef/MeasureReport-7a43460d-c5e6-4cb1-8aa0-aee2a031c30a.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS75FHIRChildrenDentalDecay
[ [cql] ](../../input/cql/CMS75FHIRChildrenDentalDecay.cql) [ [test results] ](../../input/tests/results/CMS75FHIRChildrenDentalDecay.txt)

Mismatched Test Cases (7 of  of 20)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 043f64b7-dd25-42ea-9785-0bdcbe64b27a ](../.././input/tests/measure/CMS75FHIRChildrenDentalDecay/043f64b7-dd25-42ea-9785-0bdcbe64b27a/MeasureReport-38477bd2-2869-40cc-b9bf-87411de40c43.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 26549e84-fbf3-43dc-8971-2f3baaf508d7 ](../.././input/tests/measure/CMS75FHIRChildrenDentalDecay/26549e84-fbf3-43dc-8971-2f3baaf508d7/MeasureReport-0ec4b930-0257-4c61-8caf-1889192f85ce.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 303676f7-30b4-4324-8ab3-8d5ab7e92102 ](../.././input/tests/measure/CMS75FHIRChildrenDentalDecay/303676f7-30b4-4324-8ab3-8d5ab7e92102/MeasureReport-0ad65771-f602-4a8c-b994-a6a9c2eed62d.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 326c7237-c7a4-4e1b-bd1d-ba518dc942dd ](../.././input/tests/measure/CMS75FHIRChildrenDentalDecay/326c7237-c7a4-4e1b-bd1d-ba518dc942dd/MeasureReport-0154c762-1783-46f1-a594-89b73d9b6d56.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ a42cd354-1966-45d5-aec2-2d42225e6911 ](../.././input/tests/measure/CMS75FHIRChildrenDentalDecay/a42cd354-1966-45d5-aec2-2d42225e6911/MeasureReport-e1a9f35d-af56-4fa2-a3a7-cd0f1f0ffff3.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b532c8f5-b38a-4337-8661-7b744e271a9c ](../.././input/tests/measure/CMS75FHIRChildrenDentalDecay/b532c8f5-b38a-4337-8661-7b744e271a9c/MeasureReport-b401819e-872f-4742-b02d-1e036c283c88.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ ebb4d1e8-32af-4811-adc5-f84a7318c5b8 ](../.././input/tests/measure/CMS75FHIRChildrenDentalDecay/ebb4d1e8-32af-4811-adc5-f84a7318c5b8/MeasureReport-3a37e0c2-4c25-4c5c-8ecd-4423dcbd3ee3.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS90FHIRFSAforHeartFailure
[ [cql] ](../../input/cql/CMS90FHIRFSAforHeartFailure.cql) [ [test results] ](../../input/tests/results/CMS90FHIRFSAforHeartFailure.txt)

Mismatched Test Cases (8 of  of 37)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 19608155-9049-41fc-9a02-d856e4143773 ](../.././input/tests/measure/CMS90FHIRFSAforHeartFailure/19608155-9049-41fc-9a02-d856e4143773/MeasureReport-22c744a7-8932-490c-a25f-e0d63bbf88f0.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 19a551f9-e826-4cce-bde3-cc013c182ada ](../.././input/tests/measure/CMS90FHIRFSAforHeartFailure/19a551f9-e826-4cce-bde3-cc013c182ada/MeasureReport-126e7f75-ad05-4519-be48-ee48fa4d5f4e.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 3d036fff-bb44-4911-b6d4-23e064783f3a ](../.././input/tests/measure/CMS90FHIRFSAforHeartFailure/3d036fff-bb44-4911-b6d4-23e064783f3a/MeasureReport-ddffb7a0-9b64-4e6d-88d1-91be1343e240.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4944fb9a-bf44-4b09-a49f-aae0b6c0ad82 ](../.././input/tests/measure/CMS90FHIRFSAforHeartFailure/4944fb9a-bf44-4b09-a49f-aae0b6c0ad82/MeasureReport-046337c8-6720-4b63-a353-aa47e3d51811.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 6e5db6e5-8c56-4b08-9491-1a2877933f0d ](../.././input/tests/measure/CMS90FHIRFSAforHeartFailure/6e5db6e5-8c56-4b08-9491-1a2877933f0d/MeasureReport-8591ad2a-a1f7-4cfe-ab29-52d3b7881059.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ bc42a4e7-3a06-4056-bb38-14f1e3ea3894 ](../.././input/tests/measure/CMS90FHIRFSAforHeartFailure/bc42a4e7-3a06-4056-bb38-14f1e3ea3894/MeasureReport-3dd53a3c-82a1-4f90-8aca-675e0ef8df82.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ c784c565-2714-4009-b527-bee24f78d409 ](../.././input/tests/measure/CMS90FHIRFSAforHeartFailure/c784c565-2714-4009-b527-bee24f78d409/MeasureReport-5129d8cb-cff8-4f3e-8a26-b324edbb1b5f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ ffad6c76-4ffb-4cf1-bee2-df190571f3e1 ](../.././input/tests/measure/CMS90FHIRFSAforHeartFailure/ffad6c76-4ffb-4cf1-bee2-df190571f3e1/MeasureReport-2b5291b7-15aa-4a02-a556-0f3828a9d790.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS104FHIRSTKDCAntithrombotic
[ [cql] ](../../input/cql/CMS104FHIRSTKDCAntithrombotic.cql) [ [test results] ](../../input/tests/results/CMS104FHIRSTKDCAntithrombotic.txt)

Mismatched Test Cases (15 of  of 82)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 0b1aa8ee-e8bf-49f5-b968-48c5a9702843 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0b1aa8ee-e8bf-49f5-b968-48c5a9702843/MeasureReport-38f44642-a505-41c0-b367-013e4bb44d58.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ 146a6714-8663-4f45-826a-01110ff34490 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/146a6714-8663-4f45-826a-01110ff34490/MeasureReport-e1b111ec-80f6-4548-b462-dc44dd07fd1e.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 2d54a94c-edf1-4f92-baf8-3813a8ef452d ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2d54a94c-edf1-4f92-baf8-3813a8ef452d/MeasureReport-023784a8-b40e-491b-850f-0c87cb2e5e03.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 348471db-5aaa-4bf3-a280-75222f20d599 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/348471db-5aaa-4bf3-a280-75222f20d599/MeasureReport-bf54d81d-f635-45ff-b69b-1580a144d3fb.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 3<br>3<br>1<br>1 | 1<br>1<br>0<br>0 | — |
| [ 451b6853-3734-4c1c-b37e-5904629e0350 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/451b6853-3734-4c1c-b37e-5904629e0350/MeasureReport-4eefe8af-efb3-47eb-91df-e2ea877a39e7.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 3<br>3<br>2<br>1 | 1<br>1<br>1<br>0 | — |
| [ 48952352-d74c-491c-9420-6e999e60f52a ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/48952352-d74c-491c-9420-6e999e60f52a/MeasureReport-5eeb7443-d897-40c5-8815-c5dead56e05e.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 591c23ea-1ddd-4800-9203-4b6946979818 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/591c23ea-1ddd-4800-9203-4b6946979818/MeasureReport-a871588f-5c88-44ce-890e-ccac41059f64.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 593382e8-4ad5-4300-b0ad-26c8954281c6 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/593382e8-4ad5-4300-b0ad-26c8954281c6/MeasureReport-bb6002b4-0bd0-43fa-a7a0-748bd0444688.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 5adc911a-c2a1-475c-a347-9da4ee98c6df ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/5adc911a-c2a1-475c-a347-9da4ee98c6df/MeasureReport-fbd77dd4-8f40-4bf2-bee9-e1e5ce62d7aa.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 7b1ac1a8-b7be-41ec-a77f-db545af22263 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7b1ac1a8-b7be-41ec-a77f-db545af22263/MeasureReport-373169e3-3ba1-4ace-bf0c-5c212910cccf.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ a2b8327c-eaf4-4552-863e-851426e729d4 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a2b8327c-eaf4-4552-863e-851426e729d4/MeasureReport-0ced6c1b-75a5-4ee3-a7a0-017818c03e9a.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>2 | 1<br>1<br>1 | — |
| [ ac56c496-c5d6-4c23-be20-130ee8327fd2 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ac56c496-c5d6-4c23-be20-130ee8327fd2/MeasureReport-34148ef9-fbdd-48ca-ab5d-6a11fd288074.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ c15bee15-84c1-494a-ac82-2159b06da175 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/c15bee15-84c1-494a-ac82-2159b06da175/MeasureReport-bbe28035-6557-410d-964f-21cf38904d0f.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 3<br>3<br>2 | 1<br>1<br>0 | — |
| [ e081bee5-67f8-464f-9356-9b287e32a35a ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e081bee5-67f8-464f-9356-9b287e32a35a/MeasureReport-560b8ee7-5246-423f-8065-7f02c28eb91f.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ e84c89f7-3c9e-4ee9-b71a-5025aadb5990 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e84c89f7-3c9e-4ee9-b71a-5025aadb5990/MeasureReport-51e29a50-abca-429e-95eb-8364998be573.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exception | 1<br>1<br>1 | 0<br>0<br>0 | — |


#### CMS108FHIRVTEProphylaxis
[ [cql] ](../../input/cql/CMS108FHIRVTEProphylaxis.cql) [ [test results] ](../../input/tests/results/CMS108FHIRVTEProphylaxis.txt)

Mismatched Test Cases (24 of  of 140)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 068814f1-4270-4e10-b470-9a5433bceb3e ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/068814f1-4270-4e10-b470-9a5433bceb3e/MeasureReport-22ae9d87-29d1-42c3-9908-93eff318d7b1.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 182103c1-0a38-4d85-819c-148e4e105716 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/182103c1-0a38-4d85-819c-148e4e105716/MeasureReport-ccb6ece2-ea74-4377-b826-2118740d1eee.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 2eff6dbd-f3a2-43ee-9ad3-aab4d3b84812 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/2eff6dbd-f3a2-43ee-9ad3-aab4d3b84812/MeasureReport-735dcbb8-d535-493a-a79c-ff4a9f72ee50.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 33d162ce-3bc7-4b0a-8c04-fec0a42a6263 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/33d162ce-3bc7-4b0a-8c04-fec0a42a6263/MeasureReport-da823951-b92e-4ee9-904f-839f7e8db8df.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 3c854f27-5103-4367-bdef-97c3cde1edb8 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/3c854f27-5103-4367-bdef-97c3cde1edb8/MeasureReport-1c32114e-5b9f-4f01-b021-0b3dd5bd8adf.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 3db5c5a1-2eec-4e01-8e59-ac389a0a2179 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/3db5c5a1-2eec-4e01-8e59-ac389a0a2179/MeasureReport-384a4771-57ba-472a-9ffd-17eeba8f39d7.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 41f2785f-4c4f-4497-a46b-e17fd8b5ee3f ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/41f2785f-4c4f-4497-a46b-e17fd8b5ee3f/MeasureReport-ff4c0b9f-8014-4119-ab3f-78a8e7e8f935.json) | Group_1 | Denominator Exclusion | 0 | 1 | — |
| [ 525e73f2-77be-49b1-920f-6fc31ef38d22 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/525e73f2-77be-49b1-920f-6fc31ef38d22/MeasureReport-9cb7f213-6011-4f8b-be16-010172559897.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 541ccffb-c1be-4c94-ab24-168d52e3a36b ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/541ccffb-c1be-4c94-ab24-168d52e3a36b/MeasureReport-4b90a8ef-2db7-4e28-aba4-d5404f17eb18.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 5741c41a-04ec-4967-83b2-b0d746bd0ed5 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/5741c41a-04ec-4967-83b2-b0d746bd0ed5/MeasureReport-10dddf5e-f066-457d-b056-01329b17c73e.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 575f2da0-c890-47a3-b17f-f9e134a1096e ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/575f2da0-c890-47a3-b17f-f9e134a1096e/MeasureReport-1f13d7d0-55ce-47e5-8a23-cb74963fc616.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 5f739500-ee12-4662-8980-ef95d8fa74c8 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/5f739500-ee12-4662-8980-ef95d8fa74c8/MeasureReport-5dd7eca4-05b6-49c4-87b7-a7313b46d684.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 8bb999a1-696a-497b-a5f4-aa55e146a16e ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/8bb999a1-696a-497b-a5f4-aa55e146a16e/MeasureReport-f1938984-85bf-4eff-b9b8-e89a556b2f35.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 8e2cfc29-0925-45b9-857f-b9ee9b9fa248 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/8e2cfc29-0925-45b9-857f-b9ee9b9fa248/MeasureReport-b86669af-57ea-48d3-af7b-87c11d0e94b9.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 91ff5f1a-cfdb-472d-b8c3-144f499d1ccc ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/91ff5f1a-cfdb-472d-b8c3-144f499d1ccc/MeasureReport-cee9ae71-29f6-41ee-a479-0fc2d8b338c5.json) | Group_1 | Numerator | 1 | 0 | — |
| [ b0932ba4-4dfc-43ad-aa67-fbaee9638d3b ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/b0932ba4-4dfc-43ad-aa67-fbaee9638d3b/MeasureReport-980b1611-a5d1-4bab-ae2a-974cdd0b6f75.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b7783b8c-ba46-4509-a75e-203659abab3d ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/b7783b8c-ba46-4509-a75e-203659abab3d/MeasureReport-097d962a-0304-47fe-9c77-8fd8bd4b48ac.json) | Group_1 | Numerator | 1 | 0 | — |
| [ ccd7f9d7-35e8-4623-9f2e-f229cf7d829c ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/ccd7f9d7-35e8-4623-9f2e-f229cf7d829c/MeasureReport-c8c8144b-3bac-4663-aac9-9a786e5c1810.json) | Group_1 | Numerator | 1 | 0 | — |
| [ d205878e-b861-43a8-92e8-47f680987e4d ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/d205878e-b861-43a8-92e8-47f680987e4d/MeasureReport-e96f2279-a61f-40e2-9e19-9137ee4b12e6.json) | Group_1 | Numerator | 1 | 0 | — |
| [ d9b7ffa9-ed78-484c-8880-b4cbf2b4b6a1 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/d9b7ffa9-ed78-484c-8880-b4cbf2b4b6a1/MeasureReport-43331d8f-cf2d-4a0c-a3a2-e4b8e060a7eb.json) | Group_1 | Numerator | 1 | 0 | — |
| [ dba7c9af-eb6f-4836-ba24-650a5acc87e7 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/dba7c9af-eb6f-4836-ba24-650a5acc87e7/MeasureReport-7c3e8a2e-61ff-4a73-b3e6-d6b168cb4cc6.json) | Group_1 | Numerator | 1 | 0 | — |
| [ dc0dcb01-87f0-4e65-9c36-8cf6174abef1 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/dc0dcb01-87f0-4e65-9c36-8cf6174abef1/MeasureReport-7bc64137-ecc6-421a-bb2f-0177667a25b7.json) | Group_1 | Numerator | 1 | 0 | — |
| [ dd5a1e46-1b99-45a3-b4d3-1fde205d8a11 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/dd5a1e46-1b99-45a3-b4d3-1fde205d8a11/MeasureReport-bc945d90-f897-463b-bbc2-f9b922117784.json) | Group_1 | Numerator | 1 | 0 | — |
| [ ff814452-be6d-4e4b-905b-c1ae2a551645 ](../.././input/tests/measure/CMS108FHIRVTEProphylaxis/ff814452-be6d-4e4b-905b-c1ae2a551645/MeasureReport-8f09729a-45b0-45dc-bfdd-047cf0d896ef.json) | Group_1 | Numerator | 1 | 0 | — |


#### CMS117FHIRChildImmunStatus
[ [cql] ](../../input/cql/CMS117FHIRChildImmunStatus.cql) [ [test results] ](../../input/tests/results/CMS117FHIRChildImmunStatus.txt)

Mismatched Test Cases (8 of  of 45)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 104ee6b1-c36f-420c-bedd-0a2064f748d8 ](../.././input/tests/measure/CMS117FHIRChildImmunStatus/104ee6b1-c36f-420c-bedd-0a2064f748d8/MeasureReport-52c8995d-58f1-413a-b5bb-d0e5edddeae4.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 239d5e6f-38d3-461f-a2a1-52abe106e8bb ](../.././input/tests/measure/CMS117FHIRChildImmunStatus/239d5e6f-38d3-461f-a2a1-52abe106e8bb/MeasureReport-382384c3-a4c8-4b52-a5ab-1129c957c4d5.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 92ed2664-a594-4cac-9001-3044b14a02f7 ](../.././input/tests/measure/CMS117FHIRChildImmunStatus/92ed2664-a594-4cac-9001-3044b14a02f7/MeasureReport-7508d5b2-3858-4e4b-b699-f076405b16ee.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 9e57c539-0442-415a-a187-87adc7acdd8a ](../.././input/tests/measure/CMS117FHIRChildImmunStatus/9e57c539-0442-415a-a187-87adc7acdd8a/MeasureReport-2cc8e873-5006-4a7e-9bb2-3223667c6061.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ aeb0266c-a8ec-4262-a4bc-6bc343a85230 ](../.././input/tests/measure/CMS117FHIRChildImmunStatus/aeb0266c-a8ec-4262-a4bc-6bc343a85230/MeasureReport-583b5775-ec4f-4c12-9e56-9e164a0d669b.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b5f9f533-30c2-4fbe-b06e-3f8dccc8792c ](../.././input/tests/measure/CMS117FHIRChildImmunStatus/b5f9f533-30c2-4fbe-b06e-3f8dccc8792c/MeasureReport-29cfa3eb-f8f3-44d9-b70e-2a5cc5432fdb.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ dd1e534c-aa60-4ff3-a955-109f034b408f ](../.././input/tests/measure/CMS117FHIRChildImmunStatus/dd1e534c-aa60-4ff3-a955-109f034b408f/MeasureReport-088851e5-54bf-44b4-8fe1-fa0733cdcd31.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ fe0cb80b-232c-4c84-8b2a-f27eaf3078ff ](../.././input/tests/measure/CMS117FHIRChildImmunStatus/fe0cb80b-232c-4c84-8b2a-f27eaf3078ff/MeasureReport-8e4c6c23-db3f-42f5-972a-c31f33d1fd2f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS122FHIRDiabetesAssessGT9Pct
[ [cql] ](../../input/cql/CMS122FHIRDiabetesAssessGT9Pct.cql) [ [test results] ](../../input/tests/results/CMS122FHIRDiabetesAssessGT9Pct.txt)

Mismatched Test Cases (25 of  of 55)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 12ccd41a-83aa-405a-83b3-c756564c4de5 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/12ccd41a-83aa-405a-83b3-c756564c4de5/MeasureReport-b60e15c5-d245-4c59-9089-5b3440601ae9.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 3b62b0a8-44f2-4365-bcb9-7cadef5bab2e ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/3b62b0a8-44f2-4365-bcb9-7cadef5bab2e/MeasureReport-e85cf7dc-dcfc-4e0b-b68a-4f8ed1b9ddd4.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 63ae0b9f-2636-4bf3-85ef-4ff20bdb09de ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/63ae0b9f-2636-4bf3-85ef-4ff20bdb09de/MeasureReport-df039417-d939-44cd-863b-c48f210acb40.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 64ba4a87-8cf6-4cfb-b0e7-506dd08c8bbe ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/64ba4a87-8cf6-4cfb-b0e7-506dd08c8bbe/MeasureReport-687098af-4e64-45da-86f8-6bb70be03188.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 6b6a5f96-c2a8-43f1-a353-7b5700ecb031 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/6b6a5f96-c2a8-43f1-a353-7b5700ecb031/MeasureReport-5d9e9fa7-7fb0-4ea4-9e2a-89cb9ec2b721.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 6d9426d1-5554-4d6b-9ed0-e3736dd17482 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/6d9426d1-5554-4d6b-9ed0-e3736dd17482/MeasureReport-a3fbc91c-1b80-4662-bb94-b16208051dc6.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 6f0553ac-e12a-4af5-ad27-05339f4b4ec0 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/6f0553ac-e12a-4af5-ad27-05339f4b4ec0/MeasureReport-af9e410a-aa02-4a46-a7b5-3a2830aa89be.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 7d01a597-c0da-4bff-9bdd-f3516021db34 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/7d01a597-c0da-4bff-9bdd-f3516021db34/MeasureReport-2f7961e5-23ba-47b5-b859-099596ad98b2.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 7e69124d-ff34-4daf-b626-08d1283f71ba ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/7e69124d-ff34-4daf-b626-08d1283f71ba/MeasureReport-e0f91cb5-1173-45da-9018-e38fe9e12c5f.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 85b60f52-7b08-46f3-946b-cb317b28acf5 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/85b60f52-7b08-46f3-946b-cb317b28acf5/MeasureReport-2cb54ad7-4330-49a5-b559-4331cbe5334c.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 86a25ad7-3801-4297-a9a4-b36b5308c9e2 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/86a25ad7-3801-4297-a9a4-b36b5308c9e2/MeasureReport-305a18c3-f156-4d12-8800-6e649dad30b0.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 88b67805-bfef-411c-a191-12382d2c3104 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/88b67805-bfef-411c-a191-12382d2c3104/MeasureReport-f84a2836-1491-4c2d-bc2c-57bc32709693.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 8b8ded15-0118-4d0c-ac0f-6797528cefb9 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/8b8ded15-0118-4d0c-ac0f-6797528cefb9/MeasureReport-b48301b2-d97e-4b35-a443-48cd41fac97a.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 91986c00-e45b-4e7c-afa7-734d6fe43d16 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/91986c00-e45b-4e7c-afa7-734d6fe43d16/MeasureReport-68269ed5-a460-418c-b70f-3e5c174ed019.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 96cfe7f0-b4e1-4e2e-a48d-ef64fb64343d ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/96cfe7f0-b4e1-4e2e-a48d-ef64fb64343d/MeasureReport-7cb09dcc-72e5-4c62-8637-92c1002e717f.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 9cba6cfa-9671-4850-803d-e286c7d59ee7 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/9cba6cfa-9671-4850-803d-e286c7d59ee7/MeasureReport-4cf88428-9d18-4c27-a59f-189dc83cf084.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ ac4d7076-d1cb-44c6-a94f-c2c86266d53b ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/ac4d7076-d1cb-44c6-a94f-c2c86266d53b/MeasureReport-67ce8fb1-ed41-4823-ab6b-79dee31980f4.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ b6a4b9f8-21c1-44f2-a834-72f0906b4f88 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/b6a4b9f8-21c1-44f2-a834-72f0906b4f88/MeasureReport-1f69f5d9-c1c0-48fd-80a9-843a206bab83.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ cade5021-b1bf-43e9-a0a4-659c05b386d0 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/cade5021-b1bf-43e9-a0a4-659c05b386d0/MeasureReport-373f8db2-50fb-450e-8e83-c2b1ef94aa93.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ e2b82999-6313-40af-bc8b-9ddf5f97795f ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/e2b82999-6313-40af-bc8b-9ddf5f97795f/MeasureReport-57b71351-8c5b-4c1e-b26d-537e727a527c.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ e61be907-af68-493f-a6bc-3d93ef8b6c6e ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/e61be907-af68-493f-a6bc-3d93ef8b6c6e/MeasureReport-a4a3ee93-9b96-4259-9158-e9a1f4929c1f.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ eacbadee-87f7-4ed0-bfc3-b5533128dcbc ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/eacbadee-87f7-4ed0-bfc3-b5533128dcbc/MeasureReport-07a87bff-310c-4747-89f5-dac13c140e27.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ ede0ee7a-18ab-4ba7-934c-23618f1270ea ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/ede0ee7a-18ab-4ba7-934c-23618f1270ea/MeasureReport-ac90199a-d913-470f-85f0-801ea59d5f06.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ f4eeba51-a6fc-4ffd-bd62-49fd1c375f01 ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/f4eeba51-a6fc-4ffd-bd62-49fd1c375f01/MeasureReport-e375ec29-d1c1-4b3b-ad70-82d5679427f0.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ f5771b74-a7de-439a-a51f-49a3863e086b ](../.././input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/f5771b74-a7de-439a-a51f-49a3863e086b/MeasureReport-50f84e99-bb0e-4b7c-bc0b-b81dfb59c503.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |


#### CMS124FHIRCervicalCancerScreen
[ [cql] ](../../input/cql/CMS124FHIRCervicalCancerScreen.cql) [ [test results] ](../../input/tests/results/CMS124FHIRCervicalCancerScreen.txt)

Mismatched Test Cases (13 of  of 34)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 05cbc93d-e748-4bca-b68d-3011ebf68e28 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/05cbc93d-e748-4bca-b68d-3011ebf68e28/MeasureReport-ac66e7a1-8260-427e-937a-cd9df836e72a.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 0e296f04-855b-42ad-aa20-295a719a96e5 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/0e296f04-855b-42ad-aa20-295a719a96e5/MeasureReport-fcfadc9c-df80-4993-a06a-f3a98baf6803.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 3aef97c8-9529-433c-95d3-ea01f188e156 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/3aef97c8-9529-433c-95d3-ea01f188e156/MeasureReport-e1cbc9e6-5ffe-421c-9e55-10f06668eaa4.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 62bd7a1e-f946-435f-8898-39db9d870940 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/62bd7a1e-f946-435f-8898-39db9d870940/MeasureReport-2da3f0a6-31e1-4b71-93ba-e54b17bc2126.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 65a9a258-c453-484f-902c-743e678b44a4 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/65a9a258-c453-484f-902c-743e678b44a4/MeasureReport-081101f5-4fcc-41b8-bf0f-07c681af8697.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 6ee7c92c-c8cd-4025-8002-ca1253ba830b ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/6ee7c92c-c8cd-4025-8002-ca1253ba830b/MeasureReport-2b01b1fa-eebc-4298-a356-bc06ef30edbc.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 7e41f717-097e-45a7-9a00-1e0ad852cb44 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/7e41f717-097e-45a7-9a00-1e0ad852cb44/MeasureReport-e78c1f1e-cc47-466f-a6d7-5dab77d27fe5.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 8723dbb4-f60f-488a-9da3-f02f04ea03bf ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/8723dbb4-f60f-488a-9da3-f02f04ea03bf/MeasureReport-ea856e36-a8f1-44dd-9bb4-fd97e28e0b6b.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ ab346cb5-2c55-4171-93ea-aac9d266e6c7 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/ab346cb5-2c55-4171-93ea-aac9d266e6c7/MeasureReport-35cb669c-85a8-4056-b974-a566c232962c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ c6ec1681-b011-425a-a850-4e187e9fd927 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/c6ec1681-b011-425a-a850-4e187e9fd927/MeasureReport-3afc698c-61bc-4a84-8e46-fd4768b7299d.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ cadbffa0-20b2-4c26-b202-75b9edfd0a07 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/cadbffa0-20b2-4c26-b202-75b9edfd0a07/MeasureReport-34b8c4ca-d69c-43da-87a7-a7d72ef39a09.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d986061c-de3e-4d5d-95e7-f5ec93c5665c ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/d986061c-de3e-4d5d-95e7-f5ec93c5665c/MeasureReport-ddce5657-8d40-4d72-8109-f7c3e1ebd091.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ e8813151-9334-41d7-ab4b-1d597f08d4a9 ](../.././input/tests/measure/CMS124FHIRCervicalCancerScreen/e8813151-9334-41d7-ab4b-1d597f08d4a9/MeasureReport-265b4c27-2701-4ef0-a8e0-28e1b0c0cf98.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS125FHIRBreastCancerScreen
[ [cql] ](../../input/cql/CMS125FHIRBreastCancerScreen.cql) [ [test results] ](../../input/tests/results/CMS125FHIRBreastCancerScreen.txt)

Mismatched Test Cases (26 of  of 66)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 01c88972-84e2-4594-835b-924481b9990a ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/01c88972-84e2-4594-835b-924481b9990a/MeasureReport-e676f8fb-fbc5-4323-8f2f-df0cfdd80b9d.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 0930082c-fda1-42e8-a15f-92ceaefa5908 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/0930082c-fda1-42e8-a15f-92ceaefa5908/MeasureReport-7a4f414d-68b6-4a95-9c19-e5cbec4f2605.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 0beefd14-c554-4f1e-856c-c8696177ce9e ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/0beefd14-c554-4f1e-856c-c8696177ce9e/MeasureReport-5e9d1098-0613-4441-ac17-09a992fd6dee.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 0ced1e0c-9c92-4582-a4b1-e44f130e436f ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/0ced1e0c-9c92-4582-a4b1-e44f130e436f/MeasureReport-a6399df7-7d9a-45da-a64b-97f695646ce6.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 14193177-2f4e-4480-a471-87ff9d137a8b ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/14193177-2f4e-4480-a471-87ff9d137a8b/MeasureReport-360de092-eb92-49f7-958d-47bc1e79c3cd.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 14b87edd-7f1e-4f6a-9910-f905966ec904 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/14b87edd-7f1e-4f6a-9910-f905966ec904/MeasureReport-eb7ec114-0c95-4e73-98ad-772a8197ffff.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 24557438-17c9-405c-88dc-0c0bfda17d27 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/24557438-17c9-405c-88dc-0c0bfda17d27/MeasureReport-f2a7180d-acd8-4394-acdd-8959d861ef65.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 2886b1b6-5834-4788-8cd7-b54bbda54ca9 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/2886b1b6-5834-4788-8cd7-b54bbda54ca9/MeasureReport-72062307-5e9c-4b35-858b-b1ac46b877f2.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 3ea0a87a-3ded-4939-920a-4e69bc20a26f ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/3ea0a87a-3ded-4939-920a-4e69bc20a26f/MeasureReport-6e528bdf-df67-4f23-af00-fc257b686d14.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 461f1aab-e645-4973-ae9a-4c09bfaef59a ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/461f1aab-e645-4973-ae9a-4c09bfaef59a/MeasureReport-0709b11a-1a4d-482d-b2a1-e562f15ab9f6.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4f10a0f7-bb14-40d5-beb2-c728eb88a30d ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/4f10a0f7-bb14-40d5-beb2-c728eb88a30d/MeasureReport-6b17ecfe-be06-4b57-b9dc-771f4f180d0d.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 5c8bffdf-7ef4-44e1-af5a-8a64f1b7e545 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/5c8bffdf-7ef4-44e1-af5a-8a64f1b7e545/MeasureReport-b814bacf-21ef-46e4-bd83-73c0dd5ad2a6.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 5e3f01ad-1eda-4cb7-8d37-1146beae59e9 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/5e3f01ad-1eda-4cb7-8d37-1146beae59e9/MeasureReport-ac67c1e3-d0df-4745-bc85-d4ec0a18e8f3.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 5fd02264-fd4e-4eb7-a635-0023876920ac ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/5fd02264-fd4e-4eb7-a635-0023876920ac/MeasureReport-ef76250a-2408-42d0-9147-1cc0b459090e.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 62901c95-5d12-45e8-b5b1-d131e36d8299 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/62901c95-5d12-45e8-b5b1-d131e36d8299/MeasureReport-1129152b-fe9b-4ccf-b28b-71bada6d3088.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 73f77133-4d08-438a-ac81-6bb858a74c31 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/73f77133-4d08-438a-ac81-6bb858a74c31/MeasureReport-ffe8b795-6293-4c6e-915c-ffb0923c2297.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 7a09940e-c3c8-49a7-bf09-eaf9df116dfb ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/7a09940e-c3c8-49a7-bf09-eaf9df116dfb/MeasureReport-6ee6dbd2-a3c8-4c36-b129-ef136ee08d8d.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 8278ae07-69ec-469c-ae01-e933d051f764 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/8278ae07-69ec-469c-ae01-e933d051f764/MeasureReport-ee5db0d0-8af1-4521-a060-aed5b026e194.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 8a0f6b6e-fb1c-4e60-b150-b88d1a4e487b ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/8a0f6b6e-fb1c-4e60-b150-b88d1a4e487b/MeasureReport-874b2823-67e5-48c4-916a-3457357a1508.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 99b68a44-5e66-4c37-a513-80db8b6249ce ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/99b68a44-5e66-4c37-a513-80db8b6249ce/MeasureReport-49135ebe-fd39-4017-aacf-88e191d3125d.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ adb08da2-b4d0-4916-9b9c-7c2c86e1042b ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/adb08da2-b4d0-4916-9b9c-7c2c86e1042b/MeasureReport-28a4057b-1650-4474-b2d8-14ddee97ae4b.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ bbb391da-9572-4954-be95-3ea00eb31c91 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/bbb391da-9572-4954-be95-3ea00eb31c91/MeasureReport-44e2a7d7-b35b-4902-a4d9-d89ff4221755.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ cc1a4555-2e3e-43ac-bbca-6e44ea41b2f3 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/cc1a4555-2e3e-43ac-bbca-6e44ea41b2f3/MeasureReport-ff2520e5-8d79-493c-b3a0-76278531021d.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d4540640-2561-4ebd-b7c6-15878a4dc582 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/d4540640-2561-4ebd-b7c6-15878a4dc582/MeasureReport-2e186c68-d7f4-4b2e-9f8a-e73c79905e7e.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ da85601e-ce6f-4351-b639-1e58c725bf2f ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/da85601e-ce6f-4351-b639-1e58c725bf2f/MeasureReport-699e12b2-26d4-43a8-add0-bcdd6629fe88.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ f38ce16a-658f-4aa0-b4a6-fac61d2e58a8 ](../.././input/tests/measure/CMS125FHIRBreastCancerScreen/f38ce16a-658f-4aa0-b4a6-fac61d2e58a8/MeasureReport-81d2ade5-fa91-428c-b39f-3f0b8b7b2c16.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS128FHIRAntidepressantMgmt
[ [cql] ](../../input/cql/CMS128FHIRAntidepressantMgmt.cql) [ [test results] ](../../input/tests/results/CMS128FHIRAntidepressantMgmt.txt)

Mismatched Test Cases (16 of  of 58)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 4c2caf57-7168-4149-a596-d0914d7e3fe8 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/4c2caf57-7168-4149-a596-d0914d7e3fe8/MeasureReport-d54be6a1-34f9-4bcf-8813-b88a88e77dd4.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4c2caf57-7168-4149-a596-d0914d7e3fe8 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/4c2caf57-7168-4149-a596-d0914d7e3fe8/MeasureReport-d54be6a1-34f9-4bcf-8813-b88a88e77dd4.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 62ea0c3d-46da-48a1-87dd-d1927ed2df75 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/62ea0c3d-46da-48a1-87dd-d1927ed2df75/MeasureReport-c5f73be1-d764-49ce-99f1-ff26ef3b5ab4.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 62ea0c3d-46da-48a1-87dd-d1927ed2df75 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/62ea0c3d-46da-48a1-87dd-d1927ed2df75/MeasureReport-c5f73be1-d764-49ce-99f1-ff26ef3b5ab4.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 71cc96f3-e525-4e60-b6ad-1037d16a3c17 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/71cc96f3-e525-4e60-b6ad-1037d16a3c17/MeasureReport-1a81e173-6952-4b14-a900-42bb57c7cac9.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 71cc96f3-e525-4e60-b6ad-1037d16a3c17 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/71cc96f3-e525-4e60-b6ad-1037d16a3c17/MeasureReport-1a81e173-6952-4b14-a900-42bb57c7cac9.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 76e30d44-a803-4b4b-a6ba-f11de6fa6329 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/76e30d44-a803-4b4b-a6ba-f11de6fa6329/MeasureReport-e5421ac8-6753-4347-8376-227608513a8a.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 76e30d44-a803-4b4b-a6ba-f11de6fa6329 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/76e30d44-a803-4b4b-a6ba-f11de6fa6329/MeasureReport-e5421ac8-6753-4347-8376-227608513a8a.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/7bda86fd-7b20-45e1-8c2e-e0a24c785dd0/MeasureReport-0989708a-3ae2-403b-9065-94d2956c95c8.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/7bda86fd-7b20-45e1-8c2e-e0a24c785dd0/MeasureReport-0989708a-3ae2-403b-9065-94d2956c95c8.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 925ef058-b2e2-489e-8d5e-1a33299efa30 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/925ef058-b2e2-489e-8d5e-1a33299efa30/MeasureReport-dc08d7f5-4936-4f01-b64b-5243ff9ebc40.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 925ef058-b2e2-489e-8d5e-1a33299efa30 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/925ef058-b2e2-489e-8d5e-1a33299efa30/MeasureReport-dc08d7f5-4936-4f01-b64b-5243ff9ebc40.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ b371fd28-5026-43db-840e-21466bde11c9 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/b371fd28-5026-43db-840e-21466bde11c9/MeasureReport-4d7d54dd-876b-438e-a90a-0cf9e012497f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b371fd28-5026-43db-840e-21466bde11c9 ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/b371fd28-5026-43db-840e-21466bde11c9/MeasureReport-4d7d54dd-876b-438e-a90a-0cf9e012497f.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ ee6d52b0-149c-4ffe-b260-bb214151652c ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/ee6d52b0-149c-4ffe-b260-bb214151652c/MeasureReport-d065776d-41a1-43fc-8e72-f5dc32741f4c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ ee6d52b0-149c-4ffe-b260-bb214151652c ](../.././input/tests/measure/CMS128FHIRAntidepressantMgmt/ee6d52b0-149c-4ffe-b260-bb214151652c/MeasureReport-d065776d-41a1-43fc-8e72-f5dc32741f4c.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |


#### CMS130FHIRColorectalCancerScrn
[ [cql] ](../../input/cql/CMS130FHIRColorectalCancerScrn.cql) [ [test results] ](../../input/tests/results/CMS130FHIRColorectalCancerScrn.txt)

Mismatched Test Cases (17 of  of 64)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 007ec5f1-08cf-474a-a472-f6a92cca4b79 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/007ec5f1-08cf-474a-a472-f6a92cca4b79/MeasureReport-1f790f7a-6451-49d4-8749-218958c2ae80.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 02488708-2ac0-4814-828c-04b8be9b1e70 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/02488708-2ac0-4814-828c-04b8be9b1e70/MeasureReport-1aeea0f3-b0f1-4c07-92b9-3651e1a2cdd3.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 0f930f59-9061-4b28-b2e5-21cc5ab6b613 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/0f930f59-9061-4b28-b2e5-21cc5ab6b613/MeasureReport-6ebe3bb3-aae4-4f28-8198-f00d6c451797.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 46635c8a-3f72-4424-98ae-01b849d0ff19 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/46635c8a-3f72-4424-98ae-01b849d0ff19/MeasureReport-14848d95-4b53-41e0-9f17-3677181a1c72.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4e1abf20-b68c-401b-9a33-fdf9bc765005 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/4e1abf20-b68c-401b-9a33-fdf9bc765005/MeasureReport-8c11cb6d-a601-47e7-a910-0d47488b9769.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 59128a5c-f9da-4cb3-9e98-97ee67380533 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/59128a5c-f9da-4cb3-9e98-97ee67380533/MeasureReport-2bbe889d-f9f2-44cf-8d34-eecba30afe9b.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 5fd0d61d-d5e0-4138-8a8d-6e3969af6107 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/5fd0d61d-d5e0-4138-8a8d-6e3969af6107/MeasureReport-8155e372-74fd-49e6-bd5f-330536b7bce6.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 6dbaf3b3-8c47-4e0a-91fe-2ec06f2f0339 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/6dbaf3b3-8c47-4e0a-91fe-2ec06f2f0339/MeasureReport-10e2ca7a-ea38-430d-a3b6-661c1c4562be.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 6f6cdf8c-e562-4113-bf5d-f91237b975a5 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/6f6cdf8c-e562-4113-bf5d-f91237b975a5/MeasureReport-befb6056-35d0-485b-8970-f4cd2adcfbda.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 7ee1a25c-a4c7-4bd2-8670-4083b32ecc70 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/7ee1a25c-a4c7-4bd2-8670-4083b32ecc70/MeasureReport-7d04b754-45f9-40ef-a545-5eba7a7c4c3d.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ a989a58f-82c5-4221-addb-5e29c2514df7 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/a989a58f-82c5-4221-addb-5e29c2514df7/MeasureReport-e5cf69e7-079f-49bf-9ab3-734d461bf051.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b70f2fc0-3254-4240-af70-793cd1bc90b2 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/b70f2fc0-3254-4240-af70-793cd1bc90b2/MeasureReport-ba581c59-3c3d-4422-be16-35fba150b12d.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d0c9e870-5e7b-4a9e-b34d-9d600ff8c1c6 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/d0c9e870-5e7b-4a9e-b34d-9d600ff8c1c6/MeasureReport-f894699d-3ec6-4ab7-b2e0-429332184cd3.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ dcaccac3-ef0d-4755-becd-3e6aebe2a06a ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/dcaccac3-ef0d-4755-becd-3e6aebe2a06a/MeasureReport-0c770776-f8b6-47c5-bf6c-555d4edfb807.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ df62e712-a702-4c1e-82c6-4676578371f9 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/df62e712-a702-4c1e-82c6-4676578371f9/MeasureReport-713ee9c5-7e80-4a21-89c9-3892325c33c5.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ f9ef1fd1-cced-47ad-a47b-d9c20254511c ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/f9ef1fd1-cced-47ad-a47b-d9c20254511c/MeasureReport-c93af428-5af9-4a94-bc1e-4c5aaa6ba707.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ fede210f-db17-4e0a-9bcd-5dc383f0fb93 ](../.././input/tests/measure/CMS130FHIRColorectalCancerScrn/fede210f-db17-4e0a-9bcd-5dc383f0fb93/MeasureReport-f9cd4db0-1d53-42d9-80f9-0a8a5efabb7e.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS131FHIRDiabetesEyeExam
[ [cql] ](../../input/cql/CMS131FHIRDiabetesEyeExam.cql) [ [test results] ](../../input/tests/results/CMS131FHIRDiabetesEyeExam.txt)

Mismatched Test Cases (24 of  of 63)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 01a1241d-fd97-4c72-b288-fd31c4c7ae80 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/01a1241d-fd97-4c72-b288-fd31c4c7ae80/MeasureReport-f6d5405c-4d38-4df3-985e-564f0da456f7.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 0c9d7ae1-4643-4c50-bc48-0274a3f2d234 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/0c9d7ae1-4643-4c50-bc48-0274a3f2d234/MeasureReport-bde81f56-68a5-4cff-849e-d768bd2e48a1.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 106633c6-3739-442f-b7cc-7269399481cf ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/106633c6-3739-442f-b7cc-7269399481cf/MeasureReport-6ebd64d2-c85b-4578-a340-a516a0e5675b.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 36222907-f670-4253-a251-63198bb3fc6c ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/36222907-f670-4253-a251-63198bb3fc6c/MeasureReport-e5009366-6515-4f9d-a77b-a47c0dd24f39.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 3624228c-097b-4f91-9211-f29f72b8ddaf ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/3624228c-097b-4f91-9211-f29f72b8ddaf/MeasureReport-e74b5bd2-156c-483a-a067-59b7b0e6db5e.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4eaa0238-d22c-44c2-a91e-81239a497359 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/4eaa0238-d22c-44c2-a91e-81239a497359/MeasureReport-04e1a1e5-8015-4ce9-8834-0d8d1241223c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 61dfb0bd-8fe0-4e30-a911-fa07c782afd9 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/61dfb0bd-8fe0-4e30-a911-fa07c782afd9/MeasureReport-b60724c0-7608-4762-8f76-9f60a2aa00bc.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 65c895d1-ba13-410a-bcfc-be3b771b5eb8 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/65c895d1-ba13-410a-bcfc-be3b771b5eb8/MeasureReport-34b8740c-235f-4866-affb-a92533914b6d.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 728333bf-6ff0-4d29-9181-3b6a30b7059a ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/728333bf-6ff0-4d29-9181-3b6a30b7059a/MeasureReport-a2107eca-c487-4933-a228-ad6854376616.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 8ffd1c24-67a9-4991-86cb-3378a45ffd6e ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/8ffd1c24-67a9-4991-86cb-3378a45ffd6e/MeasureReport-ca2cc70b-1082-456d-a454-48be6e80c1e4.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 97935b1b-262b-4c05-9a56-2124a3aa1de0 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/97935b1b-262b-4c05-9a56-2124a3aa1de0/MeasureReport-4b146bcc-55aa-4d05-b9f7-293f08f6c828.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ a6cd48c6-fb25-41d4-aea4-da7fb856cc12 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/a6cd48c6-fb25-41d4-aea4-da7fb856cc12/MeasureReport-20db834b-59b3-445d-8c06-5fbda3fb0d62.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b3af1243-c45d-4061-8d36-baa6de256376 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/b3af1243-c45d-4061-8d36-baa6de256376/MeasureReport-197c1fc9-63f3-481a-905f-102110b77fe8.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b7a8c85e-3608-44ec-be34-c9089fa3dd17 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/b7a8c85e-3608-44ec-be34-c9089fa3dd17/MeasureReport-050adf2c-b0d0-4601-8bb9-ddf975e090cd.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ c1340d6e-581d-4775-a0af-b8dcdbcf7320 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/c1340d6e-581d-4775-a0af-b8dcdbcf7320/MeasureReport-eb978a3a-121c-47d5-bc4a-043cab1352eb.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ c36eddf7-a780-480c-baf8-ef865ccdb9d2 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/c36eddf7-a780-480c-baf8-ef865ccdb9d2/MeasureReport-1da1fe0c-0bcc-4b24-9bd6-6d309c1ecd92.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d4091ecf-638c-41ae-bae9-2b0c3bea864e ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/d4091ecf-638c-41ae-bae9-2b0c3bea864e/MeasureReport-10397419-4a20-4c3b-a57b-317cc9b6a2a1.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d46ab51c-9b21-4b1c-b1dd-090c7f3e831a ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/d46ab51c-9b21-4b1c-b1dd-090c7f3e831a/MeasureReport-47e3c1ff-3e0f-4d77-83cd-f4acbea86e90.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d8946843-06c7-4b82-992a-91a9c20ec7c0 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/d8946843-06c7-4b82-992a-91a9c20ec7c0/MeasureReport-c2e6ca0d-6330-4d32-92b0-85150dfdd9e3.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ e9b9b388-e663-4533-8484-7d930efd1851 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/e9b9b388-e663-4533-8484-7d930efd1851/MeasureReport-c4c2d471-952b-4e84-8daf-41671f16d202.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ ea0e556f-387e-4883-a320-047aa3a238e4 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/ea0e556f-387e-4883-a320-047aa3a238e4/MeasureReport-971f1137-8a16-44a1-b144-ed49125f9a93.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ f0b61b7a-4381-486d-9eee-2128ada5280a ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/f0b61b7a-4381-486d-9eee-2128ada5280a/MeasureReport-0dfe6124-5f9e-4c30-a52f-e6c50f17e949.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ f45a1cb0-d1a7-42cf-9cae-6ea6c7799085 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/f45a1cb0-d1a7-42cf-9cae-6ea6c7799085/MeasureReport-f6941c4e-ecb8-40af-b82f-acfa7880f882.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ f77b9abc-9c77-4e75-96c8-cc3bf25e08f4 ](../.././input/tests/measure/CMS131FHIRDiabetesEyeExam/f77b9abc-9c77-4e75-96c8-cc3bf25e08f4/MeasureReport-5a0399ed-8998-4d4b-bd3a-e34e2aeea795.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS135FHIRACEIorARBorARNIforHF
[ [cql] ](../../input/cql/CMS135FHIRACEIorARBorARNIforHF.cql) [ [test results] ](../../input/tests/results/CMS135FHIRACEIorARBorARNIforHF.txt)

Missing Results (3 of 40 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ c095195c-8893-4bf1-aa7d-ad2bfd9bafa5 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/c095195c-8893-4bf1-aa7d-ad2bfd9bafa5/MeasureReport-f2d033da-6f32-46dc-86bc-69fdf82b1cfd.json) | Group_1 | E-11 — resolution pending |
| [ cba5a449-1c45-4e11-ae0b-ba3974b410f7 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/cba5a449-1c45-4e11-ae0b-ba3974b410f7/MeasureReport-ae8c4b99-af76-4577-b66d-b1230ac09aa3.json) | Group_1 | E-11 — resolution pending |
| [ ec508dbb-76f6-4878-b8a2-114ea8e82297 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/ec508dbb-76f6-4878-b8a2-114ea8e82297/MeasureReport-d1b704c8-7e95-4cd9-89e7-a8b90f925ce2.json) | Group_1 | E-11 — resolution pending |


Mismatched Test Cases (9 of  of 40)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 149c3a7c-2b80-47f8-b50d-5c1d233eedb7 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/149c3a7c-2b80-47f8-b50d-5c1d233eedb7/MeasureReport-d8d9ace4-d191-4aff-a0e4-6de581275357.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 1f64a697-a90b-4aaf-a315-fa84168ac2b4 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/1f64a697-a90b-4aaf-a315-fa84168ac2b4/MeasureReport-cf4fe385-8e6f-4642-b1e5-ca08159c0b53.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 298d5342-fa0a-4386-bf48-b9c977a1c367 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/298d5342-fa0a-4386-bf48-b9c977a1c367/MeasureReport-090aa645-1e2b-44df-b6c0-2419bea96186.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 4bc4883f-0770-4a68-824a-5fa4dba72638 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/4bc4883f-0770-4a68-824a-5fa4dba72638/MeasureReport-d4dc5571-57c9-4b1b-95d9-a09ac4c6e34d.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 5b7e720f-e2fc-4779-9b1c-3f34a0241482 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/5b7e720f-e2fc-4779-9b1c-3f34a0241482/MeasureReport-01fb5443-0f43-487e-ac44-f7cc6e163ca0.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 64e76766-9760-4385-a977-cbe8136ce425 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/64e76766-9760-4385-a977-cbe8136ce425/MeasureReport-0488a022-da7e-4dcf-a9af-7e2fbf5e9423.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 6a86918d-3f69-43c8-8863-1d0bf835a2c7 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/6a86918d-3f69-43c8-8863-1d0bf835a2c7/MeasureReport-3decfa0c-9100-4194-9643-c3065c1a253f.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ d18e37a6-7b66-4e7c-b305-692872c13f8d ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/d18e37a6-7b66-4e7c-b305-692872c13f8d/MeasureReport-ecbb5067-dcb1-48ce-8e78-6dfd556ac43d.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ d297e68e-3f02-42a8-a59f-a5a4cecbd47d ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/d297e68e-3f02-42a8-a59f-a5a4cecbd47d/MeasureReport-cc3a4e83-9689-4bb7-83e1-55cb47dc9848.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |


#### CMS136FHIRChildADHDMedFollowUp
[ [cql] ](../../input/cql/CMS136FHIRChildADHDMedFollowUp.cql) [ [test results] ](../../input/tests/results/CMS136FHIRChildADHDMedFollowUp.txt)

Mismatched Test Cases (23 of  of 128)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 0039c514-9277-46cd-9e6a-2f402b5357f5 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/0039c514-9277-46cd-9e6a-2f402b5357f5/MeasureReport-27ccf017-c687-42a0-83f3-9943fec666c4.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 0039c514-9277-46cd-9e6a-2f402b5357f5 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/0039c514-9277-46cd-9e6a-2f402b5357f5/MeasureReport-27ccf017-c687-42a0-83f3-9943fec666c4.json) | Group_2 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 00f27092-14a7-4d87-b35a-5a112ca99201 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/00f27092-14a7-4d87-b35a-5a112ca99201/MeasureReport-351a58d1-450e-4d51-bb86-85a7169aecef.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 048c41bc-fe7e-465f-bc10-6ccf7a7d5250 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/048c41bc-fe7e-465f-bc10-6ccf7a7d5250/MeasureReport-b5595330-1ead-452b-8c77-b50bcdcd54c1.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 304b7ef3-bd6c-488e-9409-70039f1da018 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/304b7ef3-bd6c-488e-9409-70039f1da018/MeasureReport-43945c4a-b476-4b15-826a-4e28eafa432a.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 304b7ef3-bd6c-488e-9409-70039f1da018 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/304b7ef3-bd6c-488e-9409-70039f1da018/MeasureReport-43945c4a-b476-4b15-826a-4e28eafa432a.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 5e536adf-1159-404e-92e7-94d4f1affd98 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/5e536adf-1159-404e-92e7-94d4f1affd98/MeasureReport-dbd4f8f2-fffe-42a7-b8bc-ce0d020db2a2.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 5e536adf-1159-404e-92e7-94d4f1affd98 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/5e536adf-1159-404e-92e7-94d4f1affd98/MeasureReport-dbd4f8f2-fffe-42a7-b8bc-ce0d020db2a2.json) | Group_2 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 6a96556a-075b-4361-8a8d-fd8c8b4f125a ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/6a96556a-075b-4361-8a8d-fd8c8b4f125a/MeasureReport-a435ce24-836c-4333-ba07-54da75315920.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 6a96556a-075b-4361-8a8d-fd8c8b4f125a ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/6a96556a-075b-4361-8a8d-fd8c8b4f125a/MeasureReport-a435ce24-836c-4333-ba07-54da75315920.json) | Group_2 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 71a21841-f5bb-4e75-9328-aedf3cdc8a34 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/71a21841-f5bb-4e75-9328-aedf3cdc8a34/MeasureReport-81da1daa-f63c-4229-80e3-f8926ede352b.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 71a21841-f5bb-4e75-9328-aedf3cdc8a34 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/71a21841-f5bb-4e75-9328-aedf3cdc8a34/MeasureReport-81da1daa-f63c-4229-80e3-f8926ede352b.json) | Group_2 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 80644a49-f67d-4124-9c58-1547b7bdd779 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/80644a49-f67d-4124-9c58-1547b7bdd779/MeasureReport-14bfe2e1-f99b-4f2c-9955-9485a9773c03.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 80644a49-f67d-4124-9c58-1547b7bdd779 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/80644a49-f67d-4124-9c58-1547b7bdd779/MeasureReport-14bfe2e1-f99b-4f2c-9955-9485a9773c03.json) | Group_2 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 98e5cde7-fc04-4b89-9aef-5272087bb5c2 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/98e5cde7-fc04-4b89-9aef-5272087bb5c2/MeasureReport-ab660866-e477-4fbd-9806-fe1ba0bc3eca.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ bee979d5-c118-4e1d-b190-62cf0e084bd1 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/bee979d5-c118-4e1d-b190-62cf0e084bd1/MeasureReport-56c09b31-05f1-4a01-a158-e33bf739b46c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ c8559a93-63e3-4bce-b0a6-01a85fb6db28 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/c8559a93-63e3-4bce-b0a6-01a85fb6db28/MeasureReport-d379a074-da21-4fb2-a7d5-4c67bcedc8ba.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ c8559a93-63e3-4bce-b0a6-01a85fb6db28 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/c8559a93-63e3-4bce-b0a6-01a85fb6db28/MeasureReport-d379a074-da21-4fb2-a7d5-4c67bcedc8ba.json) | Group_2 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ cb044844-e03d-4758-bf40-1e4db68ed10e ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/cb044844-e03d-4758-bf40-1e4db68ed10e/MeasureReport-db80673e-5d6e-4d5d-ba02-ebb033fc854f.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ cb044844-e03d-4758-bf40-1e4db68ed10e ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/cb044844-e03d-4758-bf40-1e4db68ed10e/MeasureReport-db80673e-5d6e-4d5d-ba02-ebb033fc854f.json) | Group_2 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ d95789b9-f144-43e7-81c6-fed3adba5d8f ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/d95789b9-f144-43e7-81c6-fed3adba5d8f/MeasureReport-ce74b3ee-e4db-4dd0-b489-c946a0e96df5.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ db99ef01-a9e9-47c9-a2d5-5cb9c2b23241 ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/db99ef01-a9e9-47c9-a2d5-5cb9c2b23241/MeasureReport-36fae350-739a-48f5-bbd5-b96b3e05d395.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ e5a26079-76db-4851-a15a-7dae023a25ce ](../.././input/tests/measure/CMS136FHIRChildADHDMedFollowUp/e5a26079-76db-4851-a15a-7dae023a25ce/MeasureReport-e6fb73a8-cb41-433d-97e3-f0a54f9a5659.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS137FHIRSUDTxInitEngagement
[ [cql] ](../../input/cql/CMS137FHIRSUDTxInitEngagement.cql) [ [test results] ](../../input/tests/results/CMS137FHIRSUDTxInitEngagement.txt)

Mismatched Test Cases (18 of  of 90)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 19b5b244-6834-40f7-b8a2-ff2c6fb84fb0 ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/19b5b244-6834-40f7-b8a2-ff2c6fb84fb0/MeasureReport-f5557134-9b6a-4c28-9607-151c0a3c416a.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 19b5b244-6834-40f7-b8a2-ff2c6fb84fb0 ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/19b5b244-6834-40f7-b8a2-ff2c6fb84fb0/MeasureReport-f5557134-9b6a-4c28-9607-151c0a3c416a.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 19e9d2c7-4030-46c9-80e5-8c71fcae5227 ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/19e9d2c7-4030-46c9-80e5-8c71fcae5227/MeasureReport-af961b5c-c44a-419e-9418-7d78756e7976.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 19e9d2c7-4030-46c9-80e5-8c71fcae5227 ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/19e9d2c7-4030-46c9-80e5-8c71fcae5227/MeasureReport-af961b5c-c44a-419e-9418-7d78756e7976.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 3698ad63-09e3-46e8-ba42-39c9cd235603 ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/3698ad63-09e3-46e8-ba42-39c9cd235603/MeasureReport-633918c1-ce60-4a4b-b4fe-8cbb531a2526.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 3698ad63-09e3-46e8-ba42-39c9cd235603 ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/3698ad63-09e3-46e8-ba42-39c9cd235603/MeasureReport-633918c1-ce60-4a4b-b4fe-8cbb531a2526.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 404859c4-6f6e-4376-ae4d-d02a479e62aa ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/404859c4-6f6e-4376-ae4d-d02a479e62aa/MeasureReport-09a79f8c-e435-43de-a70d-7f7ba2254bbf.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 404859c4-6f6e-4376-ae4d-d02a479e62aa ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/404859c4-6f6e-4376-ae4d-d02a479e62aa/MeasureReport-09a79f8c-e435-43de-a70d-7f7ba2254bbf.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 408f327a-94aa-4787-a1c6-e6fc7fde341d ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/408f327a-94aa-4787-a1c6-e6fc7fde341d/MeasureReport-0b364380-31bf-47ee-9c41-01fc2ab484a4.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 408f327a-94aa-4787-a1c6-e6fc7fde341d ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/408f327a-94aa-4787-a1c6-e6fc7fde341d/MeasureReport-0b364380-31bf-47ee-9c41-01fc2ab484a4.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 46954fc1-3432-4e5d-b920-a2087f01abba ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/46954fc1-3432-4e5d-b920-a2087f01abba/MeasureReport-6a3216bb-7d25-4219-aa62-58c1f0170972.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 46954fc1-3432-4e5d-b920-a2087f01abba ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/46954fc1-3432-4e5d-b920-a2087f01abba/MeasureReport-6a3216bb-7d25-4219-aa62-58c1f0170972.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 6fc30283-94af-4a06-8325-cbc65e9b4b7c ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/6fc30283-94af-4a06-8325-cbc65e9b4b7c/MeasureReport-3ec739bd-5cf7-472c-969e-d80429839200.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 6fc30283-94af-4a06-8325-cbc65e9b4b7c ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/6fc30283-94af-4a06-8325-cbc65e9b4b7c/MeasureReport-3ec739bd-5cf7-472c-969e-d80429839200.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 8715fad1-2969-418a-b3d3-45b2581f4fe3 ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/8715fad1-2969-418a-b3d3-45b2581f4fe3/MeasureReport-3aff156c-cc70-4811-a8df-a8c10f22724c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 8715fad1-2969-418a-b3d3-45b2581f4fe3 ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/8715fad1-2969-418a-b3d3-45b2581f4fe3/MeasureReport-3aff156c-cc70-4811-a8df-a8c10f22724c.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ feb97651-b478-467e-97c9-3bc514a0a26b ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/feb97651-b478-467e-97c9-3bc514a0a26b/MeasureReport-a3855237-4d8a-45bf-b31b-c2f561baadd5.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ feb97651-b478-467e-97c9-3bc514a0a26b ](../.././input/tests/measure/CMS137FHIRSUDTxInitEngagement/feb97651-b478-467e-97c9-3bc514a0a26b/MeasureReport-a3855237-4d8a-45bf-b31b-c2f561baadd5.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |


#### CMS138FHIRTobaccoScrnCessation
[ [cql] ](../../input/cql/CMS138FHIRTobaccoScrnCessation.cql) [ [test results] ](../../input/tests/results/CMS138FHIRTobaccoScrnCessation.txt)

Mismatched Test Cases (40 of  of 141)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 007fe881-a18d-418f-8ddf-0ee94fc9a10a ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/007fe881-a18d-418f-8ddf-0ee94fc9a10a/MeasureReport-45a1ad86-db80-4c37-b6f0-1dcdf04167bf.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 007fe881-a18d-418f-8ddf-0ee94fc9a10a ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/007fe881-a18d-418f-8ddf-0ee94fc9a10a/MeasureReport-45a1ad86-db80-4c37-b6f0-1dcdf04167bf.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 0d221636-5f14-4074-9337-eb4b0868fb3e ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/0d221636-5f14-4074-9337-eb4b0868fb3e/MeasureReport-59a9caa8-e71c-4bdf-90ec-ce2224d90dd5.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 0d221636-5f14-4074-9337-eb4b0868fb3e ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/0d221636-5f14-4074-9337-eb4b0868fb3e/MeasureReport-59a9caa8-e71c-4bdf-90ec-ce2224d90dd5.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 44a3e280-b4ad-4725-b806-1ea7592114d8 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/44a3e280-b4ad-4725-b806-1ea7592114d8/MeasureReport-5060c69e-b9b0-415d-8853-c6d953e17489.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 44a3e280-b4ad-4725-b806-1ea7592114d8 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/44a3e280-b4ad-4725-b806-1ea7592114d8/MeasureReport-5060c69e-b9b0-415d-8853-c6d953e17489.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 44a3e280-b4ad-4725-b806-1ea7592114d8 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/44a3e280-b4ad-4725-b806-1ea7592114d8/MeasureReport-5060c69e-b9b0-415d-8853-c6d953e17489.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 6410550a-c928-415b-b8bc-aa1284ca6933 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/6410550a-c928-415b-b8bc-aa1284ca6933/MeasureReport-b410a68c-155b-4349-ac16-a1ca9ae771ba.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 6410550a-c928-415b-b8bc-aa1284ca6933 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/6410550a-c928-415b-b8bc-aa1284ca6933/MeasureReport-b410a68c-155b-4349-ac16-a1ca9ae771ba.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 72c8b10f-fffd-411f-bf81-c7d0608ad314 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/72c8b10f-fffd-411f-bf81-c7d0608ad314/MeasureReport-a436c88f-b046-4569-bca3-55d12ef645e6.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 72c8b10f-fffd-411f-bf81-c7d0608ad314 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/72c8b10f-fffd-411f-bf81-c7d0608ad314/MeasureReport-a436c88f-b046-4569-bca3-55d12ef645e6.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 72c8b10f-fffd-411f-bf81-c7d0608ad314 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/72c8b10f-fffd-411f-bf81-c7d0608ad314/MeasureReport-a436c88f-b046-4569-bca3-55d12ef645e6.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 73d69a14-7e70-4c9f-89e3-62da4a370fd3 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/73d69a14-7e70-4c9f-89e3-62da4a370fd3/MeasureReport-78680856-a975-47b9-9b99-f2b5bb300936.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 73d69a14-7e70-4c9f-89e3-62da4a370fd3 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/73d69a14-7e70-4c9f-89e3-62da4a370fd3/MeasureReport-78680856-a975-47b9-9b99-f2b5bb300936.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 76e371e4-0363-4fad-9573-a06ada971eef ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/76e371e4-0363-4fad-9573-a06ada971eef/MeasureReport-ceee4067-e616-47a2-b1d1-25694b739861.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 76e371e4-0363-4fad-9573-a06ada971eef ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/76e371e4-0363-4fad-9573-a06ada971eef/MeasureReport-ceee4067-e616-47a2-b1d1-25694b739861.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 828caebe-4bd7-4579-85c6-d6340a9f3240 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/828caebe-4bd7-4579-85c6-d6340a9f3240/MeasureReport-c0d13ef6-090f-435c-a291-9bcb794bbc08.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 828caebe-4bd7-4579-85c6-d6340a9f3240 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/828caebe-4bd7-4579-85c6-d6340a9f3240/MeasureReport-c0d13ef6-090f-435c-a291-9bcb794bbc08.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 828caebe-4bd7-4579-85c6-d6340a9f3240 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/828caebe-4bd7-4579-85c6-d6340a9f3240/MeasureReport-c0d13ef6-090f-435c-a291-9bcb794bbc08.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 9fba5feb-b77c-496f-981f-6d062f3c1d7c ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/9fba5feb-b77c-496f-981f-6d062f3c1d7c/MeasureReport-ff3f2ea4-b4bc-434c-8800-1967f5b80b8f.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 9fba5feb-b77c-496f-981f-6d062f3c1d7c ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/9fba5feb-b77c-496f-981f-6d062f3c1d7c/MeasureReport-ff3f2ea4-b4bc-434c-8800-1967f5b80b8f.json) | Group_2 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 9fba5feb-b77c-496f-981f-6d062f3c1d7c ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/9fba5feb-b77c-496f-981f-6d062f3c1d7c/MeasureReport-ff3f2ea4-b4bc-434c-8800-1967f5b80b8f.json) | Group_3 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ a0a8fd4f-bfd4-4669-aaef-f66ae5af5eda ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/a0a8fd4f-bfd4-4669-aaef-f66ae5af5eda/MeasureReport-fe41c096-08c3-42ce-b6ad-96de6f88d808.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ a0a8fd4f-bfd4-4669-aaef-f66ae5af5eda ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/a0a8fd4f-bfd4-4669-aaef-f66ae5af5eda/MeasureReport-fe41c096-08c3-42ce-b6ad-96de6f88d808.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5/MeasureReport-02825288-0d86-4de2-8f43-b1b60c9533b7.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5/MeasureReport-02825288-0d86-4de2-8f43-b1b60c9533b7.json) | Group_2 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5/MeasureReport-02825288-0d86-4de2-8f43-b1b60c9533b7.json) | Group_3 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ bac2713c-8165-40ce-8180-fb5d44a10f7f ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/bac2713c-8165-40ce-8180-fb5d44a10f7f/MeasureReport-49095abd-511f-4e6f-a870-c7a3e6c820ed.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ bac2713c-8165-40ce-8180-fb5d44a10f7f ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/bac2713c-8165-40ce-8180-fb5d44a10f7f/MeasureReport-49095abd-511f-4e6f-a870-c7a3e6c820ed.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ c56fda5f-6cd9-4057-aaef-5c843a8241f1 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/c56fda5f-6cd9-4057-aaef-5c843a8241f1/MeasureReport-2408d069-8202-4f51-b5d2-5b886d2487de.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ c56fda5f-6cd9-4057-aaef-5c843a8241f1 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/c56fda5f-6cd9-4057-aaef-5c843a8241f1/MeasureReport-2408d069-8202-4f51-b5d2-5b886d2487de.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ c56fda5f-6cd9-4057-aaef-5c843a8241f1 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/c56fda5f-6cd9-4057-aaef-5c843a8241f1/MeasureReport-2408d069-8202-4f51-b5d2-5b886d2487de.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ e3422e20-4e31-4c24-a72b-3c1e1f47de95 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/e3422e20-4e31-4c24-a72b-3c1e1f47de95/MeasureReport-f5466b54-94e5-40d3-88c1-0b3d223d0598.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ e3422e20-4e31-4c24-a72b-3c1e1f47de95 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/e3422e20-4e31-4c24-a72b-3c1e1f47de95/MeasureReport-f5466b54-94e5-40d3-88c1-0b3d223d0598.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ e383c9a2-7b5e-4ab6-b2e2-642a2304d6e2 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/e383c9a2-7b5e-4ab6-b2e2-642a2304d6e2/MeasureReport-db328de7-8d39-4fb3-bb41-9da9de27714c.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ e383c9a2-7b5e-4ab6-b2e2-642a2304d6e2 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/e383c9a2-7b5e-4ab6-b2e2-642a2304d6e2/MeasureReport-db328de7-8d39-4fb3-bb41-9da9de27714c.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ e383c9a2-7b5e-4ab6-b2e2-642a2304d6e2 ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/e383c9a2-7b5e-4ab6-b2e2-642a2304d6e2/MeasureReport-db328de7-8d39-4fb3-bb41-9da9de27714c.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ ed2fe491-3eb7-424a-bf95-5d44b6102cec ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/ed2fe491-3eb7-424a-bf95-5d44b6102cec/MeasureReport-841afe4d-1c7b-4b08-805e-14715b1f61b0.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ ed2fe491-3eb7-424a-bf95-5d44b6102cec ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/ed2fe491-3eb7-424a-bf95-5d44b6102cec/MeasureReport-841afe4d-1c7b-4b08-805e-14715b1f61b0.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ ed2fe491-3eb7-424a-bf95-5d44b6102cec ](../.././input/tests/measure/CMS138FHIRTobaccoScrnCessation/ed2fe491-3eb7-424a-bf95-5d44b6102cec/MeasureReport-841afe4d-1c7b-4b08-805e-14715b1f61b0.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |


#### CMS139FHIRFallRiskScreening
[ [cql] ](../../input/cql/CMS139FHIRFallRiskScreening.cql) [ [test results] ](../../input/tests/results/CMS139FHIRFallRiskScreening.txt)

Mismatched Test Cases (8 of  of 29)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 1a370226-6ab1-487f-b1da-08741e08f725 ](../.././input/tests/measure/CMS139FHIRFallRiskScreening/1a370226-6ab1-487f-b1da-08741e08f725/MeasureReport-68c89ec1-05bf-42aa-9821-fd9a2279d302.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 2b6eca9d-7580-4262-ba2c-97f6c174cc33 ](../.././input/tests/measure/CMS139FHIRFallRiskScreening/2b6eca9d-7580-4262-ba2c-97f6c174cc33/MeasureReport-637df085-bb25-42cb-b3e5-9d64309c67b3.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4576786d-d477-4447-8bdb-f9d5c2e6600c ](../.././input/tests/measure/CMS139FHIRFallRiskScreening/4576786d-d477-4447-8bdb-f9d5c2e6600c/MeasureReport-e495b37a-c1b6-4da3-9486-3d5e3753bffa.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4a1c85c3-e97c-4644-b6a1-2475aa1c27e2 ](../.././input/tests/measure/CMS139FHIRFallRiskScreening/4a1c85c3-e97c-4644-b6a1-2475aa1c27e2/MeasureReport-f10e1e09-0087-49af-8edf-76a8c490870c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 65b723f6-246d-4320-a181-a64f7f1fd837 ](../.././input/tests/measure/CMS139FHIRFallRiskScreening/65b723f6-246d-4320-a181-a64f7f1fd837/MeasureReport-67507ea0-6379-4747-8e2e-e052786191ea.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 741236df-31ad-463b-b730-fb113cfa09a8 ](../.././input/tests/measure/CMS139FHIRFallRiskScreening/741236df-31ad-463b-b730-fb113cfa09a8/MeasureReport-c215cf44-4531-46ba-841f-fc2e6c3acbf5.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 839e7c3a-a94f-418f-96cb-d356bf6de1da ](../.././input/tests/measure/CMS139FHIRFallRiskScreening/839e7c3a-a94f-418f-96cb-d356bf6de1da/MeasureReport-4eeaab54-1b2f-4cd1-a276-0b729c5c134a.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b7261db5-e945-48b9-90dd-0d0761c09295 ](../.././input/tests/measure/CMS139FHIRFallRiskScreening/b7261db5-e945-48b9-90dd-0d0761c09295/MeasureReport-6ed4041b-dc15-4e7d-b9f2-43a5bf2599c1.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS142FHIRCommWithDrManagingDiab
[ [cql] ](../../input/cql/CMS142FHIRCommWithDrManagingDiab.cql) [ [test results] ](../../input/tests/results/CMS142FHIRCommWithDrManagingDiab.txt)

Mismatched Test Cases (5 of  of 32)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 05f1e2a6-b317-42bb-827f-993ca3995f5b ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/05f1e2a6-b317-42bb-827f-993ca3995f5b/MeasureReport-84bcf708-71bb-4169-8067-18fd354f3c37.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 41ae0086-ac99-4a31-9546-21b054bbf7d8 ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/41ae0086-ac99-4a31-9546-21b054bbf7d8/MeasureReport-b77a6309-214c-4fc2-a9bc-18d81c740da6.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 6aef5a18-59bd-4a47-80bc-2bd44636e41f ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/6aef5a18-59bd-4a47-80bc-2bd44636e41f/MeasureReport-e5735d61-0444-4958-8f47-165a59e91dc0.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ b85440e4-b902-49cd-b3d6-363ba7a99bce ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/b85440e4-b902-49cd-b3d6-363ba7a99bce/MeasureReport-9d61df39-18a0-451f-a795-988388d58778.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ d9840e8c-3359-42c2-b354-4b236c3c1b15 ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/d9840e8c-3359-42c2-b354-4b236c3c1b15/MeasureReport-1fbf56ab-6e60-4ce6-a1d5-b520382164bd.json) | Group_1 | Denominator Exception | 1 | 0 | — |


#### CMS144FHIRHFBetaBlockerForLVSD
[ [cql] ](../../input/cql/CMS144FHIRHFBetaBlockerForLVSD.cql) [ [test results] ](../../input/tests/results/CMS144FHIRHFBetaBlockerForLVSD.txt)

Mismatched Test Cases (3 of  of 48)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 07efd4bb-b45d-4bfd-aeb2-08de49742d91 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/07efd4bb-b45d-4bfd-aeb2-08de49742d91/MeasureReport-ad01867d-c2c7-4317-9925-deb909d156e6.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 67779bc6-07ee-42cf-8ca7-e71302915dba ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/67779bc6-07ee-42cf-8ca7-e71302915dba/MeasureReport-5b182aca-ad2a-4651-ba6b-df02e001ec36.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 7b8885c5-ad14-4361-9755-c76a6e3b8530 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/7b8885c5-ad14-4361-9755-c76a6e3b8530/MeasureReport-7e421d2a-1ee4-4c56-a454-815983c21106.json) | Group_1 | Numerator | 0 | 1 | — |


#### CMS145FHIRCADBBlockerTPMIorLVSD
[ [cql] ](../../input/cql/CMS145FHIRCADBBlockerTPMIorLVSD.cql) [ [test results] ](../../input/tests/results/CMS145FHIRCADBBlockerTPMIorLVSD.txt)

Mismatched Test Cases (6 of  of 106)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 1f70822b-c513-4c3a-8162-49f0bb9c914b ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/1f70822b-c513-4c3a-8162-49f0bb9c914b/MeasureReport-9b3577fa-355c-409d-8d3f-21e9720fb889.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e/MeasureReport-e77c61ff-cc3a-402c-9752-7a97a6727a39.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 5fd0d626-e9c5-4e6c-a10d-1a1183fa7702 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/5fd0d626-e9c5-4e6c-a10d-1a1183fa7702/MeasureReport-ce1b8712-b9dd-48e2-adf4-554ed641bee5.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 61306767-0e74-44b8-ac06-1339c3783355 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/61306767-0e74-44b8-ac06-1339c3783355/MeasureReport-6ea40199-5a45-4c8d-8a2b-c08bf93ebd8a.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ b65680a0-9768-4ce4-b08d-972fcd84e28e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/b65680a0-9768-4ce4-b08d-972fcd84e28e/MeasureReport-b5ebd0a9-a2de-4b31-b0d9-588888e95872.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ fd5fb311-a466-4c59-966d-48fa7aa88931 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/fd5fb311-a466-4c59-966d-48fa7aa88931/MeasureReport-05ffed3e-5604-40eb-bcf8-99cacecc26c0.json) | Group_1 | Denominator Exception | 1 | 0 | — |


#### CMS146FHIRApproTestPharyngitis
[ [cql] ](../../input/cql/CMS146FHIRApproTestPharyngitis.cql) [ [test results] ](../../input/tests/results/CMS146FHIRApproTestPharyngitis.txt)

Mismatched Test Cases (10 of  of 38)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 32b213a8-4071-4bc7-8db8-8ab080e5e468 ](../.././input/tests/measure/CMS146FHIRApproTestPharyngitis/32b213a8-4071-4bc7-8db8-8ab080e5e468/MeasureReport-67ff9e66-a7c8-436b-a67d-3aa602178ae6.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4b78839b-3a31-4dc7-9b6b-4e06f005c7e0 ](../.././input/tests/measure/CMS146FHIRApproTestPharyngitis/4b78839b-3a31-4dc7-9b6b-4e06f005c7e0/MeasureReport-1d6ef741-100c-4563-9ccd-9691fae93ce0.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ a6e7ec82-b80e-4f76-b382-91956c4873a9 ](../.././input/tests/measure/CMS146FHIRApproTestPharyngitis/a6e7ec82-b80e-4f76-b382-91956c4873a9/MeasureReport-9c11417a-c2cd-4457-8980-32abb9e409be.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b23aa001-1331-46f0-9818-19f6dc890668 ](../.././input/tests/measure/CMS146FHIRApproTestPharyngitis/b23aa001-1331-46f0-9818-19f6dc890668/MeasureReport-46f817e8-87c8-469f-8ba8-f3b880e4a7c2.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ c257e23d-80d0-4ab8-9374-e38815eab144 ](../.././input/tests/measure/CMS146FHIRApproTestPharyngitis/c257e23d-80d0-4ab8-9374-e38815eab144/MeasureReport-7dc95b5d-9459-43b6-82b4-b7932a722d22.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ c5401e41-5ec7-4d84-b0ab-600dd4b8cdaf ](../.././input/tests/measure/CMS146FHIRApproTestPharyngitis/c5401e41-5ec7-4d84-b0ab-600dd4b8cdaf/MeasureReport-7c1cb2ed-0171-4b32-b816-b950835f6c5b.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ c5f2b465-bfa2-4f94-8512-ff04308a8159 ](../.././input/tests/measure/CMS146FHIRApproTestPharyngitis/c5f2b465-bfa2-4f94-8512-ff04308a8159/MeasureReport-1abd2a30-2360-4dca-a0e2-b8e8e81a1226.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ c8d42ccd-9523-414f-b568-e0fdae94a84a ](../.././input/tests/measure/CMS146FHIRApproTestPharyngitis/c8d42ccd-9523-414f-b568-e0fdae94a84a/MeasureReport-3bf5f8e0-39ee-4780-9027-c464fb9d066c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ e251036b-b9dc-4c2c-8841-5d34064501ed ](../.././input/tests/measure/CMS146FHIRApproTestPharyngitis/e251036b-b9dc-4c2c-8841-5d34064501ed/MeasureReport-612900b7-2620-4c59-ae05-80da3bd37f62.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ ed5a5721-71d3-4247-9f9b-4097e55fccfb ](../.././input/tests/measure/CMS146FHIRApproTestPharyngitis/ed5a5721-71d3-4247-9f9b-4097e55fccfb/MeasureReport-b18a2c73-10ab-4edf-a7ef-a17dc0117798.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS153FHIRChlamydiaScreening
[ [cql] ](../../input/cql/CMS153FHIRChlamydiaScreening.cql) [ [test results] ](../../input/tests/results/CMS153FHIRChlamydiaScreening.txt)

Mismatched Test Cases (8 of  of 32)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 1c0607a1-de1a-46e2-98f5-5ea7c5f50506 ](../.././input/tests/measure/CMS153FHIRChlamydiaScreening/1c0607a1-de1a-46e2-98f5-5ea7c5f50506/MeasureReport-63cb19bf-8392-414f-a85a-b9b0b0b2ac27.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 5e5374d9-3830-47dd-bbf4-dbc8960c4870 ](../.././input/tests/measure/CMS153FHIRChlamydiaScreening/5e5374d9-3830-47dd-bbf4-dbc8960c4870/MeasureReport-e232afe7-1802-42c7-bece-73e3c3eed518.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 840339a3-d0c2-4fa8-8f80-cfdd57f48868 ](../.././input/tests/measure/CMS153FHIRChlamydiaScreening/840339a3-d0c2-4fa8-8f80-cfdd57f48868/MeasureReport-4bf8ced0-b40e-4dda-8e11-f21cb98a5a93.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ c0225f3d-ea64-4bb4-873b-b28ebc10050a ](../.././input/tests/measure/CMS153FHIRChlamydiaScreening/c0225f3d-ea64-4bb4-873b-b28ebc10050a/MeasureReport-68ca3d69-cb0f-4698-a7f5-f0d3dc1efe40.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ dc0d63ab-8b3a-4f90-ab19-0c4c18d398a8 ](../.././input/tests/measure/CMS153FHIRChlamydiaScreening/dc0d63ab-8b3a-4f90-ab19-0c4c18d398a8/MeasureReport-0ec39626-4340-4634-bdc4-cfdb4f7d4c16.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ dda878bb-eb46-4562-a455-862009c0f7ce ](../.././input/tests/measure/CMS153FHIRChlamydiaScreening/dda878bb-eb46-4562-a455-862009c0f7ce/MeasureReport-84a2ba68-914a-4d14-ae94-289f9d97f767.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ ec8a19c5-8fd1-40e9-974b-98fbccd921b8 ](../.././input/tests/measure/CMS153FHIRChlamydiaScreening/ec8a19c5-8fd1-40e9-974b-98fbccd921b8/MeasureReport-24ee7fa0-b85d-4687-ac57-dbc68a502ef6.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ f6a69563-6b05-4dcb-87e6-dd3bdd25f597 ](../.././input/tests/measure/CMS153FHIRChlamydiaScreening/f6a69563-6b05-4dcb-87e6-dd3bdd25f597/MeasureReport-d4d0a628-e8e4-4974-98d7-bb5e85d19f1c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS154FHIRAppropriateTxforURI
[ [cql] ](../../input/cql/CMS154FHIRAppropriateTxforURI.cql) [ [test results] ](../../input/tests/results/CMS154FHIRAppropriateTxforURI.txt)

Mismatched Test Cases (8 of  of 33)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 1b24b0b1-92fa-405d-88d1-e550896598c1 ](../.././input/tests/measure/CMS154FHIRAppropriateTxforURI/1b24b0b1-92fa-405d-88d1-e550896598c1/MeasureReport-6a48888f-f761-48e6-810e-492b3076c1bb.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 41bc23b2-9bf6-4e81-ae25-2b5f78b61b87 ](../.././input/tests/measure/CMS154FHIRAppropriateTxforURI/41bc23b2-9bf6-4e81-ae25-2b5f78b61b87/MeasureReport-5ccfac91-e99c-41f2-bfbe-41456dae9a68.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 673d2f3c-b735-4672-8a4e-2f77060e1802 ](../.././input/tests/measure/CMS154FHIRAppropriateTxforURI/673d2f3c-b735-4672-8a4e-2f77060e1802/MeasureReport-ed8267f2-2bc7-49d6-b3aa-224dc36a055a.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 78a48c68-f018-47da-a1cc-c96b63c248e8 ](../.././input/tests/measure/CMS154FHIRAppropriateTxforURI/78a48c68-f018-47da-a1cc-c96b63c248e8/MeasureReport-f9c64aec-b84d-4d2c-9b1e-394b157f7fce.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 99d50203-60f7-466b-a253-a0908d85a7a3 ](../.././input/tests/measure/CMS154FHIRAppropriateTxforURI/99d50203-60f7-466b-a253-a0908d85a7a3/MeasureReport-50040dfb-2df8-4ce1-a479-aa51b99b7e48.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ acb44fb3-b572-4dfd-891c-c8b2cc24e1b8 ](../.././input/tests/measure/CMS154FHIRAppropriateTxforURI/acb44fb3-b572-4dfd-891c-c8b2cc24e1b8/MeasureReport-21c6e892-0dc9-4961-99fe-0445b5861ec0.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ cac03a54-f595-411e-bc00-c9146222a68c ](../.././input/tests/measure/CMS154FHIRAppropriateTxforURI/cac03a54-f595-411e-bc00-c9146222a68c/MeasureReport-16148c46-b783-41b7-9a19-dd384884943e.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ dc6b0b42-949a-481e-8134-bb536a2f3fe9 ](../.././input/tests/measure/CMS154FHIRAppropriateTxforURI/dc6b0b42-949a-481e-8134-bb536a2f3fe9/MeasureReport-cd60a6e2-f676-4d4d-9dfe-4eba47c3d333.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |


#### CMS155FHIRWgtAssessCounseling
[ [cql] ](../../input/cql/CMS155FHIRWgtAssessCounseling.cql) [ [test results] ](../../input/tests/results/CMS155FHIRWgtAssessCounseling.txt)

Mismatched Test Cases (27 of  of 102)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 1e0720b0-0782-4455-a355-8c1ecec3c653 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/1e0720b0-0782-4455-a355-8c1ecec3c653/MeasureReport-7d33c5b1-3b26-4351-b0ed-4ca6a482f9b0.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 1e0720b0-0782-4455-a355-8c1ecec3c653 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/1e0720b0-0782-4455-a355-8c1ecec3c653/MeasureReport-7d33c5b1-3b26-4351-b0ed-4ca6a482f9b0.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 1e0720b0-0782-4455-a355-8c1ecec3c653 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/1e0720b0-0782-4455-a355-8c1ecec3c653/MeasureReport-7d33c5b1-3b26-4351-b0ed-4ca6a482f9b0.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 259f8551-1cea-44f5-ae9e-e3f083d9f48f ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/259f8551-1cea-44f5-ae9e-e3f083d9f48f/MeasureReport-9a4e385c-e5b5-4a1f-8c97-d46e0b8f8edc.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 259f8551-1cea-44f5-ae9e-e3f083d9f48f ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/259f8551-1cea-44f5-ae9e-e3f083d9f48f/MeasureReport-9a4e385c-e5b5-4a1f-8c97-d46e0b8f8edc.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 259f8551-1cea-44f5-ae9e-e3f083d9f48f ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/259f8551-1cea-44f5-ae9e-e3f083d9f48f/MeasureReport-9a4e385c-e5b5-4a1f-8c97-d46e0b8f8edc.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 4304f97a-e2bb-4cda-93fa-ab510a136403 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/4304f97a-e2bb-4cda-93fa-ab510a136403/MeasureReport-264e2520-208e-41be-81f4-bc4d5a572bb4.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4304f97a-e2bb-4cda-93fa-ab510a136403 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/4304f97a-e2bb-4cda-93fa-ab510a136403/MeasureReport-264e2520-208e-41be-81f4-bc4d5a572bb4.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 4304f97a-e2bb-4cda-93fa-ab510a136403 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/4304f97a-e2bb-4cda-93fa-ab510a136403/MeasureReport-264e2520-208e-41be-81f4-bc4d5a572bb4.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 4a9211fc-d757-47ae-8bc0-0803c43a6728 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/4a9211fc-d757-47ae-8bc0-0803c43a6728/MeasureReport-1dc2fe64-0680-4f05-a338-53e9683c97ed.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4a9211fc-d757-47ae-8bc0-0803c43a6728 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/4a9211fc-d757-47ae-8bc0-0803c43a6728/MeasureReport-1dc2fe64-0680-4f05-a338-53e9683c97ed.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 4a9211fc-d757-47ae-8bc0-0803c43a6728 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/4a9211fc-d757-47ae-8bc0-0803c43a6728/MeasureReport-1dc2fe64-0680-4f05-a338-53e9683c97ed.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 53711871-5aac-4e37-a047-9dae85fcf6cb ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/53711871-5aac-4e37-a047-9dae85fcf6cb/MeasureReport-67399cd5-5e99-424d-815b-a49ed47204cf.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 53711871-5aac-4e37-a047-9dae85fcf6cb ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/53711871-5aac-4e37-a047-9dae85fcf6cb/MeasureReport-67399cd5-5e99-424d-815b-a49ed47204cf.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 53711871-5aac-4e37-a047-9dae85fcf6cb ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/53711871-5aac-4e37-a047-9dae85fcf6cb/MeasureReport-67399cd5-5e99-424d-815b-a49ed47204cf.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 598662b8-30c9-4f9b-a2d1-d91bea113d77 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/598662b8-30c9-4f9b-a2d1-d91bea113d77/MeasureReport-2a08ab53-d12a-4e82-b01d-196e9bcbd06f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 598662b8-30c9-4f9b-a2d1-d91bea113d77 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/598662b8-30c9-4f9b-a2d1-d91bea113d77/MeasureReport-2a08ab53-d12a-4e82-b01d-196e9bcbd06f.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 598662b8-30c9-4f9b-a2d1-d91bea113d77 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/598662b8-30c9-4f9b-a2d1-d91bea113d77/MeasureReport-2a08ab53-d12a-4e82-b01d-196e9bcbd06f.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ a0c68789-2a1b-4bc4-b6a4-d8f6b154d8ac ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/a0c68789-2a1b-4bc4-b6a4-d8f6b154d8ac/MeasureReport-04e2d651-cddf-493d-9207-79e01e89bd25.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ a0c68789-2a1b-4bc4-b6a4-d8f6b154d8ac ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/a0c68789-2a1b-4bc4-b6a4-d8f6b154d8ac/MeasureReport-04e2d651-cddf-493d-9207-79e01e89bd25.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ a0c68789-2a1b-4bc4-b6a4-d8f6b154d8ac ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/a0c68789-2a1b-4bc4-b6a4-d8f6b154d8ac/MeasureReport-04e2d651-cddf-493d-9207-79e01e89bd25.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ bd9b9e02-ce12-43cb-af1c-25298c891e62 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/bd9b9e02-ce12-43cb-af1c-25298c891e62/MeasureReport-5d4a8d58-0826-4c1f-bcad-2165ad44e41a.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ bd9b9e02-ce12-43cb-af1c-25298c891e62 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/bd9b9e02-ce12-43cb-af1c-25298c891e62/MeasureReport-5d4a8d58-0826-4c1f-bcad-2165ad44e41a.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ bd9b9e02-ce12-43cb-af1c-25298c891e62 ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/bd9b9e02-ce12-43cb-af1c-25298c891e62/MeasureReport-5d4a8d58-0826-4c1f-bcad-2165ad44e41a.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ dbb639f6-f7b7-41c8-bc30-84e5574c08cd ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/dbb639f6-f7b7-41c8-bc30-84e5574c08cd/MeasureReport-3f78c46d-a5dc-4caf-88e4-af0b8f336db1.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ dbb639f6-f7b7-41c8-bc30-84e5574c08cd ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/dbb639f6-f7b7-41c8-bc30-84e5574c08cd/MeasureReport-3f78c46d-a5dc-4caf-88e4-af0b8f336db1.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ dbb639f6-f7b7-41c8-bc30-84e5574c08cd ](../.././input/tests/measure/CMS155FHIRWgtAssessCounseling/dbb639f6-f7b7-41c8-bc30-84e5574c08cd/MeasureReport-3f78c46d-a5dc-4caf-88e4-af0b8f336db1.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |


#### CMS156FHIRHighRiskMedsElderly
[ [cql] ](../../input/cql/CMS156FHIRHighRiskMedsElderly.cql) [ [test results] ](../../input/tests/results/CMS156FHIRHighRiskMedsElderly.txt)

Mismatched Test Cases (41 of  of 177)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 05aa403d-44c1-4c71-acb9-7808568b6a4f ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/05aa403d-44c1-4c71-acb9-7808568b6a4f/MeasureReport-01b4a3fd-29dd-470c-9eda-149b0cb3528b.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 05aa403d-44c1-4c71-acb9-7808568b6a4f ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/05aa403d-44c1-4c71-acb9-7808568b6a4f/MeasureReport-01b4a3fd-29dd-470c-9eda-149b0cb3528b.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 05aa403d-44c1-4c71-acb9-7808568b6a4f ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/05aa403d-44c1-4c71-acb9-7808568b6a4f/MeasureReport-01b4a3fd-29dd-470c-9eda-149b0cb3528b.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 28da77ab-fe4d-44f2-a2fe-9c260e941cfb ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/28da77ab-fe4d-44f2-a2fe-9c260e941cfb/MeasureReport-869d8f10-b533-4fbf-bf72-8536e0524eb6.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 28da77ab-fe4d-44f2-a2fe-9c260e941cfb ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/28da77ab-fe4d-44f2-a2fe-9c260e941cfb/MeasureReport-869d8f10-b533-4fbf-bf72-8536e0524eb6.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 28da77ab-fe4d-44f2-a2fe-9c260e941cfb ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/28da77ab-fe4d-44f2-a2fe-9c260e941cfb/MeasureReport-869d8f10-b533-4fbf-bf72-8536e0524eb6.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 32186189-fe9c-41d5-9654-68c0c60aaac6 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/32186189-fe9c-41d5-9654-68c0c60aaac6/MeasureReport-8b5f7af9-95da-4008-b4d7-acd1c843c4b9.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 32186189-fe9c-41d5-9654-68c0c60aaac6 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/32186189-fe9c-41d5-9654-68c0c60aaac6/MeasureReport-8b5f7af9-95da-4008-b4d7-acd1c843c4b9.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 32186189-fe9c-41d5-9654-68c0c60aaac6 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/32186189-fe9c-41d5-9654-68c0c60aaac6/MeasureReport-8b5f7af9-95da-4008-b4d7-acd1c843c4b9.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 35b521b6-1fdd-4742-8137-36213864b0fb ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/35b521b6-1fdd-4742-8137-36213864b0fb/MeasureReport-23f4dccf-5051-4dc7-bf21-c6e77c9b48df.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 35b521b6-1fdd-4742-8137-36213864b0fb ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/35b521b6-1fdd-4742-8137-36213864b0fb/MeasureReport-23f4dccf-5051-4dc7-bf21-c6e77c9b48df.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 35b521b6-1fdd-4742-8137-36213864b0fb ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/35b521b6-1fdd-4742-8137-36213864b0fb/MeasureReport-23f4dccf-5051-4dc7-bf21-c6e77c9b48df.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 4aa75d19-ac8b-49b0-a686-429fbc033d77 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/MeasureReport-139cc56e-5ffb-46ca-89ce-accd0bb642ab.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 4aa75d19-ac8b-49b0-a686-429fbc033d77 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/MeasureReport-139cc56e-5ffb-46ca-89ce-accd0bb642ab.json) | Group_3 | Numerator | 1 | 0 | — |
| [ 60883694-3c84-4343-b12b-b017f1c57587 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/60883694-3c84-4343-b12b-b017f1c57587/MeasureReport-c5ff3e07-42c8-446f-a8e9-757fcf945f64.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 60883694-3c84-4343-b12b-b017f1c57587 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/60883694-3c84-4343-b12b-b017f1c57587/MeasureReport-c5ff3e07-42c8-446f-a8e9-757fcf945f64.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 60883694-3c84-4343-b12b-b017f1c57587 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/60883694-3c84-4343-b12b-b017f1c57587/MeasureReport-c5ff3e07-42c8-446f-a8e9-757fcf945f64.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 688e8e1b-8054-4c30-83e8-ab99fdd7ccfb ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/688e8e1b-8054-4c30-83e8-ab99fdd7ccfb/MeasureReport-367fa966-8eaa-453c-b019-f3783fc5017a.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 688e8e1b-8054-4c30-83e8-ab99fdd7ccfb ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/688e8e1b-8054-4c30-83e8-ab99fdd7ccfb/MeasureReport-367fa966-8eaa-453c-b019-f3783fc5017a.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 688e8e1b-8054-4c30-83e8-ab99fdd7ccfb ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/688e8e1b-8054-4c30-83e8-ab99fdd7ccfb/MeasureReport-367fa966-8eaa-453c-b019-f3783fc5017a.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 8b2f163f-e180-4169-b41a-9c3b77ae0302 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8b2f163f-e180-4169-b41a-9c3b77ae0302/MeasureReport-cd8cb6ca-4a62-4609-9c22-4f64998f7a15.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 8b2f163f-e180-4169-b41a-9c3b77ae0302 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8b2f163f-e180-4169-b41a-9c3b77ae0302/MeasureReport-cd8cb6ca-4a62-4609-9c22-4f64998f7a15.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 8b2f163f-e180-4169-b41a-9c3b77ae0302 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8b2f163f-e180-4169-b41a-9c3b77ae0302/MeasureReport-cd8cb6ca-4a62-4609-9c22-4f64998f7a15.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 8b33d091-6e1e-4992-9ae6-63adc9401862 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8b33d091-6e1e-4992-9ae6-63adc9401862/MeasureReport-24189274-2376-4e39-ad9b-55c67f00e95f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 8b33d091-6e1e-4992-9ae6-63adc9401862 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8b33d091-6e1e-4992-9ae6-63adc9401862/MeasureReport-24189274-2376-4e39-ad9b-55c67f00e95f.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 8b33d091-6e1e-4992-9ae6-63adc9401862 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/8b33d091-6e1e-4992-9ae6-63adc9401862/MeasureReport-24189274-2376-4e39-ad9b-55c67f00e95f.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ 9f9302aa-f988-4131-a265-3996467aeed7 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/9f9302aa-f988-4131-a265-3996467aeed7/MeasureReport-ea703c4c-a8b9-48db-b342-4a77823746c9.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 9f9302aa-f988-4131-a265-3996467aeed7 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/9f9302aa-f988-4131-a265-3996467aeed7/MeasureReport-ea703c4c-a8b9-48db-b342-4a77823746c9.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ 9f9302aa-f988-4131-a265-3996467aeed7 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/9f9302aa-f988-4131-a265-3996467aeed7/MeasureReport-ea703c4c-a8b9-48db-b342-4a77823746c9.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ ad4aced6-dec9-4309-86a1-246b7c0dd6d9 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/ad4aced6-dec9-4309-86a1-246b7c0dd6d9/MeasureReport-ec5d0ab8-320b-4ae9-b84c-6adeaf31bc55.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ ad4aced6-dec9-4309-86a1-246b7c0dd6d9 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/ad4aced6-dec9-4309-86a1-246b7c0dd6d9/MeasureReport-ec5d0ab8-320b-4ae9-b84c-6adeaf31bc55.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ ad4aced6-dec9-4309-86a1-246b7c0dd6d9 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/ad4aced6-dec9-4309-86a1-246b7c0dd6d9/MeasureReport-ec5d0ab8-320b-4ae9-b84c-6adeaf31bc55.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ b9e0084c-8386-48e2-b17d-87c508c566f9 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/b9e0084c-8386-48e2-b17d-87c508c566f9/MeasureReport-fde74230-5c8d-495c-8488-1e46ce024a96.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b9e0084c-8386-48e2-b17d-87c508c566f9 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/b9e0084c-8386-48e2-b17d-87c508c566f9/MeasureReport-fde74230-5c8d-495c-8488-1e46ce024a96.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ b9e0084c-8386-48e2-b17d-87c508c566f9 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/b9e0084c-8386-48e2-b17d-87c508c566f9/MeasureReport-fde74230-5c8d-495c-8488-1e46ce024a96.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ bb83b7f0-6542-4105-b2f0-5d2018167a9e ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/bb83b7f0-6542-4105-b2f0-5d2018167a9e/MeasureReport-2e6ce7a3-3b86-4820-a4e2-7d8386b7a118.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ bb83b7f0-6542-4105-b2f0-5d2018167a9e ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/bb83b7f0-6542-4105-b2f0-5d2018167a9e/MeasureReport-2e6ce7a3-3b86-4820-a4e2-7d8386b7a118.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ bb83b7f0-6542-4105-b2f0-5d2018167a9e ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/bb83b7f0-6542-4105-b2f0-5d2018167a9e/MeasureReport-2e6ce7a3-3b86-4820-a4e2-7d8386b7a118.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |
| [ eac1ad4e-e4cb-49c8-a9b1-6ddf5eec85a1 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/eac1ad4e-e4cb-49c8-a9b1-6ddf5eec85a1/MeasureReport-46815b9a-4304-46ab-bfa6-7cb74306e987.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ eac1ad4e-e4cb-49c8-a9b1-6ddf5eec85a1 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/eac1ad4e-e4cb-49c8-a9b1-6ddf5eec85a1/MeasureReport-46815b9a-4304-46ab-bfa6-7cb74306e987.json) | Group_2 | Denominator Exclusion | 1 | 0 | — |
| [ eac1ad4e-e4cb-49c8-a9b1-6ddf5eec85a1 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/eac1ad4e-e4cb-49c8-a9b1-6ddf5eec85a1/MeasureReport-46815b9a-4304-46ab-bfa6-7cb74306e987.json) | Group_3 | Denominator Exclusion | 1 | 0 | — |


#### CMS157FHIRPainIntensityQuantified
[ [cql] ](../../input/cql/CMS157FHIRPainIntensityQuantified.cql) [ [test results] ](../../input/tests/results/CMS157FHIRPainIntensityQuantified.txt)

Mismatched Test Cases (19 of  of 126)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 055640ae-dc71-4e1d-918b-e367013de209 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/055640ae-dc71-4e1d-918b-e367013de209/MeasureReport-1bbaa68f-b303-4828-aa6b-c3f5d25b9246.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ 233d84af-d725-4682-8253-d6c4e02da0d5 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/233d84af-d725-4682-8253-d6c4e02da0d5/MeasureReport-8ebccd0b-cee9-43d9-b663-9d228417615d.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ 2c3f5ac5-6b7f-4bb8-a4fe-8faf0553b21e ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/2c3f5ac5-6b7f-4bb8-a4fe-8faf0553b21e/MeasureReport-c0205a42-bb91-4962-a72f-4df278aae5b7.json) | Group_2 | Initial Population<br>Denominator | 2<br>2 | 0<br>0 | — |
| [ 51d8547c-f07f-4441-b616-f458f38e4506 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/51d8547c-f07f-4441-b616-f458f38e4506/MeasureReport-54825fed-8c96-4302-90ae-f0b99310d3dd.json) | Group_2 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ 5cca62ff-f856-4b8f-9902-6a018a4599cb ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/5cca62ff-f856-4b8f-9902-6a018a4599cb/MeasureReport-c03b4642-f99f-40d7-ae8f-37795a5caf5f.json) | Group_2 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 0<br>0<br>0 | — |
| [ 66c60f6c-2a7b-4868-b9bd-5ede60b61463 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/66c60f6c-2a7b-4868-b9bd-5ede60b61463/MeasureReport-e916d4be-b50b-4fec-92aa-9b8307a9d3ed.json) | Group_2 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ 719a6ae4-ac86-406f-a762-380383e4a74d ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/719a6ae4-ac86-406f-a762-380383e4a74d/MeasureReport-84729f91-b0f3-4571-80b0-40bfa0dd05ee.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 0<br>0<br>0 | — |
| [ 757c5855-602e-4c25-8783-c22afccc1618 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/757c5855-602e-4c25-8783-c22afccc1618/MeasureReport-64d75922-fcb8-4e74-b5e0-c399e8920b43.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ 7cedf97f-741c-4c37-9ae9-40e0b8c64576 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/7cedf97f-741c-4c37-9ae9-40e0b8c64576/MeasureReport-32f463b3-7147-4a6c-aaf5-05478cb060da.json) | Group_2 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ 837cc0e4-cc26-48cd-9d34-232d7fbcd056 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/837cc0e4-cc26-48cd-9d34-232d7fbcd056/MeasureReport-8156684d-e121-4d37-81b6-58a35429e39e.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ 8e23417a-471a-45bb-b936-57466dc6592c ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/8e23417a-471a-45bb-b936-57466dc6592c/MeasureReport-c828863c-4c72-4cc4-8156-ede8adc10db1.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 0<br>0<br>0 | — |
| [ 90d3454a-ca4b-4035-a524-255a2f03bef7 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/90d3454a-ca4b-4035-a524-255a2f03bef7/MeasureReport-a518ac8d-270d-4777-b241-d68e6d89d348.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>2 | 0<br>0<br>0 | — |
| [ 9972f780-aa2f-40e0-ba7d-133d7fe38bc9 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/9972f780-aa2f-40e0-ba7d-133d7fe38bc9/MeasureReport-17ffaaff-f814-456d-a5b2-9481b621a657.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ aa355e31-8d29-4b06-8d13-7d00a2c817da ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/aa355e31-8d29-4b06-8d13-7d00a2c817da/MeasureReport-cd826ca2-6155-4ae2-884d-6fa9c5343198.json) | Group_2 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ c97c9ecf-6c31-4868-bbd3-7a5509bb3882 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/c97c9ecf-6c31-4868-bbd3-7a5509bb3882/MeasureReport-f718a369-2b4b-430a-9d24-9a4f06a7b002.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ d4b441fb-5b3a-40f7-ada1-ecf06376f4fb ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/d4b441fb-5b3a-40f7-ada1-ecf06376f4fb/MeasureReport-72e35d1c-2e54-4a52-ac2e-430785c31ee5.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ e085c0d1-a736-4596-a5cd-7de785d0d144 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/e085c0d1-a736-4596-a5cd-7de785d0d144/MeasureReport-dfa6cb5c-77dd-47e1-968c-8b280300f2d0.json) | Group_2 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ ede0d103-285f-42f0-807e-ff272f1ae70e ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/ede0d103-285f-42f0-807e-ff272f1ae70e/MeasureReport-db410136-ae00-4328-941e-366a83436c05.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ fe6ef07d-bff1-4e0e-9bf4-b0424a1d0ab4 ](../.././input/tests/measure/CMS157FHIRPainIntensityQuantified/fe6ef07d-bff1-4e0e-9bf4-b0424a1d0ab4/MeasureReport-0648e2db-7eb4-422a-b7f2-b920be7285f2.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |


#### CMS159FHIRDepRemissionat12Months
[ [cql] ](../../input/cql/CMS159FHIRDepRemissionat12Months.cql) [ [test results] ](../../input/tests/results/CMS159FHIRDepRemissionat12Months.txt)

Mismatched Test Cases (2 of  of 67)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 491f554e-e897-40c5-ad2b-0983923df4e8 ](../.././input/tests/measure/CMS159FHIRDepRemissionat12Months/491f554e-e897-40c5-ad2b-0983923df4e8/MeasureReport-580087e1-b59e-43eb-b110-692c35a82dca.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 96b6579c-1cee-423f-9433-a72db6fb8a0a ](../.././input/tests/measure/CMS159FHIRDepRemissionat12Months/96b6579c-1cee-423f-9433-a72db6fb8a0a/MeasureReport-e3ec1311-05ed-4a6f-b13f-a4d290865bb3.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | — |


#### CMS165FHIRControllingHighBP
[ [cql] ](../../input/cql/CMS165FHIRControllingHighBP.cql) [ [test results] ](../../input/tests/results/CMS165FHIRControllingHighBP.txt)

Missing Results (1 of 68 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 45e01fed-56bb-483d-a860-af3d566bda11 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/45e01fed-56bb-483d-a860-af3d566bda11/MeasureReport-02991ca7-859d-422d-8849-655760f8e10a.json) | Group_1 | E-11 — resolution pending |


Mismatched Test Cases (29 of  of 68)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 0e867903-400d-4d71-a7fd-dc9b96d94a17 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/0e867903-400d-4d71-a7fd-dc9b96d94a17/MeasureReport-24439e57-9c88-474c-baf8-4d424d40153e.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 1905549a-1783-4195-95b9-b0879cb81d96 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/1905549a-1783-4195-95b9-b0879cb81d96/MeasureReport-48b71fbe-f1ba-4c56-950a-30901e055481.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 29d930b1-1bb6-4089-9ed6-aa2b7b77d5a4 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/29d930b1-1bb6-4089-9ed6-aa2b7b77d5a4/MeasureReport-1590b87e-b65f-4331-8ffa-0d2952d2fba0.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 2c55811b-1571-43e5-919c-f90bf763b3d4 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/2c55811b-1571-43e5-919c-f90bf763b3d4/MeasureReport-75c17983-f022-41f6-8008-b1f8cc73f3c6.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 352a05d3-750c-45bd-a170-a8a8822b7697 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/352a05d3-750c-45bd-a170-a8a8822b7697/MeasureReport-4c90f4fe-b99f-4678-be7d-65a13fc481fb.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 3e214018-7420-4e1f-a24d-e9426ace2bd8 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/3e214018-7420-4e1f-a24d-e9426ace2bd8/MeasureReport-b4faaa79-84c9-4e1f-a15a-0a0267eb1a1b.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 474b2964-23a1-4c77-ad16-8a21543b2ed3 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/474b2964-23a1-4c77-ad16-8a21543b2ed3/MeasureReport-b75e4e39-b15e-4501-a40b-b207c963fd7a.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 481692c7-2cf7-48fc-8269-967f5d7753bc ](../.././input/tests/measure/CMS165FHIRControllingHighBP/481692c7-2cf7-48fc-8269-967f5d7753bc/MeasureReport-adc65eee-980f-44e9-b3b3-35c9cd8fa5b0.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4b31dc2b-7867-4766-8a8c-e1971d1e570a ](../.././input/tests/measure/CMS165FHIRControllingHighBP/4b31dc2b-7867-4766-8a8c-e1971d1e570a/MeasureReport-a85d455e-5dbe-4898-88bb-592890b57cde.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 50d7cf81-dff4-45eb-b43d-0e40b08c3a75 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/50d7cf81-dff4-45eb-b43d-0e40b08c3a75/MeasureReport-40890a29-fe94-447c-a8f2-0d28f8c549be.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 5421e420-8d42-4628-ba47-9abaf9ebfaa8 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/5421e420-8d42-4628-ba47-9abaf9ebfaa8/MeasureReport-a4ed4d25-8b89-4cf4-aa20-2d51383b9cf4.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 59d7f239-7614-4e6e-a973-fe107aee5749 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/59d7f239-7614-4e6e-a973-fe107aee5749/MeasureReport-85177012-7ab0-4a49-9def-647f931e1ab9.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 686e2c47-b08f-465c-ab31-1712dd72028b ](../.././input/tests/measure/CMS165FHIRControllingHighBP/686e2c47-b08f-465c-ab31-1712dd72028b/MeasureReport-905d2cf9-d59c-4a68-bca9-b7c30e6548c8.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 6f37e357-7575-4b40-a63e-4b882532250f ](../.././input/tests/measure/CMS165FHIRControllingHighBP/6f37e357-7575-4b40-a63e-4b882532250f/MeasureReport-fba2c617-1601-4900-a65c-6e1e1f7adcbc.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 7c59efb5-56ab-4a25-af83-bd81daeee026 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/7c59efb5-56ab-4a25-af83-bd81daeee026/MeasureReport-f6bb2b41-1e46-4339-ad99-380342b1fca0.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 821185af-e5b2-4552-a63c-36b64a9200a9 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/821185af-e5b2-4552-a63c-36b64a9200a9/MeasureReport-774c09d2-ef68-44ce-a0ac-28087cdbab94.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 926b705a-b222-4c64-9d3f-ad64ead74295 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/926b705a-b222-4c64-9d3f-ad64ead74295/MeasureReport-9147733c-3bed-4c88-b326-a16ad8407e02.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 972c7128-f3c2-401d-89f3-a0752dd02620 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/972c7128-f3c2-401d-89f3-a0752dd02620/MeasureReport-5d808dd7-654f-4f0b-baa8-5f252cb1c490.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 9f063f76-a97a-4bba-9f6a-35e7a429a72c ](../.././input/tests/measure/CMS165FHIRControllingHighBP/9f063f76-a97a-4bba-9f6a-35e7a429a72c/MeasureReport-f1c55ef7-2274-491a-b508-21faf51aacec.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ a7ec972f-f0c1-428d-aba5-ba76cba5cd73 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/a7ec972f-f0c1-428d-aba5-ba76cba5cd73/MeasureReport-d29c0a7e-08ee-46de-9f07-87dbccb13632.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ aa1f02c0-ded0-4b30-9f0d-c8be54aa436b ](../.././input/tests/measure/CMS165FHIRControllingHighBP/aa1f02c0-ded0-4b30-9f0d-c8be54aa436b/MeasureReport-5656f7bb-5437-4ccb-84ce-5946dd844837.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ aa87ac34-227b-4424-84d2-62aaba57c232 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/aa87ac34-227b-4424-84d2-62aaba57c232/MeasureReport-f10641ad-4bdc-47c4-90ae-fbc2b80200de.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ bfdc37c9-105c-4765-a2ba-d7da92ec9a47 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/bfdc37c9-105c-4765-a2ba-d7da92ec9a47/MeasureReport-16590047-b431-49be-8fc4-a67554e01c8f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ cdfb5385-a466-4d41-9dce-cc50f88d0666 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/cdfb5385-a466-4d41-9dce-cc50f88d0666/MeasureReport-d349c4bb-3aa3-4b23-b141-22f3be31387e.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d6be5093-9772-4e0f-83e1-b56b26d55529 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/d6be5093-9772-4e0f-83e1-b56b26d55529/MeasureReport-f297fa0b-4244-4f20-87be-c935674d1b6f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ e56c60ca-d0d0-4910-af2e-1d8a074d129a ](../.././input/tests/measure/CMS165FHIRControllingHighBP/e56c60ca-d0d0-4910-af2e-1d8a074d129a/MeasureReport-dc2ea439-c574-4beb-b83d-55f24ef75f67.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ f2d1fd7e-35ae-45cd-86e6-8b874c3e3fb9 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/f2d1fd7e-35ae-45cd-86e6-8b874c3e3fb9/MeasureReport-657791f1-242d-40ee-8b6a-1fdb4d85c849.json) | Group_1 | Numerator | 1 | 0 | — |
| [ f5b461d7-e382-4616-a763-d745867735d0 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/f5b461d7-e382-4616-a763-d745867735d0/MeasureReport-c40356e4-5065-4c08-b691-08705513f287.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ f9bf76c5-7b85-4fd7-b883-b7c14e8b1801 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/f9bf76c5-7b85-4fd7-b883-b7c14e8b1801/MeasureReport-941a9d79-fa3f-435d-8bf0-21c49474528f.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS177FHIRChildMDDSuicideAssmt
[ [cql] ](../../input/cql/CMS177FHIRChildMDDSuicideAssmt.cql) [ [test results] ](../../input/tests/results/CMS177FHIRChildMDDSuicideAssmt.txt)

Mismatched Test Cases (1 of  of 41)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 85e6225c-a9bb-4338-a228-297564e38c4d ](../.././input/tests/measure/CMS177FHIRChildMDDSuicideAssmt/85e6225c-a9bb-4338-a228-297564e38c4d/MeasureReport-89005c1a-09a3-421d-aa89-d44837ae5904.json) | Group_1 | Initial Population<br>Denominator | 0<br>0 | 1<br>1 | — |


#### CMS190FHIRVTEProphylaxisICU
[ [cql] ](../../input/cql/CMS190FHIRVTEProphylaxisICU.cql) [ [test results] ](../../input/tests/results/CMS190FHIRVTEProphylaxisICU.txt)

Mismatched Test Cases (24 of  of 125)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 208cb0f9-a6e9-4207-b6a4-3325fb463099 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/208cb0f9-a6e9-4207-b6a4-3325fb463099/MeasureReport-3cb6a3ba-7c97-47c9-9ac7-cd39959ecc39.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 282ae3a0-a4fd-4fed-8ce9-bff3840c7ca9 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/282ae3a0-a4fd-4fed-8ce9-bff3840c7ca9/MeasureReport-bb0ca899-9892-4d53-a171-fa41dc45d404.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 2bcbe960-db7d-4088-a574-d771baf0f9c7 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/2bcbe960-db7d-4088-a574-d771baf0f9c7/MeasureReport-cfb7bc83-85fe-45b7-b133-a2b1429e1e31.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 39215b49-af59-45a7-a773-65e8353dfafd ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/39215b49-af59-45a7-a773-65e8353dfafd/MeasureReport-4358ad9b-1c93-4569-9985-0f388fe56ebe.json) | Group_1 | Initial Population<br>Denominator | 0<br>0 | 1<br>1 | — |
| [ 4724cb2f-b5bd-4c50-85cc-4a5ba25f04ca ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/4724cb2f-b5bd-4c50-85cc-4a5ba25f04ca/MeasureReport-4ca4bed8-36fa-40a9-a273-ce3f8e9f377e.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 4c32b73b-abba-431b-a352-f0f454e7c9dd ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/4c32b73b-abba-431b-a352-f0f454e7c9dd/MeasureReport-e9ac894c-9f4c-47d8-8325-7750b25036e0.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 4fc421c7-e490-4d4e-a326-53d08635efb9 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/4fc421c7-e490-4d4e-a326-53d08635efb9/MeasureReport-c206bcec-44ba-493e-8114-8ae57bf6b7e6.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 632831b0-1ebf-47b5-b439-3a124cd77c37 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/632831b0-1ebf-47b5-b439-3a124cd77c37/MeasureReport-dff9d9bd-b0cc-400f-815b-9255b426e828.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 7e7f4563-a628-40ab-990b-ca0837313759 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/7e7f4563-a628-40ab-990b-ca0837313759/MeasureReport-6b131b52-199b-46ac-b099-fad21dbda4ad.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 8ec9cf6a-2dcd-4c2e-9e2e-1ba237b66808 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/8ec9cf6a-2dcd-4c2e-9e2e-1ba237b66808/MeasureReport-53445771-3d55-46d3-8091-a92e9f7a0915.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 95a54d01-197e-48ef-bb48-d3d398aecbe8 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/95a54d01-197e-48ef-bb48-d3d398aecbe8/MeasureReport-89a6d854-e283-4df7-bd78-60dfa86483cf.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 98d6da30-f55a-411d-94b4-359b204bcb5a ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/98d6da30-f55a-411d-94b4-359b204bcb5a/MeasureReport-6e63dc69-1e82-44f5-bccb-e417baa090e5.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 9ddea16c-55d3-4dda-a1d8-a256fbff0b64 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/9ddea16c-55d3-4dda-a1d8-a256fbff0b64/MeasureReport-90c1518e-8e3a-4f2a-b266-9210baffdcbf.json) | Group_1 | Numerator | 1 | 0 | — |
| [ a30e5588-0e2a-487c-b4d3-15d9e0006741 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/a30e5588-0e2a-487c-b4d3-15d9e0006741/MeasureReport-bdba93da-ab6a-4f3b-b72e-86f0168f9b43.json) | Group_1 | Numerator | 1 | 0 | — |
| [ a82cd0c1-900e-4ab3-a498-840ac1608486 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/a82cd0c1-900e-4ab3-a498-840ac1608486/MeasureReport-94a26fc6-de93-43a2-9be0-2ca52b24d988.json) | Group_1 | Denominator Exclusion | 0 | 1 | — |
| [ a9c75661-be1c-41b2-aa15-222cc7d2ca81 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/a9c75661-be1c-41b2-aa15-222cc7d2ca81/MeasureReport-21816bad-859d-416f-883b-24246a1db64c.json) | Group_1 | Numerator | 1 | 0 | — |
| [ c0481b47-738b-4a09-8901-915ece2beb7e ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/c0481b47-738b-4a09-8901-915ece2beb7e/MeasureReport-a28ce7c4-934f-4fac-a002-aee0c87b7cb9.json) | Group_1 | Numerator | 1 | 0 | — |
| [ dbfc823e-0e2f-409d-a409-2d9399db1118 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/dbfc823e-0e2f-409d-a409-2d9399db1118/MeasureReport-e7db6f05-3243-4d94-bf90-1b5c6cff7c10.json) | Group_1 | Numerator | 1 | 0 | — |
| [ e8931859-4ad8-49c8-9cdd-8697293456a2 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/e8931859-4ad8-49c8-9cdd-8697293456a2/MeasureReport-cfc06289-ff74-4caa-ba81-3647f98e3646.json) | Group_1 | Numerator | 1 | 0 | — |
| [ f00f3778-6ad1-466d-a3bd-bcbc63d62b55 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f00f3778-6ad1-466d-a3bd-bcbc63d62b55/MeasureReport-d3f2a4f2-6c34-484a-b29b-b2d34f1d8334.json) | Group_1 | Numerator | 1 | 0 | — |
| [ f035a977-30d0-487c-b542-a596e718420c ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f035a977-30d0-487c-b542-a596e718420c/MeasureReport-2318030c-b923-45ed-988f-5925f46200e9.json) | Group_1 | Numerator | 1 | 0 | — |
| [ f82746cf-f6cd-4fcc-bc9e-7e569ae26211 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f82746cf-f6cd-4fcc-bc9e-7e569ae26211/MeasureReport-ecd1d81f-c8df-4d19-b85f-5bb0d5c9f771.json) | Group_1 | Numerator | 1 | 0 | — |
| [ f859dd94-f201-4517-a368-32b98dd486c9 ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f859dd94-f201-4517-a368-32b98dd486c9/MeasureReport-da236e59-3d0a-46c4-a352-3eec5846dbe6.json) | Group_1 | Numerator | 1 | 0 | — |
| [ f981eba4-4aac-45ce-8c52-f0bc02c9a0dc ](../.././input/tests/measure/CMS190FHIRVTEProphylaxisICU/f981eba4-4aac-45ce-8c52-f0bc02c9a0dc/MeasureReport-01143c30-f69f-464f-99fd-405617644ce8.json) | Group_1 | Numerator | 1 | 0 | — |


#### CMS0334FHIRPCCesareanBirth
[ [cql] ](../../input/cql/CMS0334FHIRPCCesareanBirth.cql) [ [test results] ](../../input/tests/results/CMS0334FHIRPCCesareanBirth.txt)

Mismatched Test Cases (1 of  of 138)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ c58acff5-248b-49c9-b18d-69e4a84a08d9 ](../.././input/tests/measure/CMS0334FHIRPCCesareanBirth/c58acff5-248b-49c9-b18d-69e4a84a08d9/MeasureReport-920b0c2e-1f1f-42d3-ab1f-1d7b12fa4bd0.json) | Group_1 | Denominator<br>Denominator Exclusion | 1<br>1 | 0<br>0 | — |


#### CMS347FHIRStatinPreventionTxCVD
[ [cql] ](../../input/cql/CMS347FHIRStatinPreventionTxCVD.cql) [ [test results] ](../../input/tests/results/CMS347FHIRStatinPreventionTxCVD.txt)

Missing Results (4 of 752 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 6da189af-7eb0-47b0-8c77-905944706aa1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-7eb0-47b0-8c77-905944706aa1/MeasureReport-503d8574-49d9-4cc6-809a-16a00b4ddc0a.json) | Group_1 | — |
| [ 6da189af-7eb0-47b0-8c77-905944706aa1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-7eb0-47b0-8c77-905944706aa1/MeasureReport-503d8574-49d9-4cc6-809a-16a00b4ddc0a.json) | Group_2 | — |
| [ 6da189af-7eb0-47b0-8c77-905944706aa1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-7eb0-47b0-8c77-905944706aa1/MeasureReport-503d8574-49d9-4cc6-809a-16a00b4ddc0a.json) | Group_3 | — |
| [ 6da189af-7eb0-47b0-8c77-905944706aa1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-7eb0-47b0-8c77-905944706aa1/MeasureReport-503d8574-49d9-4cc6-809a-16a00b4ddc0a.json) | Group_4 | — |


Mismatched Test Cases (135 of  of 752)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 0784160c-98b6-43a2-baa1-77ea9f3fe884 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0784160c-98b6-43a2-baa1-77ea9f3fe884/MeasureReport-cafc0f57-647c-40a8-a97b-c39c16af6f01.json) | Group_4 | Denominator Exception | 1 | 0 | — |
| [ 08882e8d-afd1-4a5e-a30b-a5a0ed9e1010 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08882e8d-afd1-4a5e-a30b-a5a0ed9e1010/MeasureReport-acb75523-f5ac-4a3b-8aed-3a453acfa9a0.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 08dfc736-3cb5-467c-93cf-99146604a8f4 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/08dfc736-3cb5-467c-93cf-99146604a8f4/MeasureReport-d6036ed7-ab63-4060-bb41-d2faaae2d9c6.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 0aaae01e-d3b0-4b76-abf8-a044fd4f5d80 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0aaae01e-d3b0-4b76-abf8-a044fd4f5d80/MeasureReport-b458a7fb-93c1-4b34-9056-26bdeaac5f32.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 0ba942ff-50d6-4123-ab21-adcf5fdff0df ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ba942ff-50d6-4123-ab21-adcf5fdff0df/MeasureReport-18f93977-8cfb-4d93-82af-9d6f292eda19.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_2 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 0ce81150-5908-49a1-bef9-21406359af63 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0ce81150-5908-49a1-bef9-21406359af63/MeasureReport-fb6196c7-1a0a-4fbd-856b-f87833da5d80.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 0f204e98-0782-43a3-ae53-b516cc8d5797 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f204e98-0782-43a3-ae53-b516cc8d5797/MeasureReport-bef8645f-ffd4-409e-b05c-77d2c204fa16.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 0f853b02-7949-4d97-ab69-1e48045afe95 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/0f853b02-7949-4d97-ab69-1e48045afe95/MeasureReport-ba5835e1-5b80-4876-9fdb-2570d1c77265.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 1116b208-af60-4f6b-a5f1-448209aec45f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1116b208-af60-4f6b-a5f1-448209aec45f/MeasureReport-f837ff1a-64b4-402f-b5e9-d56eff104c52.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 113749ee-bb22-4395-9621-642f98839340 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/113749ee-bb22-4395-9621-642f98839340/MeasureReport-54e759e6-bf9e-4246-abf6-852dddcdab7a.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 15d7fcaa-773f-4888-8b13-bc077cbfdf4a ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/15d7fcaa-773f-4888-8b13-bc077cbfdf4a/MeasureReport-d9557b10-9fd8-49f3-99ee-29c1912b1bb6.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ 1831f057-fa97-4c2b-b6cc-9830e4a60e11 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1831f057-fa97-4c2b-b6cc-9830e4a60e11/MeasureReport-9dd32caf-b06d-476e-b513-b11d66040463.json) | Group_4 | Denominator Exception | 1 | 0 | — |
| [ 1ba7b147-b701-424c-bade-4e8270547030 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1ba7b147-b701-424c-bade-4e8270547030/MeasureReport-2278c703-994b-4b13-8e3b-c726ba6b8530.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 1d3021bb-b593-4efc-af5b-320243bbe9b7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1d3021bb-b593-4efc-af5b-320243bbe9b7/MeasureReport-13a9bd51-27c8-4992-bbb0-c491175b6a6e.json) | Group_3 | Initial Population<br>Denominator<br>Denominator Exception | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ 26101306-010f-48c5-aa83-8a94f280f755 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/26101306-010f-48c5-aa83-8a94f280f755/MeasureReport-0f51c880-da3c-4755-ab84-d17cbed4a744.json) | Group_4 | Denominator Exception | 1 | 0 | — |
| [ 2c5a09d4-18c9-4128-86fb-bd49871f9231 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/2c5a09d4-18c9-4128-86fb-bd49871f9231/MeasureReport-bf872dce-b795-49e9-831f-ae54ca8b92cf.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ 2cff757c-4470-46a2-a685-6e23cf82c045 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/2cff757c-4470-46a2-a685-6e23cf82c045/MeasureReport-4cae9687-fba5-4a5d-af12-fe19c1ef3760.json) | Group_4 | Numerator | 0 | 1 | — |
| [ 3137d292-5094-49ef-82da-d9809b599030 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3137d292-5094-49ef-82da-d9809b599030/MeasureReport-72cd7673-4efe-4845-a66e-6cd26e429f2f.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_3 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3b5da2bf-0fb9-4efc-bc54-4bd329ed31af/MeasureReport-770e98b5-8e09-421a-9507-0c93e75de117.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 3e09af44-0445-4077-b73c-6896fdbe49c5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/3e09af44-0445-4077-b73c-6896fdbe49c5/MeasureReport-4c837a9d-d87f-43c3-8c01-6e3e397dcb04.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ 476bff0b-a87a-413b-91ae-c3a14b7778b1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/476bff0b-a87a-413b-91ae-c3a14b7778b1/MeasureReport-e0046c1d-ca4f-4c06-8bb1-61dd26a3ed06.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 4d6fb0e2-636d-426f-802b-5ecb4f059440 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4d6fb0e2-636d-426f-802b-5ecb4f059440/MeasureReport-c57c750d-65ce-45dd-945c-fceac22889dd.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_2 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 4e72d245-e401-4be7-a743-84ab6a842871 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/4e72d245-e401-4be7-a743-84ab6a842871/MeasureReport-dc2f1b7e-7765-4512-9cd5-f0ed5b3ef7b1.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 50c7b2fc-879b-4088-88bf-36a9f8c0baf0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/50c7b2fc-879b-4088-88bf-36a9f8c0baf0/MeasureReport-e1d0af6a-2d30-49c2-8a21-06937d942360.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_2 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 52b48d35-f47c-4013-9cdc-700baad0fc0f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/52b48d35-f47c-4013-9cdc-700baad0fc0f/MeasureReport-e6197229-2386-4e05-adbb-687a89230972.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 537d14db-6ced-4cd2-9553-e88bd6551771 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/537d14db-6ced-4cd2-9553-e88bd6551771/MeasureReport-b0ed7ddf-a25e-49b6-8b0f-d8e6ff9f6726.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 59d6bb14-b82e-4295-baf1-d96be73e1e38 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/59d6bb14-b82e-4295-baf1-d96be73e1e38/MeasureReport-afb9b378-90a1-4117-9d33-cdeacf0484b6.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ 5b37b5a5-0e28-4b28-9889-8878d41ff9cf ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5b37b5a5-0e28-4b28-9889-8878d41ff9cf/MeasureReport-ab20807f-0940-40df-b735-9fa683e53672.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ 5cebab0f-d32e-4adc-bef3-90812d6c5819 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5cebab0f-d32e-4adc-bef3-90812d6c5819/MeasureReport-528b17db-921e-46ef-8130-9fa98b0d6deb.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 5f799983-39d3-4f03-9a9a-125dc6f12f13 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/5f799983-39d3-4f03-9a9a-125dc6f12f13/MeasureReport-71b469e0-0693-413d-8749-8167ef591d78.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 60b9bda6-6c16-4797-8278-0a667008a69e ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/60b9bda6-6c16-4797-8278-0a667008a69e/MeasureReport-a99921b9-62c1-4e59-b888-4d7f63a8187b.json) | Group_4 | Denominator Exception | 1 | 0 | — |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 70fd1056-5313-417f-bbbe-9f2bacf942bb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/70fd1056-5313-417f-bbbe-9f2bacf942bb/MeasureReport-fc6ec9d4-f5e2-4491-9079-d5b3567db0c9.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 716760c5-b72e-4d46-b8df-c3b0f86d90ad ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/716760c5-b72e-4d46-b8df-c3b0f86d90ad/MeasureReport-64038237-a5c7-4bbf-b444-e470967a2855.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 74499ca5-db3b-4ce1-92e0-e19c6590d138 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/74499ca5-db3b-4ce1-92e0-e19c6590d138/MeasureReport-0338f0c4-356e-463c-acac-c49e2c7ad4d6.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 74e5f17e-ae6b-4e3c-8183-e75381377d23 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/74e5f17e-ae6b-4e3c-8183-e75381377d23/MeasureReport-c5168eb6-a6e3-4187-9fc7-5b02970823a4.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 759a89b4-51ed-4622-adae-6b0930701ebb ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/759a89b4-51ed-4622-adae-6b0930701ebb/MeasureReport-cec73ecb-4bb0-4013-8a7c-d17d64b73a07.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/7b8b48b3-76d4-4492-81a1-93fdea67b0c1/MeasureReport-b69b8128-6e21-4a15-8da3-33aa315a17cf.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 821087e5-a030-49ac-95b5-5b9ab38e88da ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/821087e5-a030-49ac-95b5-5b9ab38e88da/MeasureReport-5fb8f0ee-32e7-494e-8b9d-5953d21bf5d0.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ 86bacb29-41c3-4ea8-8e4b-3e13c075e557 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/86bacb29-41c3-4ea8-8e4b-3e13c075e557/MeasureReport-d0a8a2af-6b3e-422c-b9e1-0d3d8098ff3b.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ 8927dd81-b976-4b7f-a78c-c4215ee8fc9a ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8927dd81-b976-4b7f-a78c-c4215ee8fc9a/MeasureReport-332c069b-686d-4491-862a-2963e3679d28.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_3 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 8b0f2e04-8c60-4f6e-adc5-8967a540a18f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8b0f2e04-8c60-4f6e-adc5-8967a540a18f/MeasureReport-c0b2c86c-809d-4459-9346-efccccd91090.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 8c357499-cb9a-41c9-9060-1bbbefb0fd7e ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/8c357499-cb9a-41c9-9060-1bbbefb0fd7e/MeasureReport-0939446e-5ba5-405e-a363-4f4852c6d7be.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95ab5fd7-b1be-4dd3-ba42-1b48215fab70/MeasureReport-747f10c8-ac05-4676-bad9-1dee3ceef657.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 95fad34f-db86-4e4a-a8a2-42a3b7ac15dc ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/95fad34f-db86-4e4a-a8a2-42a3b7ac15dc/MeasureReport-a06849cd-08a2-48b7-ad0a-de1af11852e6.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_3 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 9a06f385-0bed-4f35-9af4-1ff7971c07f5 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9a06f385-0bed-4f35-9af4-1ff7971c07f5/MeasureReport-2b63c3a5-c7bd-4449-acc6-6b91709d6cb6.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ 9c2afd42-581e-418b-9eaa-3ddf4918c9ac ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9c2afd42-581e-418b-9eaa-3ddf4918c9ac/MeasureReport-c4a7d373-1d9f-4885-a827-26eb666b2db2.json) | Group_4 | Denominator Exception | 1 | 0 | — |
| [ 9e01f70e-cb9c-451b-8993-8664e31d92e2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9e01f70e-cb9c-451b-8993-8664e31d92e2/MeasureReport-84293b41-76cc-4e04-9ef0-9c0872167423.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/9edcce2d-8d32-4f4f-88a5-6fa689b73f8d/MeasureReport-6b21ac9f-4a33-4694-b2b9-b90b80373d60.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ b35ba523-abea-4848-8dac-256c1727447c ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/b35ba523-abea-4848-8dac-256c1727447c/MeasureReport-f6992d99-7d19-40cc-a2fb-ec3d516910d9.json) | Group_4 | Denominator Exception | 1 | 0 | — |
| [ b88292a5-2443-44a2-a268-2a6cb95f92bd ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/b88292a5-2443-44a2-a268-2a6cb95f92bd/MeasureReport-d10fdd98-a635-40a8-ace7-7c0579f3af0f.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ be29ff82-9191-4b5f-91ca-cc5590fea905 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/be29ff82-9191-4b5f-91ca-cc5590fea905/MeasureReport-db0c826a-8851-4099-abc9-e879908519b2.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ c75e56eb-e95d-4c65-b184-3565362eb3ba ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/c75e56eb-e95d-4c65-b184-3565362eb3ba/MeasureReport-21350675-60d7-4ed0-b1ba-832ff183b480.json) | Group_4 | Denominator Exception | 1 | 0 | — |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ cbb6a940-7c9b-4d80-b9be-39a029f6f0b0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/cbb6a940-7c9b-4d80-b9be-39a029f6f0b0/MeasureReport-c2e98e8d-94a0-496e-b96e-b70a240263b2.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ d06256e5-091f-445e-898f-b8c31d8d3772 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d06256e5-091f-445e-898f-b8c31d8d3772/MeasureReport-45544d64-d0c8-4d0a-86a3-20ad5859e58d.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ d2c7d463-775a-4c8d-bcb0-35ea689b2d20 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d2c7d463-775a-4c8d-bcb0-35ea689b2d20/MeasureReport-30bac3e4-0779-4b97-8d42-9cc771b7278f.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ d3a48d69-2269-472a-9c27-da2c658e8c68 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/d3a48d69-2269-472a-9c27-da2c658e8c68/MeasureReport-1006e8a9-4ca9-43ce-8eec-3ad24503065b.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ dbca4643-bd37-4e01-8024-fb7c70692fe9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/dbca4643-bd37-4e01-8024-fb7c70692fe9/MeasureReport-41464520-8775-44ef-ac95-a781498a2deb.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ df05b853-3e6d-4a12-b1db-fd9d0ec790a2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/df05b853-3e6d-4a12-b1db-fd9d0ec790a2/MeasureReport-9dba8a63-ec06-49b0-b2f7-023afa112d14.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ e1c47dc2-2705-4c32-8000-415987028df9 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e1c47dc2-2705-4c32-8000-415987028df9/MeasureReport-78d08883-6e06-488a-82d8-6b6564cc3df4.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ e4547b2c-ce1c-4ffb-b5d4-c99687424bf0 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e4547b2c-ce1c-4ffb-b5d4-c99687424bf0/MeasureReport-6a80fdc0-8735-4945-b151-f4ad5f5dd9bf.json) | Group_4 | Denominator Exception | 1 | 0 | — |
| [ e656adac-2016-40a4-833f-0c5a02952ba3 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e656adac-2016-40a4-833f-0c5a02952ba3/MeasureReport-c0a2d9e7-1144-4e67-954f-56080d7ffd06.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ e8020421-14a3-4c64-99c4-3366c1400bd7 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/e8020421-14a3-4c64-99c4-3366c1400bd7/MeasureReport-2dbf1150-20c5-4c8b-9629-746db44ed011.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ f120b2b6-40ba-4ae3-b087-c64e8e3bdf11 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f120b2b6-40ba-4ae3-b087-c64e8e3bdf11/MeasureReport-366b9c5c-270d-4e9c-9def-70b6c21e0b9b.json) | Group_3 | Denominator Exception | 1 | 0 | — |
| [ f2136084-b5c4-4171-9d1b-d759637ddcfa ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f2136084-b5c4-4171-9d1b-d759637ddcfa/MeasureReport-0bce676f-597b-4a01-abbf-4356a5145a0e.json) | Group_4 | Denominator Exception | 1 | 0 | — |
| [ f6a5913b-bfdd-4ccf-8700-3c949b0639ed ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f6a5913b-bfdd-4ccf-8700-3c949b0639ed/MeasureReport-0baeb52a-0371-4683-b0ee-b6a0640efd2a.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ f8563fcf-4e09-4309-841b-bcce373bc4b2 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/f8563fcf-4e09-4309-841b-bcce373bc4b2/MeasureReport-adf299bf-cd60-45da-8fa2-213ad4655734.json) | Group_4 | Denominator Exception | 0 | 1 | — |
| [ fa446b35-031d-4eb5-b7f1-5782580e5209 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/fa446b35-031d-4eb5-b7f1-5782580e5209/MeasureReport-cb1ca02f-7f9b-4ae3-88d7-2ddb268f3061.json) | Group_2 | Denominator Exception | 1 | 0 | — |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_1 | Denominator Exception | 0 | 1 | — |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_2 | Denominator Exception | 0 | 1 | — |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_3 | Denominator Exception | 0 | 1 | — |
| [ faae1173-bc93-4fd2-a22f-e7726430857f ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/faae1173-bc93-4fd2-a22f-e7726430857f/MeasureReport-6db8fc35-78d7-4bd7-9941-eb7be2aef5d5.json) | Group_4 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ fcd4fe20-9013-4d1c-965b-1445f0088624 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/fcd4fe20-9013-4d1c-965b-1445f0088624/MeasureReport-54543ebb-c112-4cea-943c-79e7866e1d08.json) | Group_3 | Denominator Exception | 1 | 0 | — |


#### CMS645FHIRBoneDensityPCADTherapy
[ [cql] ](../../input/cql/CMS645FHIRBoneDensityPCADTherapy.cql) [ [test results] ](../../input/tests/results/CMS645FHIRBoneDensityPCADTherapy.txt)

Mismatched Test Cases (3 of  of 51)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 8c41481d-f89e-4113-ba12-df7c53e93d80 ](../.././input/tests/measure/CMS645FHIRBoneDensityPCADTherapy/8c41481d-f89e-4113-ba12-df7c53e93d80/MeasureReport-5199a981-c1fd-4530-bd20-438541e8993f.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6 ](../.././input/tests/measure/CMS645FHIRBoneDensityPCADTherapy/c5bfac21-0dbf-4cf5-bc92-d7eff1d0a6c6/MeasureReport-ff0dae36-899e-426e-9f9d-0b7270a49bfb.json) | Group_1 | Denominator Exception<br>Numerator | 1<br>0 | 0<br>1 | — |
| [ d07cf359-d46c-4adf-b2d4-e02a2f43b78e ](../.././input/tests/measure/CMS645FHIRBoneDensityPCADTherapy/d07cf359-d46c-4adf-b2d4-e02a2f43b78e/MeasureReport-2e25820a-ce7b-4c83-b5b6-56eeec0f5577.json) | Group_1 | Numerator | 0 | 1 | — |


#### CMS646FHIRIntravesicalBCGTherapy
[ [cql] ](../../input/cql/CMS646FHIRIntravesicalBCGTherapy.cql) [ [test results] ](../../input/tests/results/CMS646FHIRIntravesicalBCGTherapy.txt)

Missing Results (1 of 38 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/342d2bec-0acc-43e5-aaf7-3c9a65b09f91/MeasureReport-12cd358b-deb0-4130-a045-4c6b61e110c0.json) | Group_1 | — |


Mismatched Test Cases (3 of  of 38)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 10cec7db-41ae-49ad-b883-022f19d92a8b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/10cec7db-41ae-49ad-b883-022f19d92a8b/MeasureReport-b8b4961d-450b-4980-ac8f-95500c6393d4.json) | Group_1 | Denominator Exclusion | 0 | 1 | — |
| [ ab48e0c0-6543-4537-8f00-bfcdcba7a81b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/ab48e0c0-6543-4537-8f00-bfcdcba7a81b/MeasureReport-ea6cfef5-54d2-4d6d-a7aa-48cf8e749eaf.json) | Group_1 | Numerator | 0 | 1 | — |
| [ e648fa70-0532-49b0-92f6-dfb5a6d28d94 ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/e648fa70-0532-49b0-92f6-dfb5a6d28d94/MeasureReport-57107c42-23df-40d4-92fe-5f7fdd475629.json) | Group_1 | Denominator Exception | 1 | 0 | — |


#### CMS771FHIRUrinarySymptomScoreBPH
[ [cql] ](../../input/cql/CMS771FHIRUrinarySymptomScoreBPH.cql) [ [test results] ](../../input/tests/results/CMS771FHIRUrinarySymptomScoreBPH.txt)

Mismatched Test Cases (7 of  of 31)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 051c5977-9f2c-4e8b-8e02-ac3ec0c718d6 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/051c5977-9f2c-4e8b-8e02-ac3ec0c718d6/MeasureReport-13a299d2-1f32-41d7-b226-7380902e41b7.json) | Group_1 | Denominator | 1 | 0 | — |
| [ 3ab3ac1d-9b5e-4087-8862-dcb2562fb90f ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/3ab3ac1d-9b5e-4087-8862-dcb2562fb90f/MeasureReport-47dae27e-89cf-4ee5-8c8b-bf1e44997d07.json) | Group_1 | Denominator | 1 | 0 | — |
| [ 4c234ec0-3f89-4d55-b767-219d1130f634 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/4c234ec0-3f89-4d55-b767-219d1130f634/MeasureReport-47a91ced-cb5f-44c0-9417-e8efa33a4b08.json) | Group_1 | Numerator | 1 | 0 | — |
| [ 9be591a0-517b-4be2-b652-a29be0c75c15 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/9be591a0-517b-4be2-b652-a29be0c75c15/MeasureReport-004d2ae6-6c2e-49f8-bf07-26cada3bbaf3.json) | Group_1 | Numerator | 1 | 0 | — |
| [ bc79e5bc-237e-44be-b5fc-c5c4efb50286 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/bc79e5bc-237e-44be-b5fc-c5c4efb50286/MeasureReport-621196a7-ca5f-4408-8508-851332413956.json) | Group_1 | Numerator | 1 | 0 | — |
| [ bf0f8968-c2c0-4416-88db-11ea3e3da968 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/bf0f8968-c2c0-4416-88db-11ea3e3da968/MeasureReport-bcce208a-3ff4-4c82-9d49-c0b64ccb9138.json) | Group_1 | Numerator | 1 | 0 | — |
| [ e90d90a7-3071-44de-8089-ad7b6f5f3e5d ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/e90d90a7-3071-44de-8089-ad7b6f5f3e5d/MeasureReport-9ef2db11-d78a-49af-a2ac-6536fac264a1.json) | Group_1 | Numerator | 1 | 0 | — |


#### CMS816FHIRHHHypo
[ [cql] ](../../input/cql/CMS816FHIRHHHypo.cql) [ [test results] ](../../input/tests/results/CMS816FHIRHHHypo.txt)

Mismatched Test Cases (12 of  of 28)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 05c8cd12-addd-4b94-8f92-da093c556a84 ](../.././input/tests/measure/CMS816FHIRHHHypo/05c8cd12-addd-4b94-8f92-da093c556a84/MeasureReport-e66fcfe4-57f5-4259-bb05-540d4f6a864c.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ 1d2bb25a-21a7-4529-9486-a320d4864719 ](../.././input/tests/measure/CMS816FHIRHHHypo/1d2bb25a-21a7-4529-9486-a320d4864719/MeasureReport-b0513b24-8789-4c07-a13d-322d9defbeb8.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ 2adf5469-46a1-4020-be3b-01f91f8acc9d ](../.././input/tests/measure/CMS816FHIRHHHypo/2adf5469-46a1-4020-be3b-01f91f8acc9d/MeasureReport-af8c832f-f1ad-407a-9751-575339d08367.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ 304052f7-e416-4da4-87ae-488e6589cab3 ](../.././input/tests/measure/CMS816FHIRHHHypo/304052f7-e416-4da4-87ae-488e6589cab3/MeasureReport-a754b13e-2ef7-4c69-a205-f9af9a9a089e.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ 339a989b-722c-4452-9d25-454e2d53eea8 ](../.././input/tests/measure/CMS816FHIRHHHypo/339a989b-722c-4452-9d25-454e2d53eea8/MeasureReport-1f48c160-8aba-4e86-bd5d-c5c4bdef1afd.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ 37fd9c7e-bf9e-4769-b448-094ed97bd3e8 ](../.././input/tests/measure/CMS816FHIRHHHypo/37fd9c7e-bf9e-4769-b448-094ed97bd3e8/MeasureReport-6c210a7d-98b1-4d37-a268-45d14a7e7b1d.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 1<br>1<br>1 | 0<br>0<br>0 | — |
| [ 5bfa3b7e-2b6f-4eb5-b09b-7c6f1145780b ](../.././input/tests/measure/CMS816FHIRHHHypo/5bfa3b7e-2b6f-4eb5-b09b-7c6f1145780b/MeasureReport-0fb98a8a-a7ac-49a3-a1bd-e042373dc1c6.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ 6bc18290-1925-4239-81d7-0118bd062225 ](../.././input/tests/measure/CMS816FHIRHHHypo/6bc18290-1925-4239-81d7-0118bd062225/MeasureReport-1e896d30-3808-482a-b8a3-51198a58d4a6.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ 8301c6c8-e50c-4457-add0-1ebd909c8ca7 ](../.././input/tests/measure/CMS816FHIRHHHypo/8301c6c8-e50c-4457-add0-1ebd909c8ca7/MeasureReport-a821b7fb-7913-45e4-82e2-cf232818d643.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ 974284eb-fc89-452a-9b38-a884c0e0477e ](../.././input/tests/measure/CMS816FHIRHHHypo/974284eb-fc89-452a-9b38-a884c0e0477e/MeasureReport-6244d8f6-995c-4a0e-9d86-9c3abfc3fcb7.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ aa5f21cc-2d56-4749-a190-2828d579f790 ](../.././input/tests/measure/CMS816FHIRHHHypo/aa5f21cc-2d56-4749-a190-2828d579f790/MeasureReport-9eeadd82-4599-4b8b-95a5-f1d59697b451.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |
| [ ecde4132-9028-420a-aa7c-d1d14e5c1ab0 ](../.././input/tests/measure/CMS816FHIRHHHypo/ecde4132-9028-420a-aa7c-d1d14e5c1ab0/MeasureReport-b8bedfa5-6f9c-4727-be26-8b53d9a13a5b.json) | Group_1 | Initial Population<br>Denominator | 1<br>1 | 0<br>0 | — |


#### CMS819FHIRHHORAE
[ [cql] ](../../input/cql/CMS819FHIRHHORAE.cql) [ [test results] ](../../input/tests/results/CMS819FHIRHHORAE.txt)

Mismatched Test Cases (2 of  of 28)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 31b40acc-ca5f-4d1d-bd83-4b1a14eb822e ](../.././input/tests/measure/CMS819FHIRHHORAE/31b40acc-ca5f-4d1d-bd83-4b1a14eb822e/MeasureReport-c93e2b69-18fd-425e-8c71-b52eb967eda0.json) | Group_1 | Initial Population<br>Denominator | 2<br>2 | 1<br>1 | — |
| [ 73b0c1fe-874b-4982-8cb2-3c30520441de ](../.././input/tests/measure/CMS819FHIRHHORAE/73b0c1fe-874b-4982-8cb2-3c30520441de/MeasureReport-15d9e04f-4116-4856-b61a-f7c7b38e3325.json) | Group_1 | Numerator | 1 | 0 | — |


#### CMSFHIR844HybridHospitalWideMortality
[ [cql] ](../../input/cql/CMSFHIR844HybridHospitalWideMortality.cql) [ [test results] ](../../input/tests/results/CMSFHIR844HybridHospitalWideMortality.txt)

Mismatched Test Cases (2 of  of 10)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 6f22a06f-7186-4db1-9310-4f907dc49ff3 ](../.././input/tests/measure/CMSFHIR844HybridHospitalWideMortality/6f22a06f-7186-4db1-9310-4f907dc49ff3/MeasureReport-a02a261f-1274-4f8b-b1f3-5496f7885cbe.json) | Group_1 | Initial Population | 1 | 0 | — |
| [ af1b9448-3e7a-4b7f-8934-15bb63258b75 ](../.././input/tests/measure/CMSFHIR844HybridHospitalWideMortality/af1b9448-3e7a-4b7f-8934-15bb63258b75/MeasureReport-7afefb0f-3075-4fb8-8d56-474ba1112c38.json) | Group_1 | Initial Population | 2 | 1 | — |


#### CMS871FHIRHHHyper
[ [cql] ](../../input/cql/CMS871FHIRHHHyper.cql) [ [test results] ](../../input/tests/results/CMS871FHIRHHHyper.txt)

Missing Results (4 of 26 test cases)
| Test Case | Group | Known Issue |
| --- | --- | --- |
| [ 35719b1a-85bd-4072-b8d5-7218309358c6 ](../.././input/tests/measure/CMS871FHIRHHHyper/35719b1a-85bd-4072-b8d5-7218309358c6/MeasureReport-d5793b30-25e6-4cd6-8f7e-619b1c1802e5.json) | Group_1 | — |
| [ 7507debb-a991-4de0-bd71-634a684ddcd7 ](../.././input/tests/measure/CMS871FHIRHHHyper/7507debb-a991-4de0-bd71-634a684ddcd7/MeasureReport-6b01e3f8-ef51-41c3-8a23-b2868877df06.json) | Group_1 | — |
| [ 98533ccd-24ee-41b3-aab2-ef6cbf89e00d ](../.././input/tests/measure/CMS871FHIRHHHyper/98533ccd-24ee-41b3-aab2-ef6cbf89e00d/MeasureReport-82c8805c-b129-4009-8533-1ed12cf5d18f.json) | Group_1 | — |
| [ fd579f44-757b-4c98-9b09-27b17b935650 ](../.././input/tests/measure/CMS871FHIRHHHyper/fd579f44-757b-4c98-9b09-27b17b935650/MeasureReport-22df2e2a-404d-4ab0-831a-e2ab043197a2.json) | Group_1 | — |


#### CMS951FHIRKidneyHealthEval
[ [cql] ](../../input/cql/CMS951FHIRKidneyHealthEval.cql) [ [test results] ](../../input/tests/results/CMS951FHIRKidneyHealthEval.txt)

Mismatched Test Cases (13 of  of 55)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 023b65d6-0b68-4b1f-b276-f500e4b77ed2 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/023b65d6-0b68-4b1f-b276-f500e4b77ed2/MeasureReport-27aff293-4919-44c5-a689-18f57ee3c714.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 1127bc95-bf52-4921-b02a-de0902780191 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/1127bc95-bf52-4921-b02a-de0902780191/MeasureReport-1fc33681-1069-4f79-8168-2594f4a53f4e.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 1e8e8baf-0c27-42b2-93ad-5426418552c7 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/1e8e8baf-0c27-42b2-93ad-5426418552c7/MeasureReport-ff08d01e-b626-4f5b-8235-d0ab4883a313.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 2a7112e7-5937-4288-9271-cdc2d7e5eaa4 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/2a7112e7-5937-4288-9271-cdc2d7e5eaa4/MeasureReport-b78557ad-7d99-468d-99e2-bf2313f590a9.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 3f860c8e-e5fc-4843-ac4e-acb8e63471f3 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/3f860c8e-e5fc-4843-ac4e-acb8e63471f3/MeasureReport-45c6e0bf-693a-463c-a96f-90024ee92482.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 4354fbec-b63a-46ce-8465-ec82710ea1c6 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/4354fbec-b63a-46ce-8465-ec82710ea1c6/MeasureReport-e60bad0d-695a-4f82-ae72-ec04bf89fad9.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 55c5c208-190b-4f90-bdbb-0c02332df772 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/55c5c208-190b-4f90-bdbb-0c02332df772/MeasureReport-f19c1357-6d1a-4f3a-95dc-3cf4355336aa.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 61c9b47c-2223-4e45-b83b-eee21f031cad ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/61c9b47c-2223-4e45-b83b-eee21f031cad/MeasureReport-63253648-5413-4930-8270-ae38d5542c41.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 77620fcb-7a0a-4015-89cc-c32bd8681c13 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/77620fcb-7a0a-4015-89cc-c32bd8681c13/MeasureReport-1d431ce6-81be-4b71-95da-44358d8b85ca.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b1e68658-d64f-4ca4-a4ee-89c64e4536fa ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/b1e68658-d64f-4ca4-a4ee-89c64e4536fa/MeasureReport-96db6705-7dc4-4be6-90be-7adb58d9e3a5.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ b6ac3dd1-ff55-4152-be9a-153cad2ba2a2 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/b6ac3dd1-ff55-4152-be9a-153cad2ba2a2/MeasureReport-e9edfb1a-5fcc-4e64-8087-94a9c47088af.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ d7e37bcf-d13b-4415-82ac-a51b5c83151c ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/d7e37bcf-d13b-4415-82ac-a51b5c83151c/MeasureReport-ac924678-8c0d-43c9-b520-f5a3518d5f42.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ ebd7d1d0-a663-47da-8802-9088ad9d80a0 ](../.././input/tests/measure/CMS951FHIRKidneyHealthEval/ebd7d1d0-a663-47da-8802-9088ad9d80a0/MeasureReport-560a8c99-916a-49fa-92a4-aeba7b8da28a.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS986FHIRMalnutritionScore
[ [cql] ](../../input/cql/CMS986FHIRMalnutritionScore.cql) [ [test results] ](../../input/tests/results/CMS986FHIRMalnutritionScore.txt)

Mismatched Test Cases (6 of  of 876)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_1 | Measure Population Exclusion | 1 | 0 | — |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_2 | Measure Population Exclusion | 1 | 0 | — |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_3 | Measure Population Exclusion | 1 | 0 | — |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_4 | Measure Population Exclusion | 1 | 0 | — |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_5 | Measure Population Exclusion | 1 | 0 | — |
| [ a4f53b12-e0e3-4faf-8e66-6ce8193a6477 ](../.././input/tests/measure/CMS986FHIRMalnutritionScore/a4f53b12-e0e3-4faf-8e66-6ce8193a6477/MeasureReport-05d8c44d-2e5e-4b80-ab4e-3f5651bbd93c.json) | Group_6 | Measure Population Exclusion | 1 | 0 | — |


#### CMS996FHIRAptTxforSTEMI
[ [cql] ](../../input/cql/CMS996FHIRAptTxforSTEMI.cql) [ [test results] ](../../input/tests/results/CMS996FHIRAptTxforSTEMI.txt)

Mismatched Test Cases (7 of  of 114)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 60823d79-b37f-4358-819f-f39b4e885c6d ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/60823d79-b37f-4358-819f-f39b4e885c6d/MeasureReport-96a1323f-d99d-4b31-aace-c90b90f8af7a.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 7edab122-3af3-4172-9231-7c1470ecc1e0 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/7edab122-3af3-4172-9231-7c1470ecc1e0/MeasureReport-9d0666d5-6e19-4f7f-b284-1af640b254f3.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ 88d99809-90d6-4cbc-a4bb-d5d73375fc81 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/88d99809-90d6-4cbc-a4bb-d5d73375fc81/MeasureReport-8f114534-ca1f-4d09-bdf1-c683d7a680a7.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |
| [ 8bb7c40b-7447-42ca-b662-161a7026ed8f ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/8bb7c40b-7447-42ca-b662-161a7026ed8f/MeasureReport-bb15a071-2c69-428e-ac66-6405f7d75d07.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ ccc7deaf-98b7-4dad-b190-8fee10f2cf77 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/ccc7deaf-98b7-4dad-b190-8fee10f2cf77/MeasureReport-9d6a333f-3243-42df-9063-031aa80e74ff.json) | Group_1 | Denominator Exception | 1 | 0 | — |
| [ f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847/MeasureReport-f2a63299-25e1-4d91-8e5c-1bdf3b60e9cb.json) | Group_1 | Denominator Exclusion | 0 | 1 | — |
| [ f71b56bb-42fc-4db0-aa60-6b7b91333295 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/f71b56bb-42fc-4db0-aa60-6b7b91333295/MeasureReport-261ec6b2-42f5-46c2-906d-12fe22084f4c.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS1017FHIRHHFI
[ [cql] ](../../input/cql/CMS1017FHIRHHFI.cql) [ [test results] ](../../input/tests/results/CMS1017FHIRHHFI.txt)

Mismatched Test Cases (2 of  of 65)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 0dfafc1a-cf94-4ca1-becf-c1b843896810 ](../.././input/tests/measure/CMS1017FHIRHHFI/0dfafc1a-cf94-4ca1-becf-c1b843896810/MeasureReport-cd491c44-6ed1-483f-8775-516f92b9c16d.json) | Group_1 | Numerator Exclusion | 0 | 1 | — |
| [ 5ff2713d-ca89-42ae-91bb-cba3e1d9a487 ](../.././input/tests/measure/CMS1017FHIRHHFI/5ff2713d-ca89-42ae-91bb-cba3e1d9a487/MeasureReport-74f8c3e3-881b-4ba8-bfdb-ceef555ed020.json) | Group_1 | Numerator Exclusion | 0 | 1 | — |


#### CMS1028FHIRPCSevereOBComps
[ [cql] ](../../input/cql/CMS1028FHIRPCSevereOBComps.cql) [ [test results] ](../../input/tests/results/CMS1028FHIRPCSevereOBComps.txt)

Mismatched Test Cases (2 of  of 282)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ 763d86f9-d93f-4873-8b64-8439566b242e ](../.././input/tests/measure/CMS1028FHIRPCSevereOBComps/763d86f9-d93f-4873-8b64-8439566b242e/MeasureReport-7ca90ad8-935e-4d56-80d9-5470c8a98481.json) | Group_1 | Numerator | 2 | 1 | — |
| [ 763d86f9-d93f-4873-8b64-8439566b242e ](../.././input/tests/measure/CMS1028FHIRPCSevereOBComps/763d86f9-d93f-4873-8b64-8439566b242e/MeasureReport-7ca90ad8-935e-4d56-80d9-5470c8a98481.json) | Group_2 | Numerator | 2 | 1 | — |


#### CMS1154ScreeningPrediabetesFHIR
[ [cql] ](../../input/cql/CMS1154ScreeningPrediabetesFHIR.cql) [ [test results] ](../../input/tests/results/CMS1154ScreeningPrediabetesFHIR.txt)

Mismatched Test Cases (1 of  of 10)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ bc9c82ca-72b5-41c4-a9a3-7e3860a9ac2d ](../.././input/tests/measure/CMS1154ScreeningPrediabetesFHIR/bc9c82ca-72b5-41c4-a9a3-7e3860a9ac2d/MeasureReport-466dec57-6ceb-4f37-8daa-40f26f14a191.json) | Group_1 | Denominator Exclusion | 1 | 0 | — |


#### CMS1218FHIRHHRF
[ [cql] ](../../input/cql/CMS1218FHIRHHRF.cql) [ [test results] ](../../input/tests/results/CMS1218FHIRHHRF.txt)

Mismatched Test Cases (1 of  of 69)
| Test Case | Group | Population | Expected | Actual | Known Issue |
|---|---|---|:---:|:---:|---|
| [ ea9c34ee-b50e-4d13-bd9c-ab2033d15717 ](../.././input/tests/measure/CMS1218FHIRHHRF/ea9c34ee-b50e-4d13-bd9c-ab2033d15717/MeasureReport-97044259-fd76-403c-a40f-1177631abe4f.json) | Group_1 | Initial Population<br>Denominator | 0<br>0 | 1<br>1 | — |


## Engine Diff: CMS vs QI-Core (qicore-2025)

_Where the CMS engine's actual results differ from the QI-Core engine's (source of truth) on the same test case and population. QI-Core-only rows are populations the QI-Core engine produced that are absent from CMS._

| Measure | Mismatch | CMS-Only | QI-Core-Only |
| --- | ---: | ---: | ---: |
| CMS2FHIRPCSDepScreenAndFollowUp | 15 | 0 | 0 |
| CMS22FHIRPCSBPScreeningFollowUp | 14 | 0 | 0 |
| CMS56FHIRFuncStatHipReplacement | 10 | 0 | 0 |
| CMS68FHIRDocumentationCurrentMeds | 0 | 0 | 4 |
| CMS69FHIRPCSBMIScreenAndFollowUp | 33 | 0 | 0 |
| CMS71FHIRSTKAnticoagAFFlutter | 12 | 0 | 0 |
| CMS72FHIRSTKAntithromboticDay2 | 243 | 0 | 0 |
| CMS74FHIRDentalCariesPrevention | 7 | 0 | 0 |
| CMS75FHIRChildrenDentalDecay | 7 | 0 | 0 |
| CMS90FHIRFSAforHeartFailure | 8 | 0 | 0 |
| CMS104FHIRSTKDCAntithrombotic | 175 | 0 | 0 |
| CMS108FHIRVTEProphylaxis | 16 | 0 | 0 |
| CMS117FHIRChildImmunStatus | 8 | 0 | 0 |
| CMS122FHIRDiabetesAssessGT9Pct | 38 | 0 | 0 |
| CMS124FHIRCervicalCancerScreen | 13 | 0 | 0 |
| CMS125FHIRBreastCancerScreen | 18 | 0 | 0 |
| CMS128FHIRAntidepressantMgmt | 118 | 0 | 0 |
| CMS130FHIRColorectalCancerScrn | 16 | 0 | 0 |
| CMS131FHIRDiabetesEyeExam | 18 | 0 | 0 |
| CMS135FHIRACEIorARBorARNIforHF | 15 | 0 | 0 |
| CMS136FHIRChildADHDMedFollowUp | 43 | 0 | 0 |
| CMS137FHIRSUDTxInitEngagement | 18 | 0 | 0 |
| CMS138FHIRTobaccoScrnCessation | 52 | 0 | 0 |
| CMS139FHIRFallRiskScreening | 8 | 0 | 0 |
| CMS144FHIRHFBetaBlockerForLVSD | 5 | 0 | 0 |
| CMS145FHIRCADBBlockerTPMIorLVSD | 6 | 0 | 0 |
| CMS146FHIRApproTestPharyngitis | 10 | 0 | 0 |
| CMS153FHIRChlamydiaScreening | 10 | 0 | 0 |
| CMS154FHIRAppropriateTxforURI | 16 | 0 | 0 |
| CMS155FHIRWgtAssessCounseling | 27 | 0 | 0 |
| CMS156FHIRHighRiskMedsElderly | 43 | 0 | 0 |
| CMS165FHIRControllingHighBP | 20 | 0 | 0 |
| CMS177FHIRChildMDDSuicideAssmt | 2 | 0 | 0 |
| CMS190FHIRVTEProphylaxisICU | 17 | 0 | 0 |
| CMS347FHIRStatinPreventionTxCVD | 172 | 0 | 0 |
| CMS645FHIRBoneDensityPCADTherapy | 5 | 0 | 0 |
| CMS646FHIRIntravesicalBCGTherapy | 1 | 0 | 5 |
| CMS771FHIRUrinarySymptomScoreBPH | 7 | 0 | 0 |
| CMS871FHIRHHHyper | 0 | 7 | 0 |
| CMS951FHIRKidneyHealthEval | 13 | 0 | 0 |
| CMS986FHIRMalnutritionScore | 6 | 0 | 0 |
| CMS996FHIRAptTxforSTEMI | 5 | 0 | 0 |
| CMS1028FHIRPCSevereOBComps | 2 | 0 | 0 |
| CMS1264FHIRECATREHQR | 152 | 0 | 0 |
| NHSNAcuteCareHospitalMonthlyInitialPopulation1 | 27 | 0 | 0 |
| NHSNGlycemicControlHypoglycemiaInitialPopulation | 1 | 0 | 0 |

| **Total** | **1452** | **7** | **9** |

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

### CMS56FHIRFuncStatHipReplacement

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 10e6851a-0db4-4706-8a6e-7fbbb27c588e | Denominator | 0 | 1 | mismatch |
| 10e6851a-0db4-4706-8a6e-7fbbb27c588e | Denominator Exclusion | 0 | 1 | mismatch |
| 10e6851a-0db4-4706-8a6e-7fbbb27c588e | Initial Population | 0 | 1 | mismatch |
| 289b7214-0496-425b-8ffa-14b2aaa9f771 | Denominator Exclusion | 0 | 1 | mismatch |
| 34fa486b-b691-4760-9acc-1e5c0fc8a4dc | Denominator Exclusion | 0 | 1 | mismatch |
| 3574f4b8-cbdc-410b-8b6a-7f0737546e56 | Denominator Exclusion | 0 | 1 | mismatch |
| 97ec6179-f96b-4d88-a042-c482f8fe525a | Denominator Exclusion | 0 | 1 | mismatch |
| 9e3e68df-73f6-4a91-9bef-b4fb94c11756 | Denominator Exclusion | 0 | 1 | mismatch |
| d1746049-b5df-4a21-a0ea-2b1709c0c502 | Denominator Exclusion | 0 | 1 | mismatch |
| d2682114-7f8e-41a4-88b1-e96a670e964a | Denominator Exclusion | 0 | 1 | mismatch |

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
| 050201c2-c2c4-46e6-8288-a34f99caebdc | Numerator | 0 | 1 | mismatch |
| 097cbc7a-d22e-4395-9fcf-fd1f904f7c92 | Denominator Exclusion | 0 | 1 | mismatch |
| 09e4ff5a-fe3b-4c89-a36e-68f64c7e489c | Denominator Exclusion | 0 | 1 | mismatch |
| 1102009b-6f05-4bab-9fd1-191e81cf50e8 | Numerator | 0 | 1 | mismatch |
| 1b102c21-830a-41a5-ac27-9aa77ea5adfe | Denominator Exclusion | 0 | 1 | mismatch |
| 1e23fb8f-e27b-4553-a62a-f66edeb4528a | Numerator | 0 | 1 | mismatch |
| 27849d59-3cef-40bf-8338-a6ec7c0bcf81 | Numerator | 0 | 1 | mismatch |
| 353cb8b7-96ac-4b51-9a0d-60cd64e6d854 | Denominator Exclusion | 0 | 1 | mismatch |
| 3ecce155-635d-47ec-b35d-d53126423a81 | Denominator Exclusion | 0 | 1 | mismatch |
| 42e6b4d6-defc-4ec5-894f-e3333e3039a3 | Numerator | 0 | 1 | mismatch |
| 45b1ce40-0f49-4559-8c3b-5c2a8070b0a7 | Denominator Exclusion | 0 | 1 | mismatch |
| 461fdfab-fcc1-4630-9dae-2ba3a6ab0c25 | Numerator | 0 | 1 | mismatch |
| 463dd868-997d-472f-962c-96383fd2a5c4 | Numerator | 0 | 1 | mismatch |
| 57858042-c2aa-49f4-b401-1f1fd9ab289a | Denominator Exclusion | 0 | 1 | mismatch |
| 5d34e56e-f4f1-4817-b7e4-e4c57f811300 | Denominator Exclusion | 0 | 1 | mismatch |
| 5d48c3b8-93e9-4e29-8c20-a002761d9e24 | Denominator Exclusion | 0 | 1 | mismatch |
| 5ef4acf3-4b42-41fd-8793-7d1a9342865a | Denominator Exclusion | 0 | 1 | mismatch |
| 6092a810-f9e0-4975-9582-37bbb06e8e56 | Denominator Exclusion | 0 | 1 | mismatch |
| 6f0c3642-5efc-4923-ac24-9f5e9d1831d6 | Denominator Exclusion | 0 | 1 | mismatch |
| 736b5472-4a6f-4278-80d3-373d1c78c4c5 | Denominator Exclusion | 0 | 1 | mismatch |
| 7902e3dc-f3da-4cc2-9f2e-4a6c8cd33b88 | Numerator | 0 | 1 | mismatch |
| 7b34e64e-e7fe-402c-9a26-12da90662897 | Denominator Exclusion | 0 | 1 | mismatch |
| 8835a50b-0a0f-4e2f-94fa-7c180cd7f905 | Numerator | 0 | 1 | mismatch |
| 8e38b797-4dec-437d-8bf0-6f0fc78f8ea7 | Numerator | 0 | 1 | mismatch |
| 953ef59d-4c39-40ef-8067-87b5ecf84727 | Denominator Exclusion | 0 | 1 | mismatch |
| 9d92be1d-6fc8-40f2-99a0-4be9ce1f244b | Numerator | 0 | 1 | mismatch |
| c3caf126-12a2-473f-8f51-1c7828d63d16 | Numerator | 0 | 1 | mismatch |
| c84bf29f-80ac-4bf0-beeb-404ba96a3fa8 | Numerator | 0 | 1 | mismatch |
| ca6deaeb-459d-4d1a-9daf-e454ff76a6f0 | Denominator Exclusion | 0 | 1 | mismatch |
| d4d064be-d55a-47b5-9bfd-993afebd95a5 | Numerator | 0 | 1 | mismatch |
| e0821eec-ff83-49e9-950d-9219dd3612b9 | Numerator | 0 | 1 | mismatch |
| e25fc2f1-0083-4375-8fc3-9164a5aee53d | Denominator Exclusion | 0 | 1 | mismatch |
| f5ae6269-d09b-47f8-a519-f1a8a81549fc | Numerator | 0 | 1 | mismatch |

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

### CMS74FHIRDentalCariesPrevention

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 31bee4bc-9ca4-4d84-9f1a-a6a6d2d3fac0 | Denominator Exclusion | 0 | 1 | mismatch |
| 499fd8d2-0a68-4d27-a194-c61aae97e492 | Denominator Exclusion | 0 | 1 | mismatch |
| 4fc1e663-46e6-4159-853d-b2dbb146b2ac | Denominator Exclusion | 0 | 1 | mismatch |
| 70208367-16df-46d6-b49c-c1e31b7e1d5f | Denominator Exclusion | 0 | 1 | mismatch |
| 890dbdad-7466-494d-966b-a20515508db5 | Denominator Exclusion | 0 | 1 | mismatch |
| 96c38952-91cc-468c-b16b-32386bb312ec | Denominator Exclusion | 0 | 1 | mismatch |
| fe5f3172-5263-4498-b1ba-0d62de7455ef | Denominator Exclusion | 0 | 1 | mismatch |

### CMS75FHIRChildrenDentalDecay

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 043f64b7-dd25-42ea-9785-0bdcbe64b27a | Denominator Exclusion | 0 | 1 | mismatch |
| 26549e84-fbf3-43dc-8971-2f3baaf508d7 | Denominator Exclusion | 0 | 1 | mismatch |
| 303676f7-30b4-4324-8ab3-8d5ab7e92102 | Denominator Exclusion | 0 | 1 | mismatch |
| 326c7237-c7a4-4e1b-bd1d-ba518dc942dd | Denominator Exclusion | 0 | 1 | mismatch |
| a42cd354-1966-45d5-aec2-2d42225e6911 | Denominator Exclusion | 0 | 1 | mismatch |
| b532c8f5-b38a-4337-8661-7b744e271a9c | Denominator Exclusion | 0 | 1 | mismatch |
| ebb4d1e8-32af-4811-adc5-f84a7318c5b8 | Denominator Exclusion | 0 | 1 | mismatch |

### CMS90FHIRFSAforHeartFailure

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 19608155-9049-41fc-9a02-d856e4143773 | Denominator Exclusion | 0 | 1 | mismatch |
| 19a551f9-e826-4cce-bde3-cc013c182ada | Denominator Exclusion | 0 | 1 | mismatch |
| 3d036fff-bb44-4911-b6d4-23e064783f3a | Denominator Exclusion | 0 | 1 | mismatch |
| 4944fb9a-bf44-4b09-a49f-aae0b6c0ad82 | Denominator Exclusion | 0 | 1 | mismatch |
| 6e5db6e5-8c56-4b08-9491-1a2877933f0d | Denominator Exclusion | 0 | 1 | mismatch |
| bc42a4e7-3a06-4056-bb38-14f1e3ea3894 | Denominator Exclusion | 0 | 1 | mismatch |
| c784c565-2714-4009-b527-bee24f78d409 | Denominator Exclusion | 0 | 1 | mismatch |
| ffad6c76-4ffb-4cf1-bee2-df190571f3e1 | Denominator Exclusion | 0 | 1 | mismatch |

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

### CMS117FHIRChildImmunStatus

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 104ee6b1-c36f-420c-bedd-0a2064f748d8 | Denominator Exclusion | 0 | 1 | mismatch |
| 239d5e6f-38d3-461f-a2a1-52abe106e8bb | Denominator Exclusion | 0 | 1 | mismatch |
| 92ed2664-a594-4cac-9001-3044b14a02f7 | Denominator Exclusion | 0 | 1 | mismatch |
| 9e57c539-0442-415a-a187-87adc7acdd8a | Denominator Exclusion | 0 | 1 | mismatch |
| aeb0266c-a8ec-4262-a4bc-6bc343a85230 | Denominator Exclusion | 0 | 1 | mismatch |
| b5f9f533-30c2-4fbe-b06e-3f8dccc8792c | Denominator Exclusion | 0 | 1 | mismatch |
| dd1e534c-aa60-4ff3-a955-109f034b408f | Denominator Exclusion | 0 | 1 | mismatch |
| fe0cb80b-232c-4c84-8b2a-f27eaf3078ff | Denominator Exclusion | 0 | 1 | mismatch |

### CMS122FHIRDiabetesAssessGT9Pct

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 12ccd41a-83aa-405a-83b3-c756564c4de5 | Denominator Exclusion | 0 | 1 | mismatch |
| 12ccd41a-83aa-405a-83b3-c756564c4de5 | Numerator | 1 | 0 | mismatch |
| 63ae0b9f-2636-4bf3-85ef-4ff20bdb09de | Denominator Exclusion | 0 | 1 | mismatch |
| 63ae0b9f-2636-4bf3-85ef-4ff20bdb09de | Numerator | 1 | 0 | mismatch |
| 64ba4a87-8cf6-4cfb-b0e7-506dd08c8bbe | Denominator Exclusion | 0 | 1 | mismatch |
| 64ba4a87-8cf6-4cfb-b0e7-506dd08c8bbe | Numerator | 1 | 0 | mismatch |
| 6b6a5f96-c2a8-43f1-a353-7b5700ecb031 | Denominator Exclusion | 0 | 1 | mismatch |
| 6b6a5f96-c2a8-43f1-a353-7b5700ecb031 | Numerator | 1 | 0 | mismatch |
| 6d9426d1-5554-4d6b-9ed0-e3736dd17482 | Denominator Exclusion | 0 | 1 | mismatch |
| 6d9426d1-5554-4d6b-9ed0-e3736dd17482 | Numerator | 1 | 0 | mismatch |
| 6f0553ac-e12a-4af5-ad27-05339f4b4ec0 | Denominator Exclusion | 0 | 1 | mismatch |
| 6f0553ac-e12a-4af5-ad27-05339f4b4ec0 | Numerator | 1 | 0 | mismatch |
| 7d01a597-c0da-4bff-9bdd-f3516021db34 | Denominator Exclusion | 0 | 1 | mismatch |
| 7d01a597-c0da-4bff-9bdd-f3516021db34 | Numerator | 1 | 0 | mismatch |
| 7e69124d-ff34-4daf-b626-08d1283f71ba | Denominator Exclusion | 0 | 1 | mismatch |
| 7e69124d-ff34-4daf-b626-08d1283f71ba | Numerator | 1 | 0 | mismatch |
| 85b60f52-7b08-46f3-946b-cb317b28acf5 | Denominator Exclusion | 0 | 1 | mismatch |
| 85b60f52-7b08-46f3-946b-cb317b28acf5 | Numerator | 1 | 0 | mismatch |
| 86a25ad7-3801-4297-a9a4-b36b5308c9e2 | Denominator Exclusion | 0 | 1 | mismatch |
| 86a25ad7-3801-4297-a9a4-b36b5308c9e2 | Numerator | 1 | 0 | mismatch |
| 88b67805-bfef-411c-a191-12382d2c3104 | Denominator Exclusion | 0 | 1 | mismatch |
| 88b67805-bfef-411c-a191-12382d2c3104 | Numerator | 1 | 0 | mismatch |
| 8b8ded15-0118-4d0c-ac0f-6797528cefb9 | Denominator Exclusion | 0 | 1 | mismatch |
| 8b8ded15-0118-4d0c-ac0f-6797528cefb9 | Numerator | 1 | 0 | mismatch |
| 91986c00-e45b-4e7c-afa7-734d6fe43d16 | Denominator Exclusion | 0 | 1 | mismatch |
| 91986c00-e45b-4e7c-afa7-734d6fe43d16 | Numerator | 1 | 0 | mismatch |
| 96cfe7f0-b4e1-4e2e-a48d-ef64fb64343d | Denominator Exclusion | 0 | 1 | mismatch |
| 96cfe7f0-b4e1-4e2e-a48d-ef64fb64343d | Numerator | 1 | 0 | mismatch |
| ac4d7076-d1cb-44c6-a94f-c2c86266d53b | Denominator Exclusion | 0 | 1 | mismatch |
| ac4d7076-d1cb-44c6-a94f-c2c86266d53b | Numerator | 1 | 0 | mismatch |
| b6a4b9f8-21c1-44f2-a834-72f0906b4f88 | Denominator Exclusion | 0 | 1 | mismatch |
| b6a4b9f8-21c1-44f2-a834-72f0906b4f88 | Numerator | 1 | 0 | mismatch |
| e2b82999-6313-40af-bc8b-9ddf5f97795f | Denominator Exclusion | 0 | 1 | mismatch |
| e2b82999-6313-40af-bc8b-9ddf5f97795f | Numerator | 1 | 0 | mismatch |
| eacbadee-87f7-4ed0-bfc3-b5533128dcbc | Denominator Exclusion | 0 | 1 | mismatch |
| eacbadee-87f7-4ed0-bfc3-b5533128dcbc | Numerator | 1 | 0 | mismatch |
| f4eeba51-a6fc-4ffd-bd62-49fd1c375f01 | Denominator Exclusion | 0 | 1 | mismatch |
| f4eeba51-a6fc-4ffd-bd62-49fd1c375f01 | Numerator | 1 | 0 | mismatch |

### CMS124FHIRCervicalCancerScreen

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 05cbc93d-e748-4bca-b68d-3011ebf68e28 | Denominator Exclusion | 0 | 1 | mismatch |
| 0e296f04-855b-42ad-aa20-295a719a96e5 | Denominator Exclusion | 0 | 1 | mismatch |
| 3aef97c8-9529-433c-95d3-ea01f188e156 | Denominator Exclusion | 0 | 1 | mismatch |
| 62bd7a1e-f946-435f-8898-39db9d870940 | Denominator Exclusion | 0 | 1 | mismatch |
| 65a9a258-c453-484f-902c-743e678b44a4 | Denominator Exclusion | 0 | 1 | mismatch |
| 6ee7c92c-c8cd-4025-8002-ca1253ba830b | Denominator Exclusion | 0 | 1 | mismatch |
| 7e41f717-097e-45a7-9a00-1e0ad852cb44 | Denominator Exclusion | 0 | 1 | mismatch |
| 8723dbb4-f60f-488a-9da3-f02f04ea03bf | Denominator Exclusion | 0 | 1 | mismatch |
| ab346cb5-2c55-4171-93ea-aac9d266e6c7 | Denominator Exclusion | 0 | 1 | mismatch |
| c6ec1681-b011-425a-a850-4e187e9fd927 | Denominator Exclusion | 0 | 1 | mismatch |
| cadbffa0-20b2-4c26-b202-75b9edfd0a07 | Denominator Exclusion | 0 | 1 | mismatch |
| d986061c-de3e-4d5d-95e7-f5ec93c5665c | Denominator Exclusion | 0 | 1 | mismatch |
| e8813151-9334-41d7-ab4b-1d597f08d4a9 | Denominator Exclusion | 0 | 1 | mismatch |

### CMS125FHIRBreastCancerScreen

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 01c88972-84e2-4594-835b-924481b9990a | Denominator Exclusion | 0 | 1 | mismatch |
| 0930082c-fda1-42e8-a15f-92ceaefa5908 | Denominator Exclusion | 0 | 1 | mismatch |
| 0beefd14-c554-4f1e-856c-c8696177ce9e | Denominator Exclusion | 0 | 1 | mismatch |
| 14193177-2f4e-4480-a471-87ff9d137a8b | Denominator Exclusion | 0 | 1 | mismatch |
| 2886b1b6-5834-4788-8cd7-b54bbda54ca9 | Denominator Exclusion | 0 | 1 | mismatch |
| 3ea0a87a-3ded-4939-920a-4e69bc20a26f | Denominator Exclusion | 0 | 1 | mismatch |
| 461f1aab-e645-4973-ae9a-4c09bfaef59a | Denominator Exclusion | 0 | 1 | mismatch |
| 4f10a0f7-bb14-40d5-beb2-c728eb88a30d | Denominator Exclusion | 0 | 1 | mismatch |
| 5c8bffdf-7ef4-44e1-af5a-8a64f1b7e545 | Denominator Exclusion | 0 | 1 | mismatch |
| 5fd02264-fd4e-4eb7-a635-0023876920ac | Denominator Exclusion | 0 | 1 | mismatch |
| 62901c95-5d12-45e8-b5b1-d131e36d8299 | Denominator Exclusion | 0 | 1 | mismatch |
| 73f77133-4d08-438a-ac81-6bb858a74c31 | Denominator Exclusion | 0 | 1 | mismatch |
| 7a09940e-c3c8-49a7-bf09-eaf9df116dfb | Denominator Exclusion | 0 | 1 | mismatch |
| 8a0f6b6e-fb1c-4e60-b150-b88d1a4e487b | Denominator Exclusion | 0 | 1 | mismatch |
| 99b68a44-5e66-4c37-a513-80db8b6249ce | Denominator Exclusion | 0 | 1 | mismatch |
| adb08da2-b4d0-4916-9b9c-7c2c86e1042b | Denominator Exclusion | 0 | 1 | mismatch |
| bbb391da-9572-4954-be95-3ea00eb31c91 | Denominator Exclusion | 0 | 1 | mismatch |
| cc1a4555-2e3e-43ac-bbca-6e44ea41b2f3 | Denominator Exclusion | 0 | 1 | mismatch |

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
| 4c2caf57-7168-4149-a596-d0914d7e3fe8 | Initial Population | 1 | 0 | mismatch |
| 4c2caf57-7168-4149-a596-d0914d7e3fe8 | Denominator | 1 | 0 | mismatch |
| 4c2caf57-7168-4149-a596-d0914d7e3fe8 | Initial Population | 1 | 0 | mismatch |
| 62ea0c3d-46da-48a1-87dd-d1927ed2df75 | Denominator | 1 | 0 | mismatch |
| 62ea0c3d-46da-48a1-87dd-d1927ed2df75 | Initial Population | 1 | 0 | mismatch |
| 62ea0c3d-46da-48a1-87dd-d1927ed2df75 | Denominator | 1 | 0 | mismatch |
| 62ea0c3d-46da-48a1-87dd-d1927ed2df75 | Initial Population | 1 | 0 | mismatch |
| 71cc96f3-e525-4e60-b6ad-1037d16a3c17 | Denominator | 1 | 0 | mismatch |
| 71cc96f3-e525-4e60-b6ad-1037d16a3c17 | Initial Population | 1 | 0 | mismatch |
| 71cc96f3-e525-4e60-b6ad-1037d16a3c17 | Denominator | 1 | 0 | mismatch |
| 71cc96f3-e525-4e60-b6ad-1037d16a3c17 | Initial Population | 1 | 0 | mismatch |
| 76e30d44-a803-4b4b-a6ba-f11de6fa6329 | Denominator | 1 | 0 | mismatch |
| 76e30d44-a803-4b4b-a6ba-f11de6fa6329 | Initial Population | 1 | 0 | mismatch |
| 76e30d44-a803-4b4b-a6ba-f11de6fa6329 | Denominator | 1 | 0 | mismatch |
| 76e30d44-a803-4b4b-a6ba-f11de6fa6329 | Initial Population | 1 | 0 | mismatch |
| 778e804e-7356-400f-bc36-8d202d775509 | Denominator | 1 | 0 | mismatch |
| 778e804e-7356-400f-bc36-8d202d775509 | Initial Population | 1 | 0 | mismatch |
| 778e804e-7356-400f-bc36-8d202d775509 | Denominator | 1 | 0 | mismatch |
| 778e804e-7356-400f-bc36-8d202d775509 | Initial Population | 1 | 0 | mismatch |
| 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 | Denominator | 1 | 0 | mismatch |
| 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 | Initial Population | 1 | 0 | mismatch |
| 7bda86fd-7b20-45e1-8c2e-e0a24c785dd0 | Denominator | 1 | 0 | mismatch |
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
| 925ef058-b2e2-489e-8d5e-1a33299efa30 | Initial Population | 1 | 0 | mismatch |
| 925ef058-b2e2-489e-8d5e-1a33299efa30 | Denominator | 1 | 0 | mismatch |
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
| b371fd28-5026-43db-840e-21466bde11c9 | Initial Population | 1 | 0 | mismatch |
| b371fd28-5026-43db-840e-21466bde11c9 | Denominator | 1 | 0 | mismatch |
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
| ee6d52b0-149c-4ffe-b260-bb214151652c | Initial Population | 1 | 0 | mismatch |
| ee6d52b0-149c-4ffe-b260-bb214151652c | Denominator | 1 | 0 | mismatch |
| ee6d52b0-149c-4ffe-b260-bb214151652c | Initial Population | 1 | 0 | mismatch |
| fcfaba77-8917-48de-993e-438eb8d5b77b | Denominator | 1 | 0 | mismatch |
| fcfaba77-8917-48de-993e-438eb8d5b77b | Initial Population | 1 | 0 | mismatch |
| fcfaba77-8917-48de-993e-438eb8d5b77b | Numerator | 1 | 0 | mismatch |
| fcfaba77-8917-48de-993e-438eb8d5b77b | Denominator | 1 | 0 | mismatch |
| fcfaba77-8917-48de-993e-438eb8d5b77b | Initial Population | 1 | 0 | mismatch |

### CMS130FHIRColorectalCancerScrn

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 007ec5f1-08cf-474a-a472-f6a92cca4b79 | Denominator Exclusion | 0 | 1 | mismatch |
| 02488708-2ac0-4814-828c-04b8be9b1e70 | Denominator Exclusion | 0 | 1 | mismatch |
| 0f930f59-9061-4b28-b2e5-21cc5ab6b613 | Denominator Exclusion | 0 | 1 | mismatch |
| 46635c8a-3f72-4424-98ae-01b849d0ff19 | Denominator Exclusion | 0 | 1 | mismatch |
| 4e1abf20-b68c-401b-9a33-fdf9bc765005 | Denominator Exclusion | 0 | 1 | mismatch |
| 59128a5c-f9da-4cb3-9e98-97ee67380533 | Denominator Exclusion | 0 | 1 | mismatch |
| 5fd0d61d-d5e0-4138-8a8d-6e3969af6107 | Denominator Exclusion | 0 | 1 | mismatch |
| 6dbaf3b3-8c47-4e0a-91fe-2ec06f2f0339 | Denominator Exclusion | 0 | 1 | mismatch |
| 6f6cdf8c-e562-4113-bf5d-f91237b975a5 | Denominator Exclusion | 0 | 1 | mismatch |
| 7ee1a25c-a4c7-4bd2-8670-4083b32ecc70 | Denominator Exclusion | 0 | 1 | mismatch |
| a989a58f-82c5-4221-addb-5e29c2514df7 | Denominator Exclusion | 0 | 1 | mismatch |
| b70f2fc0-3254-4240-af70-793cd1bc90b2 | Denominator Exclusion | 0 | 1 | mismatch |
| d0c9e870-5e7b-4a9e-b34d-9d600ff8c1c6 | Denominator Exclusion | 0 | 1 | mismatch |
| dcaccac3-ef0d-4755-becd-3e6aebe2a06a | Denominator Exclusion | 0 | 1 | mismatch |
| df62e712-a702-4c1e-82c6-4676578371f9 | Denominator Exclusion | 0 | 1 | mismatch |
| fede210f-db17-4e0a-9bcd-5dc383f0fb93 | Denominator Exclusion | 0 | 1 | mismatch |

### CMS131FHIRDiabetesEyeExam

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0c9d7ae1-4643-4c50-bc48-0274a3f2d234 | Denominator Exclusion | 0 | 1 | mismatch |
| 106633c6-3739-442f-b7cc-7269399481cf | Denominator Exclusion | 0 | 1 | mismatch |
| 36222907-f670-4253-a251-63198bb3fc6c | Denominator Exclusion | 0 | 1 | mismatch |
| 3624228c-097b-4f91-9211-f29f72b8ddaf | Denominator Exclusion | 0 | 1 | mismatch |
| 65c895d1-ba13-410a-bcfc-be3b771b5eb8 | Denominator Exclusion | 0 | 1 | mismatch |
| 728333bf-6ff0-4d29-9181-3b6a30b7059a | Denominator Exclusion | 0 | 1 | mismatch |
| 97935b1b-262b-4c05-9a56-2124a3aa1de0 | Denominator Exclusion | 0 | 1 | mismatch |
| a6cd48c6-fb25-41d4-aea4-da7fb856cc12 | Denominator Exclusion | 0 | 1 | mismatch |
| b3af1243-c45d-4061-8d36-baa6de256376 | Denominator Exclusion | 0 | 1 | mismatch |
| b7a8c85e-3608-44ec-be34-c9089fa3dd17 | Denominator Exclusion | 0 | 1 | mismatch |
| c1340d6e-581d-4775-a0af-b8dcdbcf7320 | Denominator Exclusion | 0 | 1 | mismatch |
| c36eddf7-a780-480c-baf8-ef865ccdb9d2 | Denominator Exclusion | 0 | 1 | mismatch |
| d46ab51c-9b21-4b1c-b1dd-090c7f3e831a | Denominator Exclusion | 0 | 1 | mismatch |
| d8946843-06c7-4b82-992a-91a9c20ec7c0 | Denominator Exclusion | 0 | 1 | mismatch |
| e9b9b388-e663-4533-8484-7d930efd1851 | Denominator Exclusion | 0 | 1 | mismatch |
| ea0e556f-387e-4883-a320-047aa3a238e4 | Denominator Exclusion | 0 | 1 | mismatch |
| f0b61b7a-4381-486d-9eee-2128ada5280a | Denominator Exclusion | 0 | 1 | mismatch |
| f77b9abc-9c77-4e75-96c8-cc3bf25e08f4 | Denominator Exclusion | 0 | 1 | mismatch |

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
| 0039c514-9277-46cd-9e6a-2f402b5357f5 | Denominator Exclusion | 0 | 1 | mismatch |
| 0039c514-9277-46cd-9e6a-2f402b5357f5 | Numerator | 1 | 0 | mismatch |
| 0039c514-9277-46cd-9e6a-2f402b5357f5 | Denominator Exclusion | 0 | 1 | mismatch |
| 0039c514-9277-46cd-9e6a-2f402b5357f5 | Numerator | 1 | 0 | mismatch |
| 00f27092-14a7-4d87-b35a-5a112ca99201 | Denominator Exclusion | 0 | 1 | mismatch |
| 048c41bc-fe7e-465f-bc10-6ccf7a7d5250 | Denominator Exclusion | 0 | 1 | mismatch |
| 304b7ef3-bd6c-488e-9409-70039f1da018 | Denominator Exclusion | 0 | 1 | mismatch |
| 304b7ef3-bd6c-488e-9409-70039f1da018 | Numerator | 1 | 0 | mismatch |
| 304b7ef3-bd6c-488e-9409-70039f1da018 | Denominator Exclusion | 0 | 1 | mismatch |
| 5e536adf-1159-404e-92e7-94d4f1affd98 | Denominator Exclusion | 0 | 1 | mismatch |
| 5e536adf-1159-404e-92e7-94d4f1affd98 | Numerator | 1 | 0 | mismatch |
| 5e536adf-1159-404e-92e7-94d4f1affd98 | Denominator Exclusion | 0 | 1 | mismatch |
| 5e536adf-1159-404e-92e7-94d4f1affd98 | Numerator | 1 | 0 | mismatch |
| 6a96556a-075b-4361-8a8d-fd8c8b4f125a | Denominator Exclusion | 0 | 1 | mismatch |
| 6a96556a-075b-4361-8a8d-fd8c8b4f125a | Numerator | 1 | 0 | mismatch |
| 6a96556a-075b-4361-8a8d-fd8c8b4f125a | Denominator Exclusion | 0 | 1 | mismatch |
| 6a96556a-075b-4361-8a8d-fd8c8b4f125a | Numerator | 1 | 0 | mismatch |
| 71a21841-f5bb-4e75-9328-aedf3cdc8a34 | Denominator Exclusion | 0 | 1 | mismatch |
| 71a21841-f5bb-4e75-9328-aedf3cdc8a34 | Numerator | 1 | 0 | mismatch |
| 71a21841-f5bb-4e75-9328-aedf3cdc8a34 | Denominator Exclusion | 0 | 1 | mismatch |
| 71a21841-f5bb-4e75-9328-aedf3cdc8a34 | Numerator | 1 | 0 | mismatch |
| 80644a49-f67d-4124-9c58-1547b7bdd779 | Denominator Exclusion | 0 | 1 | mismatch |
| 80644a49-f67d-4124-9c58-1547b7bdd779 | Numerator | 1 | 0 | mismatch |
| 80644a49-f67d-4124-9c58-1547b7bdd779 | Denominator Exclusion | 0 | 1 | mismatch |
| 80644a49-f67d-4124-9c58-1547b7bdd779 | Numerator | 1 | 0 | mismatch |
| 98e5cde7-fc04-4b89-9aef-5272087bb5c2 | Denominator Exclusion | 0 | 1 | mismatch |
| a46db6aa-5016-4111-bc4e-a31156c87ec6 | Denominator | 1 | 0 | mismatch |
| a46db6aa-5016-4111-bc4e-a31156c87ec6 | Initial Population | 1 | 0 | mismatch |
| a46db6aa-5016-4111-bc4e-a31156c87ec6 | Numerator | 1 | 0 | mismatch |
| a46db6aa-5016-4111-bc4e-a31156c87ec6 | Denominator | 1 | 0 | mismatch |
| a46db6aa-5016-4111-bc4e-a31156c87ec6 | Initial Population | 1 | 0 | mismatch |
| bee979d5-c118-4e1d-b190-62cf0e084bd1 | Denominator Exclusion | 0 | 1 | mismatch |
| c8559a93-63e3-4bce-b0a6-01a85fb6db28 | Denominator Exclusion | 0 | 1 | mismatch |
| c8559a93-63e3-4bce-b0a6-01a85fb6db28 | Numerator | 1 | 0 | mismatch |
| c8559a93-63e3-4bce-b0a6-01a85fb6db28 | Denominator Exclusion | 0 | 1 | mismatch |
| c8559a93-63e3-4bce-b0a6-01a85fb6db28 | Numerator | 1 | 0 | mismatch |
| cb044844-e03d-4758-bf40-1e4db68ed10e | Denominator Exclusion | 0 | 1 | mismatch |
| cb044844-e03d-4758-bf40-1e4db68ed10e | Numerator | 1 | 0 | mismatch |
| cb044844-e03d-4758-bf40-1e4db68ed10e | Denominator Exclusion | 0 | 1 | mismatch |
| cb044844-e03d-4758-bf40-1e4db68ed10e | Numerator | 1 | 0 | mismatch |
| d95789b9-f144-43e7-81c6-fed3adba5d8f | Denominator Exclusion | 0 | 1 | mismatch |
| db99ef01-a9e9-47c9-a2d5-5cb9c2b23241 | Denominator Exclusion | 0 | 1 | mismatch |
| e5a26079-76db-4851-a15a-7dae023a25ce | Denominator Exclusion | 0 | 1 | mismatch |

### CMS137FHIRSUDTxInitEngagement

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 19b5b244-6834-40f7-b8a2-ff2c6fb84fb0 | Denominator Exclusion | 0 | 1 | mismatch |
| 19b5b244-6834-40f7-b8a2-ff2c6fb84fb0 | Denominator Exclusion | 0 | 1 | mismatch |
| 19e9d2c7-4030-46c9-80e5-8c71fcae5227 | Denominator Exclusion | 0 | 1 | mismatch |
| 19e9d2c7-4030-46c9-80e5-8c71fcae5227 | Denominator Exclusion | 0 | 1 | mismatch |
| 3698ad63-09e3-46e8-ba42-39c9cd235603 | Denominator Exclusion | 0 | 1 | mismatch |
| 3698ad63-09e3-46e8-ba42-39c9cd235603 | Denominator Exclusion | 0 | 1 | mismatch |
| 404859c4-6f6e-4376-ae4d-d02a479e62aa | Denominator Exclusion | 0 | 1 | mismatch |
| 404859c4-6f6e-4376-ae4d-d02a479e62aa | Denominator Exclusion | 0 | 1 | mismatch |
| 408f327a-94aa-4787-a1c6-e6fc7fde341d | Denominator Exclusion | 0 | 1 | mismatch |
| 408f327a-94aa-4787-a1c6-e6fc7fde341d | Denominator Exclusion | 0 | 1 | mismatch |
| 46954fc1-3432-4e5d-b920-a2087f01abba | Denominator Exclusion | 0 | 1 | mismatch |
| 46954fc1-3432-4e5d-b920-a2087f01abba | Denominator Exclusion | 0 | 1 | mismatch |
| 6fc30283-94af-4a06-8325-cbc65e9b4b7c | Denominator Exclusion | 0 | 1 | mismatch |
| 6fc30283-94af-4a06-8325-cbc65e9b4b7c | Denominator Exclusion | 0 | 1 | mismatch |
| 8715fad1-2969-418a-b3d3-45b2581f4fe3 | Denominator Exclusion | 0 | 1 | mismatch |
| 8715fad1-2969-418a-b3d3-45b2581f4fe3 | Denominator Exclusion | 0 | 1 | mismatch |
| feb97651-b478-467e-97c9-3bc514a0a26b | Denominator Exclusion | 0 | 1 | mismatch |
| feb97651-b478-467e-97c9-3bc514a0a26b | Denominator Exclusion | 0 | 1 | mismatch |

### CMS138FHIRTobaccoScrnCessation

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 007fe881-a18d-418f-8ddf-0ee94fc9a10a | Denominator Exclusion | 0 | 1 | mismatch |
| 007fe881-a18d-418f-8ddf-0ee94fc9a10a | Denominator Exclusion | 0 | 1 | mismatch |
| 0d221636-5f14-4074-9337-eb4b0868fb3e | Denominator Exclusion | 0 | 1 | mismatch |
| 0d221636-5f14-4074-9337-eb4b0868fb3e | Denominator Exclusion | 0 | 1 | mismatch |
| 44a3e280-b4ad-4725-b806-1ea7592114d8 | Denominator Exclusion | 0 | 1 | mismatch |
| 44a3e280-b4ad-4725-b806-1ea7592114d8 | Numerator | 1 | 0 | mismatch |
| 44a3e280-b4ad-4725-b806-1ea7592114d8 | Denominator Exclusion | 0 | 1 | mismatch |
| 44a3e280-b4ad-4725-b806-1ea7592114d8 | Denominator Exclusion | 0 | 1 | mismatch |
| 6410550a-c928-415b-b8bc-aa1284ca6933 | Denominator Exclusion | 0 | 1 | mismatch |
| 6410550a-c928-415b-b8bc-aa1284ca6933 | Denominator Exclusion | 0 | 1 | mismatch |
| 72c8b10f-fffd-411f-bf81-c7d0608ad314 | Denominator Exclusion | 0 | 1 | mismatch |
| 72c8b10f-fffd-411f-bf81-c7d0608ad314 | Numerator | 1 | 0 | mismatch |
| 72c8b10f-fffd-411f-bf81-c7d0608ad314 | Denominator Exclusion | 0 | 1 | mismatch |
| 72c8b10f-fffd-411f-bf81-c7d0608ad314 | Denominator Exclusion | 0 | 1 | mismatch |
| 73d69a14-7e70-4c9f-89e3-62da4a370fd3 | Denominator Exclusion | 0 | 1 | mismatch |
| 73d69a14-7e70-4c9f-89e3-62da4a370fd3 | Denominator Exclusion | 0 | 1 | mismatch |
| 76e371e4-0363-4fad-9573-a06ada971eef | Denominator Exclusion | 0 | 1 | mismatch |
| 76e371e4-0363-4fad-9573-a06ada971eef | Denominator Exclusion | 0 | 1 | mismatch |
| 828caebe-4bd7-4579-85c6-d6340a9f3240 | Denominator Exclusion | 0 | 1 | mismatch |
| 828caebe-4bd7-4579-85c6-d6340a9f3240 | Numerator | 1 | 0 | mismatch |
| 828caebe-4bd7-4579-85c6-d6340a9f3240 | Denominator Exclusion | 0 | 1 | mismatch |
| 828caebe-4bd7-4579-85c6-d6340a9f3240 | Denominator Exclusion | 0 | 1 | mismatch |
| 9fba5feb-b77c-496f-981f-6d062f3c1d7c | Denominator Exclusion | 0 | 1 | mismatch |
| 9fba5feb-b77c-496f-981f-6d062f3c1d7c | Numerator | 1 | 0 | mismatch |
| 9fba5feb-b77c-496f-981f-6d062f3c1d7c | Denominator Exclusion | 0 | 1 | mismatch |
| 9fba5feb-b77c-496f-981f-6d062f3c1d7c | Numerator | 1 | 0 | mismatch |
| 9fba5feb-b77c-496f-981f-6d062f3c1d7c | Denominator Exclusion | 0 | 1 | mismatch |
| 9fba5feb-b77c-496f-981f-6d062f3c1d7c | Numerator | 1 | 0 | mismatch |
| a0a8fd4f-bfd4-4669-aaef-f66ae5af5eda | Denominator Exclusion | 0 | 1 | mismatch |
| a0a8fd4f-bfd4-4669-aaef-f66ae5af5eda | Denominator Exclusion | 0 | 1 | mismatch |
| ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5 | Denominator Exclusion | 0 | 1 | mismatch |
| ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5 | Numerator | 1 | 0 | mismatch |
| ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5 | Denominator Exclusion | 0 | 1 | mismatch |
| ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5 | Numerator | 1 | 0 | mismatch |
| ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5 | Denominator Exclusion | 0 | 1 | mismatch |
| ab8310c0-bcc3-4197-8ec6-beeb23f0b0b5 | Numerator | 1 | 0 | mismatch |
| bac2713c-8165-40ce-8180-fb5d44a10f7f | Denominator Exclusion | 0 | 1 | mismatch |
| bac2713c-8165-40ce-8180-fb5d44a10f7f | Denominator Exclusion | 0 | 1 | mismatch |
| c56fda5f-6cd9-4057-aaef-5c843a8241f1 | Denominator Exclusion | 0 | 1 | mismatch |
| c56fda5f-6cd9-4057-aaef-5c843a8241f1 | Numerator | 1 | 0 | mismatch |
| c56fda5f-6cd9-4057-aaef-5c843a8241f1 | Denominator Exclusion | 0 | 1 | mismatch |
| c56fda5f-6cd9-4057-aaef-5c843a8241f1 | Denominator Exclusion | 0 | 1 | mismatch |
| e3422e20-4e31-4c24-a72b-3c1e1f47de95 | Denominator Exclusion | 0 | 1 | mismatch |
| e3422e20-4e31-4c24-a72b-3c1e1f47de95 | Denominator Exclusion | 0 | 1 | mismatch |
| e383c9a2-7b5e-4ab6-b2e2-642a2304d6e2 | Denominator Exclusion | 0 | 1 | mismatch |
| e383c9a2-7b5e-4ab6-b2e2-642a2304d6e2 | Numerator | 1 | 0 | mismatch |
| e383c9a2-7b5e-4ab6-b2e2-642a2304d6e2 | Denominator Exclusion | 0 | 1 | mismatch |
| e383c9a2-7b5e-4ab6-b2e2-642a2304d6e2 | Denominator Exclusion | 0 | 1 | mismatch |
| ed2fe491-3eb7-424a-bf95-5d44b6102cec | Denominator Exclusion | 0 | 1 | mismatch |
| ed2fe491-3eb7-424a-bf95-5d44b6102cec | Numerator | 1 | 0 | mismatch |
| ed2fe491-3eb7-424a-bf95-5d44b6102cec | Denominator Exclusion | 0 | 1 | mismatch |
| ed2fe491-3eb7-424a-bf95-5d44b6102cec | Denominator Exclusion | 0 | 1 | mismatch |

### CMS139FHIRFallRiskScreening

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 1a370226-6ab1-487f-b1da-08741e08f725 | Denominator Exclusion | 0 | 1 | mismatch |
| 2b6eca9d-7580-4262-ba2c-97f6c174cc33 | Denominator Exclusion | 0 | 1 | mismatch |
| 4576786d-d477-4447-8bdb-f9d5c2e6600c | Denominator Exclusion | 0 | 1 | mismatch |
| 4a1c85c3-e97c-4644-b6a1-2475aa1c27e2 | Denominator Exclusion | 0 | 1 | mismatch |
| 65b723f6-246d-4320-a181-a64f7f1fd837 | Denominator Exclusion | 0 | 1 | mismatch |
| 741236df-31ad-463b-b730-fb113cfa09a8 | Denominator Exclusion | 0 | 1 | mismatch |
| 839e7c3a-a94f-418f-96cb-d356bf6de1da | Denominator Exclusion | 0 | 1 | mismatch |
| b7261db5-e945-48b9-90dd-0d0761c09295 | Denominator Exclusion | 0 | 1 | mismatch |

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

### CMS146FHIRApproTestPharyngitis

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 32b213a8-4071-4bc7-8db8-8ab080e5e468 | Denominator Exclusion | 0 | 1 | mismatch |
| 4b78839b-3a31-4dc7-9b6b-4e06f005c7e0 | Denominator Exclusion | 0 | 1 | mismatch |
| a6e7ec82-b80e-4f76-b382-91956c4873a9 | Denominator Exclusion | 0 | 1 | mismatch |
| b23aa001-1331-46f0-9818-19f6dc890668 | Denominator Exclusion | 0 | 1 | mismatch |
| c257e23d-80d0-4ab8-9374-e38815eab144 | Denominator Exclusion | 0 | 1 | mismatch |
| c5401e41-5ec7-4d84-b0ab-600dd4b8cdaf | Denominator Exclusion | 0 | 1 | mismatch |
| c5f2b465-bfa2-4f94-8512-ff04308a8159 | Denominator Exclusion | 0 | 1 | mismatch |
| c8d42ccd-9523-414f-b568-e0fdae94a84a | Denominator Exclusion | 0 | 1 | mismatch |
| e251036b-b9dc-4c2c-8841-5d34064501ed | Denominator Exclusion | 0 | 1 | mismatch |
| ed5a5721-71d3-4247-9f9b-4097e55fccfb | Denominator Exclusion | 0 | 1 | mismatch |

### CMS153FHIRChlamydiaScreening

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 1c0607a1-de1a-46e2-98f5-5ea7c5f50506 | Denominator Exclusion | 0 | 1 | mismatch |
| 5e5374d9-3830-47dd-bbf4-dbc8960c4870 | Denominator Exclusion | 0 | 1 | mismatch |
| 6e31a1eb-0d32-4a9b-aa86-ee34436f99c1 | Denominator | 1 | 0 | mismatch |
| 6e31a1eb-0d32-4a9b-aa86-ee34436f99c1 | Initial Population | 1 | 0 | mismatch |
| 840339a3-d0c2-4fa8-8f80-cfdd57f48868 | Denominator Exclusion | 0 | 1 | mismatch |
| c0225f3d-ea64-4bb4-873b-b28ebc10050a | Denominator Exclusion | 0 | 1 | mismatch |
| dc0d63ab-8b3a-4f90-ab19-0c4c18d398a8 | Denominator Exclusion | 0 | 1 | mismatch |
| dda878bb-eb46-4562-a455-862009c0f7ce | Denominator Exclusion | 0 | 1 | mismatch |
| ec8a19c5-8fd1-40e9-974b-98fbccd921b8 | Denominator Exclusion | 0 | 1 | mismatch |
| f6a69563-6b05-4dcb-87e6-dd3bdd25f597 | Denominator Exclusion | 0 | 1 | mismatch |

### CMS154FHIRAppropriateTxforURI

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 1b24b0b1-92fa-405d-88d1-e550896598c1 | Denominator Exclusion | 0 | 1 | mismatch |
| 1b24b0b1-92fa-405d-88d1-e550896598c1 | Numerator | 1 | 0 | mismatch |
| 41bc23b2-9bf6-4e81-ae25-2b5f78b61b87 | Denominator Exclusion | 0 | 1 | mismatch |
| 41bc23b2-9bf6-4e81-ae25-2b5f78b61b87 | Numerator | 1 | 0 | mismatch |
| 673d2f3c-b735-4672-8a4e-2f77060e1802 | Denominator Exclusion | 0 | 1 | mismatch |
| 673d2f3c-b735-4672-8a4e-2f77060e1802 | Numerator | 1 | 0 | mismatch |
| 78a48c68-f018-47da-a1cc-c96b63c248e8 | Denominator Exclusion | 0 | 1 | mismatch |
| 78a48c68-f018-47da-a1cc-c96b63c248e8 | Numerator | 1 | 0 | mismatch |
| 99d50203-60f7-466b-a253-a0908d85a7a3 | Denominator Exclusion | 0 | 1 | mismatch |
| 99d50203-60f7-466b-a253-a0908d85a7a3 | Numerator | 1 | 0 | mismatch |
| acb44fb3-b572-4dfd-891c-c8b2cc24e1b8 | Denominator Exclusion | 0 | 1 | mismatch |
| acb44fb3-b572-4dfd-891c-c8b2cc24e1b8 | Numerator | 1 | 0 | mismatch |
| cac03a54-f595-411e-bc00-c9146222a68c | Denominator Exclusion | 0 | 1 | mismatch |
| cac03a54-f595-411e-bc00-c9146222a68c | Numerator | 1 | 0 | mismatch |
| dc6b0b42-949a-481e-8134-bb536a2f3fe9 | Denominator Exclusion | 0 | 1 | mismatch |
| dc6b0b42-949a-481e-8134-bb536a2f3fe9 | Numerator | 1 | 0 | mismatch |

### CMS155FHIRWgtAssessCounseling

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 1e0720b0-0782-4455-a355-8c1ecec3c653 | Denominator Exclusion | 0 | 1 | mismatch |
| 1e0720b0-0782-4455-a355-8c1ecec3c653 | Denominator Exclusion | 0 | 1 | mismatch |
| 1e0720b0-0782-4455-a355-8c1ecec3c653 | Denominator Exclusion | 0 | 1 | mismatch |
| 259f8551-1cea-44f5-ae9e-e3f083d9f48f | Denominator Exclusion | 0 | 1 | mismatch |
| 259f8551-1cea-44f5-ae9e-e3f083d9f48f | Denominator Exclusion | 0 | 1 | mismatch |
| 259f8551-1cea-44f5-ae9e-e3f083d9f48f | Denominator Exclusion | 0 | 1 | mismatch |
| 4304f97a-e2bb-4cda-93fa-ab510a136403 | Denominator Exclusion | 0 | 1 | mismatch |
| 4304f97a-e2bb-4cda-93fa-ab510a136403 | Denominator Exclusion | 0 | 1 | mismatch |
| 4304f97a-e2bb-4cda-93fa-ab510a136403 | Denominator Exclusion | 0 | 1 | mismatch |
| 4a9211fc-d757-47ae-8bc0-0803c43a6728 | Denominator Exclusion | 0 | 1 | mismatch |
| 4a9211fc-d757-47ae-8bc0-0803c43a6728 | Denominator Exclusion | 0 | 1 | mismatch |
| 4a9211fc-d757-47ae-8bc0-0803c43a6728 | Denominator Exclusion | 0 | 1 | mismatch |
| 53711871-5aac-4e37-a047-9dae85fcf6cb | Denominator Exclusion | 0 | 1 | mismatch |
| 53711871-5aac-4e37-a047-9dae85fcf6cb | Denominator Exclusion | 0 | 1 | mismatch |
| 53711871-5aac-4e37-a047-9dae85fcf6cb | Denominator Exclusion | 0 | 1 | mismatch |
| 598662b8-30c9-4f9b-a2d1-d91bea113d77 | Denominator Exclusion | 0 | 1 | mismatch |
| 598662b8-30c9-4f9b-a2d1-d91bea113d77 | Denominator Exclusion | 0 | 1 | mismatch |
| 598662b8-30c9-4f9b-a2d1-d91bea113d77 | Denominator Exclusion | 0 | 1 | mismatch |
| a0c68789-2a1b-4bc4-b6a4-d8f6b154d8ac | Denominator Exclusion | 0 | 1 | mismatch |
| a0c68789-2a1b-4bc4-b6a4-d8f6b154d8ac | Denominator Exclusion | 0 | 1 | mismatch |
| a0c68789-2a1b-4bc4-b6a4-d8f6b154d8ac | Denominator Exclusion | 0 | 1 | mismatch |
| bd9b9e02-ce12-43cb-af1c-25298c891e62 | Denominator Exclusion | 0 | 1 | mismatch |
| bd9b9e02-ce12-43cb-af1c-25298c891e62 | Denominator Exclusion | 0 | 1 | mismatch |
| bd9b9e02-ce12-43cb-af1c-25298c891e62 | Denominator Exclusion | 0 | 1 | mismatch |
| dbb639f6-f7b7-41c8-bc30-84e5574c08cd | Denominator Exclusion | 0 | 1 | mismatch |
| dbb639f6-f7b7-41c8-bc30-84e5574c08cd | Denominator Exclusion | 0 | 1 | mismatch |
| dbb639f6-f7b7-41c8-bc30-84e5574c08cd | Denominator Exclusion | 0 | 1 | mismatch |

### CMS156FHIRHighRiskMedsElderly

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 05aa403d-44c1-4c71-acb9-7808568b6a4f | Denominator Exclusion | 0 | 1 | mismatch |
| 05aa403d-44c1-4c71-acb9-7808568b6a4f | Denominator Exclusion | 0 | 1 | mismatch |
| 05aa403d-44c1-4c71-acb9-7808568b6a4f | Denominator Exclusion | 0 | 1 | mismatch |
| 07f11229-6e8f-42bf-9905-3d319460fb33 | Numerator | 1 | 0 | mismatch |
| 07f11229-6e8f-42bf-9905-3d319460fb33 | Numerator | 1 | 0 | mismatch |
| 28da77ab-fe4d-44f2-a2fe-9c260e941cfb | Denominator Exclusion | 0 | 1 | mismatch |
| 28da77ab-fe4d-44f2-a2fe-9c260e941cfb | Denominator Exclusion | 0 | 1 | mismatch |
| 28da77ab-fe4d-44f2-a2fe-9c260e941cfb | Denominator Exclusion | 0 | 1 | mismatch |
| 32186189-fe9c-41d5-9654-68c0c60aaac6 | Denominator Exclusion | 0 | 1 | mismatch |
| 32186189-fe9c-41d5-9654-68c0c60aaac6 | Denominator Exclusion | 0 | 1 | mismatch |
| 32186189-fe9c-41d5-9654-68c0c60aaac6 | Denominator Exclusion | 0 | 1 | mismatch |
| 35b521b6-1fdd-4742-8137-36213864b0fb | Denominator Exclusion | 0 | 1 | mismatch |
| 35b521b6-1fdd-4742-8137-36213864b0fb | Denominator Exclusion | 0 | 1 | mismatch |
| 35b521b6-1fdd-4742-8137-36213864b0fb | Denominator Exclusion | 0 | 1 | mismatch |
| 60883694-3c84-4343-b12b-b017f1c57587 | Denominator Exclusion | 0 | 1 | mismatch |
| 60883694-3c84-4343-b12b-b017f1c57587 | Denominator Exclusion | 0 | 1 | mismatch |
| 60883694-3c84-4343-b12b-b017f1c57587 | Denominator Exclusion | 0 | 1 | mismatch |
| 688e8e1b-8054-4c30-83e8-ab99fdd7ccfb | Denominator Exclusion | 0 | 1 | mismatch |
| 688e8e1b-8054-4c30-83e8-ab99fdd7ccfb | Denominator Exclusion | 0 | 1 | mismatch |
| 688e8e1b-8054-4c30-83e8-ab99fdd7ccfb | Denominator Exclusion | 0 | 1 | mismatch |
| 8b2f163f-e180-4169-b41a-9c3b77ae0302 | Denominator Exclusion | 0 | 1 | mismatch |
| 8b2f163f-e180-4169-b41a-9c3b77ae0302 | Denominator Exclusion | 0 | 1 | mismatch |
| 8b2f163f-e180-4169-b41a-9c3b77ae0302 | Denominator Exclusion | 0 | 1 | mismatch |
| 8b33d091-6e1e-4992-9ae6-63adc9401862 | Denominator Exclusion | 0 | 1 | mismatch |
| 8b33d091-6e1e-4992-9ae6-63adc9401862 | Denominator Exclusion | 0 | 1 | mismatch |
| 8b33d091-6e1e-4992-9ae6-63adc9401862 | Denominator Exclusion | 0 | 1 | mismatch |
| 9f9302aa-f988-4131-a265-3996467aeed7 | Denominator Exclusion | 0 | 1 | mismatch |
| 9f9302aa-f988-4131-a265-3996467aeed7 | Denominator Exclusion | 0 | 1 | mismatch |
| 9f9302aa-f988-4131-a265-3996467aeed7 | Denominator Exclusion | 0 | 1 | mismatch |
| ad4aced6-dec9-4309-86a1-246b7c0dd6d9 | Denominator Exclusion | 0 | 1 | mismatch |
| ad4aced6-dec9-4309-86a1-246b7c0dd6d9 | Denominator Exclusion | 0 | 1 | mismatch |
| ad4aced6-dec9-4309-86a1-246b7c0dd6d9 | Denominator Exclusion | 0 | 1 | mismatch |
| b9e0084c-8386-48e2-b17d-87c508c566f9 | Denominator Exclusion | 0 | 1 | mismatch |
| b9e0084c-8386-48e2-b17d-87c508c566f9 | Denominator Exclusion | 0 | 1 | mismatch |
| b9e0084c-8386-48e2-b17d-87c508c566f9 | Denominator Exclusion | 0 | 1 | mismatch |
| bb83b7f0-6542-4105-b2f0-5d2018167a9e | Denominator Exclusion | 0 | 1 | mismatch |
| bb83b7f0-6542-4105-b2f0-5d2018167a9e | Denominator Exclusion | 0 | 1 | mismatch |
| bb83b7f0-6542-4105-b2f0-5d2018167a9e | Denominator Exclusion | 0 | 1 | mismatch |
| c409fbc9-a31f-4d53-9aa7-9e443e87812a | Numerator | 1 | 0 | mismatch |
| c409fbc9-a31f-4d53-9aa7-9e443e87812a | Numerator | 1 | 0 | mismatch |
| eac1ad4e-e4cb-49c8-a9b1-6ddf5eec85a1 | Denominator Exclusion | 0 | 1 | mismatch |
| eac1ad4e-e4cb-49c8-a9b1-6ddf5eec85a1 | Denominator Exclusion | 0 | 1 | mismatch |
| eac1ad4e-e4cb-49c8-a9b1-6ddf5eec85a1 | Denominator Exclusion | 0 | 1 | mismatch |

### CMS165FHIRControllingHighBP

| Test Case | Population | CMS Actual | QI-Core Actual | Type |
|---|---|---:|---:|---|
| 0e867903-400d-4d71-a7fd-dc9b96d94a17 | Denominator Exclusion | 0 | 1 | mismatch |
| 29d930b1-1bb6-4089-9ed6-aa2b7b77d5a4 | Denominator Exclusion | 0 | 1 | mismatch |
| 352a05d3-750c-45bd-a170-a8a8822b7697 | Denominator Exclusion | 0 | 1 | mismatch |
| 3e214018-7420-4e1f-a24d-e9426ace2bd8 | Denominator Exclusion | 0 | 1 | mismatch |
| 481692c7-2cf7-48fc-8269-967f5d7753bc | Denominator Exclusion | 0 | 1 | mismatch |
| 4b31dc2b-7867-4766-8a8c-e1971d1e570a | Denominator Exclusion | 0 | 1 | mismatch |
| 50d7cf81-dff4-45eb-b43d-0e40b08c3a75 | Denominator Exclusion | 0 | 1 | mismatch |
| 6f37e357-7575-4b40-a63e-4b882532250f | Numerator | 0 | 1 | mismatch |
| 821185af-e5b2-4552-a63c-36b64a9200a9 | Denominator Exclusion | 0 | 1 | mismatch |
| 926b705a-b222-4c64-9d3f-ad64ead74295 | Denominator Exclusion | 0 | 1 | mismatch |
| 972c7128-f3c2-401d-89f3-a0752dd02620 | Denominator Exclusion | 0 | 1 | mismatch |
| aa1f02c0-ded0-4b30-9f0d-c8be54aa436b | Denominator Exclusion | 0 | 1 | mismatch |
| aa87ac34-227b-4424-84d2-62aaba57c232 | Denominator Exclusion | 0 | 1 | mismatch |
| bfdc37c9-105c-4765-a2ba-d7da92ec9a47 | Denominator Exclusion | 0 | 1 | mismatch |
| cdfb5385-a466-4d41-9dce-cc50f88d0666 | Denominator Exclusion | 0 | 1 | mismatch |
| d6be5093-9772-4e0f-83e1-b56b26d55529 | Denominator Exclusion | 0 | 1 | mismatch |
| e56c60ca-d0d0-4910-af2e-1d8a074d129a | Denominator Exclusion | 0 | 1 | mismatch |
| f2d1fd7e-35ae-45cd-86e6-8b874c3e3fb9 | Numerator | 0 | 1 | mismatch |
| f5b461d7-e382-4616-a763-d745867735d0 | Denominator Exclusion | 0 | 1 | mismatch |
| f9bf76c5-7b85-4fd7-b883-b7c14e8b1801 | Denominator Exclusion | 0 | 1 | mismatch |

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
| 0784160c-98b6-43a2-baa1-77ea9f3fe884 | Denominator Exception | 0 | 1 | mismatch |
| 08882e8d-afd1-4a5e-a30b-a5a0ed9e1010 | Denominator Exception | 0 | 1 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 0 | 1 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Numerator | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 1 | 0 | mismatch |
| 08dfc736-3cb5-467c-93cf-99146604a8f4 | Denominator Exception | 1 | 0 | mismatch |
| 0aaae01e-d3b0-4b76-abf8-a044fd4f5d80 | Denominator Exception | 0 | 1 | mismatch |
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
| 0f204e98-0782-43a3-ae53-b516cc8d5797 | Denominator Exception | 0 | 1 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Initial Population | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Numerator | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator Exception | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator Exception | 1 | 0 | mismatch |
| 0f853b02-7949-4d97-ab69-1e48045afe95 | Denominator Exception | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 0 | 1 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Numerator | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 1 | 0 | mismatch |
| 1116b208-af60-4f6b-a5f1-448209aec45f | Denominator Exception | 1 | 0 | mismatch |
| 113749ee-bb22-4395-9621-642f98839340 | Denominator Exception | 0 | 1 | mismatch |
| 15d7fcaa-773f-4888-8b13-bc077cbfdf4a | Denominator Exception | 0 | 1 | mismatch |
| 1831f057-fa97-4c2b-b6cc-9830e4a60e11 | Denominator Exception | 0 | 1 | mismatch |
| 1ba7b147-b701-424c-bade-4e8270547030 | Denominator Exception | 1 | 0 | mismatch |
| 26101306-010f-48c5-aa83-8a94f280f755 | Denominator Exception | 0 | 1 | mismatch |
| 2c5a09d4-18c9-4128-86fb-bd49871f9231 | Denominator Exception | 0 | 1 | mismatch |
| 2cff757c-4470-46a2-a685-6e23cf82c045 | Numerator | 1 | 0 | mismatch |
| 3137d292-5094-49ef-82da-d9809b599030 | Denominator Exception | 0 | 1 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 0 | 1 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Numerator | 1 | 0 | mismatch |
| 3b5da2bf-0fb9-4efc-bc54-4bd329ed31af | Denominator Exception | 1 | 0 | mismatch |
| 3e09af44-0445-4077-b73c-6896fdbe49c5 | Denominator | 1 | 0 | mismatch |
| 3e09af44-0445-4077-b73c-6896fdbe49c5 | Initial Population | 1 | 0 | mismatch |
| 4120512a-d0f4-4ffa-acd9-0191db3b7f46 | Denominator | 1 | 0 | mismatch |
| 4120512a-d0f4-4ffa-acd9-0191db3b7f46 | Denominator Exception | 1 | 0 | mismatch |
| 4120512a-d0f4-4ffa-acd9-0191db3b7f46 | Initial Population | 1 | 0 | mismatch |
| 476bff0b-a87a-413b-91ae-c3a14b7778b1 | Denominator Exception | 0 | 1 | mismatch |
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
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator Exception | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Initial Population | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Numerator | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator Exception | 1 | 0 | mismatch |
| 52b48d35-f47c-4013-9cdc-700baad0fc0f | Denominator Exception | 1 | 0 | mismatch |
| 537d14db-6ced-4cd2-9553-e88bd6551771 | Denominator | 1 | 0 | mismatch |
| 537d14db-6ced-4cd2-9553-e88bd6551771 | Initial Population | 1 | 0 | mismatch |
| 59d6bb14-b82e-4295-baf1-d96be73e1e38 | Denominator | 1 | 0 | mismatch |
| 59d6bb14-b82e-4295-baf1-d96be73e1e38 | Initial Population | 1 | 0 | mismatch |
| 5b37b5a5-0e28-4b28-9889-8878d41ff9cf | Denominator Exception | 0 | 1 | mismatch |
| 5cebab0f-d32e-4adc-bef3-90812d6c5819 | Denominator Exception | 0 | 1 | mismatch |
| 5f799983-39d3-4f03-9a9a-125dc6f12f13 | Denominator Exception | 0 | 1 | mismatch |
| 60b9bda6-6c16-4797-8278-0a667008a69e | Denominator Exception | 0 | 1 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 0 | 1 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Numerator | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 1 | 0 | mismatch |
| 70fd1056-5313-417f-bbbe-9f2bacf942bb | Denominator Exception | 1 | 0 | mismatch |
| 716760c5-b72e-4d46-b8df-c3b0f86d90ad | Denominator Exception | 0 | 1 | mismatch |
| 74499ca5-db3b-4ce1-92e0-e19c6590d138 | Denominator Exception | 0 | 1 | mismatch |
| 74e5f17e-ae6b-4e3c-8183-e75381377d23 | Denominator Exception | 0 | 1 | mismatch |
| 759a89b4-51ed-4622-adae-6b0930701ebb | Denominator | 1 | 0 | mismatch |
| 759a89b4-51ed-4622-adae-6b0930701ebb | Initial Population | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 1 | 0 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Denominator Exception | 0 | 1 | mismatch |
| 7b8b48b3-76d4-4492-81a1-93fdea67b0c1 | Numerator | 1 | 0 | mismatch |
| 821087e5-a030-49ac-95b5-5b9ab38e88da | Denominator Exception | 0 | 1 | mismatch |
| 86bacb29-41c3-4ea8-8e4b-3e13c075e557 | Denominator Exception | 0 | 1 | mismatch |
| 8927dd81-b976-4b7f-a78c-c4215ee8fc9a | Denominator Exception | 0 | 1 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 0 | 1 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Numerator | 1 | 0 | mismatch |
| 8b0f2e04-8c60-4f6e-adc5-8967a540a18f | Denominator Exception | 1 | 0 | mismatch |
| 8c357499-cb9a-41c9-9060-1bbbefb0fd7e | Denominator Exception | 0 | 1 | mismatch |
| 93aea3e2-4736-4be0-830f-54c1ef6df6d5 | Denominator | 1 | 0 | mismatch |
| 93aea3e2-4736-4be0-830f-54c1ef6df6d5 | Denominator Exception | 1 | 0 | mismatch |
| 93aea3e2-4736-4be0-830f-54c1ef6df6d5 | Initial Population | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 1 | 0 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Denominator Exception | 0 | 1 | mismatch |
| 95ab5fd7-b1be-4dd3-ba42-1b48215fab70 | Numerator | 1 | 0 | mismatch |
| 95fad34f-db86-4e4a-a8a2-42a3b7ac15dc | Denominator Exception | 0 | 1 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 0 | 1 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Numerator | 1 | 0 | mismatch |
| 9a06f385-0bed-4f35-9af4-1ff7971c07f5 | Denominator Exception | 1 | 0 | mismatch |
| 9c2afd42-581e-418b-9eaa-3ddf4918c9ac | Denominator Exception | 0 | 1 | mismatch |
| 9e01f70e-cb9c-451b-8993-8664e31d92e2 | Denominator Exception | 0 | 1 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 0 | 1 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Numerator | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 1 | 0 | mismatch |
| 9edcce2d-8d32-4f4f-88a5-6fa689b73f8d | Denominator Exception | 1 | 0 | mismatch |
| a0202aaf-756f-4d08-8329-8fd585ddda63 | Denominator | 1 | 0 | mismatch |
| a0202aaf-756f-4d08-8329-8fd585ddda63 | Initial Population | 1 | 0 | mismatch |
| a03e2988-3bed-4fc5-b1e7-70eac99f0612 | Denominator | 1 | 0 | mismatch |
| a03e2988-3bed-4fc5-b1e7-70eac99f0612 | Initial Population | 1 | 0 | mismatch |
| b35ba523-abea-4848-8dac-256c1727447c | Denominator Exception | 0 | 1 | mismatch |
| b88292a5-2443-44a2-a268-2a6cb95f92bd | Denominator Exception | 0 | 1 | mismatch |
| be29ff82-9191-4b5f-91ca-cc5590fea905 | Denominator Exception | 0 | 1 | mismatch |
| c75e56eb-e95d-4c65-b184-3565362eb3ba | Denominator Exception | 0 | 1 | mismatch |
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
| d3a48d69-2269-472a-9c27-da2c658e8c68 | Denominator Exception | 0 | 1 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 0 | 1 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Numerator | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 1 | 0 | mismatch |
| dbca4643-bd37-4e01-8024-fb7c70692fe9 | Denominator Exception | 1 | 0 | mismatch |
| df05b853-3e6d-4a12-b1db-fd9d0ec790a2 | Denominator | 1 | 0 | mismatch |
| df05b853-3e6d-4a12-b1db-fd9d0ec790a2 | Initial Population | 1 | 0 | mismatch |
| e1c47dc2-2705-4c32-8000-415987028df9 | Denominator Exception | 0 | 1 | mismatch |
| e4547b2c-ce1c-4ffb-b5d4-c99687424bf0 | Denominator Exception | 0 | 1 | mismatch |
| e656adac-2016-40a4-833f-0c5a02952ba3 | Denominator Exception | 0 | 1 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 1 | 0 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Denominator Exception | 0 | 1 | mismatch |
| e8020421-14a3-4c64-99c4-3366c1400bd7 | Numerator | 1 | 0 | mismatch |
| f120b2b6-40ba-4ae3-b087-c64e8e3bdf11 | Denominator Exception | 0 | 1 | mismatch |
| f2136084-b5c4-4171-9d1b-d759637ddcfa | Denominator Exception | 0 | 1 | mismatch |
| f6a5913b-bfdd-4ccf-8700-3c949b0639ed | Denominator Exception | 0 | 1 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 0 | 1 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Numerator | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 1 | 0 | mismatch |
| f8563fcf-4e09-4309-841b-bcce373bc4b2 | Denominator Exception | 1 | 0 | mismatch |
| fa446b35-031d-4eb5-b7f1-5782580e5209 | Denominator Exception | 0 | 1 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 1 | 0 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Denominator Exception | 0 | 1 | mismatch |
| faae1173-bc93-4fd2-a22f-e7726430857f | Numerator | 1 | 0 | mismatch |
| fcd4fe20-9013-4d1c-965b-1445f0088624 | Denominator Exception | 0 | 1 | mismatch |

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
| 023b65d6-0b68-4b1f-b276-f500e4b77ed2 | Denominator Exclusion | 0 | 1 | mismatch |
| 1127bc95-bf52-4921-b02a-de0902780191 | Denominator Exclusion | 0 | 1 | mismatch |
| 1e8e8baf-0c27-42b2-93ad-5426418552c7 | Denominator Exclusion | 0 | 1 | mismatch |
| 2a7112e7-5937-4288-9271-cdc2d7e5eaa4 | Denominator Exclusion | 0 | 1 | mismatch |
| 3f860c8e-e5fc-4843-ac4e-acb8e63471f3 | Denominator Exclusion | 0 | 1 | mismatch |
| 4354fbec-b63a-46ce-8465-ec82710ea1c6 | Denominator Exclusion | 0 | 1 | mismatch |
| 55c5c208-190b-4f90-bdbb-0c02332df772 | Denominator Exclusion | 0 | 1 | mismatch |
| 61c9b47c-2223-4e45-b83b-eee21f031cad | Denominator Exclusion | 0 | 1 | mismatch |
| 77620fcb-7a0a-4015-89cc-c32bd8681c13 | Denominator Exclusion | 0 | 1 | mismatch |
| b1e68658-d64f-4ca4-a4ee-89c64e4536fa | Denominator Exclusion | 0 | 1 | mismatch |
| b6ac3dd1-ff55-4152-be9a-153cad2ba2a2 | Denominator Exclusion | 0 | 1 | mismatch |
| d7e37bcf-d13b-4415-82ac-a51b5c83151c | Denominator Exclusion | 0 | 1 | mismatch |
| ebd7d1d0-a663-47da-8802-9088ad9d80a0 | Denominator Exclusion | 0 | 1 | mismatch |

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

