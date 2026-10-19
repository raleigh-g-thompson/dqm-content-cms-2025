# Discrepancy Report
| Details | Value |
| --- | --- |
| Generated | 2026-10-19 13:28:35.033549 |
| Total Measures | 74 |
| Total Test Cases | 3964 |
| Measures with Discrepancies | 14 |
| Pass Count | 3884 (97.98%) |
| Workaround Count | 6 (0.15%) |
| &nbsp;&nbsp;— covered by a ticketed known issue | 6 (0.15%) |
| Implementation Issue Count | 5 (0.13%) |
| Known Issue Count (ticketed) | 4 (0.10%) |
| Fail Count | 65 (1.64%) |
| &nbsp;&nbsp;— known issue, needs ticket | 3 (0.08%) |
| &nbsp;&nbsp;— known issue, no ticket expected | 14 (0.35%) |
| Known issues with an upstream ticket | 10 (0.25%) |
| Resolved (Pass + Implementation + Known Issue) | 3893 (98.21%) |


| Discrepancy Summary | Measure Count | Test Case Count |
|---|:---:|:---:|
| Missing Results | 5 | 55 |
| Missing Populations | 0 | 0 |
| Mismatched Test Cases | 9 | 19 |
| Workarounds | 2 | 13 |
| Unattributed Discrepancies | 7 | 65 |



_Note: Measures can have multiple discrepancies, so the Measures with Discrepancies count may not match the summary counts. Unattributed Discrepancies are those no implementation issue or ticketed known issue resolves. Workarounds are test cases that match their fixture only because the CQL works around an open issue: they count as neither a pass nor a failure, and the workaround is removed once the root cause is fixed._

## Populations Excluded from Automated Scoring (CQFM Aggregation)

_These populations require invoking a parameterized CQL `measure-observation` function once per member of a `cqfm-criteriaReference` population and then applying `cqfm-aggregateMethod` (Sum/Count/Average) -- a computation this harness's CQL-execution path cannot perform (no scriptable/batch CQL runner; see `defect-tracking/known-issues.md`, I-62). They are intentionally excluded from pass/fail scoring rather than reported as FAIL or silently read as 0. Every other population on these measures (Initial Population, Denominator, Numerator, exclusions, etc.) is unaffected and scored normally. Use the `mcp-cql-debug` per-member probe method documented in I-62 to manually verify any of the expected values below._

| Measure | Excluded Populations |
| --- | --- |
| CMS871FHIRHHHyper | Group_1:Denominator Observation, Group_1:Measure Observation, Group_1:Numerator Observation |
| CMS986FHIRMalnutritionScore | Group_1:Measure Observation, Group_2:Measure Observation, Group_3:Measure Observation, Group_4:Measure Observation, Group_5:Measure Observation, Group_6:Measure Observation |
| CMS1017FHIRHHFI | Group_1:Denominator Observation, Group_1:Measure Observation, Group_1:Numerator Observation |

_2085 cells excluded across 3 measures._

## Measures with No Discrepancies (60 of 74)

|  |  |  |
| --- | --- | --- |
| [ CMS2FHIRPCSDepScreenAndFollowUp ](../../input/cql/CMS2FHIRPCSDepScreenAndFollowUp.cql) [ test ](../../input/tests/results/CMS2FHIRPCSDepScreenAndFollowUp.txt) | [ CMS133FHIRCataracts2040BCVA90Days ](../../input/cql/CMS133FHIRCataracts2040BCVA90Days.cql) [ test ](../../input/tests/results/CMS133FHIRCataracts2040BCVA90Days.txt) | [ CMS645FHIRBoneDensityPCADTherapy ](../../input/cql/CMS645FHIRBoneDensityPCADTherapy.cql) [ test ](../../input/tests/results/CMS645FHIRBoneDensityPCADTherapy.txt) |
| [ CMS22FHIRPCSBPScreeningFollowUp ](../../input/cql/CMS22FHIRPCSBPScreeningFollowUp.cql) [ test ](../../input/tests/results/CMS22FHIRPCSBPScreeningFollowUp.txt) | [ CMS136FHIRChildADHDMedFollowUp ](../../input/cql/CMS136FHIRChildADHDMedFollowUp.cql) [ test ](../../input/tests/results/CMS136FHIRChildADHDMedFollowUp.txt) | [ CMS816FHIRHHHypo ](../../input/cql/CMS816FHIRHHHypo.cql) [ test ](../../input/tests/results/CMS816FHIRHHHypo.txt) |
| [ CMS50FHIRReceiptofSpecialistReport ](../../input/cql/CMS50FHIRReceiptofSpecialistReport.cql) [ test ](../../input/tests/results/CMS50FHIRReceiptofSpecialistReport.txt) | [ CMS137FHIRSUDTxInitEngagement ](../../input/cql/CMS137FHIRSUDTxInitEngagement.cql) [ test ](../../input/tests/results/CMS137FHIRSUDTxInitEngagement.txt) | [ CMS819FHIRHHORAE ](../../input/cql/CMS819FHIRHHORAE.cql) [ test ](../../input/tests/results/CMS819FHIRHHORAE.txt) |
| [ CMS56FHIRFuncStatHipReplacement ](../../input/cql/CMS56FHIRFuncStatHipReplacement.cql) [ test ](../../input/tests/results/CMS56FHIRFuncStatHipReplacement.txt) | [ CMS138FHIRTobaccoScrnCessation ](../../input/cql/CMS138FHIRTobaccoScrnCessation.cql) [ test ](../../input/tests/results/CMS138FHIRTobaccoScrnCessation.txt) | [ CMS826FHIRHHPI ](../../input/cql/CMS826FHIRHHPI.cql) [ test ](../../input/tests/results/CMS826FHIRHHPI.txt) |
| [ CMS68FHIRDocumentationCurrentMeds ](../../input/cql/CMS68FHIRDocumentationCurrentMeds.cql) [ test ](../../input/tests/results/CMS68FHIRDocumentationCurrentMeds.txt) | [ CMS139FHIRFallRiskScreening ](../../input/cql/CMS139FHIRFallRiskScreening.cql) [ test ](../../input/tests/results/CMS139FHIRFallRiskScreening.txt) | [ CMS832FHIRHHAKI ](../../input/cql/CMS832FHIRHHAKI.cql) [ test ](../../input/tests/results/CMS832FHIRHHAKI.txt) |
| [ CMS69FHIRPCSBMIScreenAndFollowUp ](../../input/cql/CMS69FHIRPCSBMIScreenAndFollowUp.cql) [ test ](../../input/tests/results/CMS69FHIRPCSBMIScreenAndFollowUp.txt) | [ CMS142FHIRCommWithDrManagingDiab ](../../input/cql/CMS142FHIRCommWithDrManagingDiab.cql) [ test ](../../input/tests/results/CMS142FHIRCommWithDrManagingDiab.txt) | [ CMS951FHIRKidneyHealthEval ](../../input/cql/CMS951FHIRKidneyHealthEval.cql) [ test ](../../input/tests/results/CMS951FHIRKidneyHealthEval.txt) |
| [ CMS71FHIRSTKAnticoagAFFlutter ](../../input/cql/CMS71FHIRSTKAnticoagAFFlutter.cql) [ test ](../../input/tests/results/CMS71FHIRSTKAnticoagAFFlutter.txt) | [ CMS143FHIRPOAGOpticNerveEval ](../../input/cql/CMS143FHIRPOAGOpticNerveEval.cql) [ test ](../../input/tests/results/CMS143FHIRPOAGOpticNerveEval.txt) | [ CMS986FHIRMalnutritionScore ](../../input/cql/CMS986FHIRMalnutritionScore.cql) [ test ](../../input/tests/results/CMS986FHIRMalnutritionScore.txt) |
| [ CMS74FHIRDentalCariesPrevention ](../../input/cql/CMS74FHIRDentalCariesPrevention.cql) [ test ](../../input/tests/results/CMS74FHIRDentalCariesPrevention.txt) | [ CMS146FHIRApproTestPharyngitis ](../../input/cql/CMS146FHIRApproTestPharyngitis.cql) [ test ](../../input/tests/results/CMS146FHIRApproTestPharyngitis.txt) | [ CMS1028FHIRPCSevereOBComps ](../../input/cql/CMS1028FHIRPCSevereOBComps.cql) [ test ](../../input/tests/results/CMS1028FHIRPCSevereOBComps.txt) |
| [ CMS75FHIRChildrenDentalDecay ](../../input/cql/CMS75FHIRChildrenDentalDecay.cql) [ test ](../../input/tests/results/CMS75FHIRChildrenDentalDecay.txt) | [ CMS149FHIRDementiaCognitiveAssess ](../../input/cql/CMS149FHIRDementiaCognitiveAssess.cql) [ test ](../../input/tests/results/CMS149FHIRDementiaCognitiveAssess.txt) | [ CMS1056FHIRCTClinical ](../../input/cql/CMS1056FHIRCTClinical.cql) [ test ](../../input/tests/results/CMS1056FHIRCTClinical.txt) |
| [ CMS90FHIRFSAforHeartFailure ](../../input/cql/CMS90FHIRFSAforHeartFailure.cql) [ test ](../../input/tests/results/CMS90FHIRFSAforHeartFailure.txt) | [ CMS153FHIRChlamydiaScreening ](../../input/cql/CMS153FHIRChlamydiaScreening.cql) [ test ](../../input/tests/results/CMS153FHIRChlamydiaScreening.txt) | [ CMS1074FHIRCTIQR ](../../input/cql/CMS1074FHIRCTIQR.cql) [ test ](../../input/tests/results/CMS1074FHIRCTIQR.txt) |
| [ CMS104FHIRSTKDCAntithrombotic ](../../input/cql/CMS104FHIRSTKDCAntithrombotic.cql) [ test ](../../input/tests/results/CMS104FHIRSTKDCAntithrombotic.txt) | [ CMS154FHIRAppropriateTxforURI ](../../input/cql/CMS154FHIRAppropriateTxforURI.cql) [ test ](../../input/tests/results/CMS154FHIRAppropriateTxforURI.txt) | [ CMS1154ScreeningPrediabetesFHIR ](../../input/cql/CMS1154ScreeningPrediabetesFHIR.cql) [ test ](../../input/tests/results/CMS1154ScreeningPrediabetesFHIR.txt) |
| [ CMS108FHIRVTEProphylaxis ](../../input/cql/CMS108FHIRVTEProphylaxis.cql) [ test ](../../input/tests/results/CMS108FHIRVTEProphylaxis.txt) | [ CMS155FHIRWgtAssessCounseling ](../../input/cql/CMS155FHIRWgtAssessCounseling.cql) [ test ](../../input/tests/results/CMS155FHIRWgtAssessCounseling.txt) | [ CMS1157FHIRHIVRetention ](../../input/cql/CMS1157FHIRHIVRetention.cql) [ test ](../../input/tests/results/CMS1157FHIRHIVRetention.txt) |
| [ CMS117FHIRChildImmunStatus ](../../input/cql/CMS117FHIRChildImmunStatus.cql) [ test ](../../input/tests/results/CMS117FHIRChildImmunStatus.txt) | [ CMS157FHIRPainIntensityQuantified ](../../input/cql/CMS157FHIRPainIntensityQuantified.cql) [ test ](../../input/tests/results/CMS157FHIRPainIntensityQuantified.txt) | [ CMS1173FHIRDiagnosticDelayVTE ](../../input/cql/CMS1173FHIRDiagnosticDelayVTE.cql) [ test ](../../input/tests/results/CMS1173FHIRDiagnosticDelayVTE.txt) |
| [ CMS122FHIRDiabetesAssessGT9Pct ](../../input/cql/CMS122FHIRDiabetesAssessGT9Pct.cql) [ test ](../../input/tests/results/CMS122FHIRDiabetesAssessGT9Pct.txt) | [ CMS159FHIRDepRemissionat12Months ](../../input/cql/CMS159FHIRDepRemissionat12Months.cql) [ test ](../../input/tests/results/CMS159FHIRDepRemissionat12Months.txt) | [ CMS1188FHIRHIVSTITesting ](../../input/cql/CMS1188FHIRHIVSTITesting.cql) [ test ](../../input/tests/results/CMS1188FHIRHIVSTITesting.txt) |
| [ CMS124FHIRCervicalCancerScreen ](../../input/cql/CMS124FHIRCervicalCancerScreen.cql) [ test ](../../input/tests/results/CMS124FHIRCervicalCancerScreen.txt) | [ CMS177FHIRChildMDDSuicideAssmt ](../../input/cql/CMS177FHIRChildMDDSuicideAssmt.cql) [ test ](../../input/tests/results/CMS177FHIRChildMDDSuicideAssmt.txt) | [ CMS1206FHIRCTOQR ](../../input/cql/CMS1206FHIRCTOQR.cql) [ test ](../../input/tests/results/CMS1206FHIRCTOQR.txt) |
| [ CMS125FHIRBreastCancerScreen ](../../input/cql/CMS125FHIRBreastCancerScreen.cql) [ test ](../../input/tests/results/CMS125FHIRBreastCancerScreen.txt) | [ CMS190FHIRVTEProphylaxisICU ](../../input/cql/CMS190FHIRVTEProphylaxisICU.cql) [ test ](../../input/tests/results/CMS190FHIRVTEProphylaxisICU.txt) | [ CMS1218FHIRHHRF ](../../input/cql/CMS1218FHIRHHRF.cql) [ test ](../../input/tests/results/CMS1218FHIRHHRF.txt) |
| [ CMS128FHIRAntidepressantMgmt ](../../input/cql/CMS128FHIRAntidepressantMgmt.cql) [ test ](../../input/tests/results/CMS128FHIRAntidepressantMgmt.txt) | [ CMS314FHIRHIVViralSuppression ](../../input/cql/CMS314FHIRHIVViralSuppression.cql) [ test ](../../input/tests/results/CMS314FHIRHIVViralSuppression.txt) | [ CMS1244FHIRECATHOQR ](../../input/cql/CMS1244FHIRECATHOQR.cql) [ test ](../../input/tests/results/CMS1244FHIRECATHOQR.txt) |
| [ CMS129FHIRProstCaBoneScanUse ](../../input/cql/CMS129FHIRProstCaBoneScanUse.cql) [ test ](../../input/tests/results/CMS129FHIRProstCaBoneScanUse.txt) | [ CMS349FHIRHIVScreening ](../../input/cql/CMS349FHIRHIVScreening.cql) [ test ](../../input/tests/results/CMS349FHIRHIVScreening.txt) | [ CMS1264FHIRECATREHQR ](../../input/cql/CMS1264FHIRECATREHQR.cql) [ test ](../../input/tests/results/CMS1264FHIRECATREHQR.txt) |
| [ CMS130FHIRColorectalCancerScrn ](../../input/cql/CMS130FHIRColorectalCancerScrn.cql) [ test ](../../input/tests/results/CMS130FHIRColorectalCancerScrn.txt) | [ CMS506FHIRSafeUseofOpioids ](../../input/cql/CMS506FHIRSafeUseofOpioids.cql) [ test ](../../input/tests/results/CMS506FHIRSafeUseofOpioids.txt) | [ NHSNAcuteCareHospitalMonthlyInitialPopulation1 ](../../input/cql/NHSNAcuteCareHospitalMonthlyInitialPopulation1.cql) [ test ](../../input/tests/results/NHSNAcuteCareHospitalMonthlyInitialPopulation1.txt) |
| [ CMS131FHIRDiabetesEyeExam ](../../input/cql/CMS131FHIRDiabetesEyeExam.cql) [ test ](../../input/tests/results/CMS131FHIRDiabetesEyeExam.txt) | [ CMSFHIR529HybridHospitalWideReadmission ](../../input/cql/CMSFHIR529HybridHospitalWideReadmission.cql) [ test ](../../input/tests/results/CMSFHIR529HybridHospitalWideReadmission.txt) | [ NHSNGlycemicControlHypoglycemiaInitialPopulation ](../../input/cql/NHSNGlycemicControlHypoglycemiaInitialPopulation.cql) [ test ](../../input/tests/results/NHSNGlycemicControlHypoglycemiaInitialPopulation.txt) |


## Measures with Discrepancies (14 of 74)

_Every measure with a test case that doesn't match its fixture MeasureReport, or that matches only because the CQL works around an open issue. The `Issue` column names the issue that explains a test case: `I-XX (implementation)` is the measure steward's test data or expectation (see `defect-tracking/known-issues.md`), and `I-XX (workaround reverted)` means the case is failing again. Any other `I-XX` is a known issue, and the `Ticketed` column beside it says whether a ticket is owed: `N/A` for an implementation issue or a defect in our own content, fixture, or harness (nothing an upstream ticket tracks), `pending` for an engine/translator defect whose ticket is not filed yet, and a link to it once one is. A test case nothing resolves is counted as Unattributed._

| Measure | Total Test Cases | Missing Results | Missing Populations | Mismatched Test Cases | Workarounds | Unattributed |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| [CMS72FHIRSTKAntithromboticDay2](#cms72fhirstkantithromboticday2) | 158 | 0 | 0 | 0.63%   (1) | 0 | 0 |
| [CMS135FHIRACEIorARBorARNIforHF](#cms135fhiraceiorarborarniforhf) | 40 | 3 | 0 | 0.00%   (0) | 0 | 0 |
| [CMS144FHIRHFBetaBlockerForLVSD](#cms144fhirhfbetablockerforlvsd) | 48 | 48 | 0 | 0.00%   (0) | 0 | 48 |
| [CMS145FHIRCADBBlockerTPMIorLVSD](#cms145fhircadbblockertpmiorlvsd) | 106 | 0 | 0 | 0.94%   (1) | 10 | 0 |
| [CMS156FHIRHighRiskMedsElderly](#cms156fhirhighriskmedselderly) | 177 | 0 | 0 | 0.00%   (0) | 3 | 0 |
| [CMS165FHIRControllingHighBP](#cms165fhircontrollinghighbp) | 68 | 1 | 0 | 0.00%   (0) | 0 | 0 |
| [CMS0334FHIRPCCesareanBirth](#cms0334fhirpccesareanbirth) | 138 | 0 | 0 | 0.72%   (1) | 0 | 1 |
| [CMS347FHIRStatinPreventionTxCVD](#cms347fhirstatinpreventiontxcvd) | 752 | 0 | 0 | 0.13%   (1) | 0 | 0 |
| [CMS646FHIRIntravesicalBCGTherapy](#cms646fhirintravesicalbcgtherapy) | 38 | 1 | 0 | 7.89%   (3) | 0 | 3 |
| [CMS771FHIRUrinarySymptomScoreBPH](#cms771fhirurinarysymptomscorebph) | 31 | 0 | 0 | 22.58%   (7) | 0 | 7 |
| [CMSFHIR844HybridHospitalWideMortality](#cmsfhir844hybridhospitalwidemortality) | 10 | 0 | 0 | 20.00%   (2) | 0 | 2 |
| [CMS871FHIRHHHyper](#cms871fhirhhhyper) | 26 | 2 | 0 | 0.00%   (0) | 0 | 2 |
| [CMS996FHIRAptTxforSTEMI](#cms996fhirapttxforstemi) | 114 | 0 | 0 | 0.88%   (1) | 0 | 0 |
| [CMS1017FHIRHHFI](#cms1017fhirhhfi) | 65 | 0 | 0 | 3.08%   (2) | 0 | 2 |



#### CMS72FHIRSTKAntithromboticDay2
[ [cql] ](../../input/cql/CMS72FHIRSTKAntithromboticDay2.cql) [ [test results] ](../../input/tests/results/CMS72FHIRSTKAntithromboticDay2.txt)

Mismatched Test Cases (1 of 158)
| Test Case | Group | Population | Expected | Actual | Issue | Ticketed |
|---|---|---|:---:|:---:|---|---|
| [ be5c4068-2639-4b0c-bea3-5b7c80a6fe3b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/be5c4068-2639-4b0c-bea3-5b7c80a6fe3b/MeasureReport-ad329961-b67b-413b-a186-d6b269572c42.json) | Group_1 | Denominator Exception | 1 | 0 | I-75 (implementation) | N/A |


Issues

- **I-75** (`implementation`): PENDING SME CONFIRMATION. INR Observation valueQuantity has UCUM unit "0" and no code; UCUM reads "0" as a factor of zero, so INR 100 evaluates as 0 and the INR > 3.5 Denominator Exception can't fire. A dimensionless unit (unit/code "1") makes the case match.


#### CMS135FHIRACEIorARBorARNIforHF
[ [cql] ](../../input/cql/CMS135FHIRACEIorARBorARNIforHF.cql) [ [test results] ](../../input/tests/results/CMS135FHIRACEIorARBorARNIforHF.txt)

Missing Results (3 of 40 test cases)
| Test Case | Group | Issue | Ticketed |
| --- | --- | --- | --- |
| [ c095195c-8893-4bf1-aa7d-ad2bfd9bafa5 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/c095195c-8893-4bf1-aa7d-ad2bfd9bafa5/MeasureReport-f2d033da-6f32-46dc-86bc-69fdf82b1cfd.json) | Group_1 | I-26 | [1868](https://github.com/cqframework/clinical_quality_language/issues/1868) |
| [ cba5a449-1c45-4e11-ae0b-ba3974b410f7 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/cba5a449-1c45-4e11-ae0b-ba3974b410f7/MeasureReport-ae8c4b99-af76-4577-b66d-b1230ac09aa3.json) | Group_1 | I-26 | [1868](https://github.com/cqframework/clinical_quality_language/issues/1868) |
| [ ec508dbb-76f6-4878-b8a2-114ea8e82297 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/ec508dbb-76f6-4878-b8a2-114ea8e82297/MeasureReport-d1b704c8-7e95-4cd9-89e7-a8b90f925ce2.json) | Group_1 | I-26 | [1868](https://github.com/cqframework/clinical_quality_language/issues/1868) |


Issues

- **I-26** (`engine`, [cqframework/clinical_quality_language#1868](https://github.com/cqframework/clinical_quality_language/issues/1868)): MedicationRequest.medication is a Reference(Medication); the engine's code extraction throws before any define runs.


#### CMS144FHIRHFBetaBlockerForLVSD
[ [cql] ](../../input/cql/CMS144FHIRHFBetaBlockerForLVSD.cql) [ [test results] ](../../input/tests/results/CMS144FHIRHFBetaBlockerForLVSD.txt)

Missing Results (48 of 48 test cases)
| Test Case | Group |
| --- | --- |
| [ 00cd231f-4460-4d84-8e04-d8b0a04d8afd ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/00cd231f-4460-4d84-8e04-d8b0a04d8afd/MeasureReport-9df792f0-e780-4846-93b7-971ca7d3bdfa.json) | Group_1 |
| [ 07efd4bb-b45d-4bfd-aeb2-08de49742d91 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/07efd4bb-b45d-4bfd-aeb2-08de49742d91/MeasureReport-ad01867d-c2c7-4317-9925-deb909d156e6.json) | Group_1 |
| [ 0d2e80d0-d70a-4eba-9bfe-23d1dfeb546e ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/0d2e80d0-d70a-4eba-9bfe-23d1dfeb546e/MeasureReport-fcb45595-e142-4d88-babe-93f00fa3add9.json) | Group_1 |
| [ 1d166b9e-0d11-4247-a9a8-f610f36a74f2 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/1d166b9e-0d11-4247-a9a8-f610f36a74f2/MeasureReport-4ee219d2-84cf-43a0-a7ca-f25f51ebc1af.json) | Group_1 |
| [ 2333a56a-8346-4afc-93a2-0626a9a1ac2d ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/2333a56a-8346-4afc-93a2-0626a9a1ac2d/MeasureReport-e102f0b5-239f-48d2-82c2-63b6aec801be.json) | Group_1 |
| [ 23f874f8-e7e6-4bee-aa0c-140afae4ac16 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/23f874f8-e7e6-4bee-aa0c-140afae4ac16/MeasureReport-0810a4d7-223f-44ea-87f0-d337dc21a131.json) | Group_1 |
| [ 29effff8-4b34-43a2-8619-c69f02688f41 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/29effff8-4b34-43a2-8619-c69f02688f41/MeasureReport-01fac9a2-fe13-482f-b9c7-991bd6a016b9.json) | Group_1 |
| [ 2f695158-3d0a-42ac-b747-dc355ed01e94 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/2f695158-3d0a-42ac-b747-dc355ed01e94/MeasureReport-f505e02e-3d3d-485b-ba13-84768ad37c0b.json) | Group_1 |
| [ 37e926e5-d9d4-4066-b991-e9b3d58fd8ca ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/37e926e5-d9d4-4066-b991-e9b3d58fd8ca/MeasureReport-61337141-384e-41f2-8a8a-07d0be259c45.json) | Group_1 |
| [ 426369f7-d9ec-4923-8628-465229c5f958 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/426369f7-d9ec-4923-8628-465229c5f958/MeasureReport-79d2fc58-71b1-49a3-a119-25127666e470.json) | Group_1 |
| [ 442a40d5-a18e-434b-ad30-7760e73adac2 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/442a40d5-a18e-434b-ad30-7760e73adac2/MeasureReport-bd903d29-d5a7-4cae-ab39-9958c9ac5948.json) | Group_1 |
| [ 4d13ba5c-740b-4263-ae55-3a40a3f73846 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/4d13ba5c-740b-4263-ae55-3a40a3f73846/MeasureReport-0ea06957-bab5-4b8c-9962-3488709beda3.json) | Group_1 |
| [ 559a2377-0faa-44f8-bf81-bbf3df4d3edf ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/559a2377-0faa-44f8-bf81-bbf3df4d3edf/MeasureReport-0b602d26-67bf-46f2-a563-c39d9e977516.json) | Group_1 |
| [ 5914457a-816c-43fe-86da-07fcc42ebe61 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/5914457a-816c-43fe-86da-07fcc42ebe61/MeasureReport-b45fc8fb-0d08-44ad-a1e6-e9afab020617.json) | Group_1 |
| [ 5adba588-db7d-4921-be95-4fb872be59de ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/5adba588-db7d-4921-be95-4fb872be59de/MeasureReport-9ec25590-4573-47e5-93af-35493d09a26e.json) | Group_1 |
| [ 5e965079-8e53-4011-b4e8-cb3829257450 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/5e965079-8e53-4011-b4e8-cb3829257450/MeasureReport-2e52a172-8401-4ed8-bdae-ef772adfc64f.json) | Group_1 |
| [ 66ef197c-3807-41cf-acd0-f188836e2762 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/66ef197c-3807-41cf-acd0-f188836e2762/MeasureReport-bc6db55e-c15e-434a-8312-277369869d3b.json) | Group_1 |
| [ 67779bc6-07ee-42cf-8ca7-e71302915dba ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/67779bc6-07ee-42cf-8ca7-e71302915dba/MeasureReport-5b182aca-ad2a-4651-ba6b-df02e001ec36.json) | Group_1 |
| [ 6dc1b14d-143b-4657-a96a-f743f438d0dd ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/6dc1b14d-143b-4657-a96a-f743f438d0dd/MeasureReport-49965d92-0c98-40ef-99c1-66d72db952bf.json) | Group_1 |
| [ 704612ca-2d86-4f6b-88d6-d8f3e9cb031f ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/704612ca-2d86-4f6b-88d6-d8f3e9cb031f/MeasureReport-1a5c4eb1-964f-4a03-a72c-ed4375f32513.json) | Group_1 |
| [ 71dd3794-ce93-473b-8d90-9d68c0dac6d8 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/71dd3794-ce93-473b-8d90-9d68c0dac6d8/MeasureReport-b5771105-992d-417e-a984-1685c2721deb.json) | Group_1 |
| [ 729c5ee9-d55f-47f2-aee1-d8d9fb25fe38 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/729c5ee9-d55f-47f2-aee1-d8d9fb25fe38/MeasureReport-747cfb68-ca63-44b5-a3f0-0e20f37c885e.json) | Group_1 |
| [ 798bb898-32dc-4f83-b700-792efe0a65db ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/798bb898-32dc-4f83-b700-792efe0a65db/MeasureReport-5302a9c9-3b74-4203-ae9c-be45d8b04aa0.json) | Group_1 |
| [ 7b8885c5-ad14-4361-9755-c76a6e3b8530 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/7b8885c5-ad14-4361-9755-c76a6e3b8530/MeasureReport-7e421d2a-1ee4-4c56-a454-815983c21106.json) | Group_1 |
| [ 7fc4e721-68e5-4ed8-ab41-b5fb0d1ff43a ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/7fc4e721-68e5-4ed8-ab41-b5fb0d1ff43a/MeasureReport-f975b8f2-53b3-4c9a-bec0-614425494f9d.json) | Group_1 |
| [ 84b7f540-8e30-41e8-9691-f9ccd6924e0d ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/84b7f540-8e30-41e8-9691-f9ccd6924e0d/MeasureReport-eb35446e-3bd9-446c-9453-8a8db6d2d30a.json) | Group_1 |
| [ 8c717fb3-e820-44c6-b4ef-e9a7f46cd85b ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/8c717fb3-e820-44c6-b4ef-e9a7f46cd85b/MeasureReport-5c36cccb-2bf5-4737-9a88-a531e21c6199.json) | Group_1 |
| [ 9038ae82-f60c-467a-94b4-76e644e707e3 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/9038ae82-f60c-467a-94b4-76e644e707e3/MeasureReport-c404b532-98ab-470c-997d-24f54726cbb8.json) | Group_1 |
| [ 9d197057-c39b-4799-ab3a-b81df7a6199b ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/9d197057-c39b-4799-ab3a-b81df7a6199b/MeasureReport-cedda318-4f86-4723-84d9-d8e9bdaf6ab1.json) | Group_1 |
| [ a245856e-f7bf-46a8-8742-f84d87c6d5ef ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/a245856e-f7bf-46a8-8742-f84d87c6d5ef/MeasureReport-35b361bb-7cd7-48a4-926f-fa8444c632f7.json) | Group_1 |
| [ a45954f4-f511-4409-8d7a-6ad606d1e5e2 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/a45954f4-f511-4409-8d7a-6ad606d1e5e2/MeasureReport-65ca4baa-3631-4cc3-9f4e-9cfe4ef0ccc5.json) | Group_1 |
| [ a6c8dee0-0110-4814-ae0d-37a2a03b2f72 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/a6c8dee0-0110-4814-ae0d-37a2a03b2f72/MeasureReport-6ceb833c-217f-4521-8f64-6bb8eadd26a9.json) | Group_1 |
| [ b3faae0f-8dba-49d9-8dd9-94bc8e532753 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/b3faae0f-8dba-49d9-8dd9-94bc8e532753/MeasureReport-7bff689b-1d3f-4758-9394-2604700a8241.json) | Group_1 |
| [ b6995e3c-319c-45f1-8aca-4db81b6730f6 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/b6995e3c-319c-45f1-8aca-4db81b6730f6/MeasureReport-0c508e3f-3eb3-4453-91a3-06f5e4cf6dcb.json) | Group_1 |
| [ b7060d53-0742-484c-a9b9-3ff63fbb8d5c ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/b7060d53-0742-484c-a9b9-3ff63fbb8d5c/MeasureReport-0226014e-9bc0-4eca-8c52-ad0aacb6df42.json) | Group_1 |
| [ b74609f2-2fe7-4969-8b2b-215e7c21dfce ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/b74609f2-2fe7-4969-8b2b-215e7c21dfce/MeasureReport-3e793879-5a1a-4ef5-8dac-fd7ae13c7e5a.json) | Group_1 |
| [ b7c0a554-4fab-41c9-9bd6-c9c67230da52 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/b7c0a554-4fab-41c9-9bd6-c9c67230da52/MeasureReport-83ba0229-7eb8-4759-bd51-9bc9481fc6bc.json) | Group_1 |
| [ bc710459-b3f7-4a3b-a640-3bf541fea336 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/bc710459-b3f7-4a3b-a640-3bf541fea336/MeasureReport-33071b35-61eb-4e5f-9f14-b083228c835e.json) | Group_1 |
| [ c58e088d-0b16-4e3d-aaad-3577cdec75d3 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/c58e088d-0b16-4e3d-aaad-3577cdec75d3/MeasureReport-c4facb39-df83-41eb-aeb5-5f52c9d03e91.json) | Group_1 |
| [ c786457e-4e5b-4c6a-b05b-1600045a6243 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/c786457e-4e5b-4c6a-b05b-1600045a6243/MeasureReport-9275d2b1-db94-4e55-aeb2-934ae23e70bb.json) | Group_1 |
| [ cdcbc425-203d-4ebb-81f3-8d5f50253eed ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/cdcbc425-203d-4ebb-81f3-8d5f50253eed/MeasureReport-e554914e-b819-479e-8616-995fc0b6a6a4.json) | Group_1 |
| [ ce22375e-abe0-4fd5-a4ad-07d8a6056045 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/ce22375e-abe0-4fd5-a4ad-07d8a6056045/MeasureReport-ccef4bb4-8add-4cb7-bdb9-d760e02e53fc.json) | Group_1 |
| [ de470536-41fa-4b2f-8355-2790e90ece22 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/de470536-41fa-4b2f-8355-2790e90ece22/MeasureReport-9718406e-d086-4847-b819-7849b26c5ebe.json) | Group_1 |
| [ ec0aa07d-f1b1-4c5d-a9f3-e5bbdd77cdfa ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/ec0aa07d-f1b1-4c5d-a9f3-e5bbdd77cdfa/MeasureReport-5c161b17-1c7f-4ec0-8541-3529c0dcbefd.json) | Group_1 |
| [ f00cb244-a7cc-408f-ab47-7c3ac82469b1 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/f00cb244-a7cc-408f-ab47-7c3ac82469b1/MeasureReport-f338c93f-ac92-4c5c-ac29-b4d8c1a2df24.json) | Group_1 |
| [ f5c049d5-d1b8-413b-bdad-ff54a5d20df3 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/f5c049d5-d1b8-413b-bdad-ff54a5d20df3/MeasureReport-c39329c5-eb15-4dbc-9aca-6153118151af.json) | Group_1 |
| [ f6cdc95d-7796-4dad-a5da-d2e1fddc8687 ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/f6cdc95d-7796-4dad-a5da-d2e1fddc8687/MeasureReport-8232b929-8129-4c21-ace2-f3080629f7c3.json) | Group_1 |
| [ faf3e31d-533a-484f-9b9d-4ded0c32c84f ](../.././input/tests/measure/CMS144FHIRHFBetaBlockerForLVSD/faf3e31d-533a-484f-9b9d-4ded0c32c84f/MeasureReport-d06939c8-bfaa-45fd-8fe5-5c2e8b0f71b0.json) | Group_1 |



#### CMS145FHIRCADBBlockerTPMIorLVSD
[ [cql] ](../../input/cql/CMS145FHIRCADBBlockerTPMIorLVSD.cql) [ [test results] ](../../input/tests/results/CMS145FHIRCADBBlockerTPMIorLVSD.txt)

Mismatched Test Cases (1 of 106)
| Test Case | Group | Population | Expected | Actual | Issue | Ticketed |
|---|---|---|:---:|:---:|---|---|
| [ 1f70822b-c513-4c3a-8162-49f0bb9c914b ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/1f70822b-c513-4c3a-8162-49f0bb9c914b/MeasureReport-9b3577fa-355c-409d-8d3f-21e9720fb889.json) | Group_2 | Denominator Exception | 0 | 1 | I-72 (implementation) | N/A |


Workarounds (10 of 106 test cases)
_These cases match their fixture only because the measure's CQL works around an open issue. Removing the workaround makes them fail again, so they count as neither a pass nor a failure._
_Beta-blocker allergy/intolerance diagnosis not counted as a Denominator Exception. The five cases fail against the original CQL and pass only because the workaround is applied._

| Test Case | Group | Issue | Ticketed |
| --- | --- | --- | --- |
| [ 4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e/MeasureReport-e77c61ff-cc3a-402c-9752-7a97a6727a39.json) | Group_1 | I-78 (workaround applied) | [1872](https://github.com/cqframework/clinical_quality_language/issues/1872) |
| [ 4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e/MeasureReport-e77c61ff-cc3a-402c-9752-7a97a6727a39.json) | Group_2 | I-78 (workaround applied) | [1872](https://github.com/cqframework/clinical_quality_language/issues/1872) |
| [ 5fd0d626-e9c5-4e6c-a10d-1a1183fa7702 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/5fd0d626-e9c5-4e6c-a10d-1a1183fa7702/MeasureReport-ce1b8712-b9dd-48e2-adf4-554ed641bee5.json) | Group_1 | I-78 (workaround applied) | [1872](https://github.com/cqframework/clinical_quality_language/issues/1872) |
| [ 5fd0d626-e9c5-4e6c-a10d-1a1183fa7702 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/5fd0d626-e9c5-4e6c-a10d-1a1183fa7702/MeasureReport-ce1b8712-b9dd-48e2-adf4-554ed641bee5.json) | Group_2 | I-78 (workaround applied) | [1872](https://github.com/cqframework/clinical_quality_language/issues/1872) |
| [ 61306767-0e74-44b8-ac06-1339c3783355 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/61306767-0e74-44b8-ac06-1339c3783355/MeasureReport-6ea40199-5a45-4c8d-8a2b-c08bf93ebd8a.json) | Group_1 | I-78 (workaround applied) | [1872](https://github.com/cqframework/clinical_quality_language/issues/1872) |
| [ 61306767-0e74-44b8-ac06-1339c3783355 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/61306767-0e74-44b8-ac06-1339c3783355/MeasureReport-6ea40199-5a45-4c8d-8a2b-c08bf93ebd8a.json) | Group_2 | I-78 (workaround applied) | [1872](https://github.com/cqframework/clinical_quality_language/issues/1872) |
| [ b65680a0-9768-4ce4-b08d-972fcd84e28e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/b65680a0-9768-4ce4-b08d-972fcd84e28e/MeasureReport-b5ebd0a9-a2de-4b31-b0d9-588888e95872.json) | Group_1 | I-78 (workaround applied) | [1872](https://github.com/cqframework/clinical_quality_language/issues/1872) |
| [ b65680a0-9768-4ce4-b08d-972fcd84e28e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/b65680a0-9768-4ce4-b08d-972fcd84e28e/MeasureReport-b5ebd0a9-a2de-4b31-b0d9-588888e95872.json) | Group_2 | I-78 (workaround applied) | [1872](https://github.com/cqframework/clinical_quality_language/issues/1872) |
| [ fd5fb311-a466-4c59-966d-48fa7aa88931 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/fd5fb311-a466-4c59-966d-48fa7aa88931/MeasureReport-05ffed3e-5604-40eb-bcf8-99cacecc26c0.json) | Group_1 | I-78 (workaround applied) | [1872](https://github.com/cqframework/clinical_quality_language/issues/1872) |
| [ fd5fb311-a466-4c59-966d-48fa7aa88931 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/fd5fb311-a466-4c59-966d-48fa7aa88931/MeasureReport-05ffed3e-5604-40eb-bcf8-99cacecc26c0.json) | Group_2 | I-78 (workaround applied) | [1872](https://github.com/cqframework/clinical_quality_language/issues/1872) |


Issues

- **I-72** (`implementation`): Test expects no Group_2 Denominator Exception because the patient-reason beta-blocker not-ordered falls on the first visit, but the CQL accepts the reason at any qualifying visit, and both visits qualify. Twin case b19af44d (reason at the last visit) expects 1. The steward must change the expectation or the CQL.
- **I-78** (`translator`, **workaround applied**): Beta-blocker allergy/intolerance diagnosis not counted as a Denominator Exception. The five cases fail against the original CQL and pass only because the workaround is applied.


#### CMS156FHIRHighRiskMedsElderly
[ [cql] ](../../input/cql/CMS156FHIRHighRiskMedsElderly.cql) [ [test results] ](../../input/tests/results/CMS156FHIRHighRiskMedsElderly.txt)

Workarounds (3 of 177 test cases)
_These cases match their fixture only because the measure's CQL works around an open issue. Removing the workaround makes them fail again, so they count as neither a pass nor a failure._
_Worked around in CMS156 averageDailyDose(); listed for its upstream ticket._

| Test Case | Group | Issue | Ticketed |
| --- | --- | --- | --- |
| [ 4aa75d19-ac8b-49b0-a686-429fbc033d77 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/MeasureReport-139cc56e-5ffb-46ca-89ce-accd0bb642ab.json) | Group_1 | I-24 (workaround applied) | [1690](https://github.com/cqframework/clinical_quality_language/issues/1690) |
| [ 4aa75d19-ac8b-49b0-a686-429fbc033d77 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/MeasureReport-139cc56e-5ffb-46ca-89ce-accd0bb642ab.json) | Group_2 | I-24 (workaround applied) | [1690](https://github.com/cqframework/clinical_quality_language/issues/1690) |
| [ 4aa75d19-ac8b-49b0-a686-429fbc033d77 ](../.././input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/MeasureReport-139cc56e-5ffb-46ca-89ce-accd0bb642ab.json) | Group_3 | I-24 (workaround applied) | [1690](https://github.com/cqframework/clinical_quality_language/issues/1690) |


Issues

- **I-24** (`engine`, **workaround applied**): Worked around in CMS156 averageDailyDose(); listed for its upstream ticket.


#### CMS165FHIRControllingHighBP
[ [cql] ](../../input/cql/CMS165FHIRControllingHighBP.cql) [ [test results] ](../../input/tests/results/CMS165FHIRControllingHighBP.txt)

Missing Results (1 of 68 test cases)
| Test Case | Group | Issue | Ticketed |
| --- | --- | --- | --- |
| [ 45e01fed-56bb-483d-a860-af3d566bda11 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/45e01fed-56bb-483d-a860-af3d566bda11/MeasureReport-02991ca7-859d-422d-8849-655760f8e10a.json) | Group_1 | I-26 | [1868](https://github.com/cqframework/clinical_quality_language/issues/1868) |


Issues

- **I-26** (`engine`, [cqframework/clinical_quality_language#1868](https://github.com/cqframework/clinical_quality_language/issues/1868)): MedicationRequest.medication is a Reference(Medication); the engine's code extraction throws before any define runs.


#### CMS0334FHIRPCCesareanBirth
[ [cql] ](../../input/cql/CMS0334FHIRPCCesareanBirth.cql) [ [test results] ](../../input/tests/results/CMS0334FHIRPCCesareanBirth.txt)

Mismatched Test Cases (1 of 138)
| Test Case | Group | Population | Expected | Actual | Issue | Ticketed |
|---|---|---|:---:|:---:|---|---|
| [ c58acff5-248b-49c9-b18d-69e4a84a08d9 ](../.././input/tests/measure/CMS0334FHIRPCCesareanBirth/c58acff5-248b-49c9-b18d-69e4a84a08d9/MeasureReport-920b0c2e-1f1f-42d3-ab1f-1d7b12fa4bd0.json) | Group_1 | Denominator<br>Denominator Exclusion | 1<br>1 | 0<br>0 | I-12 | N/A |


Issues

- **I-12** (`fixture`): The case's Encounter points at its delivery diagnosis with `reasonReference: "delivery-of-singleton-0997"` — not a valid `Type/id` reference, and no Condition in the case has that id (the Condition was re-identified when the case was imported). `CQMCommon.encounterDiagnosis()` therefore finds no diagnosis, `Encounter With Singleton Delivery` is empty, and both Denominator and Denominator Exclusion stay 0 against an expected 1. This is test data, not a CQL or engine defect: five companion CMS0334 cases pass on the same logic, and the source MADiE bundle for this case names the diagnosis the same way but with a resolvable `Condition/…` reference. The fixtures are intentionally left unchanged, so this case remains a known, unfixed test-data defect.


#### CMS347FHIRStatinPreventionTxCVD
[ [cql] ](../../input/cql/CMS347FHIRStatinPreventionTxCVD.cql) [ [test results] ](../../input/tests/results/CMS347FHIRStatinPreventionTxCVD.txt)

Mismatched Test Cases (1 of 752)
| Test Case | Group | Population | Expected | Actual | Issue | Ticketed |
|---|---|---|:---:|:---:|---|---|
| [ 1ba7b147-b701-424c-bade-4e8270547030 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1ba7b147-b701-424c-bade-4e8270547030/MeasureReport-2278c703-994b-4b13-8e3b-c726ba6b8530.json) | Group_4 | Denominator Exception | 0 | 1 | I-67 (implementation) | N/A |


Issues

- **I-67** (`implementation`): ESRD Condition uses a start-only abatementPeriod to mean resolved 2025-12-31; the steward should use abatementDateTime or add an end.


#### CMS646FHIRIntravesicalBCGTherapy
[ [cql] ](../../input/cql/CMS646FHIRIntravesicalBCGTherapy.cql) [ [test results] ](../../input/tests/results/CMS646FHIRIntravesicalBCGTherapy.txt)

Missing Results (1 of 38 test cases)
| Test Case | Group | Issue | Ticketed |
| --- | --- | --- | --- |
| [ 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/342d2bec-0acc-43e5-aaf7-3c9a65b09f91/MeasureReport-12cd358b-deb0-4130-a045-4c6b61e110c0.json) | Group_1 | I-33 (workaround reverted) | pending |


Mismatched Test Cases (3 of 38)
| Test Case | Group | Population | Expected | Actual | Issue | Ticketed |
|---|---|---|:---:|:---:|---|---|
| [ 10cec7db-41ae-49ad-b883-022f19d92a8b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/10cec7db-41ae-49ad-b883-022f19d92a8b/MeasureReport-b8b4961d-450b-4980-ac8f-95500c6393d4.json) | Group_1 | Denominator Exclusion | 0 | 1 | I-15 | N/A |
| [ 6e9a1974-7334-4c41-b8dd-8413ae0caa29 ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/6e9a1974-7334-4c41-b8dd-8413ae0caa29/MeasureReport-82d2900a-e69c-4b3c-a6f9-c173759b3f1d.json) | Group_1 | Numerator | 0 | 1 | I-79 | N/A |
| [ ab48e0c0-6543-4537-8f00-bfcdcba7a81b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/ab48e0c0-6543-4537-8f00-bfcdcba7a81b/MeasureReport-ea6cfef5-54d2-4d6d-a7aa-48cf8e749eaf.json) | Group_1 | Numerator | 0 | 1 | I-70 (implementation) | N/A |


Issues

- **I-15** (`content`): Fixture MeasureReport expects a value the resources present can't produce.
- **I-33** (`engine`, **workaround applied**): "Values FHIR.dateTime and FHIR.dateTime are not comparable" aborts the case. The FHIRHelpers.ToDateTime() workaround is applied to CMS156 only.
- **I-70** (`implementation`): Test expects Numerator 0 because BCG was given before staging results came back, but the CQL measures from the start of the staging Procedure and the BCG dose starts at the same moment (2026-04-02T08:00), so the case can't fail the Numerator.
- **I-79** (`content`): Correcting the wrong-patient reference (I-11 sweep) made the BCG administration visible to the measure, and the CQL counts it: an in-progress BCG recorded at the same instant as the staging procedure satisfies the measure's "6 months or less after day of staging" rule, while the test case is written to expect no BCG counted. Open question is whether the CQL rule, the fixture timestamps, or the expected value is wrong — the case itself says the BCG "should fail".


#### CMS771FHIRUrinarySymptomScoreBPH
[ [cql] ](../../input/cql/CMS771FHIRUrinarySymptomScoreBPH.cql) [ [test results] ](../../input/tests/results/CMS771FHIRUrinarySymptomScoreBPH.txt)

Mismatched Test Cases (7 of 31)
| Test Case | Group | Population | Expected | Actual | Issue | Ticketed |
|---|---|---|:---:|:---:|---|---|
| [ 051c5977-9f2c-4e8b-8e02-ac3ec0c718d6 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/051c5977-9f2c-4e8b-8e02-ac3ec0c718d6/MeasureReport-13a299d2-1f32-41d7-b226-7380902e41b7.json) | Group_1 | Denominator | 1 | 0 | I-65 | N/A |
| [ 3ab3ac1d-9b5e-4087-8862-dcb2562fb90f ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/3ab3ac1d-9b5e-4087-8862-dcb2562fb90f/MeasureReport-47dae27e-89cf-4ee5-8c8b-bf1e44997d07.json) | Group_1 | Denominator | 1 | 0 | I-65 | N/A |
| [ 4c234ec0-3f89-4d55-b767-219d1130f634 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/4c234ec0-3f89-4d55-b767-219d1130f634/MeasureReport-47a91ced-cb5f-44c0-9417-e8efa33a4b08.json) | Group_1 | Numerator | 1 | 0 | I-65 | N/A |
| [ 9be591a0-517b-4be2-b652-a29be0c75c15 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/9be591a0-517b-4be2-b652-a29be0c75c15/MeasureReport-004d2ae6-6c2e-49f8-bf07-26cada3bbaf3.json) | Group_1 | Numerator | 1 | 0 | I-65 | N/A |
| [ bc79e5bc-237e-44be-b5fc-c5c4efb50286 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/bc79e5bc-237e-44be-b5fc-c5c4efb50286/MeasureReport-621196a7-ca5f-4408-8508-851332413956.json) | Group_1 | Numerator | 1 | 0 | I-65 | N/A |
| [ bf0f8968-c2c0-4416-88db-11ea3e3da968 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/bf0f8968-c2c0-4416-88db-11ea3e3da968/MeasureReport-bcce208a-3ff4-4c82-9d49-c0b64ccb9138.json) | Group_1 | Numerator | 1 | 0 | I-65 | N/A |
| [ e90d90a7-3071-44de-8089-ad7b6f5f3e5d ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/e90d90a7-3071-44de-8089-ad7b6f5f3e5d/MeasureReport-9ef2db11-d78a-49af-a2ac-6536fac264a1.json) | Group_1 | Numerator | 1 | 0 | I-65 | N/A |


Issues

- **I-65** (`content`): Filed under I-65, but the failing cell is Denominator or Numerator, not Denominator Exception. Needs re-triage.


#### CMSFHIR844HybridHospitalWideMortality
[ [cql] ](../../input/cql/CMSFHIR844HybridHospitalWideMortality.cql) [ [test results] ](../../input/tests/results/CMSFHIR844HybridHospitalWideMortality.txt)

Mismatched Test Cases (2 of 10)
| Test Case | Group | Population | Expected | Actual | Issue | Ticketed |
|---|---|---|:---:|:---:|---|---|
| [ 6f22a06f-7186-4db1-9310-4f907dc49ff3 ](../.././input/tests/measure/CMSFHIR844HybridHospitalWideMortality/6f22a06f-7186-4db1-9310-4f907dc49ff3/MeasureReport-a02a261f-1274-4f8b-b1f3-5496f7885cbe.json) | Group_1 | Initial Population | 1 | 0 | I-14 | N/A |
| [ af1b9448-3e7a-4b7f-8934-15bb63258b75 ](../.././input/tests/measure/CMSFHIR844HybridHospitalWideMortality/af1b9448-3e7a-4b7f-8934-15bb63258b75/MeasureReport-7afefb0f-3075-4fb8-8d56-474ba1112c38.json) | Group_1 | Initial Population | 2 | 1 | I-14 | N/A |


Issues

- **I-14** (`content`): Fixture MeasureReport hand-authors the Initial Population; the test data supports the computed value.


#### CMS871FHIRHHHyper
[ [cql] ](../../input/cql/CMS871FHIRHHHyper.cql) [ [test results] ](../../input/tests/results/CMS871FHIRHHHyper.txt)

Missing Results (2 of 26 test cases)
| Test Case | Group | Issue | Ticketed |
| --- | --- | --- | --- |
| [ 98533ccd-24ee-41b3-aab2-ef6cbf89e00d ](../.././input/tests/measure/CMS871FHIRHHHyper/98533ccd-24ee-41b3-aab2-ef6cbf89e00d/MeasureReport-82c8805c-b129-4009-8533-1ed12cf5d18f.json) | Group_1 | I-63 | pending |
| [ fd579f44-757b-4c98-9b09-27b17b935650 ](../.././input/tests/measure/CMS871FHIRHHHyper/fd579f44-757b-4c98-9b09-27b17b935650/MeasureReport-22df2e2a-404d-4ab0-831a-e2ab043197a2.json) | Group_1 | I-63 | pending |


Issues

- **I-63** (`engine`): "Invalid Interval - the ending boundary (0) must be greater than or equal to the starting boundary (1)." aborts the case; root cause not yet traced.


#### CMS996FHIRAptTxforSTEMI
[ [cql] ](../../input/cql/CMS996FHIRAptTxforSTEMI.cql) [ [test results] ](../../input/tests/results/CMS996FHIRAptTxforSTEMI.txt)

Mismatched Test Cases (1 of 114)
| Test Case | Group | Population | Expected | Actual | Issue | Ticketed |
|---|---|---|:---:|:---:|---|---|
| [ f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847/MeasureReport-f2a63299-25e1-4d91-8e5c-1bdf3b60e9cb.json) | Group_1 | Denominator Exclusion | 0 | 1 | I-71 (implementation) | N/A |


Issues

- **I-71** (`implementation`): Active thrombolytic allergy uses onsetPeriod.end to mean it ended before the ED visit; with no abatement, prevalenceInterval() correctly runs to end of time and overlaps the encounter. The steward should mark it resolved with an allergyintolerance-abatement extension, or expect Denominator Exclusion 1.


#### CMS1017FHIRHHFI
[ [cql] ](../../input/cql/CMS1017FHIRHHFI.cql) [ [test results] ](../../input/tests/results/CMS1017FHIRHHFI.txt)

Mismatched Test Cases (2 of 65)
| Test Case | Group | Population | Expected | Actual | Issue | Ticketed |
|---|---|---|:---:|:---:|---|---|
| [ 0dfafc1a-cf94-4ca1-becf-c1b843896810 ](../.././input/tests/measure/CMS1017FHIRHHFI/0dfafc1a-cf94-4ca1-becf-c1b843896810/MeasureReport-cd491c44-6ed1-483f-8775-516f92b9c16d.json) | Group_1 | Numerator Exclusion | 0 | 1 | I-05 | N/A |
| [ 5ff2713d-ca89-42ae-91bb-cba3e1d9a487 ](../.././input/tests/measure/CMS1017FHIRHHFI/5ff2713d-ca89-42ae-91bb-cba3e1d9a487/MeasureReport-74f8c3e3-881b-4ba8-bfdb-ceef555ed020.json) | Group_1 | Numerator Exclusion | 0 | 1 | I-05 | N/A |


Issues

- **I-05** (`content`): Numerator Exclusion 0 → 1. Cause not yet documented; needs re-triage.



_Known issues are listed in `defect-tracking/known-issues.md`; the test cases they explain, and their tickets, in `./scripts/comparison/known_issues.csv` and `./scripts/comparison/implementation_issues.csv`._
