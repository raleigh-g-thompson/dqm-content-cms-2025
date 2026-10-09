# Discrepancy Report
| Details | Value |
| --- | --- |
| Generated | 2026-10-09 09:37:33.317930 |
| Total Measures | 74 |
| Total Test Cases | 3964 |
| Measures with Discrepancies | 18 |
| Pass Count | 3911 (98.66%) |
| Fail Count | 53 (1.34%) |
| CMS Fail / QI-Core OK | 0 test cases (0 measures) |
| Implementation Issues (not parity) | 4 test cases (4 measures) |


| Discrepancy Summary | Measure Count | Test Case Count |
|---|:---:|:---:|
| Missing Results | 4 | 7 |
| Missing Populations | 0 | 0 |
| Mismatched Test Cases | 16 | 47 |



_Note: Measures can have multiple discrepancies, so the Measures with Discrepancies count may not match the summary counts._

## Populations Excluded from Automated Scoring (CQFM Aggregation)

_These populations require invoking a parameterized CQL `measure-observation` function once per member of a `cqfm-criteriaReference` population and then applying `cqfm-aggregateMethod` (Sum/Count/Average) -- a computation this harness's CQL-execution path cannot perform (no scriptable/batch CQL runner; see `defect-tracking/known-issues.md`, I-62). They are intentionally excluded from pass/fail scoring rather than reported as FAIL or silently read as 0. Every other population on these measures (Initial Population, Denominator, Numerator, exclusions, etc.) is unaffected and scored normally. Use the `mcp-cql-debug` per-member probe method documented in I-62 to manually verify any of the expected values below._

| Measure | Excluded Populations |
| --- | --- |
| CMS871FHIRHHHyper | Group_1:Denominator Observation, Group_1:Measure Observation, Group_1:Numerator Observation |
| CMS986FHIRMalnutritionScore | Group_1:Measure Observation, Group_2:Measure Observation, Group_3:Measure Observation, Group_4:Measure Observation, Group_5:Measure Observation, Group_6:Measure Observation |
| CMS1017FHIRHHFI | Group_1:Denominator Observation, Group_1:Measure Observation, Group_1:Numerator Observation |

_2085 cells excluded across 3 measures._

## QI-Core Parity
_Every test case scored below, including the ones UQC passes. QI-Core results come from `./scripts/comparison/qicore_actual_results.csv`; verdicts are scored against the same fixture MeasureReport expectations used for the UQC columns. A case where the two engines disagree is not by itself evidence that either is wrong -- read the cross-tab first, then the two asymmetric buckets._

_**Unit of account.** A test case here is one (measure, GUID, group) triple, the same unit the per-measure tables use, so these counts total 5933 rather than the header's 3964 test cases -- that one counts each patient once per measure. On measures with several groups per patient the two differ; reconcile by group, not by totals._

CMS and QI-Core disagree on 764 test cases (1597 of 23722 population cells).

### Verdicts by Engine

| CMS verdict \ QI-Core verdict | PASS | FAIL | MISSING |
|---|:---:|:---:|:---:|
| PASS | 5141 | 732 | 6 |
| FAIL | 3 | 44 | 0 |
| MISSING | 0 | 1 | 6 |


_The two asymmetric buckets point in opposite directions, and only one of them is work for this repo. `FAIL`/`PASS` (0 cases, 0 measures) is a migration regression to fix here; it is broken out per measure and per test case below, and per measure by the `CMS Fail / QI-Core OK` column further down. `PASS`/`FAIL` (732 cases) plus `PASS`/`MISSING` (6 cases) is the reverse: those cases match their fixture expectations here and not on QI-Core. Both repositories agree on the expected values for them, so the difference is engine output rather than content and QI-Core's copy of the results is the stale side -- catalogued as I-01. Counted, not listed: it is a refresh signal for the QI-Core baseline, not CMS work. `MISSING` means that engine emitted no result for the case at all and counts as non-PASS in both directions._

_The cross-tab also counts 4 cases that the two buckets above leave out, because the difference comes from the measure steward's test data, not either engine. They are listed under Implementation Issues below._

### CMS Fails, QI-Core Reproduces (0 test cases, 0 measures)

_No test case falls in this bucket._


### Implementation Issues, Not Parity Defects (4 test cases, 4 measures)

_The engines score these cases differently only because the measure steward's test data misuses FHIR or CQL semantics (class `implementation` in `defect-tracking/known-issues.md`). Neither side owes a fix, so they are left out of the two asymmetric buckets and the `CMS Fail / QI-Core OK` counts. PASS/FAIL scoring is unchanged: a case that fails here still counts as a failure above. Listed in `./scripts/comparison/implementation_issues.csv`._

| Measure | Test Cases | Population Cells |
|---|:---:|:---:|
| [CMS145FHIRCADBBlockerTPMIorLVSD](#cms145fhircadbblockertpmiorlvsd) | 1 | 1 |
| [CMS347FHIRStatinPreventionTxCVD](#cms347fhirstatinpreventiontxcvd) | 1 | 1 |
| [CMS646FHIRIntravesicalBCGTherapy](#cms646fhirintravesicalbcgtherapy) | 1 | 1 |
| [CMS996FHIRAptTxforSTEMI](#cms996fhirapttxforstemi) | 1 | 1 |


- **CMS145FHIRCADBBlockerTPMIorLVSD** (1)
  - [ 1f70822b-c513-4c3a-8162-49f0bb9c914b ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/1f70822b-c513-4c3a-8162-49f0bb9c914b/MeasureReport-9b3577fa-355c-409d-8d3f-21e9720fb889.json) (Group_1: CMS PASS / QI-Core FAIL): **I-72**. Test expects no Group_2 Denominator Exception because the patient-reason beta-blocker not-ordered falls on the first visit, but the CQL accepts the reason at any qualifying visit, and both visits qualify. Twin case b19af44d (reason at the last visit) expects 1. The steward must change the expectation or the CQL in both versions. QI-Core passes only by accident (I-38: it scores the patient out of every population).
- **CMS347FHIRStatinPreventionTxCVD** (1)
  - [ 1ba7b147-b701-424c-bade-4e8270547030 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1ba7b147-b701-424c-bade-4e8270547030/MeasureReport-2278c703-994b-4b13-8e3b-c726ba6b8530.json) (Group_4: CMS FAIL / QI-Core PASS): **I-67**. ESRD Condition uses a start-only abatementPeriod to mean resolved 2025-12-31; the steward should use abatementDateTime or add an end. QI-Core passes only by accident (I-69, I-68).
- **CMS646FHIRIntravesicalBCGTherapy** (1)
  - [ ab48e0c0-6543-4537-8f00-bfcdcba7a81b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/ab48e0c0-6543-4537-8f00-bfcdcba7a81b/MeasureReport-ea6cfef5-54d2-4d6d-a7aa-48cf8e749eaf.json) (Group_1: CMS FAIL / QI-Core PASS): **I-70**. Test expects Numerator 0 because BCG was given before staging results came back, but the CQL measures from the start of the staging Procedure and the BCG dose starts at the same moment (2026-04-02T08:00), so the case can't fail the Numerator. QI-Core passes only by accident (I-69: its start-only onsetPeriod nulls onset.toInterval()).
- **CMS996FHIRAptTxforSTEMI** (1)
  - [ f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847/MeasureReport-f2a63299-25e1-4d91-8e5c-1bdf3b60e9cb.json) (Group_1: CMS FAIL / QI-Core PASS): **I-71**. Active thrombolytic allergy uses onsetPeriod.end to mean it ended before the ED visit; with no abatement, prevalenceInterval() correctly runs to end of time and overlaps the encounter. The steward should mark it resolved with an allergyintolerance-abatement extension, or expect Denominator Exclusion 1. QI-Core differs only because its CQL reads onset.toInterval() (CMS changed upstream in 6cccf5ce).

## Measures with No Discrepancies (56 of 74)

|  |  |  |
| --- | --- | --- |
| [ CMS2FHIRPCSDepScreenAndFollowUp ](../../input/cql/CMS2FHIRPCSDepScreenAndFollowUp.cql) [ test ](../../input/tests/results/CMS2FHIRPCSDepScreenAndFollowUp.txt) | [ CMS133FHIRCataracts2040BCVA90Days ](../../input/cql/CMS133FHIRCataracts2040BCVA90Days.cql) [ test ](../../input/tests/results/CMS133FHIRCataracts2040BCVA90Days.txt) | [ CMSFHIR529HybridHospitalWideReadmission ](../../input/cql/CMSFHIR529HybridHospitalWideReadmission.cql) [ test ](../../input/tests/results/CMSFHIR529HybridHospitalWideReadmission.txt) |
| [ CMS22FHIRPCSBPScreeningFollowUp ](../../input/cql/CMS22FHIRPCSBPScreeningFollowUp.cql) [ test ](../../input/tests/results/CMS22FHIRPCSBPScreeningFollowUp.txt) | [ CMS136FHIRChildADHDMedFollowUp ](../../input/cql/CMS136FHIRChildADHDMedFollowUp.cql) [ test ](../../input/tests/results/CMS136FHIRChildADHDMedFollowUp.txt) | [ CMS645FHIRBoneDensityPCADTherapy ](../../input/cql/CMS645FHIRBoneDensityPCADTherapy.cql) [ test ](../../input/tests/results/CMS645FHIRBoneDensityPCADTherapy.txt) |
| [ CMS50FHIRReceiptofSpecialistReport ](../../input/cql/CMS50FHIRReceiptofSpecialistReport.cql) [ test ](../../input/tests/results/CMS50FHIRReceiptofSpecialistReport.txt) | [ CMS137FHIRSUDTxInitEngagement ](../../input/cql/CMS137FHIRSUDTxInitEngagement.cql) [ test ](../../input/tests/results/CMS137FHIRSUDTxInitEngagement.txt) | [ CMS816FHIRHHHypo ](../../input/cql/CMS816FHIRHHHypo.cql) [ test ](../../input/tests/results/CMS816FHIRHHHypo.txt) |
| [ CMS56FHIRFuncStatHipReplacement ](../../input/cql/CMS56FHIRFuncStatHipReplacement.cql) [ test ](../../input/tests/results/CMS56FHIRFuncStatHipReplacement.txt) | [ CMS138FHIRTobaccoScrnCessation ](../../input/cql/CMS138FHIRTobaccoScrnCessation.cql) [ test ](../../input/tests/results/CMS138FHIRTobaccoScrnCessation.txt) | [ CMS826FHIRHHPI ](../../input/cql/CMS826FHIRHHPI.cql) [ test ](../../input/tests/results/CMS826FHIRHHPI.txt) |
| [ CMS68FHIRDocumentationCurrentMeds ](../../input/cql/CMS68FHIRDocumentationCurrentMeds.cql) [ test ](../../input/tests/results/CMS68FHIRDocumentationCurrentMeds.txt) | [ CMS139FHIRFallRiskScreening ](../../input/cql/CMS139FHIRFallRiskScreening.cql) [ test ](../../input/tests/results/CMS139FHIRFallRiskScreening.txt) | [ CMS832FHIRHHAKI ](../../input/cql/CMS832FHIRHHAKI.cql) [ test ](../../input/tests/results/CMS832FHIRHHAKI.txt) |
| [ CMS69FHIRPCSBMIScreenAndFollowUp ](../../input/cql/CMS69FHIRPCSBMIScreenAndFollowUp.cql) [ test ](../../input/tests/results/CMS69FHIRPCSBMIScreenAndFollowUp.txt) | [ CMS143FHIRPOAGOpticNerveEval ](../../input/cql/CMS143FHIRPOAGOpticNerveEval.cql) [ test ](../../input/tests/results/CMS143FHIRPOAGOpticNerveEval.txt) | [ CMS951FHIRKidneyHealthEval ](../../input/cql/CMS951FHIRKidneyHealthEval.cql) [ test ](../../input/tests/results/CMS951FHIRKidneyHealthEval.txt) |
| [ CMS71FHIRSTKAnticoagAFFlutter ](../../input/cql/CMS71FHIRSTKAnticoagAFFlutter.cql) [ test ](../../input/tests/results/CMS71FHIRSTKAnticoagAFFlutter.txt) | [ CMS144FHIRHFBetaBlockerForLVSD ](../../input/cql/CMS144FHIRHFBetaBlockerForLVSD.cql) [ test ](../../input/tests/results/CMS144FHIRHFBetaBlockerForLVSD.txt) | [ CMS986FHIRMalnutritionScore ](../../input/cql/CMS986FHIRMalnutritionScore.cql) [ test ](../../input/tests/results/CMS986FHIRMalnutritionScore.txt) |
| [ CMS74FHIRDentalCariesPrevention ](../../input/cql/CMS74FHIRDentalCariesPrevention.cql) [ test ](../../input/tests/results/CMS74FHIRDentalCariesPrevention.txt) | [ CMS146FHIRApproTestPharyngitis ](../../input/cql/CMS146FHIRApproTestPharyngitis.cql) [ test ](../../input/tests/results/CMS146FHIRApproTestPharyngitis.txt) | [ CMS1056FHIRCTClinical ](../../input/cql/CMS1056FHIRCTClinical.cql) [ test ](../../input/tests/results/CMS1056FHIRCTClinical.txt) |
| [ CMS75FHIRChildrenDentalDecay ](../../input/cql/CMS75FHIRChildrenDentalDecay.cql) [ test ](../../input/tests/results/CMS75FHIRChildrenDentalDecay.txt) | [ CMS149FHIRDementiaCognitiveAssess ](../../input/cql/CMS149FHIRDementiaCognitiveAssess.cql) [ test ](../../input/tests/results/CMS149FHIRDementiaCognitiveAssess.txt) | [ CMS1074FHIRCTIQR ](../../input/cql/CMS1074FHIRCTIQR.cql) [ test ](../../input/tests/results/CMS1074FHIRCTIQR.txt) |
| [ CMS90FHIRFSAforHeartFailure ](../../input/cql/CMS90FHIRFSAforHeartFailure.cql) [ test ](../../input/tests/results/CMS90FHIRFSAforHeartFailure.txt) | [ CMS153FHIRChlamydiaScreening ](../../input/cql/CMS153FHIRChlamydiaScreening.cql) [ test ](../../input/tests/results/CMS153FHIRChlamydiaScreening.txt) | [ CMS1157FHIRHIVRetention ](../../input/cql/CMS1157FHIRHIVRetention.cql) [ test ](../../input/tests/results/CMS1157FHIRHIVRetention.txt) |
| [ CMS108FHIRVTEProphylaxis ](../../input/cql/CMS108FHIRVTEProphylaxis.cql) [ test ](../../input/tests/results/CMS108FHIRVTEProphylaxis.txt) | [ CMS154FHIRAppropriateTxforURI ](../../input/cql/CMS154FHIRAppropriateTxforURI.cql) [ test ](../../input/tests/results/CMS154FHIRAppropriateTxforURI.txt) | [ CMS1173FHIRDiagnosticDelayVTE ](../../input/cql/CMS1173FHIRDiagnosticDelayVTE.cql) [ test ](../../input/tests/results/CMS1173FHIRDiagnosticDelayVTE.txt) |
| [ CMS117FHIRChildImmunStatus ](../../input/cql/CMS117FHIRChildImmunStatus.cql) [ test ](../../input/tests/results/CMS117FHIRChildImmunStatus.txt) | [ CMS155FHIRWgtAssessCounseling ](../../input/cql/CMS155FHIRWgtAssessCounseling.cql) [ test ](../../input/tests/results/CMS155FHIRWgtAssessCounseling.txt) | [ CMS1188FHIRHIVSTITesting ](../../input/cql/CMS1188FHIRHIVSTITesting.cql) [ test ](../../input/tests/results/CMS1188FHIRHIVSTITesting.txt) |
| [ CMS122FHIRDiabetesAssessGT9Pct ](../../input/cql/CMS122FHIRDiabetesAssessGT9Pct.cql) [ test ](../../input/tests/results/CMS122FHIRDiabetesAssessGT9Pct.txt) | [ CMS156FHIRHighRiskMedsElderly ](../../input/cql/CMS156FHIRHighRiskMedsElderly.cql) [ test ](../../input/tests/results/CMS156FHIRHighRiskMedsElderly.txt) | [ CMS1206FHIRCTOQR ](../../input/cql/CMS1206FHIRCTOQR.cql) [ test ](../../input/tests/results/CMS1206FHIRCTOQR.txt) |
| [ CMS124FHIRCervicalCancerScreen ](../../input/cql/CMS124FHIRCervicalCancerScreen.cql) [ test ](../../input/tests/results/CMS124FHIRCervicalCancerScreen.txt) | [ CMS157FHIRPainIntensityQuantified ](../../input/cql/CMS157FHIRPainIntensityQuantified.cql) [ test ](../../input/tests/results/CMS157FHIRPainIntensityQuantified.txt) | [ CMS1218FHIRHHRF ](../../input/cql/CMS1218FHIRHHRF.cql) [ test ](../../input/tests/results/CMS1218FHIRHHRF.txt) |
| [ CMS125FHIRBreastCancerScreen ](../../input/cql/CMS125FHIRBreastCancerScreen.cql) [ test ](../../input/tests/results/CMS125FHIRBreastCancerScreen.txt) | [ CMS177FHIRChildMDDSuicideAssmt ](../../input/cql/CMS177FHIRChildMDDSuicideAssmt.cql) [ test ](../../input/tests/results/CMS177FHIRChildMDDSuicideAssmt.txt) | [ CMS1244FHIRECATHOQR ](../../input/cql/CMS1244FHIRECATHOQR.cql) [ test ](../../input/tests/results/CMS1244FHIRECATHOQR.txt) |
| [ CMS128FHIRAntidepressantMgmt ](../../input/cql/CMS128FHIRAntidepressantMgmt.cql) [ test ](../../input/tests/results/CMS128FHIRAntidepressantMgmt.txt) | [ CMS190FHIRVTEProphylaxisICU ](../../input/cql/CMS190FHIRVTEProphylaxisICU.cql) [ test ](../../input/tests/results/CMS190FHIRVTEProphylaxisICU.txt) | [ CMS1264FHIRECATREHQR ](../../input/cql/CMS1264FHIRECATREHQR.cql) [ test ](../../input/tests/results/CMS1264FHIRECATREHQR.txt) |
| [ CMS129FHIRProstCaBoneScanUse ](../../input/cql/CMS129FHIRProstCaBoneScanUse.cql) [ test ](../../input/tests/results/CMS129FHIRProstCaBoneScanUse.txt) | [ CMS314FHIRHIVViralSuppression ](../../input/cql/CMS314FHIRHIVViralSuppression.cql) [ test ](../../input/tests/results/CMS314FHIRHIVViralSuppression.txt) | [ NHSNAcuteCareHospitalMonthlyInitialPopulation1 ](../../input/cql/NHSNAcuteCareHospitalMonthlyInitialPopulation1.cql) [ test ](../../input/tests/results/NHSNAcuteCareHospitalMonthlyInitialPopulation1.txt) |
| [ CMS130FHIRColorectalCancerScrn ](../../input/cql/CMS130FHIRColorectalCancerScrn.cql) [ test ](../../input/tests/results/CMS130FHIRColorectalCancerScrn.txt) | [ CMS349FHIRHIVScreening ](../../input/cql/CMS349FHIRHIVScreening.cql) [ test ](../../input/tests/results/CMS349FHIRHIVScreening.txt) | [ NHSNGlycemicControlHypoglycemiaInitialPopulation ](../../input/cql/NHSNGlycemicControlHypoglycemiaInitialPopulation.cql) [ test ](../../input/tests/results/NHSNGlycemicControlHypoglycemiaInitialPopulation.txt) |
| [ CMS131FHIRDiabetesEyeExam ](../../input/cql/CMS131FHIRDiabetesEyeExam.cql) [ test ](../../input/tests/results/CMS131FHIRDiabetesEyeExam.txt) | [ CMS506FHIRSafeUseofOpioids ](../../input/cql/CMS506FHIRSafeUseofOpioids.cql) [ test ](../../input/tests/results/CMS506FHIRSafeUseofOpioids.txt) |  |


## Measures with Discrepancies (18 of 74)
| Measure | Total Test Cases | Missing Results | Missing Populations | Mismatched Test Cases | QI-Core Also Failing | CMS Fail / QI-Core OK |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| [CMS72FHIRSTKAntithromboticDay2](#cms72fhirstkantithromboticday2) | 158 | 0 | 0 | 3.16%   (5) | 5 of 5 | 0 of 5 |
| [CMS104FHIRSTKDCAntithrombotic](#cms104fhirstkdcantithrombotic) | 82 | 0 | 0 | 7.32%   (6) | 6 of 6 | 0 of 6 |
| [CMS135FHIRACEIorARBorARNIforHF](#cms135fhiraceiorarborarniforhf) | 40 | 3 | 0 | 5.00%   (2) | 5 of 5 | 0 of 5 |
| [CMS142FHIRCommWithDrManagingDiab](#cms142fhircommwithdrmanagingdiab) | 32 | 0 | 0 | 15.62%   (5) | 5 of 5 | 0 of 5 |
| [CMS145FHIRCADBBlockerTPMIorLVSD](#cms145fhircadbblockertpmiorlvsd) | 106 | 0 | 0 | 5.66%   (6) | 6 of 6 | 0 of 6 |
| [CMS159FHIRDepRemissionat12Months](#cms159fhirdepremissionat12months) | 67 | 0 | 0 | 2.99%   (2) | 2 of 2 | 0 of 2 |
| [CMS165FHIRControllingHighBP](#cms165fhircontrollinghighbp) | 68 | 1 | 0 | 0.00%   (0) | 1 of 1 | 0 of 1 |
| [CMS0334FHIRPCCesareanBirth](#cms0334fhirpccesareanbirth) | 138 | 0 | 0 | 0.72%   (1) | 1 of 1 | 0 of 1 |
| [CMS347FHIRStatinPreventionTxCVD](#cms347fhirstatinpreventiontxcvd) | 752 | 0 | 0 | 0.13%   (1) | 0 of 1 | 0 of 1 |
| [CMS646FHIRIntravesicalBCGTherapy](#cms646fhirintravesicalbcgtherapy) | 38 | 1 | 0 | 5.26%   (2) | 2 of 3 | 0 of 3 |
| [CMS771FHIRUrinarySymptomScoreBPH](#cms771fhirurinarysymptomscorebph) | 31 | 0 | 0 | 22.58%   (7) | 7 of 7 | 0 of 7 |
| [CMS819FHIRHHORAE](#cms819fhirhhorae) | 28 | 0 | 0 | 7.14%   (2) | 2 of 2 | 0 of 2 |
| [CMSFHIR844HybridHospitalWideMortality](#cmsfhir844hybridhospitalwidemortality) | 10 | 0 | 0 | 20.00%   (2) | 2 of 2 | 0 of 2 |
| [CMS871FHIRHHHyper](#cms871fhirhhhyper) | 26 | 2 | 0 | 0.00%   (0) | 2 of 2 | 0 of 2 |
| [CMS996FHIRAptTxforSTEMI](#cms996fhirapttxforstemi) | 114 | 0 | 0 | 0.88%   (1) | 0 of 1 | 0 of 1 |
| [CMS1017FHIRHHFI](#cms1017fhirhhfi) | 65 | 0 | 0 | 3.08%   (2) | 2 of 2 | 0 of 2 |
| [CMS1028FHIRPCSevereOBComps](#cms1028fhirpcsevereobcomps) | 282 | 0 | 0 | 0.71%   (2) | 2 of 2 | 0 of 2 |
| [CMS1154ScreeningPrediabetesFHIR](#cms1154screeningprediabetesfhir) | 10 | 0 | 0 | 10.00%   (1) | 1 of 1 | 0 of 1 |



_3 failing cases are implementation issues (see `## QI-Core Parity`) and count in neither column, so on those measures the two columns can add up to less than the failing total._

_QI-Core columns compare against `./scripts/comparison/qicore_actual_results.csv` (a copy of the QI-Core project's `actual_results.csv`). Status is scored against the same fixture MeasureReport expectations used for the UQC columns, not against the UQC engine's output: `PASS` means QI-Core reproduced every expected population, so a UQC failure on the same case is a migration regression rather than a parity gap; `FAIL` means QI-Core is wrong the same way; `MISSING` means QI-Core produced no result for that case at all. The `QI-Core Also Failing` column counts `MISSING` as non-PASS -- it is an absence of evidence, not a confirmed pass -- so read it as "not reproduced on QI-Core" rather than "confirmed broken on QI-Core". `CMS Fail / QI-Core OK` is the same denominator read the other way: of this measure's failing cases, how many QI-Core reproduces. `0 of m` marks a measure as fully actionable here. The denominators count failing (test case, group) pairs, matching the tables below, while `Fail Count` counts each test case once across groups, so the two totals need not match._

_These columns only cover cases UQC already fails, so they cannot answer "how far apart are these two engines". The `## QI-Core Parity` section scores every test case both ways; note that its total disagreement count is larger than the sum of the two asymmetric buckets, because two engines can disagree about a case both of them get wrong the same way._

#### CMS72FHIRSTKAntithromboticDay2
[ [cql] ](../../input/cql/CMS72FHIRSTKAntithromboticDay2.cql) [ [test results] ](../../input/tests/results/CMS72FHIRSTKAntithromboticDay2.txt)

Mismatched Test Cases (5 of 158)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 5a329008-fcc1-4168-ab9c-89cb5dd6ff32 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5a329008-fcc1-4168-ab9c-89cb5dd6ff32/MeasureReport-dda268cb-4395-4776-acd8-0fee046d392a.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>1 | 1<br>1<br>0 | FAIL | 0<br>0<br>0 |
| [ be5c4068-2639-4b0c-bea3-5b7c80a6fe3b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/be5c4068-2639-4b0c-bea3-5b7c80a6fe3b/MeasureReport-ad329961-b67b-413b-a186-d6b269572c42.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |
| [ cb7c95fc-6d6b-4e07-81e8-a79385142b94 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cb7c95fc-6d6b-4e07-81e8-a79385142b94/MeasureReport-6844e7ed-08a4-43d5-be1c-720dc795b3cf.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 3<br>3<br>2 | 1<br>1<br>0 | FAIL | 0<br>0<br>0 |
| [ e126cdec-dbc8-4ee8-964f-e88e46c04f88 ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e126cdec-dbc8-4ee8-964f-e88e46c04f88/MeasureReport-58249af5-0abc-464b-9e0a-456f7c31b4cf.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion | 1<br>1<br>1 | 0<br>0<br>0 | FAIL | 0<br>0<br>0 |
| [ febd4b3e-99bc-4c55-bba9-3b2136c2160b ](../.././input/tests/measure/CMS72FHIRSTKAntithromboticDay2/febd4b3e-99bc-4c55-bba9-3b2136c2160b/MeasureReport-4f80f98a-71ab-45d6-bdda-d0875ec02ec9.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 4<br>4<br>2<br>2 | 1<br>1<br>0<br>1 | FAIL | 0<br>0<br>0<br>0 |



#### CMS104FHIRSTKDCAntithrombotic
[ [cql] ](../../input/cql/CMS104FHIRSTKDCAntithrombotic.cql) [ [test results] ](../../input/tests/results/CMS104FHIRSTKDCAntithrombotic.txt)

Mismatched Test Cases (6 of 82)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 0b1aa8ee-e8bf-49f5-b968-48c5a9702843 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0b1aa8ee-e8bf-49f5-b968-48c5a9702843/MeasureReport-38f44642-a505-41c0-b367-013e4bb44d58.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion | 1<br>1<br>1 | 0<br>0<br>0 | FAIL | 0<br>0<br>0 |
| [ 348471db-5aaa-4bf3-a280-75222f20d599 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/348471db-5aaa-4bf3-a280-75222f20d599/MeasureReport-bf54d81d-f635-45ff-b69b-1580a144d3fb.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 3<br>3<br>1<br>1 | 1<br>1<br>0<br>0 | FAIL | 0<br>0<br>0<br>0 |
| [ 451b6853-3734-4c1c-b37e-5904629e0350 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/451b6853-3734-4c1c-b37e-5904629e0350/MeasureReport-4eefe8af-efb3-47eb-91df-e2ea877a39e7.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exclusion<br>Numerator | 3<br>3<br>2<br>1 | 1<br>1<br>1<br>0 | FAIL | 0<br>0<br>0<br>0 |
| [ a2b8327c-eaf4-4552-863e-851426e729d4 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a2b8327c-eaf4-4552-863e-851426e729d4/MeasureReport-0ced6c1b-75a5-4ee3-a7a0-017818c03e9a.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 2<br>2<br>2 | 1<br>1<br>1 | FAIL | 0<br>0<br>0 |
| [ c15bee15-84c1-494a-ac82-2159b06da175 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/c15bee15-84c1-494a-ac82-2159b06da175/MeasureReport-bbe28035-6557-410d-964f-21cf38904d0f.json) | Group_1 | Initial Population<br>Denominator<br>Numerator | 3<br>3<br>2 | 1<br>1<br>0 | FAIL | 0<br>0<br>0 |
| [ e84c89f7-3c9e-4ee9-b71a-5025aadb5990 ](../.././input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e84c89f7-3c9e-4ee9-b71a-5025aadb5990/MeasureReport-51e29a50-abca-429e-95eb-8364998be573.json) | Group_1 | Initial Population<br>Denominator<br>Denominator Exception | 1<br>1<br>1 | 0<br>0<br>0 | FAIL | 0<br>0<br>0 |



#### CMS135FHIRACEIorARBorARNIforHF
[ [cql] ](../../input/cql/CMS135FHIRACEIorARBorARNIforHF.cql) [ [test results] ](../../input/tests/results/CMS135FHIRACEIorARBorARNIforHF.txt)

Missing Results (3 of 40 test cases)
| Test Case | Group | QI-Core |
| --- | --- | --- |
| [ c095195c-8893-4bf1-aa7d-ad2bfd9bafa5 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/c095195c-8893-4bf1-aa7d-ad2bfd9bafa5/MeasureReport-f2d033da-6f32-46dc-86bc-69fdf82b1cfd.json) | Group_1 | MISSING |
| [ cba5a449-1c45-4e11-ae0b-ba3974b410f7 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/cba5a449-1c45-4e11-ae0b-ba3974b410f7/MeasureReport-ae8c4b99-af76-4577-b66d-b1230ac09aa3.json) | Group_1 | MISSING |
| [ ec508dbb-76f6-4878-b8a2-114ea8e82297 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/ec508dbb-76f6-4878-b8a2-114ea8e82297/MeasureReport-d1b704c8-7e95-4cd9-89e7-a8b90f925ce2.json) | Group_1 | MISSING |


Mismatched Test Cases (2 of 40)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 5b7e720f-e2fc-4779-9b1c-3f34a0241482 ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/5b7e720f-e2fc-4779-9b1c-3f34a0241482/MeasureReport-01fb5443-0f43-487e-ac44-f7cc6e163ca0.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |
| [ d18e37a6-7b66-4e7c-b305-692872c13f8d ](../.././input/tests/measure/CMS135FHIRACEIorARBorARNIforHF/d18e37a6-7b66-4e7c-b305-692872c13f8d/MeasureReport-ecbb5067-dcb1-48ce-8e78-6dfd556ac43d.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |



#### CMS142FHIRCommWithDrManagingDiab
[ [cql] ](../../input/cql/CMS142FHIRCommWithDrManagingDiab.cql) [ [test results] ](../../input/tests/results/CMS142FHIRCommWithDrManagingDiab.txt)

Mismatched Test Cases (5 of 32)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 05f1e2a6-b317-42bb-827f-993ca3995f5b ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/05f1e2a6-b317-42bb-827f-993ca3995f5b/MeasureReport-84bcf708-71bb-4169-8067-18fd354f3c37.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |
| [ 41ae0086-ac99-4a31-9546-21b054bbf7d8 ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/41ae0086-ac99-4a31-9546-21b054bbf7d8/MeasureReport-b77a6309-214c-4fc2-a9bc-18d81c740da6.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |
| [ 6aef5a18-59bd-4a47-80bc-2bd44636e41f ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/6aef5a18-59bd-4a47-80bc-2bd44636e41f/MeasureReport-e5735d61-0444-4958-8f47-165a59e91dc0.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |
| [ b85440e4-b902-49cd-b3d6-363ba7a99bce ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/b85440e4-b902-49cd-b3d6-363ba7a99bce/MeasureReport-9d61df39-18a0-451f-a795-988388d58778.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |
| [ d9840e8c-3359-42c2-b354-4b236c3c1b15 ](../.././input/tests/measure/CMS142FHIRCommWithDrManagingDiab/d9840e8c-3359-42c2-b354-4b236c3c1b15/MeasureReport-1fbf56ab-6e60-4ce6-a1d5-b520382164bd.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |



#### CMS145FHIRCADBBlockerTPMIorLVSD
[ [cql] ](../../input/cql/CMS145FHIRCADBBlockerTPMIorLVSD.cql) [ [test results] ](../../input/tests/results/CMS145FHIRCADBBlockerTPMIorLVSD.txt)

Mismatched Test Cases (6 of 106)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 1f70822b-c513-4c3a-8162-49f0bb9c914b ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/1f70822b-c513-4c3a-8162-49f0bb9c914b/MeasureReport-9b3577fa-355c-409d-8d3f-21e9720fb889.json) | Group_2 | Denominator Exception | 0 | 1 | FAIL | 0 |
| [ 4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/4f4a65f4-a4c6-47e7-b37e-3ad9a9c9342e/MeasureReport-e77c61ff-cc3a-402c-9752-7a97a6727a39.json) | Group_2 | Denominator Exception | 1 | 0 | FAIL | 0 |
| [ 5fd0d626-e9c5-4e6c-a10d-1a1183fa7702 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/5fd0d626-e9c5-4e6c-a10d-1a1183fa7702/MeasureReport-ce1b8712-b9dd-48e2-adf4-554ed641bee5.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |
| [ 61306767-0e74-44b8-ac06-1339c3783355 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/61306767-0e74-44b8-ac06-1339c3783355/MeasureReport-6ea40199-5a45-4c8d-8a2b-c08bf93ebd8a.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |
| [ b65680a0-9768-4ce4-b08d-972fcd84e28e ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/b65680a0-9768-4ce4-b08d-972fcd84e28e/MeasureReport-b5ebd0a9-a2de-4b31-b0d9-588888e95872.json) | Group_2 | Denominator Exception | 1 | 0 | FAIL | 0 |
| [ fd5fb311-a466-4c59-966d-48fa7aa88931 ](../.././input/tests/measure/CMS145FHIRCADBBlockerTPMIorLVSD/fd5fb311-a466-4c59-966d-48fa7aa88931/MeasureReport-05ffed3e-5604-40eb-bcf8-99cacecc26c0.json) | Group_1 | Denominator Exception | 1 | 0 | FAIL | 0 |



#### CMS159FHIRDepRemissionat12Months
[ [cql] ](../../input/cql/CMS159FHIRDepRemissionat12Months.cql) [ [test results] ](../../input/tests/results/CMS159FHIRDepRemissionat12Months.txt)

Mismatched Test Cases (2 of 67)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 491f554e-e897-40c5-ad2b-0983923df4e8 ](../.././input/tests/measure/CMS159FHIRDepRemissionat12Months/491f554e-e897-40c5-ad2b-0983923df4e8/MeasureReport-580087e1-b59e-43eb-b110-692c35a82dca.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | FAIL | 0<br>1 |
| [ 96b6579c-1cee-423f-9433-a72db6fb8a0a ](../.././input/tests/measure/CMS159FHIRDepRemissionat12Months/96b6579c-1cee-423f-9433-a72db6fb8a0a/MeasureReport-e3ec1311-05ed-4a6f-b13f-a4d290865bb3.json) | Group_1 | Denominator Exclusion<br>Numerator | 1<br>0 | 0<br>1 | FAIL | 0<br>1 |



#### CMS165FHIRControllingHighBP
[ [cql] ](../../input/cql/CMS165FHIRControllingHighBP.cql) [ [test results] ](../../input/tests/results/CMS165FHIRControllingHighBP.txt)

Missing Results (1 of 68 test cases)
| Test Case | Group | QI-Core |
| --- | --- | --- |
| [ 45e01fed-56bb-483d-a860-af3d566bda11 ](../.././input/tests/measure/CMS165FHIRControllingHighBP/45e01fed-56bb-483d-a860-af3d566bda11/MeasureReport-02991ca7-859d-422d-8849-655760f8e10a.json) | Group_1 | MISSING |


#### CMS0334FHIRPCCesareanBirth
[ [cql] ](../../input/cql/CMS0334FHIRPCCesareanBirth.cql) [ [test results] ](../../input/tests/results/CMS0334FHIRPCCesareanBirth.txt)

Mismatched Test Cases (1 of 138)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ c58acff5-248b-49c9-b18d-69e4a84a08d9 ](../.././input/tests/measure/CMS0334FHIRPCCesareanBirth/c58acff5-248b-49c9-b18d-69e4a84a08d9/MeasureReport-920b0c2e-1f1f-42d3-ab1f-1d7b12fa4bd0.json) | Group_1 | Denominator<br>Denominator Exclusion | 1<br>1 | 0<br>0 | FAIL | 0<br>0 |



#### CMS347FHIRStatinPreventionTxCVD
[ [cql] ](../../input/cql/CMS347FHIRStatinPreventionTxCVD.cql) [ [test results] ](../../input/tests/results/CMS347FHIRStatinPreventionTxCVD.txt)

Mismatched Test Cases (1 of 752)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 1ba7b147-b701-424c-bade-4e8270547030 ](../.././input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1ba7b147-b701-424c-bade-4e8270547030/MeasureReport-2278c703-994b-4b13-8e3b-c726ba6b8530.json) | Group_4 | Denominator Exception | 0 | 1 | PASS | 0 |



#### CMS646FHIRIntravesicalBCGTherapy
[ [cql] ](../../input/cql/CMS646FHIRIntravesicalBCGTherapy.cql) [ [test results] ](../../input/tests/results/CMS646FHIRIntravesicalBCGTherapy.txt)

Missing Results (1 of 38 test cases)
| Test Case | Group | QI-Core |
| --- | --- | --- |
| [ 342d2bec-0acc-43e5-aaf7-3c9a65b09f91 ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/342d2bec-0acc-43e5-aaf7-3c9a65b09f91/MeasureReport-12cd358b-deb0-4130-a045-4c6b61e110c0.json) | Group_1 | FAIL |


Mismatched Test Cases (2 of 38)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 10cec7db-41ae-49ad-b883-022f19d92a8b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/10cec7db-41ae-49ad-b883-022f19d92a8b/MeasureReport-b8b4961d-450b-4980-ac8f-95500c6393d4.json) | Group_1 | Denominator Exclusion | 0 | 1 | FAIL | 1 |
| [ ab48e0c0-6543-4537-8f00-bfcdcba7a81b ](../.././input/tests/measure/CMS646FHIRIntravesicalBCGTherapy/ab48e0c0-6543-4537-8f00-bfcdcba7a81b/MeasureReport-ea6cfef5-54d2-4d6d-a7aa-48cf8e749eaf.json) | Group_1 | Numerator | 0 | 1 | PASS | 0 |



#### CMS771FHIRUrinarySymptomScoreBPH
[ [cql] ](../../input/cql/CMS771FHIRUrinarySymptomScoreBPH.cql) [ [test results] ](../../input/tests/results/CMS771FHIRUrinarySymptomScoreBPH.txt)

Mismatched Test Cases (7 of 31)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 051c5977-9f2c-4e8b-8e02-ac3ec0c718d6 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/051c5977-9f2c-4e8b-8e02-ac3ec0c718d6/MeasureReport-13a299d2-1f32-41d7-b226-7380902e41b7.json) | Group_1 | Denominator | 1 | 0 | FAIL | 0 |
| [ 3ab3ac1d-9b5e-4087-8862-dcb2562fb90f ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/3ab3ac1d-9b5e-4087-8862-dcb2562fb90f/MeasureReport-47dae27e-89cf-4ee5-8c8b-bf1e44997d07.json) | Group_1 | Denominator | 1 | 0 | FAIL | 0 |
| [ 4c234ec0-3f89-4d55-b767-219d1130f634 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/4c234ec0-3f89-4d55-b767-219d1130f634/MeasureReport-47a91ced-cb5f-44c0-9417-e8efa33a4b08.json) | Group_1 | Numerator | 1 | 0 | FAIL | 0 |
| [ 9be591a0-517b-4be2-b652-a29be0c75c15 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/9be591a0-517b-4be2-b652-a29be0c75c15/MeasureReport-004d2ae6-6c2e-49f8-bf07-26cada3bbaf3.json) | Group_1 | Numerator | 1 | 0 | FAIL | 0 |
| [ bc79e5bc-237e-44be-b5fc-c5c4efb50286 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/bc79e5bc-237e-44be-b5fc-c5c4efb50286/MeasureReport-621196a7-ca5f-4408-8508-851332413956.json) | Group_1 | Numerator | 1 | 0 | FAIL | 0 |
| [ bf0f8968-c2c0-4416-88db-11ea3e3da968 ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/bf0f8968-c2c0-4416-88db-11ea3e3da968/MeasureReport-bcce208a-3ff4-4c82-9d49-c0b64ccb9138.json) | Group_1 | Numerator | 1 | 0 | FAIL | 0 |
| [ e90d90a7-3071-44de-8089-ad7b6f5f3e5d ](../.././input/tests/measure/CMS771FHIRUrinarySymptomScoreBPH/e90d90a7-3071-44de-8089-ad7b6f5f3e5d/MeasureReport-9ef2db11-d78a-49af-a2ac-6536fac264a1.json) | Group_1 | Numerator | 1 | 0 | FAIL | 0 |



#### CMS819FHIRHHORAE
[ [cql] ](../../input/cql/CMS819FHIRHHORAE.cql) [ [test results] ](../../input/tests/results/CMS819FHIRHHORAE.txt)

Mismatched Test Cases (2 of 28)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 31b40acc-ca5f-4d1d-bd83-4b1a14eb822e ](../.././input/tests/measure/CMS819FHIRHHORAE/31b40acc-ca5f-4d1d-bd83-4b1a14eb822e/MeasureReport-c93e2b69-18fd-425e-8c71-b52eb967eda0.json) | Group_1 | Initial Population<br>Denominator | 2<br>2 | 1<br>1 | FAIL | 1<br>1 |
| [ 73b0c1fe-874b-4982-8cb2-3c30520441de ](../.././input/tests/measure/CMS819FHIRHHORAE/73b0c1fe-874b-4982-8cb2-3c30520441de/MeasureReport-15d9e04f-4116-4856-b61a-f7c7b38e3325.json) | Group_1 | Numerator | 1 | 0 | FAIL | 0 |



#### CMSFHIR844HybridHospitalWideMortality
[ [cql] ](../../input/cql/CMSFHIR844HybridHospitalWideMortality.cql) [ [test results] ](../../input/tests/results/CMSFHIR844HybridHospitalWideMortality.txt)

Mismatched Test Cases (2 of 10)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 6f22a06f-7186-4db1-9310-4f907dc49ff3 ](../.././input/tests/measure/CMSFHIR844HybridHospitalWideMortality/6f22a06f-7186-4db1-9310-4f907dc49ff3/MeasureReport-a02a261f-1274-4f8b-b1f3-5496f7885cbe.json) | Group_1 | Initial Population | 1 | 0 | FAIL | 0 |
| [ af1b9448-3e7a-4b7f-8934-15bb63258b75 ](../.././input/tests/measure/CMSFHIR844HybridHospitalWideMortality/af1b9448-3e7a-4b7f-8934-15bb63258b75/MeasureReport-7afefb0f-3075-4fb8-8d56-474ba1112c38.json) | Group_1 | Initial Population | 2 | 1 | FAIL | 1 |



#### CMS871FHIRHHHyper
[ [cql] ](../../input/cql/CMS871FHIRHHHyper.cql) [ [test results] ](../../input/tests/results/CMS871FHIRHHHyper.txt)

Missing Results (2 of 26 test cases)
| Test Case | Group | QI-Core |
| --- | --- | --- |
| [ 98533ccd-24ee-41b3-aab2-ef6cbf89e00d ](../.././input/tests/measure/CMS871FHIRHHHyper/98533ccd-24ee-41b3-aab2-ef6cbf89e00d/MeasureReport-82c8805c-b129-4009-8533-1ed12cf5d18f.json) | Group_1 | MISSING |
| [ fd579f44-757b-4c98-9b09-27b17b935650 ](../.././input/tests/measure/CMS871FHIRHHHyper/fd579f44-757b-4c98-9b09-27b17b935650/MeasureReport-22df2e2a-404d-4ab0-831a-e2ab043197a2.json) | Group_1 | MISSING |


#### CMS996FHIRAptTxforSTEMI
[ [cql] ](../../input/cql/CMS996FHIRAptTxforSTEMI.cql) [ [test results] ](../../input/tests/results/CMS996FHIRAptTxforSTEMI.txt)

Mismatched Test Cases (1 of 114)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847 ](../.././input/tests/measure/CMS996FHIRAptTxforSTEMI/f6c7dbc1-9ca7-46cd-bcbe-29d8fae4e847/MeasureReport-f2a63299-25e1-4d91-8e5c-1bdf3b60e9cb.json) | Group_1 | Denominator Exclusion | 0 | 1 | PASS | 0 |



#### CMS1017FHIRHHFI
[ [cql] ](../../input/cql/CMS1017FHIRHHFI.cql) [ [test results] ](../../input/tests/results/CMS1017FHIRHHFI.txt)

Mismatched Test Cases (2 of 65)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 0dfafc1a-cf94-4ca1-becf-c1b843896810 ](../.././input/tests/measure/CMS1017FHIRHHFI/0dfafc1a-cf94-4ca1-becf-c1b843896810/MeasureReport-cd491c44-6ed1-483f-8775-516f92b9c16d.json) | Group_1 | Numerator Exclusion | 0 | 1 | FAIL | 1 |
| [ 5ff2713d-ca89-42ae-91bb-cba3e1d9a487 ](../.././input/tests/measure/CMS1017FHIRHHFI/5ff2713d-ca89-42ae-91bb-cba3e1d9a487/MeasureReport-74f8c3e3-881b-4ba8-bfdb-ceef555ed020.json) | Group_1 | Numerator Exclusion | 0 | 1 | FAIL | 1 |



#### CMS1028FHIRPCSevereOBComps
[ [cql] ](../../input/cql/CMS1028FHIRPCSevereOBComps.cql) [ [test results] ](../../input/tests/results/CMS1028FHIRPCSevereOBComps.txt)

Mismatched Test Cases (2 of 282)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ 763d86f9-d93f-4873-8b64-8439566b242e ](../.././input/tests/measure/CMS1028FHIRPCSevereOBComps/763d86f9-d93f-4873-8b64-8439566b242e/MeasureReport-7ca90ad8-935e-4d56-80d9-5470c8a98481.json) | Group_1 | Numerator | 2 | 1 | FAIL | 1 |
| [ 763d86f9-d93f-4873-8b64-8439566b242e ](../.././input/tests/measure/CMS1028FHIRPCSevereOBComps/763d86f9-d93f-4873-8b64-8439566b242e/MeasureReport-7ca90ad8-935e-4d56-80d9-5470c8a98481.json) | Group_2 | Numerator | 2 | 1 | FAIL | 1 |



#### CMS1154ScreeningPrediabetesFHIR
[ [cql] ](../../input/cql/CMS1154ScreeningPrediabetesFHIR.cql) [ [test results] ](../../input/tests/results/CMS1154ScreeningPrediabetesFHIR.txt)

Mismatched Test Cases (1 of 10)
| Test Case | Group | Population | Expected | Actual | QI-Core | QI-Core Actual |
|---|---|---|:---:|:---:|:---:|:---:|
| [ bc9c82ca-72b5-41c4-a9a3-7e3860a9ac2d ](../.././input/tests/measure/CMS1154ScreeningPrediabetesFHIR/bc9c82ca-72b5-41c4-a9a3-7e3860a9ac2d/MeasureReport-466dec57-6ceb-4f37-8daa-40f26f14a191.json) | Group_1 | Denominator Exclusion | 1 | 0 | FAIL | 0 |




_Known issues are tracked by hand in `defect-tracking/known-issues.md`._
