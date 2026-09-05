# QICore Baseline Audit

- baseline: `scripts/comparison/_archive/qicore-2025-actual-results-20260905-WIP-from-HEAD.csv`
- fresh:    `scripts/comparison/_archive/qicore-2025-actual-results-20260905-fresh-extract.csv`

## Summary

| Bucket | Count |
|---|---:|
| Unchanged rows | 23641 |
| Added in fresh | 7 |
| Removed vs baseline | 0 |
| Changed (bas != fresh) | 1065 |

## Per-measure drift

| Measure | Unchanged | Added | Removed | Changed |
|---|---:|---:|---:|---:|
| CMS0334FHIRPCCesareanBirth | 551 | 0 | 0 | 1 |
| CMS1017FHIRHHFI | 455 | 0 | 0 | 0 |
| CMS1028FHIRPCSevereOBComps | 1118 | 0 | 0 | 10 |
| CMS104FHIRSTKDCAntithrombotic | 410 | 0 | 0 | 0 |
| CMS1056FHIRCTClinical | 40 | 0 | 0 | 0 |
| CMS1074FHIRCTIQR | 40 | 0 | 0 | 0 |
| CMS108FHIRVTEProphylaxis | 545 | 0 | 0 | 15 |
| CMS1154ScreeningPrediabetesFHIR | 40 | 0 | 0 | 0 |
| CMS1157FHIRHIVRetention | 81 | 0 | 0 | 0 |
| CMS1173FHIRDiagnosticDelayVTE | 256 | 0 | 0 | 4 |
| CMS117FHIRChildImmunStatus | 180 | 0 | 0 | 0 |
| CMS1188FHIRHIVSTITesting | 100 | 0 | 0 | 2 |
| CMS1206FHIRCTOQR | 40 | 0 | 0 | 0 |
| CMS1218FHIRHHRF | 276 | 0 | 0 | 0 |
| CMS122FHIRDiabetesAssessGT9Pct | 208 | 0 | 0 | 12 |
| CMS1244FHIRECATHOQR | 216 | 0 | 0 | 0 |
| CMS124FHIRCervicalCancerScreen | 136 | 0 | 0 | 0 |
| CMS125FHIRBreastCancerScreen | 256 | 0 | 0 | 8 |
| CMS1264FHIRECATREHQR | 174 | 0 | 0 | 0 |
| CMS128FHIRAntidepressantMgmt | 98 | 0 | 0 | 134 |
| CMS129FHIRProstCaBoneScanUse | 137 | 0 | 0 | 67 |
| CMS130FHIRColorectalCancerScrn | 255 | 0 | 0 | 1 |
| CMS131FHIRDiabetesEyeExam | 246 | 0 | 0 | 6 |
| CMS133FHIRCataracts2040BCVA90Days | 292 | 0 | 0 | 0 |
| CMS135FHIRACEIorARBorARNIforHF | 108 | 0 | 0 | 77 |
| CMS136FHIRChildADHDMedFollowUp | 507 | 0 | 0 | 5 |
| CMS137FHIRSUDTxInitEngagement | 360 | 0 | 0 | 0 |
| CMS138FHIRTobaccoScrnCessation | 564 | 0 | 0 | 0 |
| CMS139FHIRFallRiskScreening | 116 | 0 | 0 | 0 |
| CMS142FHIRCommWithDrManagingDiab | 128 | 0 | 0 | 0 |
| CMS143FHIRPOAGOpticNerveEval | 128 | 0 | 0 | 0 |
| CMS144FHIRHFBetaBlockerForLVSD | 129 | 0 | 0 | 111 |
| CMS145FHIRCADBBlockerTPMIorLVSD | 260 | 0 | 0 | 164 |
| CMS146FHIRApproTestPharyngitis | 152 | 0 | 0 | 0 |
| CMS149FHIRDementiaCognitiveAssess | 111 | 0 | 0 | 21 |
| CMS153FHIRChlamydiaScreening | 126 | 0 | 0 | 2 |
| CMS154FHIRAppropriateTxforURI | 132 | 0 | 0 | 0 |
| CMS155FHIRWgtAssessCounseling | 408 | 0 | 0 | 0 |
| CMS156FHIRHighRiskMedsElderly | 704 | 0 | 0 | 4 |
| CMS157FHIRPainIntensityQuantified | 378 | 0 | 0 | 0 |
| CMS159FHIRDepRemissionat12Months | 268 | 0 | 0 | 0 |
| CMS165FHIRControllingHighBP | 259 | 0 | 0 | 9 |
| CMS177FHIRChildMDDSuicideAssmt | 123 | 0 | 0 | 0 |
| CMS190FHIRVTEProphylaxisICU | 609 | 0 | 0 | 16 |
| CMS22FHIRPCSBPScreeningFollowUp | 219 | 0 | 0 | 1 |
| CMS2FHIRPCSDepScreenAndFollowUp | 173 | 0 | 0 | 7 |
| CMS314FHIRHIVViralSuppression | 129 | 0 | 0 | 0 |
| CMS347FHIRStatinPreventionTxCVD | 3523 | 0 | 0 | 217 |
| CMS349FHIRHIVScreening | 180 | 0 | 0 | 0 |
| CMS506FHIRSafeUseofOpioids | 199 | 0 | 0 | 5 |
| CMS50FHIRReceiptofSpecialistReport | 99 | 0 | 0 | 0 |
| CMS56FHIRFuncStatHipReplacement | 232 | 0 | 0 | 0 |
| CMS645FHIRBoneDensityPCADTherapy | 125 | 0 | 0 | 79 |
| CMS646FHIRIntravesicalBCGTherapy | 180 | 0 | 0 | 10 |
| CMS68FHIRDocumentationCurrentMeds | 76 | 0 | 0 | 0 |
| CMS69FHIRPCSBMIScreenAndFollowUp | 315 | 0 | 0 | 0 |
| CMS71FHIRSTKAnticoagAFFlutter | 414 | 0 | 0 | 1 |
| CMS72FHIRSTKAntithromboticDay2 | 781 | 0 | 0 | 9 |
| CMS74FHIRDentalCariesPrevention | 80 | 0 | 0 | 0 |
| CMS75FHIRChildrenDentalDecay | 80 | 0 | 0 | 0 |
| CMS771FHIRUrinarySymptomScoreBPH | 70 | 0 | 0 | 54 |
| CMS816FHIRHHHypo | 84 | 0 | 0 | 0 |
| CMS819FHIRHHORAE | 82 | 0 | 0 | 2 |
| CMS826FHIRHHPI | 36 | 0 | 0 | 0 |
| CMS832FHIRHHAKI | 148 | 0 | 0 | 0 |
| CMS871FHIRHHHyper | 147 | 7 | 0 | 0 |
| CMS90FHIRFSAforHeartFailure | 148 | 0 | 0 | 0 |
| CMS951FHIRKidneyHealthEval | 220 | 0 | 0 | 0 |
| CMS986FHIRMalnutritionScore | 3504 | 0 | 0 | 0 |
| CMS996FHIRAptTxforSTEMI | 562 | 0 | 0 | 8 |
| CMSFHIR529HybridHospitalWideReadmission | 1 | 0 | 0 | 0 |
| CMSFHIR844HybridHospitalWideMortality | 10 | 0 | 0 | 0 |
| NHSNAcuteCareHospitalMonthlyInitialPopulation1 | 27 | 0 | 0 | 0 |
| NHSNGlycemicControlHypoglycemiaInitialPopulation | 77 | 0 | 0 | 3 |

## Sample changes

| Measure | Guid | Population | Baseline | Fresh |
|---|---|---|---:|---:|
| CMS0334FHIRPCCesareanBirth | 912e076d-3b5d-46cc-b2cb-c78172b295a3 | Group_1:Initial Population | 1 | 0 |
| CMS1028FHIRPCSevereOBComps | 02cdc116-49ce-4277-ad9e-de6bc2a3274d | Group_1:Denominator | 1 | 0 |
| CMS1028FHIRPCSevereOBComps | 02cdc116-49ce-4277-ad9e-de6bc2a3274d | Group_1:Initial Population | 1 | 0 |
| CMS1028FHIRPCSevereOBComps | 02cdc116-49ce-4277-ad9e-de6bc2a3274d | Group_2:Denominator | 1 | 0 |
| CMS1028FHIRPCSevereOBComps | 02cdc116-49ce-4277-ad9e-de6bc2a3274d | Group_2:Initial Population | 1 | 0 |
| CMS1028FHIRPCSevereOBComps | 3d4b6868-31ce-42f8-87c1-ab06d851d53f | Group_1:Initial Population | 1 | 0 |
| CMS1028FHIRPCSevereOBComps | 3d4b6868-31ce-42f8-87c1-ab06d851d53f | Group_2:Initial Population | 1 | 0 |
| CMS1028FHIRPCSevereOBComps | 4e9a1928-a33f-4be3-aa05-c69e9fc4bff7 | Group_1:Denominator | 1 | 0 |
| CMS1028FHIRPCSevereOBComps | 4e9a1928-a33f-4be3-aa05-c69e9fc4bff7 | Group_1:Initial Population | 1 | 0 |
| CMS1028FHIRPCSevereOBComps | 4e9a1928-a33f-4be3-aa05-c69e9fc4bff7 | Group_2:Denominator | 1 | 0 |
| CMS1028FHIRPCSevereOBComps | 4e9a1928-a33f-4be3-aa05-c69e9fc4bff7 | Group_2:Initial Population | 1 | 0 |
| CMS108FHIRVTEProphylaxis | 0ddb05b5-03af-4d2a-9d9c-0be8034d1ff4 | Group_1:Denominator Exclusion | 1 | 0 |
| CMS108FHIRVTEProphylaxis | 1b450176-8caa-4133-bc9a-c066969f72ce | Group_1:Denominator Exclusion | 1 | 0 |
| CMS108FHIRVTEProphylaxis | 52790be5-0f6e-4ebd-85f5-57f35db8b56b | Group_1:Numerator | 1 | 0 |
| CMS108FHIRVTEProphylaxis | 543248c8-b6af-407d-b435-7e867c4770b4 | Group_1:Numerator | 1 | 0 |
| CMS108FHIRVTEProphylaxis | 610c90c9-f387-40f8-9bd7-710d20dfd6f0 | Group_1:Numerator | 1 | 0 |
| CMS108FHIRVTEProphylaxis | 73673965-9b35-446e-bad7-1701991e6906 | Group_1:Numerator | 1 | 0 |
| CMS108FHIRVTEProphylaxis | 77c1bf41-fce8-4044-9eeb-c205b8fdc0a9 | Group_1:Numerator | 1 | 0 |
| CMS108FHIRVTEProphylaxis | 900f47c2-3615-4ffe-a9e0-3e7e70469ffb | Group_1:Denominator Exclusion | 1 | 0 |
| CMS108FHIRVTEProphylaxis | 96c7b8e3-2c28-4b46-a579-f56d644cf762 | Group_1:Denominator Exclusion | 1 | 0 |
| CMS108FHIRVTEProphylaxis | a3e0cca4-bdb1-4972-b6ca-84841cb66859 | Group_1:Numerator | 1 | 0 |
| CMS108FHIRVTEProphylaxis | a8083d97-85af-4e1e-8770-30c49a287194 | Group_1:Numerator | 1 | 0 |
| CMS108FHIRVTEProphylaxis | afad0252-21ef-48ee-9a5a-33dab92d8709 | Group_1:Denominator Exclusion | 1 | 0 |
| CMS108FHIRVTEProphylaxis | cbd1de91-441a-4588-b000-2e589d099ab6 | Group_1:Numerator | 1 | 0 |
| CMS108FHIRVTEProphylaxis | ea49dc35-7378-4436-aa37-53ec9f13b05d | Group_1:Numerator | 2 | 1 |
| CMS108FHIRVTEProphylaxis | ef0bebdc-61bf-4233-abd3-1f3c99a2cd8d | Group_1:Numerator | 1 | 0 |
| CMS1173FHIRDiagnosticDelayVTE | 3739ae38-6a2c-4197-bda6-e493c9df60e3 | Group_1:Denominator Exclusion | 1 | 0 |
| CMS1173FHIRDiagnosticDelayVTE | 3739ae38-6a2c-4197-bda6-e493c9df60e3 | Group_1:Numerator | 0 | 1 |
| CMS1173FHIRDiagnosticDelayVTE | cfa235c3-3b8b-4cbb-a78f-5c4fd2af04df | Group_1:Denominator Exclusion | 1 | 0 |
| CMS1173FHIRDiagnosticDelayVTE | cfa235c3-3b8b-4cbb-a78f-5c4fd2af04df | Group_1:Numerator | 0 | 1 |
| CMS1188FHIRHIVSTITesting | 6c08efb1-922f-4e66-98bc-0de25182e723 | Group_1:Numerator | 1 | 0 |
| CMS1188FHIRHIVSTITesting | 77973e2d-625a-4c4f-aa69-2c716af0ad3c | Group_1:Numerator | 1 | 0 |
| CMS122FHIRDiabetesAssessGT9Pct | 3b62b0a8-44f2-4365-bcb9-7cadef5bab2e | Group_1:Denominator Exclusion | 0 | 1 |
| CMS122FHIRDiabetesAssessGT9Pct | 3b62b0a8-44f2-4365-bcb9-7cadef5bab2e | Group_1:Numerator | 1 | 0 |
| CMS122FHIRDiabetesAssessGT9Pct | 9cba6cfa-9671-4850-803d-e286c7d59ee7 | Group_1:Denominator Exclusion | 0 | 1 |
| CMS122FHIRDiabetesAssessGT9Pct | 9cba6cfa-9671-4850-803d-e286c7d59ee7 | Group_1:Numerator | 1 | 0 |
| CMS122FHIRDiabetesAssessGT9Pct | cade5021-b1bf-43e9-a0a4-659c05b386d0 | Group_1:Denominator Exclusion | 0 | 1 |
| CMS122FHIRDiabetesAssessGT9Pct | cade5021-b1bf-43e9-a0a4-659c05b386d0 | Group_1:Numerator | 1 | 0 |
| CMS122FHIRDiabetesAssessGT9Pct | e61be907-af68-493f-a6bc-3d93ef8b6c6e | Group_1:Denominator Exclusion | 0 | 1 |
| CMS122FHIRDiabetesAssessGT9Pct | e61be907-af68-493f-a6bc-3d93ef8b6c6e | Group_1:Numerator | 1 | 0 |
| CMS122FHIRDiabetesAssessGT9Pct | ede0ee7a-18ab-4ba7-934c-23618f1270ea | Group_1:Denominator Exclusion | 0 | 1 |
| CMS122FHIRDiabetesAssessGT9Pct | ede0ee7a-18ab-4ba7-934c-23618f1270ea | Group_1:Numerator | 1 | 0 |
| CMS122FHIRDiabetesAssessGT9Pct | f5771b74-a7de-439a-a51f-49a3863e086b | Group_1:Denominator Exclusion | 0 | 1 |
| CMS122FHIRDiabetesAssessGT9Pct | f5771b74-a7de-439a-a51f-49a3863e086b | Group_1:Numerator | 1 | 0 |
| CMS125FHIRBreastCancerScreen | 0ced1e0c-9c92-4582-a4b1-e44f130e436f | Group_1:Denominator Exclusion | 0 | 1 |
| CMS125FHIRBreastCancerScreen | 14b87edd-7f1e-4f6a-9910-f905966ec904 | Group_1:Denominator Exclusion | 0 | 1 |
| CMS125FHIRBreastCancerScreen | 24557438-17c9-405c-88dc-0c0bfda17d27 | Group_1:Denominator Exclusion | 0 | 1 |
| CMS125FHIRBreastCancerScreen | 5e3f01ad-1eda-4cb7-8d37-1146beae59e9 | Group_1:Denominator Exclusion | 0 | 1 |
| CMS125FHIRBreastCancerScreen | 8278ae07-69ec-469c-ae01-e933d051f764 | Group_1:Denominator Exclusion | 0 | 1 |
| CMS125FHIRBreastCancerScreen | d4540640-2561-4ebd-b7c6-15878a4dc582 | Group_1:Denominator Exclusion | 0 | 1 |

_truncated; showing 50 of 1065 changed rows._
