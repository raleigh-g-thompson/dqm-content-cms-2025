# Cross-Engine Shared-Issue Detection: CMS131FHIRDiabetesEyeExam

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 0 |
| shared-direction | 0 |
| cms-only | 0 |
| qicore-only | 6 |
| conflicting | 0 |
| incomplete | 0 |
| pass | 246 |
| **total cells** | 252 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 0.0%** (0 / 6).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [01a1241d-fd97-4c72-b288-fd31c4c7ae80](../../input/tests/measure/CMS131FHIRDiabetesEyeExam/01a1241d-fd97-4c72-b288-fd31c4c7ae80/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [4eaa0238-d22c-44c2-a91e-81239a497359](../../input/tests/measure/CMS131FHIRDiabetesEyeExam/4eaa0238-d22c-44c2-a91e-81239a497359/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [61dfb0bd-8fe0-4e30-a911-fa07c782afd9](../../input/tests/measure/CMS131FHIRDiabetesEyeExam/61dfb0bd-8fe0-4e30-a911-fa07c782afd9/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [8ffd1c24-67a9-4991-86cb-3378a45ffd6e](../../input/tests/measure/CMS131FHIRDiabetesEyeExam/8ffd1c24-67a9-4991-86cb-3378a45ffd6e/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [d4091ecf-638c-41ae-bae9-2b0c3bea864e](../../input/tests/measure/CMS131FHIRDiabetesEyeExam/d4091ecf-638c-41ae-bae9-2b0c3bea864e/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [f45a1cb0-d1a7-42cf-9cae-6ea6c7799085](../../input/tests/measure/CMS131FHIRDiabetesEyeExam/f45a1cb0-d1a7-42cf-9cae-6ea6c7799085/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |

