# Cross-Engine Shared-Issue Detection: CMS125FHIRBreastCancerScreen

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 0 |
| shared-direction | 0 |
| cms-only | 0 |
| qicore-only | 8 |
| conflicting | 0 |
| incomplete | 0 |
| pass | 256 |
| **total cells** | 264 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 0.0%** (0 / 8).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [0ced1e0c-9c92-4582-a4b1-e44f130e436f](../../input/tests/measure/CMS125FHIRBreastCancerScreen/0ced1e0c-9c92-4582-a4b1-e44f130e436f/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [14b87edd-7f1e-4f6a-9910-f905966ec904](../../input/tests/measure/CMS125FHIRBreastCancerScreen/14b87edd-7f1e-4f6a-9910-f905966ec904/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [24557438-17c9-405c-88dc-0c0bfda17d27](../../input/tests/measure/CMS125FHIRBreastCancerScreen/24557438-17c9-405c-88dc-0c0bfda17d27/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [5e3f01ad-1eda-4cb7-8d37-1146beae59e9](../../input/tests/measure/CMS125FHIRBreastCancerScreen/5e3f01ad-1eda-4cb7-8d37-1146beae59e9/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [8278ae07-69ec-469c-ae01-e933d051f764](../../input/tests/measure/CMS125FHIRBreastCancerScreen/8278ae07-69ec-469c-ae01-e933d051f764/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [d4540640-2561-4ebd-b7c6-15878a4dc582](../../input/tests/measure/CMS125FHIRBreastCancerScreen/d4540640-2561-4ebd-b7c6-15878a4dc582/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [da85601e-ce6f-4351-b639-1e58c725bf2f](../../input/tests/measure/CMS125FHIRBreastCancerScreen/da85601e-ce6f-4351-b639-1e58c725bf2f/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [f38ce16a-658f-4aa0-b4a6-fac61d2e58a8](../../input/tests/measure/CMS125FHIRBreastCancerScreen/f38ce16a-658f-4aa0-b4a6-fac61d2e58a8/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |

