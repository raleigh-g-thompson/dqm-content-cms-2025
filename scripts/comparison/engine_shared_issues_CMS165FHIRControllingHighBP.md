# Cross-Engine Shared-Issue Detection: CMS165FHIRControllingHighBP

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 0 |
| shared-direction | 0 |
| cms-only | 2 |
| qicore-only | 9 |
| conflicting | 0 |
| incomplete | 4 |
| pass | 257 |
| **total cells** | 272 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 0.0%** (0 / 11).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [6f37e357-7575-4b40-a63e-4b882532250f](../../input/tests/measure/CMS165FHIRControllingHighBP/6f37e357-7575-4b40-a63e-4b882532250f/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [f2d1fd7e-35ae-45cd-86e6-8b874c3e3fb9](../../input/tests/measure/CMS165FHIRControllingHighBP/f2d1fd7e-35ae-45cd-86e6-8b874c3e3fb9/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [1905549a-1783-4195-95b9-b0879cb81d96](../../input/tests/measure/CMS165FHIRControllingHighBP/1905549a-1783-4195-95b9-b0879cb81d96/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [2c55811b-1571-43e5-919c-f90bf763b3d4](../../input/tests/measure/CMS165FHIRControllingHighBP/2c55811b-1571-43e5-919c-f90bf763b3d4/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [474b2964-23a1-4c77-ad16-8a21543b2ed3](../../input/tests/measure/CMS165FHIRControllingHighBP/474b2964-23a1-4c77-ad16-8a21543b2ed3/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [5421e420-8d42-4628-ba47-9abaf9ebfaa8](../../input/tests/measure/CMS165FHIRControllingHighBP/5421e420-8d42-4628-ba47-9abaf9ebfaa8/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [59d7f239-7614-4e6e-a973-fe107aee5749](../../input/tests/measure/CMS165FHIRControllingHighBP/59d7f239-7614-4e6e-a973-fe107aee5749/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [686e2c47-b08f-465c-ab31-1712dd72028b](../../input/tests/measure/CMS165FHIRControllingHighBP/686e2c47-b08f-465c-ab31-1712dd72028b/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [7c59efb5-56ab-4a25-af83-bd81daeee026](../../input/tests/measure/CMS165FHIRControllingHighBP/7c59efb5-56ab-4a25-af83-bd81daeee026/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [9f063f76-a97a-4bba-9f6a-35e7a429a72c](../../input/tests/measure/CMS165FHIRControllingHighBP/9f063f76-a97a-4bba-9f6a-35e7a429a72c/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [a7ec972f-f0c1-428d-aba5-ba76cba5cd73](../../input/tests/measure/CMS165FHIRControllingHighBP/a7ec972f-f0c1-428d-aba5-ba76cba5cd73/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [45e01fed-56bb-483d-a860-af3d566bda11](../../input/tests/measure/CMS165FHIRControllingHighBP/45e01fed-56bb-483d-a860-af3d566bda11/) | Group_1:Denominator | 1 | None | None | den | incomplete |
| [45e01fed-56bb-483d-a860-af3d566bda11](../../input/tests/measure/CMS165FHIRControllingHighBP/45e01fed-56bb-483d-a860-af3d566bda11/) | Group_1:Denominator Exclusion | 1 | None | None | den | incomplete |
| [45e01fed-56bb-483d-a860-af3d566bda11](../../input/tests/measure/CMS165FHIRControllingHighBP/45e01fed-56bb-483d-a860-af3d566bda11/) | Group_1:Initial Population | 1 | None | None | ini | incomplete |
| [45e01fed-56bb-483d-a860-af3d566bda11](../../input/tests/measure/CMS165FHIRControllingHighBP/45e01fed-56bb-483d-a860-af3d566bda11/) | Group_1:Numerator | 0 | None | None | num | incomplete |

