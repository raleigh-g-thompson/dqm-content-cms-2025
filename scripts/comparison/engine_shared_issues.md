# Cross-Engine Shared-Issue Detection: CMS157FHIRPainIntensityQuantified

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 46 |
| shared-direction | 0 |
| cms-only | 0 |
| qicore-only | 0 |
| conflicting | 0 |
| incomplete | 0 |
| pass | 332 |
| **total cells** | 378 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 100.0%** (46 / 46).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [055640ae-dc71-4e1d-918b-e367013de209](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/055640ae-dc71-4e1d-918b-e367013de209/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [055640ae-dc71-4e1d-918b-e367013de209](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/055640ae-dc71-4e1d-918b-e367013de209/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [233d84af-d725-4682-8253-d6c4e02da0d5](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/233d84af-d725-4682-8253-d6c4e02da0d5/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [233d84af-d725-4682-8253-d6c4e02da0d5](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/233d84af-d725-4682-8253-d6c4e02da0d5/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [233d84af-d725-4682-8253-d6c4e02da0d5](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/233d84af-d725-4682-8253-d6c4e02da0d5/) | Group_1:Numerator | 1 | 0 | 0 | num | shared |
| [2c3f5ac5-6b7f-4bb8-a4fe-8faf0553b21e](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/2c3f5ac5-6b7f-4bb8-a4fe-8faf0553b21e/) | Group_2:Denominator | 2 | 0 | 0 | den | shared |
| [2c3f5ac5-6b7f-4bb8-a4fe-8faf0553b21e](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/2c3f5ac5-6b7f-4bb8-a4fe-8faf0553b21e/) | Group_2:Initial Population | 2 | 0 | 0 | ini | shared |
| [51d8547c-f07f-4441-b616-f458f38e4506](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/51d8547c-f07f-4441-b616-f458f38e4506/) | Group_2:Denominator | 1 | 0 | 0 | den | shared |
| [51d8547c-f07f-4441-b616-f458f38e4506](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/51d8547c-f07f-4441-b616-f458f38e4506/) | Group_2:Initial Population | 1 | 0 | 0 | ini | shared |
| [51d8547c-f07f-4441-b616-f458f38e4506](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/51d8547c-f07f-4441-b616-f458f38e4506/) | Group_2:Numerator | 1 | 0 | 0 | num | shared |
| [5cca62ff-f856-4b8f-9902-6a018a4599cb](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/5cca62ff-f856-4b8f-9902-6a018a4599cb/) | Group_2:Denominator | 2 | 0 | 0 | den | shared |
| [5cca62ff-f856-4b8f-9902-6a018a4599cb](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/5cca62ff-f856-4b8f-9902-6a018a4599cb/) | Group_2:Initial Population | 2 | 0 | 0 | ini | shared |
| [5cca62ff-f856-4b8f-9902-6a018a4599cb](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/5cca62ff-f856-4b8f-9902-6a018a4599cb/) | Group_2:Numerator | 1 | 0 | 0 | num | shared |
| [66c60f6c-2a7b-4868-b9bd-5ede60b61463](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/66c60f6c-2a7b-4868-b9bd-5ede60b61463/) | Group_2:Denominator | 1 | 0 | 0 | den | shared |
| [66c60f6c-2a7b-4868-b9bd-5ede60b61463](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/66c60f6c-2a7b-4868-b9bd-5ede60b61463/) | Group_2:Initial Population | 1 | 0 | 0 | ini | shared |
| [719a6ae4-ac86-406f-a762-380383e4a74d](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/719a6ae4-ac86-406f-a762-380383e4a74d/) | Group_1:Denominator | 2 | 0 | 0 | den | shared |
| [719a6ae4-ac86-406f-a762-380383e4a74d](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/719a6ae4-ac86-406f-a762-380383e4a74d/) | Group_1:Initial Population | 2 | 0 | 0 | ini | shared |
| [719a6ae4-ac86-406f-a762-380383e4a74d](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/719a6ae4-ac86-406f-a762-380383e4a74d/) | Group_1:Numerator | 1 | 0 | 0 | num | shared |
| [757c5855-602e-4c25-8783-c22afccc1618](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/757c5855-602e-4c25-8783-c22afccc1618/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [757c5855-602e-4c25-8783-c22afccc1618](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/757c5855-602e-4c25-8783-c22afccc1618/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [7cedf97f-741c-4c37-9ae9-40e0b8c64576](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/7cedf97f-741c-4c37-9ae9-40e0b8c64576/) | Group_2:Denominator | 1 | 0 | 0 | den | shared |
| [7cedf97f-741c-4c37-9ae9-40e0b8c64576](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/7cedf97f-741c-4c37-9ae9-40e0b8c64576/) | Group_2:Initial Population | 1 | 0 | 0 | ini | shared |
| [7cedf97f-741c-4c37-9ae9-40e0b8c64576](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/7cedf97f-741c-4c37-9ae9-40e0b8c64576/) | Group_2:Numerator | 1 | 0 | 0 | num | shared |
| [837cc0e4-cc26-48cd-9d34-232d7fbcd056](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/837cc0e4-cc26-48cd-9d34-232d7fbcd056/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [837cc0e4-cc26-48cd-9d34-232d7fbcd056](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/837cc0e4-cc26-48cd-9d34-232d7fbcd056/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [8e23417a-471a-45bb-b936-57466dc6592c](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/8e23417a-471a-45bb-b936-57466dc6592c/) | Group_1:Denominator | 2 | 0 | 0 | den | shared |
| [8e23417a-471a-45bb-b936-57466dc6592c](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/8e23417a-471a-45bb-b936-57466dc6592c/) | Group_1:Initial Population | 2 | 0 | 0 | ini | shared |
| [8e23417a-471a-45bb-b936-57466dc6592c](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/8e23417a-471a-45bb-b936-57466dc6592c/) | Group_1:Numerator | 1 | 0 | 0 | num | shared |
| [90d3454a-ca4b-4035-a524-255a2f03bef7](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/90d3454a-ca4b-4035-a524-255a2f03bef7/) | Group_1:Denominator | 2 | 0 | 0 | den | shared |
| [90d3454a-ca4b-4035-a524-255a2f03bef7](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/90d3454a-ca4b-4035-a524-255a2f03bef7/) | Group_1:Initial Population | 2 | 0 | 0 | ini | shared |
| [90d3454a-ca4b-4035-a524-255a2f03bef7](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/90d3454a-ca4b-4035-a524-255a2f03bef7/) | Group_1:Numerator | 2 | 0 | 0 | num | shared |
| [9972f780-aa2f-40e0-ba7d-133d7fe38bc9](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/9972f780-aa2f-40e0-ba7d-133d7fe38bc9/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [9972f780-aa2f-40e0-ba7d-133d7fe38bc9](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/9972f780-aa2f-40e0-ba7d-133d7fe38bc9/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [aa355e31-8d29-4b06-8d13-7d00a2c817da](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/aa355e31-8d29-4b06-8d13-7d00a2c817da/) | Group_2:Denominator | 1 | 0 | 0 | den | shared |
| [aa355e31-8d29-4b06-8d13-7d00a2c817da](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/aa355e31-8d29-4b06-8d13-7d00a2c817da/) | Group_2:Initial Population | 1 | 0 | 0 | ini | shared |
| [c97c9ecf-6c31-4868-bbd3-7a5509bb3882](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/c97c9ecf-6c31-4868-bbd3-7a5509bb3882/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [c97c9ecf-6c31-4868-bbd3-7a5509bb3882](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/c97c9ecf-6c31-4868-bbd3-7a5509bb3882/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [d4b441fb-5b3a-40f7-ada1-ecf06376f4fb](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/d4b441fb-5b3a-40f7-ada1-ecf06376f4fb/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [d4b441fb-5b3a-40f7-ada1-ecf06376f4fb](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/d4b441fb-5b3a-40f7-ada1-ecf06376f4fb/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [e085c0d1-a736-4596-a5cd-7de785d0d144](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/e085c0d1-a736-4596-a5cd-7de785d0d144/) | Group_2:Denominator | 1 | 0 | 0 | den | shared |
| [e085c0d1-a736-4596-a5cd-7de785d0d144](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/e085c0d1-a736-4596-a5cd-7de785d0d144/) | Group_2:Initial Population | 1 | 0 | 0 | ini | shared |
| [e085c0d1-a736-4596-a5cd-7de785d0d144](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/e085c0d1-a736-4596-a5cd-7de785d0d144/) | Group_2:Numerator | 1 | 0 | 0 | num | shared |
| [ede0d103-285f-42f0-807e-ff272f1ae70e](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/ede0d103-285f-42f0-807e-ff272f1ae70e/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [ede0d103-285f-42f0-807e-ff272f1ae70e](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/ede0d103-285f-42f0-807e-ff272f1ae70e/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [fe6ef07d-bff1-4e0e-9bf4-b0424a1d0ab4](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/fe6ef07d-bff1-4e0e-9bf4-b0424a1d0ab4/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [fe6ef07d-bff1-4e0e-9bf4-b0424a1d0ab4](../../input/tests/measure/CMS157FHIRPainIntensityQuantified/fe6ef07d-bff1-4e0e-9bf4-b0424a1d0ab4/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |

