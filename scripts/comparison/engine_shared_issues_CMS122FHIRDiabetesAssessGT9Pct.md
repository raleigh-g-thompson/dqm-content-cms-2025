# Cross-Engine Shared-Issue Detection: CMS122FHIRDiabetesAssessGT9Pct

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 0 |
| shared-direction | 0 |
| cms-only | 0 |
| qicore-only | 12 |
| conflicting | 0 |
| incomplete | 0 |
| pass | 208 |
| **total cells** | 220 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 0.0%** (0 / 12).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [3b62b0a8-44f2-4365-bcb9-7cadef5bab2e](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/3b62b0a8-44f2-4365-bcb9-7cadef5bab2e/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [3b62b0a8-44f2-4365-bcb9-7cadef5bab2e](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/3b62b0a8-44f2-4365-bcb9-7cadef5bab2e/) | Group_1:Numerator | 0 | 0 | 1 | num | qicore-only |
| [9cba6cfa-9671-4850-803d-e286c7d59ee7](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/9cba6cfa-9671-4850-803d-e286c7d59ee7/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [9cba6cfa-9671-4850-803d-e286c7d59ee7](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/9cba6cfa-9671-4850-803d-e286c7d59ee7/) | Group_1:Numerator | 0 | 0 | 1 | num | qicore-only |
| [cade5021-b1bf-43e9-a0a4-659c05b386d0](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/cade5021-b1bf-43e9-a0a4-659c05b386d0/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [cade5021-b1bf-43e9-a0a4-659c05b386d0](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/cade5021-b1bf-43e9-a0a4-659c05b386d0/) | Group_1:Numerator | 0 | 0 | 1 | num | qicore-only |
| [e61be907-af68-493f-a6bc-3d93ef8b6c6e](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/e61be907-af68-493f-a6bc-3d93ef8b6c6e/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [e61be907-af68-493f-a6bc-3d93ef8b6c6e](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/e61be907-af68-493f-a6bc-3d93ef8b6c6e/) | Group_1:Numerator | 0 | 0 | 1 | num | qicore-only |
| [ede0ee7a-18ab-4ba7-934c-23618f1270ea](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/ede0ee7a-18ab-4ba7-934c-23618f1270ea/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [ede0ee7a-18ab-4ba7-934c-23618f1270ea](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/ede0ee7a-18ab-4ba7-934c-23618f1270ea/) | Group_1:Numerator | 0 | 0 | 1 | num | qicore-only |
| [f5771b74-a7de-439a-a51f-49a3863e086b](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/f5771b74-a7de-439a-a51f-49a3863e086b/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [f5771b74-a7de-439a-a51f-49a3863e086b](../../input/tests/measure/CMS122FHIRDiabetesAssessGT9Pct/f5771b74-a7de-439a-a51f-49a3863e086b/) | Group_1:Numerator | 0 | 0 | 1 | num | qicore-only |

