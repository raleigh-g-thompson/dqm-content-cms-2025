# Cross-Engine Shared-Issue Detection: CMS156FHIRHighRiskMedsElderly

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 2 |
| shared-direction | 0 |
| cms-only | 0 |
| qicore-only | 4 |
| conflicting | 0 |
| incomplete | 0 |
| pass | 702 |
| **total cells** | 708 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 33.3%** (2 / 6).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [4aa75d19-ac8b-49b0-a686-429fbc033d77](../../input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/) | Group_1:Numerator | 1 | 0 | 0 | num | shared |
| [4aa75d19-ac8b-49b0-a686-429fbc033d77](../../input/tests/measure/CMS156FHIRHighRiskMedsElderly/4aa75d19-ac8b-49b0-a686-429fbc033d77/) | Group_3:Numerator | 1 | 0 | 0 | num | shared |
| [07f11229-6e8f-42bf-9905-3d319460fb33](../../input/tests/measure/CMS156FHIRHighRiskMedsElderly/07f11229-6e8f-42bf-9905-3d319460fb33/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [07f11229-6e8f-42bf-9905-3d319460fb33](../../input/tests/measure/CMS156FHIRHighRiskMedsElderly/07f11229-6e8f-42bf-9905-3d319460fb33/) | Group_3:Numerator | 1 | 1 | 0 | num | qicore-only |
| [c409fbc9-a31f-4d53-9aa7-9e443e87812a](../../input/tests/measure/CMS156FHIRHighRiskMedsElderly/c409fbc9-a31f-4d53-9aa7-9e443e87812a/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [c409fbc9-a31f-4d53-9aa7-9e443e87812a](../../input/tests/measure/CMS156FHIRHighRiskMedsElderly/c409fbc9-a31f-4d53-9aa7-9e443e87812a/) | Group_3:Numerator | 1 | 1 | 0 | num | qicore-only |

