# Cross-Engine Shared-Issue Detection: CMSFHIR844HybridHospitalWideMortality

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 2 |
| shared-direction | 0 |
| cms-only | 0 |
| qicore-only | 0 |
| conflicting | 0 |
| incomplete | 0 |
| pass | 8 |
| **total cells** | 10 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 100.0%** (2 / 2).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [6f22a06f-7186-4db1-9310-4f907dc49ff3](../../input/tests/measure/CMSFHIR844HybridHospitalWideMortality/6f22a06f-7186-4db1-9310-4f907dc49ff3/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [af1b9448-3e7a-4b7f-8934-15bb63258b75](../../input/tests/measure/CMSFHIR844HybridHospitalWideMortality/af1b9448-3e7a-4b7f-8934-15bb63258b75/) | Group_1:Initial Population | 2 | 1 | 1 | ini | shared |

