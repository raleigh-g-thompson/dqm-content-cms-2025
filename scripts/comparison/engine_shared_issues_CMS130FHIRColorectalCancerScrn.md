# Cross-Engine Shared-Issue Detection: CMS130FHIRColorectalCancerScrn

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 0 |
| shared-direction | 0 |
| cms-only | 0 |
| qicore-only | 1 |
| conflicting | 0 |
| incomplete | 0 |
| pass | 255 |
| **total cells** | 256 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 0.0%** (0 / 1).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [f9ef1fd1-cced-47ad-a47b-d9c20254511c](../../input/tests/measure/CMS130FHIRColorectalCancerScrn/f9ef1fd1-cced-47ad-a47b-d9c20254511c/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |

