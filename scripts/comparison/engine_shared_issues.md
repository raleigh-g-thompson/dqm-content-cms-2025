# Cross-Engine Shared-Issue Detection: CMS68FHIRDocumentationCurrentMeds

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 0 |
| shared-direction | 0 |
| cms-only | 0 |
| qicore-only | 0 |
| conflicting | 0 |
| incomplete | 4 |
| pass | 72 |
| **total cells** | 76 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 0.0%** (0 / 0).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [f2e2e1c0-9e35-4592-9579-72a236cb2f56](../../input/tests/measure/CMS68FHIRDocumentationCurrentMeds/f2e2e1c0-9e35-4592-9579-72a236cb2f56/) | Group_1:Denominator | 1 | None | 1 | den | incomplete |
| [f2e2e1c0-9e35-4592-9579-72a236cb2f56](../../input/tests/measure/CMS68FHIRDocumentationCurrentMeds/f2e2e1c0-9e35-4592-9579-72a236cb2f56/) | Group_1:Denominator Exception | 1 | None | 1 | den | incomplete |
| [f2e2e1c0-9e35-4592-9579-72a236cb2f56](../../input/tests/measure/CMS68FHIRDocumentationCurrentMeds/f2e2e1c0-9e35-4592-9579-72a236cb2f56/) | Group_1:Initial Population | 1 | None | 1 | ini | incomplete |
| [f2e2e1c0-9e35-4592-9579-72a236cb2f56](../../input/tests/measure/CMS68FHIRDocumentationCurrentMeds/f2e2e1c0-9e35-4592-9579-72a236cb2f56/) | Group_1:Numerator | 0 | None | 0 | num | incomplete |

