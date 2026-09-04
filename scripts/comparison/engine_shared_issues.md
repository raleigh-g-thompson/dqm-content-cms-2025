# Cross-Engine Shared-Issue Detection: CMS69FHIRPCSBMIScreenAndFollowUp

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 0 |
| shared-direction | 0 |
| cms-only | 17 |
| qicore-only | 0 |
| conflicting | 0 |
| incomplete | 0 |
| pass | 298 |
| **total cells** | 315 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 0.0%** (0 / 17).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [050201c2-c2c4-46e6-8288-a34f99caebdc](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/050201c2-c2c4-46e6-8288-a34f99caebdc/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [1102009b-6f05-4bab-9fd1-191e81cf50e8](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/1102009b-6f05-4bab-9fd1-191e81cf50e8/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [1e23fb8f-e27b-4553-a62a-f66edeb4528a](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/1e23fb8f-e27b-4553-a62a-f66edeb4528a/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [27849d59-3cef-40bf-8338-a6ec7c0bcf81](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/27849d59-3cef-40bf-8338-a6ec7c0bcf81/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [42e6b4d6-defc-4ec5-894f-e3333e3039a3](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/42e6b4d6-defc-4ec5-894f-e3333e3039a3/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [461fdfab-fcc1-4630-9dae-2ba3a6ab0c25](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/461fdfab-fcc1-4630-9dae-2ba3a6ab0c25/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [463dd868-997d-472f-962c-96383fd2a5c4](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/463dd868-997d-472f-962c-96383fd2a5c4/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [7902e3dc-f3da-4cc2-9f2e-4a6c8cd33b88](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/7902e3dc-f3da-4cc2-9f2e-4a6c8cd33b88/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [8835a50b-0a0f-4e2f-94fa-7c180cd7f905](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/8835a50b-0a0f-4e2f-94fa-7c180cd7f905/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [8e38b797-4dec-437d-8bf0-6f0fc78f8ea7](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/8e38b797-4dec-437d-8bf0-6f0fc78f8ea7/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [9d92be1d-6fc8-40f2-99a0-4be9ce1f244b](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/9d92be1d-6fc8-40f2-99a0-4be9ce1f244b/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [c3caf126-12a2-473f-8f51-1c7828d63d16](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/c3caf126-12a2-473f-8f51-1c7828d63d16/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [c84bf29f-80ac-4bf0-beeb-404ba96a3fa8](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/c84bf29f-80ac-4bf0-beeb-404ba96a3fa8/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [d4d064be-d55a-47b5-9bfd-993afebd95a5](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/d4d064be-d55a-47b5-9bfd-993afebd95a5/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [e0821eec-ff83-49e9-950d-9219dd3612b9](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/e0821eec-ff83-49e9-950d-9219dd3612b9/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |
| [e25fc2f1-0083-4375-8fc3-9164a5aee53d](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/e25fc2f1-0083-4375-8fc3-9164a5aee53d/) | Group_1:Denominator Exclusion | 1 | 0 | 1 | den | cms-only |
| [f5ae6269-d09b-47f8-a519-f1a8a81549fc](../../input/tests/measure/CMS69FHIRPCSBMIScreenAndFollowUp/f5ae6269-d09b-47f8-a519-f1a8a81549fc/) | Group_1:Numerator | 1 | 0 | 1 | num | cms-only |

