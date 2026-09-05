# Cross-Engine Shared-Issue Detection: CMS1017FHIRHHFI

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 87 |
| shared-direction | 0 |
| cms-only | 0 |
| qicore-only | 0 |
| conflicting | 0 |
| incomplete | 4 |
| pass | 325 |
| **total cells** | 416 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 100.0%** (87 / 87).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [02d5c5f5-9487-42af-bb5e-dfc3aaeb70eb](../../input/tests/measure/CMS1017FHIRHHFI/02d5c5f5-9487-42af-bb5e-dfc3aaeb70eb/) | Group_1:Denominator Observation | 2 | 0 | 0 | den | shared |
| [02d5c5f5-9487-42af-bb5e-dfc3aaeb70eb](../../input/tests/measure/CMS1017FHIRHHFI/02d5c5f5-9487-42af-bb5e-dfc3aaeb70eb/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [0653f9dc-8461-484e-a901-f17709f04776](../../input/tests/measure/CMS1017FHIRHHFI/0653f9dc-8461-484e-a901-f17709f04776/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [0884b17b-baa4-47c0-a2b6-5849230dcf43](../../input/tests/measure/CMS1017FHIRHHFI/0884b17b-baa4-47c0-a2b6-5849230dcf43/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [0884b17b-baa4-47c0-a2b6-5849230dcf43](../../input/tests/measure/CMS1017FHIRHHFI/0884b17b-baa4-47c0-a2b6-5849230dcf43/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [0dfafc1a-cf94-4ca1-becf-c1b843896810](../../input/tests/measure/CMS1017FHIRHHFI/0dfafc1a-cf94-4ca1-becf-c1b843896810/) | Group_1:Denominator Observation | 7 | 0 | 0 | den | shared |
| [0dfafc1a-cf94-4ca1-becf-c1b843896810](../../input/tests/measure/CMS1017FHIRHHFI/0dfafc1a-cf94-4ca1-becf-c1b843896810/) | Group_1:Numerator Exclusion | 0 | 1 | 1 | num | shared |
| [0dfafc1a-cf94-4ca1-becf-c1b843896810](../../input/tests/measure/CMS1017FHIRHHFI/0dfafc1a-cf94-4ca1-becf-c1b843896810/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [113d43da-4cad-4907-804e-63afb7652b27](../../input/tests/measure/CMS1017FHIRHHFI/113d43da-4cad-4907-804e-63afb7652b27/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [113d43da-4cad-4907-804e-63afb7652b27](../../input/tests/measure/CMS1017FHIRHHFI/113d43da-4cad-4907-804e-63afb7652b27/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [1ac96e3e-856c-417a-9c68-0df860ce73c8](../../input/tests/measure/CMS1017FHIRHHFI/1ac96e3e-856c-417a-9c68-0df860ce73c8/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [1b700def-98b0-418c-b0a7-80ecb275597f](../../input/tests/measure/CMS1017FHIRHHFI/1b700def-98b0-418c-b0a7-80ecb275597f/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [1e2ddc95-76d4-4be7-8273-2b35371a727b](../../input/tests/measure/CMS1017FHIRHHFI/1e2ddc95-76d4-4be7-8273-2b35371a727b/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [25c753a7-b6b4-4335-bd7c-05b68b0324a4](../../input/tests/measure/CMS1017FHIRHHFI/25c753a7-b6b4-4335-bd7c-05b68b0324a4/) | Group_1:Denominator Observation | 2 | 0 | 0 | den | shared |
| [25c753a7-b6b4-4335-bd7c-05b68b0324a4](../../input/tests/measure/CMS1017FHIRHHFI/25c753a7-b6b4-4335-bd7c-05b68b0324a4/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [28684639-1aa3-429c-9cac-4e8217995b13](../../input/tests/measure/CMS1017FHIRHHFI/28684639-1aa3-429c-9cac-4e8217995b13/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [3279a237-292f-47b1-9039-ba46b57e765a](../../input/tests/measure/CMS1017FHIRHHFI/3279a237-292f-47b1-9039-ba46b57e765a/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [365ed821-88d1-4459-9d32-dd4fa6426335](../../input/tests/measure/CMS1017FHIRHHFI/365ed821-88d1-4459-9d32-dd4fa6426335/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [38d7ec48-dc28-4875-8f24-451ecd3dab5a](../../input/tests/measure/CMS1017FHIRHHFI/38d7ec48-dc28-4875-8f24-451ecd3dab5a/) | Group_1:Denominator Observation | 3 | 0 | 0 | den | shared |
| [3c34a5af-1ef1-4b86-a41f-ac1d44e96ca0](../../input/tests/measure/CMS1017FHIRHHFI/3c34a5af-1ef1-4b86-a41f-ac1d44e96ca0/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [3ee27450-2fd5-4930-bfbb-e718074e4087](../../input/tests/measure/CMS1017FHIRHHFI/3ee27450-2fd5-4930-bfbb-e718074e4087/) | Group_1:Denominator Observation | 1 | 0 | 0 | den | shared |
| [404570c9-b21f-4fa2-be5d-6d02c910fea6](../../input/tests/measure/CMS1017FHIRHHFI/404570c9-b21f-4fa2-be5d-6d02c910fea6/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [4402a9b9-3d48-4472-a000-579b7baa88fa](../../input/tests/measure/CMS1017FHIRHHFI/4402a9b9-3d48-4472-a000-579b7baa88fa/) | Group_1:Denominator Observation | 12 | 0 | 0 | den | shared |
| [4402a9b9-3d48-4472-a000-579b7baa88fa](../../input/tests/measure/CMS1017FHIRHHFI/4402a9b9-3d48-4472-a000-579b7baa88fa/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [55931284-058b-4776-882c-720caddd3366](../../input/tests/measure/CMS1017FHIRHHFI/55931284-058b-4776-882c-720caddd3366/) | Group_1:Denominator Observation | 2 | 0 | 0 | den | shared |
| [55931284-058b-4776-882c-720caddd3366](../../input/tests/measure/CMS1017FHIRHHFI/55931284-058b-4776-882c-720caddd3366/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [55b18e1a-c949-4d85-b1c5-caa91fc6ac4b](../../input/tests/measure/CMS1017FHIRHHFI/55b18e1a-c949-4d85-b1c5-caa91fc6ac4b/) | Group_1:Denominator Observation | 2 | 0 | 0 | den | shared |
| [55b18e1a-c949-4d85-b1c5-caa91fc6ac4b](../../input/tests/measure/CMS1017FHIRHHFI/55b18e1a-c949-4d85-b1c5-caa91fc6ac4b/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [56d5fe48-53b2-4599-870c-58bfc4ba0145](../../input/tests/measure/CMS1017FHIRHHFI/56d5fe48-53b2-4599-870c-58bfc4ba0145/) | Group_1:Denominator Observation | 12 | 0 | 0 | den | shared |
| [56d5fe48-53b2-4599-870c-58bfc4ba0145](../../input/tests/measure/CMS1017FHIRHHFI/56d5fe48-53b2-4599-870c-58bfc4ba0145/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [58079a8d-2808-4029-b1fb-67ab4a841aa9](../../input/tests/measure/CMS1017FHIRHHFI/58079a8d-2808-4029-b1fb-67ab4a841aa9/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [58079a8d-2808-4029-b1fb-67ab4a841aa9](../../input/tests/measure/CMS1017FHIRHHFI/58079a8d-2808-4029-b1fb-67ab4a841aa9/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [5926d62e-8eb9-4742-9bdf-efdcbd08ab7b](../../input/tests/measure/CMS1017FHIRHHFI/5926d62e-8eb9-4742-9bdf-efdcbd08ab7b/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [5926d62e-8eb9-4742-9bdf-efdcbd08ab7b](../../input/tests/measure/CMS1017FHIRHHFI/5926d62e-8eb9-4742-9bdf-efdcbd08ab7b/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [5d7d44f5-aa75-4889-89d6-e7586ac5de4a](../../input/tests/measure/CMS1017FHIRHHFI/5d7d44f5-aa75-4889-89d6-e7586ac5de4a/) | Group_1:Denominator Observation | 2 | 0 | 0 | den | shared |
| [5d7d44f5-aa75-4889-89d6-e7586ac5de4a](../../input/tests/measure/CMS1017FHIRHHFI/5d7d44f5-aa75-4889-89d6-e7586ac5de4a/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [5ff2713d-ca89-42ae-91bb-cba3e1d9a487](../../input/tests/measure/CMS1017FHIRHHFI/5ff2713d-ca89-42ae-91bb-cba3e1d9a487/) | Group_1:Denominator Observation | 5 | 0 | 0 | den | shared |
| [5ff2713d-ca89-42ae-91bb-cba3e1d9a487](../../input/tests/measure/CMS1017FHIRHHFI/5ff2713d-ca89-42ae-91bb-cba3e1d9a487/) | Group_1:Numerator Exclusion | 0 | 1 | 1 | num | shared |
| [5ff2713d-ca89-42ae-91bb-cba3e1d9a487](../../input/tests/measure/CMS1017FHIRHHFI/5ff2713d-ca89-42ae-91bb-cba3e1d9a487/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [632475eb-a20a-43d6-baa7-f377ef8c5324](../../input/tests/measure/CMS1017FHIRHHFI/632475eb-a20a-43d6-baa7-f377ef8c5324/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [6442de45-b65d-4bda-8143-0e9c28d19485](../../input/tests/measure/CMS1017FHIRHHFI/6442de45-b65d-4bda-8143-0e9c28d19485/) | Group_1:Denominator Observation | 2 | 0 | 0 | den | shared |
| [65163106-0b19-4548-a994-f44b35e162e0](../../input/tests/measure/CMS1017FHIRHHFI/65163106-0b19-4548-a994-f44b35e162e0/) | Group_1:Denominator Observation | 2 | 0 | 0 | den | shared |
| [65163106-0b19-4548-a994-f44b35e162e0](../../input/tests/measure/CMS1017FHIRHHFI/65163106-0b19-4548-a994-f44b35e162e0/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [66ee842d-d852-42dd-928e-7f9bff5c52cd](../../input/tests/measure/CMS1017FHIRHHFI/66ee842d-d852-42dd-928e-7f9bff5c52cd/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [6916a60d-ffa4-4d2c-8d96-73a31aa18854](../../input/tests/measure/CMS1017FHIRHHFI/6916a60d-ffa4-4d2c-8d96-73a31aa18854/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [735a7993-89b8-4b21-87c2-d5f57df0f5a8](../../input/tests/measure/CMS1017FHIRHHFI/735a7993-89b8-4b21-87c2-d5f57df0f5a8/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [735a7993-89b8-4b21-87c2-d5f57df0f5a8](../../input/tests/measure/CMS1017FHIRHHFI/735a7993-89b8-4b21-87c2-d5f57df0f5a8/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [7c4f1e55-2462-45e5-9062-5cf8d04b40e2](../../input/tests/measure/CMS1017FHIRHHFI/7c4f1e55-2462-45e5-9062-5cf8d04b40e2/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [7c4f1e55-2462-45e5-9062-5cf8d04b40e2](../../input/tests/measure/CMS1017FHIRHHFI/7c4f1e55-2462-45e5-9062-5cf8d04b40e2/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [8045dee0-645e-497b-a5e8-ad659cdcf8c6](../../input/tests/measure/CMS1017FHIRHHFI/8045dee0-645e-497b-a5e8-ad659cdcf8c6/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [8045dee0-645e-497b-a5e8-ad659cdcf8c6](../../input/tests/measure/CMS1017FHIRHHFI/8045dee0-645e-497b-a5e8-ad659cdcf8c6/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [8552c09f-c2db-4069-9d15-41efafe4711a](../../input/tests/measure/CMS1017FHIRHHFI/8552c09f-c2db-4069-9d15-41efafe4711a/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [8b607dee-4e17-492d-9949-0c69c10587e3](../../input/tests/measure/CMS1017FHIRHHFI/8b607dee-4e17-492d-9949-0c69c10587e3/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [8b63d691-46a8-4ce6-8dee-60aea7f34f82](../../input/tests/measure/CMS1017FHIRHHFI/8b63d691-46a8-4ce6-8dee-60aea7f34f82/) | Group_1:Denominator Observation | 1 | 0 | 0 | den | shared |
| [966cc666-8f6e-4d6e-93a9-0c4b6345e966](../../input/tests/measure/CMS1017FHIRHHFI/966cc666-8f6e-4d6e-93a9-0c4b6345e966/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [966cc666-8f6e-4d6e-93a9-0c4b6345e966](../../input/tests/measure/CMS1017FHIRHHFI/966cc666-8f6e-4d6e-93a9-0c4b6345e966/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [972573e8-bd51-4b77-a954-39babec1a055](../../input/tests/measure/CMS1017FHIRHHFI/972573e8-bd51-4b77-a954-39babec1a055/) | Group_1:Denominator Observation | 3 | 0 | 0 | den | shared |
| [972573e8-bd51-4b77-a954-39babec1a055](../../input/tests/measure/CMS1017FHIRHHFI/972573e8-bd51-4b77-a954-39babec1a055/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [9897529f-07ba-42c6-a7b2-5d856a925a6a](../../input/tests/measure/CMS1017FHIRHHFI/9897529f-07ba-42c6-a7b2-5d856a925a6a/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [a0c41f8f-c8d9-4ae5-a07d-e87b16ae1367](../../input/tests/measure/CMS1017FHIRHHFI/a0c41f8f-c8d9-4ae5-a07d-e87b16ae1367/) | Group_1:Denominator Observation | 11 | 0 | 0 | den | shared |
| [a0c41f8f-c8d9-4ae5-a07d-e87b16ae1367](../../input/tests/measure/CMS1017FHIRHHFI/a0c41f8f-c8d9-4ae5-a07d-e87b16ae1367/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [a2b51ea0-5a72-4bda-abe5-dd393bfa0545](../../input/tests/measure/CMS1017FHIRHHFI/a2b51ea0-5a72-4bda-abe5-dd393bfa0545/) | Group_1:Denominator Observation | 12 | 0 | 0 | den | shared |
| [a2b51ea0-5a72-4bda-abe5-dd393bfa0545](../../input/tests/measure/CMS1017FHIRHHFI/a2b51ea0-5a72-4bda-abe5-dd393bfa0545/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [c9413f99-840f-449d-b4ab-427fb7de6aa0](../../input/tests/measure/CMS1017FHIRHHFI/c9413f99-840f-449d-b4ab-427fb7de6aa0/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [ca728a1a-9a25-46b6-80bc-bfffae233f6c](../../input/tests/measure/CMS1017FHIRHHFI/ca728a1a-9a25-46b6-80bc-bfffae233f6c/) | Group_1:Denominator Observation | 12 | 0 | 0 | den | shared |
| [ca728a1a-9a25-46b6-80bc-bfffae233f6c](../../input/tests/measure/CMS1017FHIRHHFI/ca728a1a-9a25-46b6-80bc-bfffae233f6c/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [d305cce9-ad3c-4259-9f9d-6026974fa847](../../input/tests/measure/CMS1017FHIRHHFI/d305cce9-ad3c-4259-9f9d-6026974fa847/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [d305cce9-ad3c-4259-9f9d-6026974fa847](../../input/tests/measure/CMS1017FHIRHHFI/d305cce9-ad3c-4259-9f9d-6026974fa847/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [d62aa527-2547-48a1-aece-d649b05c7d6e](../../input/tests/measure/CMS1017FHIRHHFI/d62aa527-2547-48a1-aece-d649b05c7d6e/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [e287cd76-85bd-4f51-9c41-f19551e83c14](../../input/tests/measure/CMS1017FHIRHHFI/e287cd76-85bd-4f51-9c41-f19551e83c14/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [e3212bfa-f3a9-4323-8993-9ef74c2f8d89](../../input/tests/measure/CMS1017FHIRHHFI/e3212bfa-f3a9-4323-8993-9ef74c2f8d89/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [e3212bfa-f3a9-4323-8993-9ef74c2f8d89](../../input/tests/measure/CMS1017FHIRHHFI/e3212bfa-f3a9-4323-8993-9ef74c2f8d89/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [e6383b7c-aa91-42f7-8dc8-693a8c7dcaf3](../../input/tests/measure/CMS1017FHIRHHFI/e6383b7c-aa91-42f7-8dc8-693a8c7dcaf3/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [e6383b7c-aa91-42f7-8dc8-693a8c7dcaf3](../../input/tests/measure/CMS1017FHIRHHFI/e6383b7c-aa91-42f7-8dc8-693a8c7dcaf3/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [e6d91b78-a66f-4fcc-a9a7-edb3d862803e](../../input/tests/measure/CMS1017FHIRHHFI/e6d91b78-a66f-4fcc-a9a7-edb3d862803e/) | Group_1:Denominator Observation | 3 | 0 | 0 | den | shared |
| [e6d91b78-a66f-4fcc-a9a7-edb3d862803e](../../input/tests/measure/CMS1017FHIRHHFI/e6d91b78-a66f-4fcc-a9a7-edb3d862803e/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [ea6b3f44-c6cd-4417-ae60-e97564bf24f9](../../input/tests/measure/CMS1017FHIRHHFI/ea6b3f44-c6cd-4417-ae60-e97564bf24f9/) | Group_1:Denominator Observation | 5 | 0 | 0 | den | shared |
| [f18417e1-5990-40b7-b927-5b50015380a2](../../input/tests/measure/CMS1017FHIRHHFI/f18417e1-5990-40b7-b927-5b50015380a2/) | Group_1:Denominator Observation | 2 | 0 | 0 | den | shared |
| [f18417e1-5990-40b7-b927-5b50015380a2](../../input/tests/measure/CMS1017FHIRHHFI/f18417e1-5990-40b7-b927-5b50015380a2/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [f4708272-e5d3-4b6a-9431-a3cd2eb6809f](../../input/tests/measure/CMS1017FHIRHHFI/f4708272-e5d3-4b6a-9431-a3cd2eb6809f/) | Group_1:Denominator Observation | 6 | 0 | 0 | den | shared |
| [f771a339-74f0-4651-90f3-1c820edea547](../../input/tests/measure/CMS1017FHIRHHFI/f771a339-74f0-4651-90f3-1c820edea547/) | Group_1:Denominator Observation | 12 | 0 | 0 | den | shared |
| [f771a339-74f0-4651-90f3-1c820edea547](../../input/tests/measure/CMS1017FHIRHHFI/f771a339-74f0-4651-90f3-1c820edea547/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [f90151aa-9bd6-4c0c-bed8-5d43fe7fb8bb](../../input/tests/measure/CMS1017FHIRHHFI/f90151aa-9bd6-4c0c-bed8-5d43fe7fb8bb/) | Group_1:Denominator Observation | 2 | 0 | 0 | den | shared |
| [fbbd3f4a-3e8e-40b9-ac83-0e80b2c129ec](../../input/tests/measure/CMS1017FHIRHHFI/fbbd3f4a-3e8e-40b9-ac83-0e80b2c129ec/) | Group_1:Denominator Observation | 2 | 0 | 0 | den | shared |
| [fbbd3f4a-3e8e-40b9-ac83-0e80b2c129ec](../../input/tests/measure/CMS1017FHIRHHFI/fbbd3f4a-3e8e-40b9-ac83-0e80b2c129ec/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [fd4ea84b-cd39-4d03-9641-9ca8d10bbe67](../../input/tests/measure/CMS1017FHIRHHFI/fd4ea84b-cd39-4d03-9641-9ca8d10bbe67/) | Group_1:Denominator Observation | 4 | 0 | 0 | den | shared |
| [fd4ea84b-cd39-4d03-9641-9ca8d10bbe67](../../input/tests/measure/CMS1017FHIRHHFI/fd4ea84b-cd39-4d03-9641-9ca8d10bbe67/) | Group_1:Numerator Observation | 1 | 0 | 0 | num | shared |
| [0884b17b-baa4-47c0-a2b6-5849230dcf43](../../input/tests/measure/CMS1017FHIRHHFI/0884b17b-baa4-47c0-a2b6-5849230dcf43/) | Group_1:Measure Observation | 1 | None | None | mobs | incomplete |
| [58079a8d-2808-4029-b1fb-67ab4a841aa9](../../input/tests/measure/CMS1017FHIRHHFI/58079a8d-2808-4029-b1fb-67ab4a841aa9/) | Group_1:Measure Observation | 1 | None | None | mobs | incomplete |
| [5926d62e-8eb9-4742-9bdf-efdcbd08ab7b](../../input/tests/measure/CMS1017FHIRHHFI/5926d62e-8eb9-4742-9bdf-efdcbd08ab7b/) | Group_1:Measure Observation | 1 | None | None | mobs | incomplete |
| [7c4f1e55-2462-45e5-9062-5cf8d04b40e2](../../input/tests/measure/CMS1017FHIRHHFI/7c4f1e55-2462-45e5-9062-5cf8d04b40e2/) | Group_1:Measure Observation | 3 | None | None | mobs | incomplete |

