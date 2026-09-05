# Cross-Engine Shared-Issue Detection: CMS72FHIRSTKAntithromboticDay2

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 15 |
| shared-direction | 7 |
| cms-only | 0 |
| qicore-only | 236 |
| conflicting | 0 |
| incomplete | 0 |
| pass | 532 |
| **total cells** | 790 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 5.8%** (15 / 258).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [2f7681fa-66b0-4395-aa35-7622e37709ae](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2f7681fa-66b0-4395-aa35-7622e37709ae/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [3432dedb-7130-4614-9283-6c1569fab90f](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3432dedb-7130-4614-9283-6c1569fab90f/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [5a329008-fcc1-4168-ab9c-89cb5dd6ff32](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5a329008-fcc1-4168-ab9c-89cb5dd6ff32/) | Group_1:Numerator | 1 | 0 | 0 | num | shared |
| [7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [82399522-ba6c-4997-afc9-23f55bb7da89](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/82399522-ba6c-4997-afc9-23f55bb7da89/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [a1a37483-1a67-4dd9-a8ca-b4d49a28a19d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a1a37483-1a67-4dd9-a8ca-b4d49a28a19d/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [be5c4068-2639-4b0c-bea3-5b7c80a6fe3b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/be5c4068-2639-4b0c-bea3-5b7c80a6fe3b/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [cb7c95fc-6d6b-4e07-81e8-a79385142b94](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cb7c95fc-6d6b-4e07-81e8-a79385142b94/) | Group_1:Numerator | 2 | 0 | 0 | num | shared |
| [d496f08e-c55b-44b1-97a7-f86cf9ead1e2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d496f08e-c55b-44b1-97a7-f86cf9ead1e2/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [dc187313-245c-4ed6-b6bb-fcb94c117fec](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/dc187313-245c-4ed6-b6bb-fcb94c117fec/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [e126cdec-dbc8-4ee8-964f-e88e46c04f88](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e126cdec-dbc8-4ee8-964f-e88e46c04f88/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [e126cdec-dbc8-4ee8-964f-e88e46c04f88](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e126cdec-dbc8-4ee8-964f-e88e46c04f88/) | Group_1:Denominator Exclusion | 1 | 0 | 0 | den | shared |
| [e126cdec-dbc8-4ee8-964f-e88e46c04f88](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e126cdec-dbc8-4ee8-964f-e88e46c04f88/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [ed638412-155e-4349-8461-4550fd4fae3b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ed638412-155e-4349-8461-4550fd4fae3b/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [febd4b3e-99bc-4c55-bba9-3b2136c2160b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/febd4b3e-99bc-4c55-bba9-3b2136c2160b/) | Group_1:Denominator Exclusion | 2 | 0 | 0 | den | shared |
| [5a329008-fcc1-4168-ab9c-89cb5dd6ff32](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5a329008-fcc1-4168-ab9c-89cb5dd6ff32/) | Group_1:Denominator | 2 | 1 | 0 | den | shared-direction |
| [5a329008-fcc1-4168-ab9c-89cb5dd6ff32](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5a329008-fcc1-4168-ab9c-89cb5dd6ff32/) | Group_1:Initial Population | 2 | 1 | 0 | ini | shared-direction |
| [cb7c95fc-6d6b-4e07-81e8-a79385142b94](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cb7c95fc-6d6b-4e07-81e8-a79385142b94/) | Group_1:Denominator | 3 | 1 | 0 | den | shared-direction |
| [cb7c95fc-6d6b-4e07-81e8-a79385142b94](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cb7c95fc-6d6b-4e07-81e8-a79385142b94/) | Group_1:Initial Population | 3 | 1 | 0 | ini | shared-direction |
| [febd4b3e-99bc-4c55-bba9-3b2136c2160b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/febd4b3e-99bc-4c55-bba9-3b2136c2160b/) | Group_1:Denominator | 4 | 1 | 0 | den | shared-direction |
| [febd4b3e-99bc-4c55-bba9-3b2136c2160b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/febd4b3e-99bc-4c55-bba9-3b2136c2160b/) | Group_1:Initial Population | 4 | 1 | 0 | ini | shared-direction |
| [febd4b3e-99bc-4c55-bba9-3b2136c2160b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/febd4b3e-99bc-4c55-bba9-3b2136c2160b/) | Group_1:Numerator | 2 | 1 | 0 | num | shared-direction |
| [036c2c6b-c5b7-4e1f-8a85-ac0787e3a15d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/036c2c6b-c5b7-4e1f-8a85-ac0787e3a15d/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [036c2c6b-c5b7-4e1f-8a85-ac0787e3a15d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/036c2c6b-c5b7-4e1f-8a85-ac0787e3a15d/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [05ec524f-1d2d-4f9e-8eaa-cc2662030fc6](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/05ec524f-1d2d-4f9e-8eaa-cc2662030fc6/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [05ec524f-1d2d-4f9e-8eaa-cc2662030fc6](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/05ec524f-1d2d-4f9e-8eaa-cc2662030fc6/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [05ec524f-1d2d-4f9e-8eaa-cc2662030fc6](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/05ec524f-1d2d-4f9e-8eaa-cc2662030fc6/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [072fc02e-93db-449c-a293-2e8525a49694](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/072fc02e-93db-449c-a293-2e8525a49694/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [072fc02e-93db-449c-a293-2e8525a49694](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/072fc02e-93db-449c-a293-2e8525a49694/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [072fc02e-93db-449c-a293-2e8525a49694](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/072fc02e-93db-449c-a293-2e8525a49694/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [09a132b9-b03c-4a8d-a09f-f18c544bb660](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/09a132b9-b03c-4a8d-a09f-f18c544bb660/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [09a132b9-b03c-4a8d-a09f-f18c544bb660](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/09a132b9-b03c-4a8d-a09f-f18c544bb660/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [09a132b9-b03c-4a8d-a09f-f18c544bb660](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/09a132b9-b03c-4a8d-a09f-f18c544bb660/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [09a4fe70-dc7a-48ed-9b97-47f0a119eabd](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/09a4fe70-dc7a-48ed-9b97-47f0a119eabd/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [09a4fe70-dc7a-48ed-9b97-47f0a119eabd](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/09a4fe70-dc7a-48ed-9b97-47f0a119eabd/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [0c8a299c-b082-4383-b0b4-aebbb0fa9fb4](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/0c8a299c-b082-4383-b0b4-aebbb0fa9fb4/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [0c8a299c-b082-4383-b0b4-aebbb0fa9fb4](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/0c8a299c-b082-4383-b0b4-aebbb0fa9fb4/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [0eecd949-77bf-4ded-bb95-40e11c2116c7](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/0eecd949-77bf-4ded-bb95-40e11c2116c7/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [0eecd949-77bf-4ded-bb95-40e11c2116c7](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/0eecd949-77bf-4ded-bb95-40e11c2116c7/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [11fc1901-7cc7-46c6-bbd0-58b614082170](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/11fc1901-7cc7-46c6-bbd0-58b614082170/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [11fc1901-7cc7-46c6-bbd0-58b614082170](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/11fc1901-7cc7-46c6-bbd0-58b614082170/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [1216ea1e-aee7-4c75-8e5d-7f712f6ba3f7](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/1216ea1e-aee7-4c75-8e5d-7f712f6ba3f7/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [1216ea1e-aee7-4c75-8e5d-7f712f6ba3f7](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/1216ea1e-aee7-4c75-8e5d-7f712f6ba3f7/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [144370a9-c9cf-43db-ba18-f92f4f8cec29](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/144370a9-c9cf-43db-ba18-f92f4f8cec29/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [144370a9-c9cf-43db-ba18-f92f4f8cec29](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/144370a9-c9cf-43db-ba18-f92f4f8cec29/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [144370a9-c9cf-43db-ba18-f92f4f8cec29](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/144370a9-c9cf-43db-ba18-f92f4f8cec29/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [155afb0b-baef-4e1a-8255-dd3bc96c9c0d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/155afb0b-baef-4e1a-8255-dd3bc96c9c0d/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [155afb0b-baef-4e1a-8255-dd3bc96c9c0d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/155afb0b-baef-4e1a-8255-dd3bc96c9c0d/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [1ef5e77a-dea5-4f1f-873b-44ea79810330](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/1ef5e77a-dea5-4f1f-873b-44ea79810330/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [1ef5e77a-dea5-4f1f-873b-44ea79810330](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/1ef5e77a-dea5-4f1f-873b-44ea79810330/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [1ef5e77a-dea5-4f1f-873b-44ea79810330](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/1ef5e77a-dea5-4f1f-873b-44ea79810330/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [2a1812bc-465a-438c-934c-e85a3591512a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2a1812bc-465a-438c-934c-e85a3591512a/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [2a1812bc-465a-438c-934c-e85a3591512a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2a1812bc-465a-438c-934c-e85a3591512a/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [2a1812bc-465a-438c-934c-e85a3591512a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2a1812bc-465a-438c-934c-e85a3591512a/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [2ecbb381-211e-421a-8053-21c820f33043](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2ecbb381-211e-421a-8053-21c820f33043/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [2ecbb381-211e-421a-8053-21c820f33043](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2ecbb381-211e-421a-8053-21c820f33043/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [2ecbb381-211e-421a-8053-21c820f33043](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2ecbb381-211e-421a-8053-21c820f33043/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [2f7681fa-66b0-4395-aa35-7622e37709ae](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2f7681fa-66b0-4395-aa35-7622e37709ae/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [2f7681fa-66b0-4395-aa35-7622e37709ae](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/2f7681fa-66b0-4395-aa35-7622e37709ae/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [3264d587-3c02-45ff-b989-044fcc30abae](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3264d587-3c02-45ff-b989-044fcc30abae/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [3264d587-3c02-45ff-b989-044fcc30abae](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3264d587-3c02-45ff-b989-044fcc30abae/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [3264d587-3c02-45ff-b989-044fcc30abae](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3264d587-3c02-45ff-b989-044fcc30abae/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [3432dedb-7130-4614-9283-6c1569fab90f](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3432dedb-7130-4614-9283-6c1569fab90f/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [3432dedb-7130-4614-9283-6c1569fab90f](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3432dedb-7130-4614-9283-6c1569fab90f/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [388557b1-cf25-4750-88b2-751e475b433f](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/388557b1-cf25-4750-88b2-751e475b433f/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [388557b1-cf25-4750-88b2-751e475b433f](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/388557b1-cf25-4750-88b2-751e475b433f/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [3ab85f43-dd45-4827-8f13-ad9d1208d2e0](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3ab85f43-dd45-4827-8f13-ad9d1208d2e0/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [3ab85f43-dd45-4827-8f13-ad9d1208d2e0](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3ab85f43-dd45-4827-8f13-ad9d1208d2e0/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [3ab85f43-dd45-4827-8f13-ad9d1208d2e0](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/3ab85f43-dd45-4827-8f13-ad9d1208d2e0/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [4b6a9c86-3aad-4828-be61-bab6cd0c3140](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/4b6a9c86-3aad-4828-be61-bab6cd0c3140/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [4b6a9c86-3aad-4828-be61-bab6cd0c3140](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/4b6a9c86-3aad-4828-be61-bab6cd0c3140/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [4b6a9c86-3aad-4828-be61-bab6cd0c3140](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/4b6a9c86-3aad-4828-be61-bab6cd0c3140/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [4f8b0ca2-baf1-4ce6-8b9a-c3220097cf7c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/4f8b0ca2-baf1-4ce6-8b9a-c3220097cf7c/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [4f8b0ca2-baf1-4ce6-8b9a-c3220097cf7c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/4f8b0ca2-baf1-4ce6-8b9a-c3220097cf7c/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [4f8b0ca2-baf1-4ce6-8b9a-c3220097cf7c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/4f8b0ca2-baf1-4ce6-8b9a-c3220097cf7c/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [54381296-da32-4474-85b7-209d99c52e7e](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/54381296-da32-4474-85b7-209d99c52e7e/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [54381296-da32-4474-85b7-209d99c52e7e](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/54381296-da32-4474-85b7-209d99c52e7e/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [5736982d-6c82-4815-b0d2-3416ebe105f4](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5736982d-6c82-4815-b0d2-3416ebe105f4/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [5736982d-6c82-4815-b0d2-3416ebe105f4](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5736982d-6c82-4815-b0d2-3416ebe105f4/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [5736982d-6c82-4815-b0d2-3416ebe105f4](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5736982d-6c82-4815-b0d2-3416ebe105f4/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [58169ea2-037f-4302-9c37-4239fe24f73d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/58169ea2-037f-4302-9c37-4239fe24f73d/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [58169ea2-037f-4302-9c37-4239fe24f73d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/58169ea2-037f-4302-9c37-4239fe24f73d/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [58169ea2-037f-4302-9c37-4239fe24f73d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/58169ea2-037f-4302-9c37-4239fe24f73d/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [5a329008-fcc1-4168-ab9c-89cb5dd6ff32](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5a329008-fcc1-4168-ab9c-89cb5dd6ff32/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [5adf0120-b2f5-415f-b1ff-1684d9f4af7a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5adf0120-b2f5-415f-b1ff-1684d9f4af7a/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [5adf0120-b2f5-415f-b1ff-1684d9f4af7a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/5adf0120-b2f5-415f-b1ff-1684d9f4af7a/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [64a75df8-8bed-49ea-9c90-ee3569d233df](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/64a75df8-8bed-49ea-9c90-ee3569d233df/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [64a75df8-8bed-49ea-9c90-ee3569d233df](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/64a75df8-8bed-49ea-9c90-ee3569d233df/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [64a75df8-8bed-49ea-9c90-ee3569d233df](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/64a75df8-8bed-49ea-9c90-ee3569d233df/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [6678ed6f-3c94-4630-a7c5-d35a003b4535](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/6678ed6f-3c94-4630-a7c5-d35a003b4535/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [6678ed6f-3c94-4630-a7c5-d35a003b4535](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/6678ed6f-3c94-4630-a7c5-d35a003b4535/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [6bbc3f38-7f5f-4da9-9beb-eb32874fd1ed](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/6bbc3f38-7f5f-4da9-9beb-eb32874fd1ed/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [6bbc3f38-7f5f-4da9-9beb-eb32874fd1ed](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/6bbc3f38-7f5f-4da9-9beb-eb32874fd1ed/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [70e86911-43d6-41de-bfb9-933d8f539b98](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/70e86911-43d6-41de-bfb9-933d8f539b98/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [70e86911-43d6-41de-bfb9-933d8f539b98](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/70e86911-43d6-41de-bfb9-933d8f539b98/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [70e86911-43d6-41de-bfb9-933d8f539b98](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/70e86911-43d6-41de-bfb9-933d8f539b98/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [7317795b-638b-4d0c-9e9e-b55ade45958c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7317795b-638b-4d0c-9e9e-b55ade45958c/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [7317795b-638b-4d0c-9e9e-b55ade45958c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7317795b-638b-4d0c-9e9e-b55ade45958c/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [7317795b-638b-4d0c-9e9e-b55ade45958c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7317795b-638b-4d0c-9e9e-b55ade45958c/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [763c581d-7398-47e7-ba78-eaa5853df551](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/763c581d-7398-47e7-ba78-eaa5853df551/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [763c581d-7398-47e7-ba78-eaa5853df551](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/763c581d-7398-47e7-ba78-eaa5853df551/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [77a6cd7b-4322-4c29-b248-64d8af106ce7](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/77a6cd7b-4322-4c29-b248-64d8af106ce7/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [77a6cd7b-4322-4c29-b248-64d8af106ce7](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/77a6cd7b-4322-4c29-b248-64d8af106ce7/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [77bba430-02fc-4ac7-ab49-f57fd73daa9b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/77bba430-02fc-4ac7-ab49-f57fd73daa9b/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [77bba430-02fc-4ac7-ab49-f57fd73daa9b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/77bba430-02fc-4ac7-ab49-f57fd73daa9b/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [79a2dd53-a342-41d9-a5c9-1b565bd06fe7](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/79a2dd53-a342-41d9-a5c9-1b565bd06fe7/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [79a2dd53-a342-41d9-a5c9-1b565bd06fe7](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/79a2dd53-a342-41d9-a5c9-1b565bd06fe7/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [79a2dd53-a342-41d9-a5c9-1b565bd06fe7](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/79a2dd53-a342-41d9-a5c9-1b565bd06fe7/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [79f6bb60-1bdb-4dff-857d-65311e9ccea5](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/79f6bb60-1bdb-4dff-857d-65311e9ccea5/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [79f6bb60-1bdb-4dff-857d-65311e9ccea5](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/79f6bb60-1bdb-4dff-857d-65311e9ccea5/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [7abd0282-c461-4c61-9669-f261a689f485](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7abd0282-c461-4c61-9669-f261a689f485/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [7abd0282-c461-4c61-9669-f261a689f485](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7abd0282-c461-4c61-9669-f261a689f485/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [7abd0282-c461-4c61-9669-f261a689f485](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7abd0282-c461-4c61-9669-f261a689f485/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [7ce35cc8-ed8f-46f0-9ba1-8421a760bdc8](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7ce35cc8-ed8f-46f0-9ba1-8421a760bdc8/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [7ce35cc8-ed8f-46f0-9ba1-8421a760bdc8](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7ce35cc8-ed8f-46f0-9ba1-8421a760bdc8/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [7d9affce-5c31-4fcb-b9e5-c0304c3f9406](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7d9affce-5c31-4fcb-b9e5-c0304c3f9406/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [7d9affce-5c31-4fcb-b9e5-c0304c3f9406](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7d9affce-5c31-4fcb-b9e5-c0304c3f9406/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [7d9affce-5c31-4fcb-b9e5-c0304c3f9406](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7d9affce-5c31-4fcb-b9e5-c0304c3f9406/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7ddb2db9-020e-45b1-aaf5-2fbcf281d6b8/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [7e3bf20a-7a5b-4d50-aa34-267ab19da7b2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7e3bf20a-7a5b-4d50-aa34-267ab19da7b2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [7e3bf20a-7a5b-4d50-aa34-267ab19da7b2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7e3bf20a-7a5b-4d50-aa34-267ab19da7b2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [7e3bf20a-7a5b-4d50-aa34-267ab19da7b2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/7e3bf20a-7a5b-4d50-aa34-267ab19da7b2/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [82399522-ba6c-4997-afc9-23f55bb7da89](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/82399522-ba6c-4997-afc9-23f55bb7da89/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [82399522-ba6c-4997-afc9-23f55bb7da89](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/82399522-ba6c-4997-afc9-23f55bb7da89/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [82fd75d8-4816-4d24-b18c-0e454c430eb5](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/82fd75d8-4816-4d24-b18c-0e454c430eb5/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [82fd75d8-4816-4d24-b18c-0e454c430eb5](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/82fd75d8-4816-4d24-b18c-0e454c430eb5/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [82fd75d8-4816-4d24-b18c-0e454c430eb5](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/82fd75d8-4816-4d24-b18c-0e454c430eb5/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [844d9440-ab79-4206-9893-bcf9a786970e](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/844d9440-ab79-4206-9893-bcf9a786970e/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [844d9440-ab79-4206-9893-bcf9a786970e](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/844d9440-ab79-4206-9893-bcf9a786970e/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [844d9440-ab79-4206-9893-bcf9a786970e](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/844d9440-ab79-4206-9893-bcf9a786970e/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [89275dc4-f4c1-41b5-a215-9c7228933cc0](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/89275dc4-f4c1-41b5-a215-9c7228933cc0/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [89275dc4-f4c1-41b5-a215-9c7228933cc0](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/89275dc4-f4c1-41b5-a215-9c7228933cc0/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [89275dc4-f4c1-41b5-a215-9c7228933cc0](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/89275dc4-f4c1-41b5-a215-9c7228933cc0/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [93798745-af1c-4eb6-8dc4-446a531c05a4](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/93798745-af1c-4eb6-8dc4-446a531c05a4/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [93798745-af1c-4eb6-8dc4-446a531c05a4](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/93798745-af1c-4eb6-8dc4-446a531c05a4/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [93798745-af1c-4eb6-8dc4-446a531c05a4](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/93798745-af1c-4eb6-8dc4-446a531c05a4/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [96266910-a2b3-4294-9dc5-8a812622b70b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/96266910-a2b3-4294-9dc5-8a812622b70b/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [96266910-a2b3-4294-9dc5-8a812622b70b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/96266910-a2b3-4294-9dc5-8a812622b70b/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [9843e92a-751f-4b3c-86b8-50397a64c8fd](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9843e92a-751f-4b3c-86b8-50397a64c8fd/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [9843e92a-751f-4b3c-86b8-50397a64c8fd](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9843e92a-751f-4b3c-86b8-50397a64c8fd/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [9843e92a-751f-4b3c-86b8-50397a64c8fd](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9843e92a-751f-4b3c-86b8-50397a64c8fd/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [9a297d79-90eb-46f1-9068-1a7c7b6c7147](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9a297d79-90eb-46f1-9068-1a7c7b6c7147/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [9a297d79-90eb-46f1-9068-1a7c7b6c7147](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9a297d79-90eb-46f1-9068-1a7c7b6c7147/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [9a42c820-29ec-464e-b2f5-eb8114985a0c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9a42c820-29ec-464e-b2f5-eb8114985a0c/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [9a42c820-29ec-464e-b2f5-eb8114985a0c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9a42c820-29ec-464e-b2f5-eb8114985a0c/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [9a42c820-29ec-464e-b2f5-eb8114985a0c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9a42c820-29ec-464e-b2f5-eb8114985a0c/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [9a8c51a0-bf53-42b6-927d-c1f90b81a31a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9a8c51a0-bf53-42b6-927d-c1f90b81a31a/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [9a8c51a0-bf53-42b6-927d-c1f90b81a31a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9a8c51a0-bf53-42b6-927d-c1f90b81a31a/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [9bfee327-99be-48de-ba09-5b64e4435f8d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9bfee327-99be-48de-ba09-5b64e4435f8d/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [9bfee327-99be-48de-ba09-5b64e4435f8d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9bfee327-99be-48de-ba09-5b64e4435f8d/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [9bfee327-99be-48de-ba09-5b64e4435f8d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9bfee327-99be-48de-ba09-5b64e4435f8d/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [9f4bc5cc-b5a4-4d67-a11f-9f171b62fd9f](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9f4bc5cc-b5a4-4d67-a11f-9f171b62fd9f/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [9f4bc5cc-b5a4-4d67-a11f-9f171b62fd9f](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/9f4bc5cc-b5a4-4d67-a11f-9f171b62fd9f/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [a0ced1fb-191d-404b-80f4-761e51cf9de2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a0ced1fb-191d-404b-80f4-761e51cf9de2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [a0ced1fb-191d-404b-80f4-761e51cf9de2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a0ced1fb-191d-404b-80f4-761e51cf9de2/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [a0ced1fb-191d-404b-80f4-761e51cf9de2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a0ced1fb-191d-404b-80f4-761e51cf9de2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [a1a37483-1a67-4dd9-a8ca-b4d49a28a19d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a1a37483-1a67-4dd9-a8ca-b4d49a28a19d/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [a1a37483-1a67-4dd9-a8ca-b4d49a28a19d](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a1a37483-1a67-4dd9-a8ca-b4d49a28a19d/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [a2cb4956-d7e5-45a9-8007-80dcb893203c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a2cb4956-d7e5-45a9-8007-80dcb893203c/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [a2cb4956-d7e5-45a9-8007-80dcb893203c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a2cb4956-d7e5-45a9-8007-80dcb893203c/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [a2cb4956-d7e5-45a9-8007-80dcb893203c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a2cb4956-d7e5-45a9-8007-80dcb893203c/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [a5feebb4-d3c0-4435-aed5-9579b75a8a52](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a5feebb4-d3c0-4435-aed5-9579b75a8a52/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [a5feebb4-d3c0-4435-aed5-9579b75a8a52](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a5feebb4-d3c0-4435-aed5-9579b75a8a52/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [a938e0ff-51b3-4001-b33e-5fd2c00a9147](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a938e0ff-51b3-4001-b33e-5fd2c00a9147/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [a938e0ff-51b3-4001-b33e-5fd2c00a9147](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a938e0ff-51b3-4001-b33e-5fd2c00a9147/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [a938e0ff-51b3-4001-b33e-5fd2c00a9147](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/a938e0ff-51b3-4001-b33e-5fd2c00a9147/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [aadbfade-4898-4931-9e11-e5d7ba64ab27](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/aadbfade-4898-4931-9e11-e5d7ba64ab27/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [aadbfade-4898-4931-9e11-e5d7ba64ab27](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/aadbfade-4898-4931-9e11-e5d7ba64ab27/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [aadbfade-4898-4931-9e11-e5d7ba64ab27](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/aadbfade-4898-4931-9e11-e5d7ba64ab27/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ab024aef-425c-43ba-a856-882a3e3c91f1](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ab024aef-425c-43ba-a856-882a3e3c91f1/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ab024aef-425c-43ba-a856-882a3e3c91f1](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ab024aef-425c-43ba-a856-882a3e3c91f1/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [ab024aef-425c-43ba-a856-882a3e3c91f1](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ab024aef-425c-43ba-a856-882a3e3c91f1/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ab28178c-eadb-41a3-861e-ee22c8f12d16](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ab28178c-eadb-41a3-861e-ee22c8f12d16/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ab28178c-eadb-41a3-861e-ee22c8f12d16](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ab28178c-eadb-41a3-861e-ee22c8f12d16/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ac23e6a6-3f36-49db-9eba-2da744a41c57](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ac23e6a6-3f36-49db-9eba-2da744a41c57/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ac23e6a6-3f36-49db-9eba-2da744a41c57](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ac23e6a6-3f36-49db-9eba-2da744a41c57/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [ac23e6a6-3f36-49db-9eba-2da744a41c57](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ac23e6a6-3f36-49db-9eba-2da744a41c57/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ad35c913-a8ba-4d29-b6e9-8652aa5ca20c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ad35c913-a8ba-4d29-b6e9-8652aa5ca20c/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ad35c913-a8ba-4d29-b6e9-8652aa5ca20c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ad35c913-a8ba-4d29-b6e9-8652aa5ca20c/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [ad35c913-a8ba-4d29-b6e9-8652aa5ca20c](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ad35c913-a8ba-4d29-b6e9-8652aa5ca20c/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [b3043789-f91a-42f6-848d-6bfd7df331fe](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b3043789-f91a-42f6-848d-6bfd7df331fe/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [b3043789-f91a-42f6-848d-6bfd7df331fe](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b3043789-f91a-42f6-848d-6bfd7df331fe/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [b3043789-f91a-42f6-848d-6bfd7df331fe](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b3043789-f91a-42f6-848d-6bfd7df331fe/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [b4cd9b20-6d41-4034-907c-b24e362a0699](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b4cd9b20-6d41-4034-907c-b24e362a0699/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [b4cd9b20-6d41-4034-907c-b24e362a0699](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b4cd9b20-6d41-4034-907c-b24e362a0699/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [b4cd9b20-6d41-4034-907c-b24e362a0699](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b4cd9b20-6d41-4034-907c-b24e362a0699/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [b569157b-b263-4b72-ab40-132bea1d8f71](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b569157b-b263-4b72-ab40-132bea1d8f71/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [b569157b-b263-4b72-ab40-132bea1d8f71](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b569157b-b263-4b72-ab40-132bea1d8f71/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [b569157b-b263-4b72-ab40-132bea1d8f71](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b569157b-b263-4b72-ab40-132bea1d8f71/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [b86e54d1-f8ca-44b6-99a5-d455c5649104](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b86e54d1-f8ca-44b6-99a5-d455c5649104/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [b86e54d1-f8ca-44b6-99a5-d455c5649104](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/b86e54d1-f8ca-44b6-99a5-d455c5649104/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [bda91aac-a815-4a22-b505-36cef1080d49](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/bda91aac-a815-4a22-b505-36cef1080d49/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [bda91aac-a815-4a22-b505-36cef1080d49](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/bda91aac-a815-4a22-b505-36cef1080d49/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [bda91aac-a815-4a22-b505-36cef1080d49](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/bda91aac-a815-4a22-b505-36cef1080d49/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [c014ff5d-792f-45c9-9659-4999537005b0](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c014ff5d-792f-45c9-9659-4999537005b0/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [c014ff5d-792f-45c9-9659-4999537005b0](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c014ff5d-792f-45c9-9659-4999537005b0/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [c014ff5d-792f-45c9-9659-4999537005b0](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c014ff5d-792f-45c9-9659-4999537005b0/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [c1ee2b07-f5a7-451c-8bc5-cb97b1cbcca2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c1ee2b07-f5a7-451c-8bc5-cb97b1cbcca2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [c1ee2b07-f5a7-451c-8bc5-cb97b1cbcca2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c1ee2b07-f5a7-451c-8bc5-cb97b1cbcca2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [c48c3487-44cf-4a09-bc17-e60e66d19002](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c48c3487-44cf-4a09-bc17-e60e66d19002/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [c48c3487-44cf-4a09-bc17-e60e66d19002](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c48c3487-44cf-4a09-bc17-e60e66d19002/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [c48c3487-44cf-4a09-bc17-e60e66d19002](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c48c3487-44cf-4a09-bc17-e60e66d19002/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [c5085136-65ef-498f-8aa9-449bf48f6a63](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c5085136-65ef-498f-8aa9-449bf48f6a63/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [c5085136-65ef-498f-8aa9-449bf48f6a63](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c5085136-65ef-498f-8aa9-449bf48f6a63/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [c7382fb6-053b-4424-b5c2-87d79179b016](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c7382fb6-053b-4424-b5c2-87d79179b016/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [c7382fb6-053b-4424-b5c2-87d79179b016](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c7382fb6-053b-4424-b5c2-87d79179b016/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [c787d9c8-9645-4da6-a607-85dbefdf129e](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c787d9c8-9645-4da6-a607-85dbefdf129e/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [c787d9c8-9645-4da6-a607-85dbefdf129e](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c787d9c8-9645-4da6-a607-85dbefdf129e/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [c787d9c8-9645-4da6-a607-85dbefdf129e](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c787d9c8-9645-4da6-a607-85dbefdf129e/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [c84cc10b-29f5-41cb-84a7-fbb23f52e0d5](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c84cc10b-29f5-41cb-84a7-fbb23f52e0d5/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [c84cc10b-29f5-41cb-84a7-fbb23f52e0d5](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/c84cc10b-29f5-41cb-84a7-fbb23f52e0d5/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [cb7c95fc-6d6b-4e07-81e8-a79385142b94](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cb7c95fc-6d6b-4e07-81e8-a79385142b94/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [cc23329d-6635-4347-8669-a98c921f4381](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cc23329d-6635-4347-8669-a98c921f4381/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [cc23329d-6635-4347-8669-a98c921f4381](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cc23329d-6635-4347-8669-a98c921f4381/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [cc23329d-6635-4347-8669-a98c921f4381](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/cc23329d-6635-4347-8669-a98c921f4381/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [d0a59b97-c3ab-4028-9109-a31359a93c47](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d0a59b97-c3ab-4028-9109-a31359a93c47/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [d0a59b97-c3ab-4028-9109-a31359a93c47](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d0a59b97-c3ab-4028-9109-a31359a93c47/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [d0a59b97-c3ab-4028-9109-a31359a93c47](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d0a59b97-c3ab-4028-9109-a31359a93c47/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [d496f08e-c55b-44b1-97a7-f86cf9ead1e2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d496f08e-c55b-44b1-97a7-f86cf9ead1e2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [d496f08e-c55b-44b1-97a7-f86cf9ead1e2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d496f08e-c55b-44b1-97a7-f86cf9ead1e2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [d82d5f38-a1b7-4f28-a3db-25f42f7e64b2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d82d5f38-a1b7-4f28-a3db-25f42f7e64b2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [d82d5f38-a1b7-4f28-a3db-25f42f7e64b2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d82d5f38-a1b7-4f28-a3db-25f42f7e64b2/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [d82d5f38-a1b7-4f28-a3db-25f42f7e64b2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/d82d5f38-a1b7-4f28-a3db-25f42f7e64b2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [da480fb9-7501-46f5-9575-f15a638bc751](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/da480fb9-7501-46f5-9575-f15a638bc751/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [da480fb9-7501-46f5-9575-f15a638bc751](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/da480fb9-7501-46f5-9575-f15a638bc751/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [dc187313-245c-4ed6-b6bb-fcb94c117fec](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/dc187313-245c-4ed6-b6bb-fcb94c117fec/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [dc187313-245c-4ed6-b6bb-fcb94c117fec](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/dc187313-245c-4ed6-b6bb-fcb94c117fec/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [dd40e582-8c3f-44a2-b781-84acead6120f](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/dd40e582-8c3f-44a2-b781-84acead6120f/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [dd40e582-8c3f-44a2-b781-84acead6120f](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/dd40e582-8c3f-44a2-b781-84acead6120f/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [dd40e582-8c3f-44a2-b781-84acead6120f](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/dd40e582-8c3f-44a2-b781-84acead6120f/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [de4005d0-549c-40bb-93b9-26650c194d04](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/de4005d0-549c-40bb-93b9-26650c194d04/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [de4005d0-549c-40bb-93b9-26650c194d04](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/de4005d0-549c-40bb-93b9-26650c194d04/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [e0286677-4610-4138-b9fe-3ed648ed45f8](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e0286677-4610-4138-b9fe-3ed648ed45f8/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [e0286677-4610-4138-b9fe-3ed648ed45f8](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e0286677-4610-4138-b9fe-3ed648ed45f8/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [e89c4eae-404c-44b9-8be5-c8a8b481813a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e89c4eae-404c-44b9-8be5-c8a8b481813a/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [e89c4eae-404c-44b9-8be5-c8a8b481813a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/e89c4eae-404c-44b9-8be5-c8a8b481813a/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ea737165-ca06-4304-9964-c157d504c3ee](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ea737165-ca06-4304-9964-c157d504c3ee/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ea737165-ca06-4304-9964-c157d504c3ee](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ea737165-ca06-4304-9964-c157d504c3ee/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [ea737165-ca06-4304-9964-c157d504c3ee](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ea737165-ca06-4304-9964-c157d504c3ee/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [eafd6c1f-c099-48b8-8101-b24b4a49cd0b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/eafd6c1f-c099-48b8-8101-b24b4a49cd0b/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [eafd6c1f-c099-48b8-8101-b24b4a49cd0b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/eafd6c1f-c099-48b8-8101-b24b4a49cd0b/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [eafd6c1f-c099-48b8-8101-b24b4a49cd0b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/eafd6c1f-c099-48b8-8101-b24b4a49cd0b/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ed638412-155e-4349-8461-4550fd4fae3b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ed638412-155e-4349-8461-4550fd4fae3b/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ed638412-155e-4349-8461-4550fd4fae3b](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ed638412-155e-4349-8461-4550fd4fae3b/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [f0d37c4e-7377-4876-8533-f955963f96f9](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/f0d37c4e-7377-4876-8533-f955963f96f9/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [f0d37c4e-7377-4876-8533-f955963f96f9](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/f0d37c4e-7377-4876-8533-f955963f96f9/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [f25baf5f-2980-416c-a8ef-3b9e42d751c3](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/f25baf5f-2980-416c-a8ef-3b9e42d751c3/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [f25baf5f-2980-416c-a8ef-3b9e42d751c3](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/f25baf5f-2980-416c-a8ef-3b9e42d751c3/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [f5f317c7-69f1-4a89-850a-8a58789c80f2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/f5f317c7-69f1-4a89-850a-8a58789c80f2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [f5f317c7-69f1-4a89-850a-8a58789c80f2](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/f5f317c7-69f1-4a89-850a-8a58789c80f2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [fed17706-6d92-4092-a9b1-9b7e47847f2a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/fed17706-6d92-4092-a9b1-9b7e47847f2a/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [fed17706-6d92-4092-a9b1-9b7e47847f2a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/fed17706-6d92-4092-a9b1-9b7e47847f2a/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [fed17706-6d92-4092-a9b1-9b7e47847f2a](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/fed17706-6d92-4092-a9b1-9b7e47847f2a/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [fed7bfb0-5746-4029-a64c-f40cc30ce946](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/fed7bfb0-5746-4029-a64c-f40cc30ce946/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [fed7bfb0-5746-4029-a64c-f40cc30ce946](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/fed7bfb0-5746-4029-a64c-f40cc30ce946/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ff9ea2c7-7a68-486d-809e-f0e2cd94d6eb](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ff9ea2c7-7a68-486d-809e-f0e2cd94d6eb/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ff9ea2c7-7a68-486d-809e-f0e2cd94d6eb](../../input/tests/measure/CMS72FHIRSTKAntithromboticDay2/ff9ea2c7-7a68-486d-809e-f0e2cd94d6eb/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |

