# Cross-Engine Shared-Issue Detection: CMS104FHIRSTKDCAntithrombotic

Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).

## Summary

| Bucket | Count |
|---|---:|
| shared | 19 |
| shared-direction | 10 |
| cms-only | 9 |
| qicore-only | 156 |
| conflicting | 0 |
| incomplete | 0 |
| pass | 216 |
| **total cells** | 410 |

**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: 9.8%** (19 / 194).

Interpretation: exact-magnitude agreement between two different engine versions
on the same logical population cell is the strongest available signal of an
engine-level bug shared by both engines. Cells where only one engine deviates
(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.

## Per-bucket cells

| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |
|---|---|---:|---:|---:|---|---|
| [0b1aa8ee-e8bf-49f5-b968-48c5a9702843](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0b1aa8ee-e8bf-49f5-b968-48c5a9702843/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [0b1aa8ee-e8bf-49f5-b968-48c5a9702843](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0b1aa8ee-e8bf-49f5-b968-48c5a9702843/) | Group_1:Denominator Exclusion | 1 | 0 | 0 | den | shared |
| [0b1aa8ee-e8bf-49f5-b968-48c5a9702843](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0b1aa8ee-e8bf-49f5-b968-48c5a9702843/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [146a6714-8663-4f45-826a-01110ff34490](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/146a6714-8663-4f45-826a-01110ff34490/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [2d54a94c-edf1-4f92-baf8-3813a8ef452d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2d54a94c-edf1-4f92-baf8-3813a8ef452d/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [348471db-5aaa-4bf3-a280-75222f20d599](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/348471db-5aaa-4bf3-a280-75222f20d599/) | Group_1:Denominator Exclusion | 1 | 0 | 0 | den | shared |
| [348471db-5aaa-4bf3-a280-75222f20d599](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/348471db-5aaa-4bf3-a280-75222f20d599/) | Group_1:Numerator | 1 | 0 | 0 | num | shared |
| [451b6853-3734-4c1c-b37e-5904629e0350](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/451b6853-3734-4c1c-b37e-5904629e0350/) | Group_1:Numerator | 1 | 0 | 0 | num | shared |
| [48952352-d74c-491c-9420-6e999e60f52a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/48952352-d74c-491c-9420-6e999e60f52a/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [591c23ea-1ddd-4800-9203-4b6946979818](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/591c23ea-1ddd-4800-9203-4b6946979818/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [593382e8-4ad5-4300-b0ad-26c8954281c6](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/593382e8-4ad5-4300-b0ad-26c8954281c6/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [5adc911a-c2a1-475c-a347-9da4ee98c6df](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/5adc911a-c2a1-475c-a347-9da4ee98c6df/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [7b1ac1a8-b7be-41ec-a77f-db545af22263](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7b1ac1a8-b7be-41ec-a77f-db545af22263/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [ac56c496-c5d6-4c23-be20-130ee8327fd2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ac56c496-c5d6-4c23-be20-130ee8327fd2/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [c15bee15-84c1-494a-ac82-2159b06da175](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/c15bee15-84c1-494a-ac82-2159b06da175/) | Group_1:Numerator | 2 | 0 | 0 | num | shared |
| [e081bee5-67f8-464f-9356-9b287e32a35a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e081bee5-67f8-464f-9356-9b287e32a35a/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [e84c89f7-3c9e-4ee9-b71a-5025aadb5990](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e84c89f7-3c9e-4ee9-b71a-5025aadb5990/) | Group_1:Denominator | 1 | 0 | 0 | den | shared |
| [e84c89f7-3c9e-4ee9-b71a-5025aadb5990](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e84c89f7-3c9e-4ee9-b71a-5025aadb5990/) | Group_1:Denominator Exception | 1 | 0 | 0 | den | shared |
| [e84c89f7-3c9e-4ee9-b71a-5025aadb5990](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e84c89f7-3c9e-4ee9-b71a-5025aadb5990/) | Group_1:Initial Population | 1 | 0 | 0 | ini | shared |
| [348471db-5aaa-4bf3-a280-75222f20d599](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/348471db-5aaa-4bf3-a280-75222f20d599/) | Group_1:Denominator | 3 | 1 | 0 | den | shared-direction |
| [348471db-5aaa-4bf3-a280-75222f20d599](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/348471db-5aaa-4bf3-a280-75222f20d599/) | Group_1:Initial Population | 3 | 1 | 0 | ini | shared-direction |
| [451b6853-3734-4c1c-b37e-5904629e0350](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/451b6853-3734-4c1c-b37e-5904629e0350/) | Group_1:Denominator | 3 | 1 | 0 | den | shared-direction |
| [451b6853-3734-4c1c-b37e-5904629e0350](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/451b6853-3734-4c1c-b37e-5904629e0350/) | Group_1:Denominator Exclusion | 2 | 1 | 0 | den | shared-direction |
| [451b6853-3734-4c1c-b37e-5904629e0350](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/451b6853-3734-4c1c-b37e-5904629e0350/) | Group_1:Initial Population | 3 | 1 | 0 | ini | shared-direction |
| [a2b8327c-eaf4-4552-863e-851426e729d4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a2b8327c-eaf4-4552-863e-851426e729d4/) | Group_1:Denominator | 2 | 1 | 0 | den | shared-direction |
| [a2b8327c-eaf4-4552-863e-851426e729d4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a2b8327c-eaf4-4552-863e-851426e729d4/) | Group_1:Initial Population | 2 | 1 | 0 | ini | shared-direction |
| [a2b8327c-eaf4-4552-863e-851426e729d4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a2b8327c-eaf4-4552-863e-851426e729d4/) | Group_1:Numerator | 2 | 1 | 0 | num | shared-direction |
| [c15bee15-84c1-494a-ac82-2159b06da175](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/c15bee15-84c1-494a-ac82-2159b06da175/) | Group_1:Denominator | 3 | 1 | 0 | den | shared-direction |
| [c15bee15-84c1-494a-ac82-2159b06da175](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/c15bee15-84c1-494a-ac82-2159b06da175/) | Group_1:Initial Population | 3 | 1 | 0 | ini | shared-direction |
| [146a6714-8663-4f45-826a-01110ff34490](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/146a6714-8663-4f45-826a-01110ff34490/) | Group_1:Numerator | 0 | 1 | 0 | num | cms-only |
| [2d54a94c-edf1-4f92-baf8-3813a8ef452d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2d54a94c-edf1-4f92-baf8-3813a8ef452d/) | Group_1:Numerator | 0 | 1 | 0 | num | cms-only |
| [48952352-d74c-491c-9420-6e999e60f52a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/48952352-d74c-491c-9420-6e999e60f52a/) | Group_1:Numerator | 0 | 1 | 0 | num | cms-only |
| [591c23ea-1ddd-4800-9203-4b6946979818](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/591c23ea-1ddd-4800-9203-4b6946979818/) | Group_1:Numerator | 0 | 1 | 0 | num | cms-only |
| [593382e8-4ad5-4300-b0ad-26c8954281c6](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/593382e8-4ad5-4300-b0ad-26c8954281c6/) | Group_1:Numerator | 0 | 1 | 0 | num | cms-only |
| [5adc911a-c2a1-475c-a347-9da4ee98c6df](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/5adc911a-c2a1-475c-a347-9da4ee98c6df/) | Group_1:Numerator | 0 | 1 | 0 | num | cms-only |
| [7b1ac1a8-b7be-41ec-a77f-db545af22263](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7b1ac1a8-b7be-41ec-a77f-db545af22263/) | Group_1:Numerator | 0 | 1 | 0 | num | cms-only |
| [ac56c496-c5d6-4c23-be20-130ee8327fd2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ac56c496-c5d6-4c23-be20-130ee8327fd2/) | Group_1:Numerator | 0 | 1 | 0 | num | cms-only |
| [e081bee5-67f8-464f-9356-9b287e32a35a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e081bee5-67f8-464f-9356-9b287e32a35a/) | Group_1:Numerator | 0 | 1 | 0 | num | cms-only |
| [003b2da3-b46a-4b24-91be-65ef27eef3bc](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/003b2da3-b46a-4b24-91be-65ef27eef3bc/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [003b2da3-b46a-4b24-91be-65ef27eef3bc](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/003b2da3-b46a-4b24-91be-65ef27eef3bc/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [0852e05c-94f3-4467-ad2c-255ffc5050e9](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0852e05c-94f3-4467-ad2c-255ffc5050e9/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [0852e05c-94f3-4467-ad2c-255ffc5050e9](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0852e05c-94f3-4467-ad2c-255ffc5050e9/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [0edb029c-ae5a-492a-ad4c-79ea0f8059d4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0edb029c-ae5a-492a-ad4c-79ea0f8059d4/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [0edb029c-ae5a-492a-ad4c-79ea0f8059d4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0edb029c-ae5a-492a-ad4c-79ea0f8059d4/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [0edb029c-ae5a-492a-ad4c-79ea0f8059d4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/0edb029c-ae5a-492a-ad4c-79ea0f8059d4/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [146a6714-8663-4f45-826a-01110ff34490](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/146a6714-8663-4f45-826a-01110ff34490/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [146a6714-8663-4f45-826a-01110ff34490](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/146a6714-8663-4f45-826a-01110ff34490/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [15e67912-9913-4b22-9f1b-3e86879e1d6d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/15e67912-9913-4b22-9f1b-3e86879e1d6d/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [15e67912-9913-4b22-9f1b-3e86879e1d6d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/15e67912-9913-4b22-9f1b-3e86879e1d6d/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [15e67912-9913-4b22-9f1b-3e86879e1d6d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/15e67912-9913-4b22-9f1b-3e86879e1d6d/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [162a5913-9989-42f2-8d6a-ae460e245e4c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/162a5913-9989-42f2-8d6a-ae460e245e4c/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [162a5913-9989-42f2-8d6a-ae460e245e4c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/162a5913-9989-42f2-8d6a-ae460e245e4c/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [1ec7f3ad-fe6d-486b-829b-101ebb721824](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/1ec7f3ad-fe6d-486b-829b-101ebb721824/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [1ec7f3ad-fe6d-486b-829b-101ebb721824](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/1ec7f3ad-fe6d-486b-829b-101ebb721824/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [2326f161-b68e-4034-91cb-4eae3c2ba587](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2326f161-b68e-4034-91cb-4eae3c2ba587/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [2326f161-b68e-4034-91cb-4eae3c2ba587](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2326f161-b68e-4034-91cb-4eae3c2ba587/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [264ec8d1-8e92-4b73-a6cb-e8856b22890d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/264ec8d1-8e92-4b73-a6cb-e8856b22890d/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [264ec8d1-8e92-4b73-a6cb-e8856b22890d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/264ec8d1-8e92-4b73-a6cb-e8856b22890d/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [264ec8d1-8e92-4b73-a6cb-e8856b22890d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/264ec8d1-8e92-4b73-a6cb-e8856b22890d/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [2d54a94c-edf1-4f92-baf8-3813a8ef452d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2d54a94c-edf1-4f92-baf8-3813a8ef452d/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [2d54a94c-edf1-4f92-baf8-3813a8ef452d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2d54a94c-edf1-4f92-baf8-3813a8ef452d/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [2e0b5b75-22d9-4607-b8fe-f31c86620554](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2e0b5b75-22d9-4607-b8fe-f31c86620554/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [2e0b5b75-22d9-4607-b8fe-f31c86620554](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2e0b5b75-22d9-4607-b8fe-f31c86620554/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [2ffdd04b-5cee-4904-9ce8-2f68dada9941](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2ffdd04b-5cee-4904-9ce8-2f68dada9941/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [2ffdd04b-5cee-4904-9ce8-2f68dada9941](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2ffdd04b-5cee-4904-9ce8-2f68dada9941/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [2ffdd04b-5cee-4904-9ce8-2f68dada9941](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/2ffdd04b-5cee-4904-9ce8-2f68dada9941/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [302f7629-15c3-4e52-86df-5677eab6770c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/302f7629-15c3-4e52-86df-5677eab6770c/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [302f7629-15c3-4e52-86df-5677eab6770c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/302f7629-15c3-4e52-86df-5677eab6770c/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [302f7629-15c3-4e52-86df-5677eab6770c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/302f7629-15c3-4e52-86df-5677eab6770c/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [348471db-5aaa-4bf3-a280-75222f20d599](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/348471db-5aaa-4bf3-a280-75222f20d599/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [34d3361c-95b3-43bf-a2a8-380914e06acb](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/34d3361c-95b3-43bf-a2a8-380914e06acb/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [34d3361c-95b3-43bf-a2a8-380914e06acb](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/34d3361c-95b3-43bf-a2a8-380914e06acb/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [34d3361c-95b3-43bf-a2a8-380914e06acb](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/34d3361c-95b3-43bf-a2a8-380914e06acb/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [3da60e55-4952-4341-b2eb-a79707f4ec3e](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/3da60e55-4952-4341-b2eb-a79707f4ec3e/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [3da60e55-4952-4341-b2eb-a79707f4ec3e](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/3da60e55-4952-4341-b2eb-a79707f4ec3e/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [3da60e55-4952-4341-b2eb-a79707f4ec3e](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/3da60e55-4952-4341-b2eb-a79707f4ec3e/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [3f089430-0edb-485d-9844-b2c58fb715e2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/3f089430-0edb-485d-9844-b2c58fb715e2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [3f089430-0edb-485d-9844-b2c58fb715e2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/3f089430-0edb-485d-9844-b2c58fb715e2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [48952352-d74c-491c-9420-6e999e60f52a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/48952352-d74c-491c-9420-6e999e60f52a/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [48952352-d74c-491c-9420-6e999e60f52a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/48952352-d74c-491c-9420-6e999e60f52a/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [4d94ffcd-39a0-4e40-83c1-6093ff82d641](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/4d94ffcd-39a0-4e40-83c1-6093ff82d641/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [4d94ffcd-39a0-4e40-83c1-6093ff82d641](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/4d94ffcd-39a0-4e40-83c1-6093ff82d641/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [52a258e1-0a79-4bb7-8f50-1aa519aa4e00](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/52a258e1-0a79-4bb7-8f50-1aa519aa4e00/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [52a258e1-0a79-4bb7-8f50-1aa519aa4e00](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/52a258e1-0a79-4bb7-8f50-1aa519aa4e00/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [52a258e1-0a79-4bb7-8f50-1aa519aa4e00](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/52a258e1-0a79-4bb7-8f50-1aa519aa4e00/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [591c23ea-1ddd-4800-9203-4b6946979818](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/591c23ea-1ddd-4800-9203-4b6946979818/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [591c23ea-1ddd-4800-9203-4b6946979818](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/591c23ea-1ddd-4800-9203-4b6946979818/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [593382e8-4ad5-4300-b0ad-26c8954281c6](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/593382e8-4ad5-4300-b0ad-26c8954281c6/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [593382e8-4ad5-4300-b0ad-26c8954281c6](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/593382e8-4ad5-4300-b0ad-26c8954281c6/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [5adc911a-c2a1-475c-a347-9da4ee98c6df](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/5adc911a-c2a1-475c-a347-9da4ee98c6df/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [5adc911a-c2a1-475c-a347-9da4ee98c6df](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/5adc911a-c2a1-475c-a347-9da4ee98c6df/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [5aee33a0-e42c-4a79-97b7-40e7ac8b270e](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/5aee33a0-e42c-4a79-97b7-40e7ac8b270e/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [5aee33a0-e42c-4a79-97b7-40e7ac8b270e](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/5aee33a0-e42c-4a79-97b7-40e7ac8b270e/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [5aee33a0-e42c-4a79-97b7-40e7ac8b270e](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/5aee33a0-e42c-4a79-97b7-40e7ac8b270e/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [65ef54b4-48ea-4fc0-a9a7-79b3be807393](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/65ef54b4-48ea-4fc0-a9a7-79b3be807393/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [65ef54b4-48ea-4fc0-a9a7-79b3be807393](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/65ef54b4-48ea-4fc0-a9a7-79b3be807393/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [65ef54b4-48ea-4fc0-a9a7-79b3be807393](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/65ef54b4-48ea-4fc0-a9a7-79b3be807393/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [6abe0474-e60b-438d-b661-4be178e6b4bd](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/6abe0474-e60b-438d-b661-4be178e6b4bd/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [6abe0474-e60b-438d-b661-4be178e6b4bd](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/6abe0474-e60b-438d-b661-4be178e6b4bd/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [6abe0474-e60b-438d-b661-4be178e6b4bd](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/6abe0474-e60b-438d-b661-4be178e6b4bd/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [6cf51e7c-99f4-4c6d-9b1c-6e371c96b742](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/6cf51e7c-99f4-4c6d-9b1c-6e371c96b742/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [6cf51e7c-99f4-4c6d-9b1c-6e371c96b742](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/6cf51e7c-99f4-4c6d-9b1c-6e371c96b742/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [6e82e823-f955-43fa-8b8a-b9cd4ae27778](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/6e82e823-f955-43fa-8b8a-b9cd4ae27778/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [6e82e823-f955-43fa-8b8a-b9cd4ae27778](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/6e82e823-f955-43fa-8b8a-b9cd4ae27778/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [728a543b-9149-4b2a-9e65-3fb41ce3f35b](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/728a543b-9149-4b2a-9e65-3fb41ce3f35b/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [728a543b-9149-4b2a-9e65-3fb41ce3f35b](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/728a543b-9149-4b2a-9e65-3fb41ce3f35b/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [7b1ac1a8-b7be-41ec-a77f-db545af22263](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7b1ac1a8-b7be-41ec-a77f-db545af22263/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [7b1ac1a8-b7be-41ec-a77f-db545af22263](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7b1ac1a8-b7be-41ec-a77f-db545af22263/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [7c3ee345-c9da-4ce2-97e8-727de2e5023a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7c3ee345-c9da-4ce2-97e8-727de2e5023a/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [7c3ee345-c9da-4ce2-97e8-727de2e5023a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7c3ee345-c9da-4ce2-97e8-727de2e5023a/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [7c3ee345-c9da-4ce2-97e8-727de2e5023a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7c3ee345-c9da-4ce2-97e8-727de2e5023a/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [7e22eabf-ac1f-4209-a8f6-dcc8b548b71c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7e22eabf-ac1f-4209-a8f6-dcc8b548b71c/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [7e22eabf-ac1f-4209-a8f6-dcc8b548b71c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/7e22eabf-ac1f-4209-a8f6-dcc8b548b71c/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [8493a3fb-9501-4aa2-83a3-39fbafa6644c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/8493a3fb-9501-4aa2-83a3-39fbafa6644c/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [8493a3fb-9501-4aa2-83a3-39fbafa6644c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/8493a3fb-9501-4aa2-83a3-39fbafa6644c/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [87b7df35-0de4-4c6a-a030-8afac02454f2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/87b7df35-0de4-4c6a-a030-8afac02454f2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [87b7df35-0de4-4c6a-a030-8afac02454f2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/87b7df35-0de4-4c6a-a030-8afac02454f2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [88c4fed3-bef0-450a-b9ff-d736d4568838](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/88c4fed3-bef0-450a-b9ff-d736d4568838/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [88c4fed3-bef0-450a-b9ff-d736d4568838](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/88c4fed3-bef0-450a-b9ff-d736d4568838/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [8e28076e-2fc9-4170-95e9-a4de9e04fd5e](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/8e28076e-2fc9-4170-95e9-a4de9e04fd5e/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [8e28076e-2fc9-4170-95e9-a4de9e04fd5e](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/8e28076e-2fc9-4170-95e9-a4de9e04fd5e/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [93459ee6-e397-477e-b7da-250fb75f5974](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/93459ee6-e397-477e-b7da-250fb75f5974/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [93459ee6-e397-477e-b7da-250fb75f5974](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/93459ee6-e397-477e-b7da-250fb75f5974/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [93459ee6-e397-477e-b7da-250fb75f5974](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/93459ee6-e397-477e-b7da-250fb75f5974/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [964f8143-6ff7-4b80-ad76-4dc59de2af37](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/964f8143-6ff7-4b80-ad76-4dc59de2af37/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [964f8143-6ff7-4b80-ad76-4dc59de2af37](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/964f8143-6ff7-4b80-ad76-4dc59de2af37/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [999617b0-b41a-4a82-910d-f707ce1d7779](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/999617b0-b41a-4a82-910d-f707ce1d7779/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [999617b0-b41a-4a82-910d-f707ce1d7779](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/999617b0-b41a-4a82-910d-f707ce1d7779/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [999617b0-b41a-4a82-910d-f707ce1d7779](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/999617b0-b41a-4a82-910d-f707ce1d7779/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [9f18a5c2-e59f-4582-91b5-401a86234284](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/9f18a5c2-e59f-4582-91b5-401a86234284/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [9f18a5c2-e59f-4582-91b5-401a86234284](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/9f18a5c2-e59f-4582-91b5-401a86234284/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [a7b90108-4f50-4164-87b9-73817e9fdac2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a7b90108-4f50-4164-87b9-73817e9fdac2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [a7b90108-4f50-4164-87b9-73817e9fdac2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a7b90108-4f50-4164-87b9-73817e9fdac2/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [a7b90108-4f50-4164-87b9-73817e9fdac2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a7b90108-4f50-4164-87b9-73817e9fdac2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [a86dcf01-3c5f-43ca-a426-c118d5974332](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a86dcf01-3c5f-43ca-a426-c118d5974332/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [a86dcf01-3c5f-43ca-a426-c118d5974332](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a86dcf01-3c5f-43ca-a426-c118d5974332/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [a9c3e62b-fd84-4701-8024-7e3e60af9ed1](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a9c3e62b-fd84-4701-8024-7e3e60af9ed1/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [a9c3e62b-fd84-4701-8024-7e3e60af9ed1](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a9c3e62b-fd84-4701-8024-7e3e60af9ed1/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [a9c3e62b-fd84-4701-8024-7e3e60af9ed1](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/a9c3e62b-fd84-4701-8024-7e3e60af9ed1/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [ac56c496-c5d6-4c23-be20-130ee8327fd2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ac56c496-c5d6-4c23-be20-130ee8327fd2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ac56c496-c5d6-4c23-be20-130ee8327fd2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ac56c496-c5d6-4c23-be20-130ee8327fd2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ad8c4056-7c25-4dba-a861-ec201afd16fb](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ad8c4056-7c25-4dba-a861-ec201afd16fb/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ad8c4056-7c25-4dba-a861-ec201afd16fb](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ad8c4056-7c25-4dba-a861-ec201afd16fb/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [ad8c4056-7c25-4dba-a861-ec201afd16fb](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ad8c4056-7c25-4dba-a861-ec201afd16fb/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [b536acae-02c7-4c6e-914b-4ea199d98f79](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/b536acae-02c7-4c6e-914b-4ea199d98f79/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [b536acae-02c7-4c6e-914b-4ea199d98f79](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/b536acae-02c7-4c6e-914b-4ea199d98f79/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [b536acae-02c7-4c6e-914b-4ea199d98f79](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/b536acae-02c7-4c6e-914b-4ea199d98f79/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [b9d52b97-7602-457d-a96d-a1950a01b42a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/b9d52b97-7602-457d-a96d-a1950a01b42a/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [b9d52b97-7602-457d-a96d-a1950a01b42a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/b9d52b97-7602-457d-a96d-a1950a01b42a/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ba8bb5f1-966b-4ac1-a311-b2550c0e4858](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ba8bb5f1-966b-4ac1-a311-b2550c0e4858/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ba8bb5f1-966b-4ac1-a311-b2550c0e4858](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ba8bb5f1-966b-4ac1-a311-b2550c0e4858/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [ba8bb5f1-966b-4ac1-a311-b2550c0e4858](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ba8bb5f1-966b-4ac1-a311-b2550c0e4858/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [c15bee15-84c1-494a-ac82-2159b06da175](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/c15bee15-84c1-494a-ac82-2159b06da175/) | Group_1:Denominator Exception | 1 | 1 | 0 | den | qicore-only |
| [cf0c5672-d86d-47fa-b13b-9bdb299c1d47](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/cf0c5672-d86d-47fa-b13b-9bdb299c1d47/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [cf0c5672-d86d-47fa-b13b-9bdb299c1d47](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/cf0c5672-d86d-47fa-b13b-9bdb299c1d47/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [cf0c5672-d86d-47fa-b13b-9bdb299c1d47](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/cf0c5672-d86d-47fa-b13b-9bdb299c1d47/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [cfe6d907-c9fa-4d4c-9889-803315e8f707](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/cfe6d907-c9fa-4d4c-9889-803315e8f707/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [cfe6d907-c9fa-4d4c-9889-803315e8f707](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/cfe6d907-c9fa-4d4c-9889-803315e8f707/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [cfe6d907-c9fa-4d4c-9889-803315e8f707](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/cfe6d907-c9fa-4d4c-9889-803315e8f707/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [d21be273-87ad-4ab5-a936-9de820872e73](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/d21be273-87ad-4ab5-a936-9de820872e73/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [d21be273-87ad-4ab5-a936-9de820872e73](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/d21be273-87ad-4ab5-a936-9de820872e73/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [d8ea50e2-e1a9-41ae-ac73-480bb198d963](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/d8ea50e2-e1a9-41ae-ac73-480bb198d963/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [d8ea50e2-e1a9-41ae-ac73-480bb198d963](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/d8ea50e2-e1a9-41ae-ac73-480bb198d963/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [d8ea50e2-e1a9-41ae-ac73-480bb198d963](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/d8ea50e2-e1a9-41ae-ac73-480bb198d963/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [db5afa02-02e2-4c0d-88c8-d3c0682333a1](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/db5afa02-02e2-4c0d-88c8-d3c0682333a1/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [db5afa02-02e2-4c0d-88c8-d3c0682333a1](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/db5afa02-02e2-4c0d-88c8-d3c0682333a1/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [dd6c17ad-396b-4ff5-9538-e06da5f0a39c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/dd6c17ad-396b-4ff5-9538-e06da5f0a39c/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [dd6c17ad-396b-4ff5-9538-e06da5f0a39c](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/dd6c17ad-396b-4ff5-9538-e06da5f0a39c/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [e081bee5-67f8-464f-9356-9b287e32a35a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e081bee5-67f8-464f-9356-9b287e32a35a/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [e081bee5-67f8-464f-9356-9b287e32a35a](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e081bee5-67f8-464f-9356-9b287e32a35a/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [e13ab79b-1b28-4a37-96cc-e63baa5f88cd](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e13ab79b-1b28-4a37-96cc-e63baa5f88cd/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [e13ab79b-1b28-4a37-96cc-e63baa5f88cd](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e13ab79b-1b28-4a37-96cc-e63baa5f88cd/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [e13ab79b-1b28-4a37-96cc-e63baa5f88cd](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e13ab79b-1b28-4a37-96cc-e63baa5f88cd/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [e6f270ed-ddb3-43cf-a2f7-ef26df352d4d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e6f270ed-ddb3-43cf-a2f7-ef26df352d4d/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [e6f270ed-ddb3-43cf-a2f7-ef26df352d4d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e6f270ed-ddb3-43cf-a2f7-ef26df352d4d/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [e6f270ed-ddb3-43cf-a2f7-ef26df352d4d](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e6f270ed-ddb3-43cf-a2f7-ef26df352d4d/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [e8e62dbf-ce54-4f04-b2d9-f574b8ded2c4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e8e62dbf-ce54-4f04-b2d9-f574b8ded2c4/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [e8e62dbf-ce54-4f04-b2d9-f574b8ded2c4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e8e62dbf-ce54-4f04-b2d9-f574b8ded2c4/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [e8e62dbf-ce54-4f04-b2d9-f574b8ded2c4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e8e62dbf-ce54-4f04-b2d9-f574b8ded2c4/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [e9074892-9513-48d7-999e-afeace427512](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e9074892-9513-48d7-999e-afeace427512/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [e9074892-9513-48d7-999e-afeace427512](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/e9074892-9513-48d7-999e-afeace427512/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ea41e48d-7b6d-4c9e-a8a1-f9c4bcf30785](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ea41e48d-7b6d-4c9e-a8a1-f9c4bcf30785/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ea41e48d-7b6d-4c9e-a8a1-f9c4bcf30785](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ea41e48d-7b6d-4c9e-a8a1-f9c4bcf30785/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [eb5173bb-769a-4c95-b0e9-362a271f72ea](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/eb5173bb-769a-4c95-b0e9-362a271f72ea/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [eb5173bb-769a-4c95-b0e9-362a271f72ea](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/eb5173bb-769a-4c95-b0e9-362a271f72ea/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [eb5173bb-769a-4c95-b0e9-362a271f72ea](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/eb5173bb-769a-4c95-b0e9-362a271f72ea/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [ed5ae1dd-5a2e-4b69-9044-1f4cbed2fcfc](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ed5ae1dd-5a2e-4b69-9044-1f4cbed2fcfc/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [ed5ae1dd-5a2e-4b69-9044-1f4cbed2fcfc](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/ed5ae1dd-5a2e-4b69-9044-1f4cbed2fcfc/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [f705cc70-0d7d-4dc1-88f7-9b37ab5290d2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/f705cc70-0d7d-4dc1-88f7-9b37ab5290d2/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [f705cc70-0d7d-4dc1-88f7-9b37ab5290d2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/f705cc70-0d7d-4dc1-88f7-9b37ab5290d2/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |
| [f705cc70-0d7d-4dc1-88f7-9b37ab5290d2](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/f705cc70-0d7d-4dc1-88f7-9b37ab5290d2/) | Group_1:Numerator | 1 | 1 | 0 | num | qicore-only |
| [fdd3fe25-b12c-4417-a999-91e4583f6cd4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/fdd3fe25-b12c-4417-a999-91e4583f6cd4/) | Group_1:Denominator | 1 | 1 | 0 | den | qicore-only |
| [fdd3fe25-b12c-4417-a999-91e4583f6cd4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/fdd3fe25-b12c-4417-a999-91e4583f6cd4/) | Group_1:Denominator Exclusion | 1 | 1 | 0 | den | qicore-only |
| [fdd3fe25-b12c-4417-a999-91e4583f6cd4](../../input/tests/measure/CMS104FHIRSTKDCAntithrombotic/fdd3fe25-b12c-4417-a999-91e4583f6cd4/) | Group_1:Initial Population | 1 | 1 | 0 | ini | qicore-only |

