+++
schema_version = 1
generated_from = "defect-tracking/issues/"
+++
# Engine / Translator Issues Tracker

Running list of confirmed, suspected, and unverified engine/translator issues surfaced by the
QICore → USQualityCore migration test suite. These are issues in `clinical_quality_language`
(cql-engine, cql-to-elm) and/or `clinical-reasoning` (cqf-fhir-cql), **not** bugs in this
repo's CQL or test fixtures. Every confirmed issue has a reproducible symptom and a
currently-applied CQL-level workaround (where one exists); the workaround is a mitigation, not
proof the underlying behavior is correct.

Measures are classified in the per-issue entries (category `engine` / `content` / `migration` /
`fixture`); the depth analysis lives in `defect-tracking/proposed-engine-fixes.md` and the
generated outputs in `defect-tracking/engine-issues.md` + `scripts/comparison/`.

---