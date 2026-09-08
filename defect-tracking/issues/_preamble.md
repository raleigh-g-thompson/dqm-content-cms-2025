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

Cross-referenced to `conversion-notes.md` entries (#N) and `change-classification.md` (§5).

---