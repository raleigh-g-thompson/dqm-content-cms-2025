# Engine Fixes

Changelog of fixes applied to measure CQL to work around or correct engine
behavior, and to adhere to recommended FHIR/CQL authoring patterns. Each entry
records the measure, the change, and the reason.

## Raw Choice values in temporal operators (engine issue E-02)

### Symptom

Using a raw `Choice<FHIR.dateTime, FHIR.Period>` element (such as
`DiagnosticReportNote.effective`) as the right-hand operand of quantity-offset
temporal operators fails engine overload resolution. The comparison evaluates
to `null`, which empties the enclosing `from`/`where` result.

Broken pattern:

```cql
AntiCoagulantOrdered.authoredOn 12 hours or less before VTEStudy.effective
IndexPCP.period starts 30 days or less on or before VTEStudy.effective
```

### Fix applied

Normalize the choice-typed operand with the `.toInterval()` fluent function,
then take `start of` the resulting interval for the point comparison:

```cql
AntiCoagulantOrdered.authoredOn 12 hours or less before start of VTEStudy.effective.toInterval ( )
or AntiCoagulantOrdered.authoredOn 12 hours or less after start of VTEStudy.effective.toInterval ( )
IndexPCP.period starts 30 days or less on or before start of VTEStudy.effective.toInterval ( )
```

`.toInterval()` handles both branches of the choice (`effectiveDateTime` and
`effectivePeriod`). A `.value` extraction only resolves the `dateTime` branch
and is not robust to `effectivePeriod` instances, so it is not used.

### Measures changed

- CMS1173 `"Qualified VTE Encounters"`.

### Reference

HL7 FHIR/CQL Implementation Guide — Patterns, "Choices":

https://hl7.org/fhir/uv/cql/3.0.0-202609-ballot/en/patterns.html#choices