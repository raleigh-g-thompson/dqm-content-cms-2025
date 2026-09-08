"""Canonical measure-population vocabulary, shared by every report.

Why this exists
---------------
The three CSVs that feed the reports do not agree on population names, because
they are produced by different extractors:

  * ``expected_results.csv`` takes the name from the fixture MeasureReport's
    ``code.coding[0].display``, except for two hardcoded MeasureObservation ids
    (see ``extract_population_expected.py``). CMS986 therefore says
    "Measure Population Observation".
  * ``actual_results.csv`` / ``qicore-2025-actual-results.csv`` say
    "Measure Observation" for that same population.

Comparison keys include the population name, so without a single canonical form
the CMS986 expected rows can never match their actual rows.

Separately, ``compare_results.ValidMeasurePopulationTypes`` historically listed
*plural* spellings ("Numerator Observations", "Denominator Observations") that
appear in none of the CSVs, while the singular forms the data actually uses were
absent. Cells whose population was not on that list were silently skipped by the
scorer -- 1,031 of 24,853 expected cells (4.1%), across CMS986, CMS1017 and
CMS871. Two catalog issues (C-03, C-04) were tracking defects in rows that the
scorer structurally could not fail on.

Canonicalise once, at CSV-read time, so every downstream key agrees.

The canonical names follow the HL7 measure-population CodeSystem
(https://terminology.hl7.org/CodeSystem-measure-population.html), plus the
"Denominator Observation"/"Numerator Observation" refinements the tooling emits
when a group carries more than one measure-observation.
"""

from typing import Dict, FrozenSet, Tuple

# Populations that participate in scoring, in canonical spelling.
CANONICAL_POPULATIONS: FrozenSet[str] = frozenset({
    "Initial Population",
    "Denominator",
    "Denominator Exclusion",
    "Denominator Exception",
    "Denominator Observation",
    "Numerator",
    "Numerator Exclusion",
    "Numerator Observation",
    "Measure Population",
    "Measure Population Exclusion",
    "Measure Observation",
})

# Non-canonical spellings seen in real inputs -> canonical form.
#
# "Measure Population Observation" is the load-bearing one: it is what the
# expected-results extractor emits for CMS986 while both engines emit
# "Measure Observation". The rest are defensive -- older exports and the
# hyphenated lowercase variants that were carried in the previous allowlist.
POPULATION_ALIASES: Dict[str, str] = {
    "Measure Population Observation": "Measure Observation",
    "Numerator Observations": "Numerator Observation",
    "Denominator Observations": "Denominator Observation",
    "Denominator-exclusion": "Denominator Exclusion",
    "Denominator-exception": "Denominator Exception",
}


def canonical_population(name: str) -> str:
    """Map a raw population name to its canonical spelling.

    Unknown names are returned unchanged so the caller can report them as
    unscored rather than having them silently disappear.
    """
    return POPULATION_ALIASES.get(name, name)


def is_scored(name: str) -> bool:
    """True if ``name`` (raw or canonical) participates in scoring."""
    return canonical_population(name) in CANONICAL_POPULATIONS


def split_population(population: str) -> Tuple[str, str]:
    """Split a ``"Group_N:Population Name"`` cell into (group, canonical name).

    Raises ValueError if the string is not in ``group:population`` form, rather
    than silently mangling it -- a malformed population column means the
    upstream extractor is broken and should fail loudly.
    """
    group, _, name = population.partition(":")
    if not _:
        raise ValueError(
            f"population {population!r} is not in 'Group_N:Population' form")
    return group, canonical_population(name)


def canonical_cell(population: str) -> str:
    """Return the ``"Group_N:Population"`` string with the name canonicalised."""
    group, name = split_population(population)
    return f"{group}:{name}"
