#!/usr/bin/env python3
"""Validate that each test case's resource date values fall within the expected
measurement-period bounds (or the extended 1-year-before window), cross-referenced
with the case's stated intent (cqfm-testCaseDescription) and its expected
MeasureReport population counts.

Tier-1 structural tool: it does NOT parse measure logic. It classifies every
date/datetime leaf against two computation windows derived from the global
``Measurement Period`` supplied via ``input/tests/config.json``:

  * mp   = the measurement period itself, e.g. [2026-01-01Z, 2027-01-01Z)
  * ext  = the 1-year-before window,       e.g. [2025-01-01Z, 2027-01-01Z)
           (Hospice/Palliative/AdvancedIllness-and-Frailty use "year before or
           during" and overlap semantics against MP)

Output is one concise JSON object per test case (JSON only; no CSV yet):

  {
    "case": "f9ef1fd1",
    "description": "Patient has dementia that starts during the measaurement period",
    "populations": {"initial-population":1,"denominator":1,"denominator-exclusion":1,"numerator":0},
    "resources": [ {"type":"Condition","path":"onsetDateTime","raw":"...","mp":"inside","ext":"inside"}, ... ],
    "flags": ["Condition.onsetPeriod::overlaps"]
  }

A reviewer cross-references description + populations + each flagged date to spot
resources that a description implies should be inside MP but fall outside (or vice
versa).
"""
import argparse
import collections
import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

ISO_RE = re.compile(
    r"^(?P<y>\d{4})(?:-(?P<mo>\d{2})(?:-(?P<d>\d{2})"
    r"(?:[T ](?P<h>\d{2}):(?P<mi>\d{2})(?::(?P<s>\d{2})(?:\.(?P<f>\d+))?)?"
    r"(?P<z>Z|[+-]\d{2}:?\d{2})?)?)?)?$"
)

# Resource-type -> list of date-ish JSON paths the measurement logic keys on.
RESOURCE_DATE_PATHS = {
    "Encounter": ["period"],
    "Observation": ["effectiveDateTime", "effectivePeriod", "issued"],
    "Condition": ["onsetDateTime", "onsetPeriod", "recordedDate", "abatementPeriod"],
    "Procedure": ["performedDateTime", "performedPeriod"],
    "MedicationRequest": ["authoredOn"],
    "DeviceRequest": ["authoredOn"],
    "ServiceRequest": ["authoredOn"],
    "Patient": ["birthDate", "deceasedDateTime"],
    "Coverage": ["period"],
    # Clinically-relevant types carrying event-in-MP dates. Metadata-only types
    # (Organization/Practitioner/Location/Device/Medication.batch.expirationDate)
    # are deliberately excluded — their dates are not clinical events.
    "Claim": ["created", "item.servicedDate", "billablePeriod"],
    "Immunization": ["occurrenceDateTime", "recorded"],
    "MedicationAdministration": ["effectiveDateTime", "effectivePeriod", "extension.valueDateTime"],
    "DiagnosticReport": ["effectiveDateTime"],
    "MedicationDispense": ["whenHandedOver"],
    "Task": ["executionPeriod"],
    "Specimen": ["receivedTime", "collection.collectedDateTime"],
    "AdverseEvent": ["date", "recordedDate", "detected"],
    "AllergyIntolerance": ["onsetDateTime", "onsetPeriod", "recordedDate"],
    "Communication": ["sent", "extension.valueDateTime"],
}

# Measure dirs whose library declares a non-annual (e.g. monthly) Measurement
# Period default and so cannot be validated against the global annual MP.
# Excluded from --all; their own config drives a different window.
MONTHLY_MP_MEASURES = {"NHSNAcuteCareHospitalMonthlyInitialPopulation1"}

# Shared class-B libraries; used to group measures in the aggregate so the
# Hospice/Palliative/AIF boundary patterns can be compared inside vs outside
# the shared-lib family.
SHARED_LIB_MEASURES = {}
for _lib, _measures in {
    "Hospice": "CMS117FHIRChildImmunStatus CMS122FHIRDiabetesAssessGT9Pct CMS124FHIRCervicalCancerScreen CMS125FHIRBreastCancerScreen CMS128FHIRAntidepressantMgmt CMS130FHIRColorectalCancerScrn CMS131FHIRDiabetesEyeExam CMS136FHIRChildADHDMedFollowUp CMS137FHIRSUDTxInitEngagement CMS138FHIRTobaccoScrnCessation CMS139FHIRFallRiskScreening CMS146FHIRApproTestPharyngitis CMS153FHIRChlamydiaScreening CMS154FHIRAppropriateTxforURI CMS155FHIRWgtAssessCounseling CMS156FHIRHighRiskMedsElderly CMS165FHIRControllingHighBP CMS347FHIRStatinPreventionTxCVD CMS56FHIRFuncStatHipReplacement CMS69FHIRPCSBMIScreenAndFollowUp CMS74FHIRDentalCariesPrevention CMS75FHIRChildrenDentalDecay CMS90FHIRFSAforHeartFailure CMS951FHIRKidneyHealthEval",
    "PalliativeCare": "CMS122FHIRDiabetesAssessGT9Pct CMS124FHIRCervicalCancerScreen CMS125FHIRBreastCancerScreen CMS130FHIRColorectalCancerScrn CMS131FHIRDiabetesEyeExam CMS156FHIRHighRiskMedsElderly CMS165FHIRControllingHighBP CMS347FHIRStatinPreventionTxCVD CMS69FHIRPCSBMIScreenAndFollowUp CMS951FHIRKidneyHealthEval",
    "AdvancedIllnessandFrailty": "CMS122FHIRDiabetesAssessGT9Pct CMS125FHIRBreastCancerScreen CMS130FHIRColorectalCancerScrn CMS131FHIRDiabetesEyeExam CMS165FHIRControllingHighBP",
}.items():
    for _m in _measures.split():
        SHARED_LIB_MEASURES.setdefault(_m, []).append(_lib)


DESC_EXT = "http://hl7.org/fhir/us/cqfmeasures/StructureDefinition/cqfm-testCaseDescription"

# Relationship labels that count as "the resource overlaps / is within the
# measurement period" for trigger detection (the resource could have matched).
IN_MP = {
    "inside",
    "overlaps",
    "overlaps-start",
    "overlaps-end",
    "starts-at-start",
    "ends-at-end",
    "contains-mp",
}

# Description keywords that signal an intentional lookback/out-of-MP negative
# control (screening lookbacks, before/after-MP intents). Suppresses the
# low-confidence "num-before-event" rule.
LOOKBACK_RE = re.compile(
    r"prior|years? (before|prior)|before the|after the|not performed|before mp|after mp|"
    r"\b(?:two|three|four|five|six|seven|eight|nine|ten)\s+years?",
    re.I,
)


def parse_dt(raw):
    """Parse an ISO date/dateTime string to (value, kind)."""
    if not isinstance(raw, str):
        return None, None
    m = ISO_RE.match(raw.strip())
    if not m:
        return None, None
    y = int(m.group("y"))
    mo = int(m.group("mo") or 1)
    d = int(m.group("d") or 1)
    h = int(m.group("h") or 0)
    mi = int(m.group("mi") or 0)
    s = int(m.group("s") or 0)
    f = m.group("f")
    us = int((f or "")[:6].ljust(6, "0"))
    tz = m.group("z")
    if tz in (None, ""):
        kw = None
    elif tz == "Z":
        kw = timezone.utc
    else:
        sign = 1 if tz[0] == "+" else -1
        tzc = re.sub(r":", "", tz[1:]).ljust(4, "0")
        kw = timezone(sign * timedelta(hours=int(tzc[:2]), minutes=int(tzc[2:4])))
    dt = datetime(y, mo, d, h, mi, s, us, tzinfo=kw)
    has_dt = bool(h or mi or s or us)
    kind = "datetime" if has_dt else ("date" if m.group("d") else ("year-month" if m.group("mo") else "year"))
    return dt, kind


def as_datetime(v):
    """UTC-aware datetime for an ISO scalar, or None. Naive values (no zone,
    e.g. a date-only birthDate) are interpreted as UTC."""
    if v is None:
        return None
    dt, _ = parse_dt(v)
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=timezone.utc)


def age_at(mp_end_dt, birth_dt):
    """Whole-year age of a birth datetime at the MP-end datetime, or None."""
    if mp_end_dt is None or birth_dt is None or birth_dt > mp_end_dt:
        return None
    return mp_end_dt.year - birth_dt.year - (
        (mp_end_dt.month, mp_end_dt.day) < (birth_dt.month, birth_dt.day)
    )


def classify_scalar(raw, mp_start, mp_end, ext_start):
    """mp/ext classification for a single date/datetime scalar."""
    dt = as_datetime(raw)
    if dt is None:
        return None, None
    if dt == mp_start:
        mpc = "boundary-start"
    elif dt == mp_end:
        mpc = "boundary-end"
    elif mp_start < dt < mp_end:
        mpc = "inside"
    else:
        mpc = "outside"
    extc = "inside" if ext_start <= dt < mp_end else "outside"
    return mpc, extc


def classify_period(raws, mp_start, mp_end, ext_start):
    """Classify a FHIR period against mp/ext using overlap semantics.

    raws is a list of {path_suffix, raw} collected from the same parent path
    (e.g. period.start / period.end). Missing bound -> point-like.
    """
    rmap = {}
    for r in raws:
        rmap[r[0]] = r[1]
    s = as_datetime(rmap.get("start"))
    e = as_datetime(rmap.get("end"))
    if s is None and e is None:
        return None, None
    has_both = s is not None and e is not None
    if has_both and e < s:
        s, e = e, s
    lo = s if s is not None else e
    hi = e if e is not None else s

    def cmp_rel(lo, hi):
        if (hi is not None and hi < mp_start):
            return "before"
        if (lo is not None and lo > mp_end):
            return "after"
        # MP is half-open [mp_start, mp_end); surface boundary-touching
        # placements, which are the edge cases MP-bound validation targets.
        if has_both and hi is not None and hi == mp_end:
            return "ends-at-end"
        if has_both and lo is not None and lo == mp_start:
            return "starts-at-start"
        lo_ok = lo is not None and lo >= mp_start
        hi_ok = hi is not None and hi <= mp_end
        if has_both and lo_ok and hi_ok:
            return "inside"
        if has_both and lo is not None and lo < mp_start and hi is not None and hi > mp_end:
            return "contains-mp"
        if has_both:
            return "overlaps"
        # single-bound -> point-like using the present bound
        pt = lo if s is not None else hi
        if pt == mp_start:
            return "boundary-start"
        if pt == mp_end:
            return "boundary-end"
        if mp_start < pt < mp_end:
            return "inside"
        return "outside"

    rel = cmp_rel(lo, hi)
    # relationship to ext window (point-like or interval)
    if (lo is not None and lo >= ext_start) and (hi is not None and hi <= mp_end):
        extc = "inside"
    elif (lo is None or lo < ext_start) and (hi is not None and hi > ext_start):
        extc = "overlaps"
    elif (lo is not None and ext_start <= lo <= mp_end) and (hi is None or hi > mp_end):
        extc = "overlaps"
    elif lo is not None and lo == hi:
        extc = "inside" if ext_start <= lo <= mp_end else "outside"
    else:
        extc = "outside"
    return rel, extc


def gather_leaf_dates(obj, path, out):
    """Collect every (dotted-path, raw) ISO date leaf under a JSON value."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            gather_leaf_dates(v, path + [k], out)
    elif isinstance(obj, list):
        for v in obj:
            gather_leaf_dates(v, path, out)
    elif isinstance(obj, str) and ISO_RE.match(obj):
        out.append((".".join(path), obj))


def build_windows(config):
    """Return (mp_start, mp_end, ext_start) from the global MP in config.json."""
    mp = None
    for p in config.get("parameters", []):
        if p.get("name") == "Measurement Period":
            mp = p.get("value")
            break
    if not mp:
        raise SystemExit("config.json: no global 'Measurement Period' parameter found")
    m = re.search(r"@([^,\])]+)\s*,\s*@([^)\]]+)", mp)
    if not m:
        raise SystemExit(f"config.json: cannot parse Measurement Period: {mp}")
    mp_start = as_datetime(m.group(1))
    mp_end = as_datetime(m.group(2))
    ext_start = mp_start - timedelta(days=365)
    return mp_start, mp_end, ext_start


def process_resources(case_dir, mp_start, mp_end, ext_start):
    """Return (resources, flags) for all non-MeasureReport resources in a case dir."""
    resources = []
    flags = []
    # First pass: gather all leaf dates per resource.
    for f in sorted(case_dir.glob("*.json")):
        if f.name.startswith("MeasureReport-"):
            continue
        r = json.load(open(f))
        rt = r.get("resourceType")
        rid = r.get("id")
        leaves = []
        gather_leaf_dates(r, [], leaves)
        # bucket leaves by configured path
        buckets = {}
        for dotted, raw in leaves:
            # map dotted path to the configured parent (period or scalar)
            bucket = None
            for cfg in RESOURCE_DATE_PATHS.get(rt, []):
                base = cfg.split(".")[0] if "." in cfg else cfg
                if dotted == cfg or dotted.startswith(cfg + "."):
                    bucket = cfg
                    break
            if bucket is None:
                continue
            buckets.setdefault(bucket, []).append((dotted.rsplit(".", 1)[-1], raw))
        for cfg, raws in buckets.items():
            is_period = "period" in cfg or "Period" in cfg
            if is_period:
                rel, extc = classify_period(raws, mp_start, mp_end, ext_start)
                raw_str = "[" + ", ".join(raw for _, raw in sorted(raws)) + "]"
                rec = {"type": rt, "path": cfg, "raw": raw_str, "mp": rel, "ext": extc}
                if rid:
                    rec["id"] = rid
                resources.append(rec)
                if rt == "Patient" and cfg == "deceasedDateTime":
                    continue
                if rel in ("before", "after", "contains-mp", "overlaps",
                           "ends-at-end", "starts-at-start", "overlaps-start", "overlaps-end"):
                    flags.append(f"{rt}.{cfg}::{rel}")
            else:
                for suffix, raw in raws:
                    mpc, extc = classify_scalar(raw, mp_start, mp_end, ext_start)
                    rec = {"type": rt, "path": cfg, "raw": raw, "mp": mpc, "ext": extc}
                    if rid:
                        rec["id"] = rid
                    if rt == "Patient" and cfg == "birthDate":
                        age = age_at(mp_end, as_datetime(raw))
                        if age is not None:
                            rec["ageAtEnd"] = age
                    resources.append(rec)
                    if rt == "Patient" and cfg in ("birthDate", "deceasedDateTime"):
                        # these are intentionally outside MP (born/deceased); not a date-bound anomaly
                        continue
                    if mpc == "outside" and extc == "outside":
                        flags.append(f"{rt}.{cfg}@{raw}::outside")
    return resources, sorted(set(flags))


def process_measure(measure, tests_root, mp_start, mp_end, ext_start):
    """Return the list of case dicts for a single measure dir."""
    measure_dir = tests_root / "measure" / measure
    cases = []
    for case_dir in sorted(measure_dir.iterdir()):
        if not case_dir.is_dir():
            continue
        mrs = list(case_dir.glob("MeasureReport-*.json"))
        if not mrs:
            print(f"[skip] {case_dir.name}: no MeasureReport-*.json", file=sys.stderr)
            continue
        mr = json.load(open(mrs[0]))
        desc = None
        for e in mr.get("extension", []):
            if e.get("url") == DESC_EXT:
                desc = e.get("valueMarkdown")
        pops = {}
        for g in mr.get("group", []):
            for p in g.get("population", []):
                code = (p.get("code", {}).get("coding") or [{}])[0].get("code")
                if code:
                    pops[code] = p.get("count")
        resources, flags = process_resources(case_dir, mp_start, mp_end, ext_start)
        contradictions = detect_contradictions(desc, pops, resources)
        cases.append(
            {
                "case": case_dir.name[:8],
                "id": case_dir.name,
                "description": desc,
                "populations": pops,
                "resources": resources,
                "flags": flags,
                "contradictions": contradictions,
            }
        )
    return cases


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--measure",
        default=None,
        help="measure dir under input/tests/measure (default CMS130FHIRColorectalCancerScrn)",
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="run every measure dir with test cases (skips monthly-MP measures)",
    )
    ap.add_argument(
        "--aggregate",
        action="store_true",
        help="write aggregate_mp_patterns.json: cross-measure tallies, contradiction "
        "rule frequency, and shared-lib family grouping from per-measure outputs",
    )
    ap.add_argument(
        "--tests-root",
        default=None,
        help="override input/tests root (default <repo>/input/tests)",
    )
    ap.add_argument(
        "--summary",
        action="store_true",
        help="also print a compact one-line-per-case summary to stdout",
    )
    ap.add_argument(
        "--contradictions-only",
        action="store_true",
        help="with --summary, print only cases that have inferred contradictions",
    )
    args = ap.parse_args()

    repo = Path(__file__).resolve().parents[2]
    tests_root = Path(args.tests_root) if args.tests_root else repo / "input/tests"
    config_path = tests_root / "config.json"
    out_root = repo / "scripts" / "comparison" / "output"
    out_root.mkdir(parents=True, exist_ok=True)
    with open(config_path) as f:
        config = json.load(f)
    mp_start, mp_end, ext_start = build_windows(config)

    measures = []
    if args.all:
        if args.measure:
            raise SystemExit("--measure cannot be combined with --all")
        measures_root = tests_root / "measure"
        for m in sorted(p.name for p in measures_root.iterdir() if p.is_dir()):
            if m in MONTHLY_MP_MEASURES:
                print(f"[skip] {m}: monthly Measurement Period (not validated)", file=sys.stderr)
                continue
            if not any((measures_root / m).glob("*/MeasureReport-*.json")):
                continue
            measures.append(m)
    elif args.measure:
        measures.append(args.measure)
    elif not args.aggregate:
        # no flags: default to CMS130 for backward compatibility
        measures.append("CMS130FHIRColorectalCancerScrn")

    for measure in measures:
        measure_dir = tests_root / "measure" / measure
        if not measure_dir.is_dir():
            raise SystemExit(f"measure dir not found: {measure_dir}")
        cases = process_measure(measure, tests_root, mp_start, mp_end, ext_start)
        out_path = out_root / f"testcase_mp_validate_{measure}.json"
        with open(out_path, "w") as f:
            json.dump(cases, f, indent=2)
        print(f"wrote {len(cases)} cases -> {out_path}")
        if args.summary and not args.all:
            _print_summary(cases, contradictions_only=args.contradictions_only)

    if args.aggregate:
        aggregate_path = aggregate(out_root)
        print(f"wrote aggregate -> {aggregate_path}")


def detect_contradictions(desc, pops, resources):
    """Return a list of {rules, detail} contradiction entries inferred from a
    case's population counts vs. its resource date placements.

    Tier-1 heuristic: no measure-logic parsing. Patient birthDate/deceased are
    excluded (they are never exclusion triggers). Conservative so genuinely
    misaligned cases surface; intended negative-controls remain flagged (MED)
    for the reviewer to confirm via the description.
    """
    events = [r for r in resources if r.get("type") != "Patient"]
    trigger_in = [r for r in events if r.get("mp") in IN_MP]
    outside = [r for r in events if r.get("mp") == "outside" and r.get("ext") == "outside"]
    after = [r for r in events if r.get("mp") == "after"]
    before = [r for r in events if r.get("mp") == "before"]
    den = pops.get("denominator", 0)
    dexc = pops.get("denominator-exclusion", 0)
    num = pops.get("numerator", 0)
    ini = pops.get("initial-population", 0)
    pos_claim = ini or den or dexc or num
    lookback = bool(LOOKBACK_RE.search(desc or ""))

    def entry(rules, detail):
        return {"rules": sorted(rules), "detail": detail}

    out = []
    if pos_claim and not trigger_in:
        out.append(entry(["claim-no-trigger"], "no event resource overlaps the measurement period"))
    if (den or dexc or ini) and outside:
        d = "; ".join(f"{r['type']}.{r['path']}@{r['raw']}" for r in outside)
        out.append(entry(["outside-event-with-claim"], d))
    if (den or dexc or ini) and after:
        d = "; ".join(f"{r['type']}.{r['path']}::{r['mp']}" for r in after)
        out.append(entry(["after-event-with-claim"], d))
    if num and before and not trigger_in and not lookback:
        d = "; ".join(f"{r['type']}.{r['path']}::{r['mp']}" for r in before)
        out.append(entry(["num-before-event"], d))
    if den and not any(r.get("type") == "Encounter" and r.get("mp") in IN_MP for r in events):
        out.append(entry(["den-without-in-mp-encounter"], "denominator=1 but no in-MP Encounter"))
    return out


POP_ABBREV = {
    "initial-population": "ini",
    "denominator": "den",
    "denominator-exclusion": "dexc",
    "denominator-exception": "dxcp",
    "numerator": "num",
    "measure-population": "mpop",
    "measure-population-exclusion": "mpx",
    "measure-observation": "obs",
}


def _abbrev_pop(code):
    if code in POP_ABBREV:
        return POP_ABBREV[code]
    # prefix match (e.g. a measure-specific population id)
    for k, v in POP_ABBREV.items():
        if code.startswith(k):
            return v
    # fall back to first token
    return code.split("-")[0][:3]


def _print_summary(cases, contradictions_only=False):
    """Compact one-line-per-case review table (stdout only; JSON is the artifact)."""
    rows = [c for c in cases if (not contradictions_only or c["contradictions"])]
    print()
    print(f"{'case':9} {'pops':22} {'contradictions/date-flags'}")
    for c in rows:
        pops = ",".join(
            f"{_abbrev_pop(k)}={v}" for k, v in sorted(c["populations"].items())
        )
        print(f"{c['case']:9} {pops:22} {_disp(c)}")


def _disp(c):
    cont = "; ".join(sorted({r for e in c["contradictions"] for r in e["rules"]}))
    if cont:
        return f"⚠ {cont}"
    if c.get("flags"):
        return "; ".join(c["flags"])
    return "."


REL_LABELS = {
    "inside": "inside",
    "outside": "outside",
    "before": "before",
    "after": "after",
    "boundary-start": "boundary-start",
    "boundary-end": "boundary-end",
    "overlaps": "period-overlaps",
    "overlaps-start": "period-overlaps-start",
    "overlaps-end": "period-overlaps-end",
    "starts-at-start": "period-starts-at-start",
    "ends-at-end": "period-ends-at-end",
    "contains-mp": "period-contains-mp",
}


def aggregate(out_root):
    """Collate per-measure outputs into cross-measure pattern tallies.

    Reads every testcase_mp_validate_<measure>.json in out_root, then writes
    aggregate_mp_patterns.json and prints a summary table. Grouping by shared
    class-B library lets us compare the Hospice/Palliative/AIF boundary patterns
    inside vs. outside that family.
    """
    files = sorted(out_root.glob("testcase_mp_validate_*.json"))
    per_measure = {}
    for f in files:
        measure = f.name[len("testcase_mp_validate_"):-len(".json")]
        per_measure[measure] = json.load(open(f))

    # --- tallies ---------------------------------------------------------
    # flags by relationship label, per resource type and per measure
    rel_by_type = collections.Counter()
    rel_by_measure = collections.defaultdict(collections.Counter)
    # contradiction rule frequency
    rule_freq = collections.Counter()
    cases_with_contra = 0
    total_cases = 0
    contra_per_measure = {}
    flag_per_type = collections.Counter()

    for measure, cases in per_measure.items():
        n_contra = 0
        for c in cases:
            total_cases += 1
            flags = set(c["flags"])
            if c["contradictions"]:
                n_contra += 1
                cases_with_contra += 1
                for e in c["contradictions"]:
                    for r in e["rules"]:
                        rule_freq[r] += 1
            for fl in flags:
                # flag forms: "Type.path::{rel}" (period) or "Type.path@raw::outside" (scalar)
                type_name = fl.split(".")[0]
                flag_per_type[type_name] += 1
                if "::" in fl:
                    rel = fl.split("::", 1)[1]
                    rel_by_type[(type_name, rel)] += 1
                    rel_by_measure[measure][rel] += 1
        contra_per_measure[measure] = n_contra

    # resource count by type (for context in the report)
    res_type_counts = collections.Counter()
    for cases in per_measure.values():
        for c in cases:
            for r in c["resources"]:
                res_type_counts[r["type"]] += 1

    # shared-lib grouping
    family = {"shared-lib": {"measures": set(), "measures_any": set(), "contra": 0, "cases": 0,
                             "rel": collections.Counter()},
              "non-shared": {"measures": set(), "measures_any": set(), "contra": 0, "cases": 0,
                             "rel": collections.Counter()}}
    for measure, cases in per_measure.items():
        fam = "shared-lib" if measure in SHARED_LIB_MEASURES else "non-shared"
        family[fam]["measures"].add(measure)
        for c in cases:
            family[fam]["cases"] += 1
            if c["contradictions"]:
                family[fam]["contra"] += 1
            for fl in c["flags"]:
                if "::" in fl:
                    family[fam]["rel"][fl.split("::", 1)[1]] += 1

    rules_overall = sorted(rule_freq.items(), key=lambda x: -x[1])
    measures_overall = sorted(contra_per_measure.items(), key=lambda x: -x[1])

    report = {
        "generated": str(datetime.now(timezone.utc)),
        "measures": len(per_measure),
        "cases": total_cases,
        "cases_with_contradictions": cases_with_contra,
        "resource_types_count": dict(sorted(res_type_counts.items(), key=lambda x: -x[1])),
        "contradiction_rules": [{"rule": r, "count": k} for r, k in rules_overall],
        "contradictions_per_measure": [{"measure": m, "count": k} for m, k in measures_overall],
        "flag_relationship_by_resource_type": [
            {"type": (k := key)[0], "relationship": key[1], "count": v}
            for key, v in sorted(rel_by_type.items(), key=lambda x: -x[1])
        ],
        "flag_relationship_by_measure": {
            m: dict(sorted(v.items(), key=lambda x: -x[1]))
            for m, v in sorted(rel_by_measure.items(), key=lambda x: -sum(x[1].values()))
        },
        "shared_lib_family": {
            fam: {
                "measures": sorted(d["measures"]),
                "cases": d["cases"],
                "cases_with_contradictions": d["contra"],
                "relationships": dict(sorted(d["rel"].items(), key=lambda x: -x[1])),
            }
            for fam, d in family.items()
        },
    }

    out_path = out_root / "aggregate_mp_patterns.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)

    # --- stdout summary --------------------------------------------------
    print(f"\n=== cross-measure date-placement patterns ({len(per_measure)} measures, {total_cases} cases) ===")
    print(f"cases with contradictions: {cases_with_contra} ({100*cases_with_contra/max(total_cases,1):.0f}%)")
    print("\n-- contradiction rules (count) --")
    for r, k in rules_overall:
        print(f"  {k:5}  {r}")
    print("\n-- top measures by contradictions --")
    for m, k in measures_overall[:15]:
        print(f"  {k:4}  {m}")
    print("\n-- shared-lib family comparison --")
    for fam, d in family.items():
        rels = ", ".join(f"{r}={k}" for r, k in d["rel"].most_common(6))
        print(f"  {fam:12} measures={len(d['measures']):3} cases={d['cases']:4} "
              f"contra={d['contra']:4}  {rels}")
    print("\n-- flag relationships by resource type (top) --")
    for key, v in sorted(rel_by_type.items(), key=lambda x: -x[1])[:20]:
        print(f"  {v:5}  {key[0]}.{key[1]}")
    return out_path


if __name__ == "__main__":
    main()
