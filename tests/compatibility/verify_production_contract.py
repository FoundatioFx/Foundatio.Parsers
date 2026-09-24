"""Fail closed on incomplete native xUnit contract evidence (Python standard library only)."""
import collections
import itertools
import json
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

CLASS = "Foundatio.Parsers.ElasticQueries.Tests.ProductionQueryContractTests"
CORPORA = {
    "BuildQueryAsync_WithContractCase_PreservesDocumentSet": ("production-cases.tsv", True),
    "BuildQueryAsync_WithMigration_PreservesIndependentExpectedSet": ("production-migrations.tsv", True),
    "BuildQueryAsync_WithDateBoundary_PreservesUtcAndNanosecondBounds": ("production-dates.tsv", False),
}
CONTROLS = {
    "AnalyzeAsync_WithProfileAnalyzer_PreservesTokenContract": 4,
    "BuildQueryAsync_WithScoringControl_AgreesWithSameIndexReference": 4,
    "BuildQueryAsync_WithBoost_PreservesMembershipAndMultipliesScores": 7,
    "BuildQueryAsync_WithOptionalClause_ChangesScoresNotMembership": 1,
    "BuildQueryAsync_WithApplicationFilter_CannotBroadenOuterScope": 4,
    "BuildQueryAsync_WithDefaultFields_RestrictsSelection": 5,
    "BuildQueryAsync_WithDisallowedInput_RejectsBeforeSearch": 11,
    "BuildQueryAsync_WithLiteralWildcard_DoesNotRejectAsPattern": 3,
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_rows(path):
    rows = [line.split("\t") for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")]
    require(bool(rows), f"Empty corpus: {path}")
    require(all(len(row) == 4 and all(row) for row in rows), f"Invalid columns: {path}")
    ids = [row[0] for row in rows]
    require(len(ids) == len(set(ids)), f"Duplicate case IDs: {path}")
    require(all(re.fullmatch(r"[a-z0-9-]+", value) for value in ids), f"Invalid case ID: {path}")
    return rows


def verify_matrix(tests, ids, operators):
    actual = []
    for test in tests:
        name = test.attrib.get("name", "").replace('\\"', '"')
        case = re.search(r'\bid: "([a-z0-9-]+)"', name)
        scoring = re.search(r'\bscoring: (True|False)\b', name)
        operator = re.search(r'\boperatorName: "(DEFAULT|AND|OR)"', name)
        require(case is not None and scoring is not None and (not operators or operator is not None),
                f"Missing configuration identity: {name}")
        actual.append((case[1], operator[1] if operators else None, scoring[1]))
    expected = list(itertools.product(ids, ["DEFAULT", "AND", "OR"] if operators else [None], ["False", "True"]))
    require(collections.Counter(actual) == collections.Counter(expected), "Missing, duplicated or unexpected case/configuration")


def verify_report(root, folder):
    require(root.tag == "assemblies", "Expected native xUnit XML")
    assemblies = list(root.iter("assembly"))
    require(bool(assemblies), "No test assemblies")
    require(all(a.get("errors") == "0" and a.get("failed") == "0" and a.get("skipped") == "0"
                and a.get("not-run", "0") == "0" for a in assemblies), "Assembly errors, failures or skipped tests")
    tests = list(root.iter("test"))
    require(bool(tests), "No tests executed")
    require(all(t.get("type") == CLASS and t.get("result") == "Pass" for t in tests), "Wrong class or non-passing test")
    require(sum(int(a.get("total", "-1")) for a in assemblies) == len(tests), "Assembly total differs from evidence")
    require(len({t.get("name") for t in tests}) == len(tests), "Duplicate test names")
    by_method = collections.defaultdict(list)
    for test in tests:
        by_method[test.get("method")].append(test)
    expected_counts = dict(CONTROLS)
    profile = json.loads((folder / "production-profile.json").read_text(encoding="utf-8"))
    require(profile.get("version") == 1, "Unsupported contract profile version")
    for method, (filename, operators) in CORPORA.items():
        rows = read_rows(folder / filename)
        expected_counts[method] = len(rows) * (6 if operators else 2)
        verify_matrix(by_method[method], [row[0] for row in rows], operators)
        document_ids = {d["id"] for d in profile["boundaries" if filename == "production-dates.tsv" else "events"]}
        for row in rows:
            columns = [2, 3] if filename == "production-cases.tsv" else [2 if filename == "production-dates.tsv" else 3]
            for column in columns:
                expected = [] if row[column] == "-" else row[column].split(",")
                require(expected == sorted(set(expected)) and set(expected) <= document_ids, f"Invalid expected IDs: {row[0]}")
            if filename == "production-dates.tsv":
                require(not re.search(r"\bnow\b", row[1]), "Date cases must have fixed anchors")
                require(bool(json.loads(row[3])), f"Empty date reference: {row[0]}")
    counts = {method: len(values) for method, values in by_method.items()}
    require(counts == expected_counts, f"Unexpected method inventory: {counts}; expected {expected_counts}")
    return len(tests)


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: verify_production_contract.py REPORT.xml CORPUS_DIRECTORY")
    try:
        count = verify_report(ET.parse(sys.argv[1]).getroot(), pathlib.Path(sys.argv[2]))
    except (OSError, ValueError, KeyError, ET.ParseError) as error:
        raise SystemExit(f"Contract evidence rejected: {error}") from error
    print(f"Verified {count} passing contract tests; every case/configuration is present exactly once.")


if __name__ == "__main__":
    main()
