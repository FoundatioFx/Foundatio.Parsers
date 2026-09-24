import itertools
import json
import pathlib
import tempfile
import unittest
import xml.etree.ElementTree as ET

from verify_production_contract import CLASS, CONTROLS, CORPORA, verify_matrix, verify_report


class EvidenceMatrixTests(unittest.TestCase):
    def matrix(self):
        return [ET.Element("test", name=f'test(id: \\"{case}\\", operatorName: \\"{op}\\", scoring: {scoring})')
                for case, op, scoring in itertools.product(["a", "b"], ["DEFAULT", "AND", "OR"], ["False", "True"])]

    def test_complete_matrix(self):
        verify_matrix(self.matrix(), ["a", "b"], True)

    def test_missing_configuration(self):
        with self.assertRaises(ValueError):
            verify_matrix(self.matrix()[:-1], ["a", "b"], True)

    def test_duplicate_replacing_missing_configuration(self):
        tests = self.matrix()
        tests[-1] = tests[0]
        with self.assertRaises(ValueError):
            verify_matrix(tests, ["a", "b"], True)

    def test_wrong_case(self):
        with self.assertRaises(ValueError):
            verify_matrix(self.matrix(), ["a", "other"], True)

    def test_empty_execution(self):
        with self.assertRaises(ValueError):
            verify_matrix([], ["a"], True)

    def test_missing_identity(self):
        with self.assertRaises(ValueError):
            verify_matrix([ET.Element("test", name="test")], ["a"], True)

    def test_date_scoring_modes(self):
        tests = [ET.Element("test", name=f'test(id: "date", scoring: {value})') for value in ["False", "True"]]
        verify_matrix(tests, ["date"], False)


class EvidenceReportTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.folder = pathlib.Path(self.directory.name)
        (self.folder / "production-profile.json").write_text(json.dumps({
            "version": 1, "events": [{"id": "doc"}], "boundaries": [{"id": "doc"}]
        }), encoding="utf-8")
        self.root = ET.Element("assemblies")
        self.assembly = ET.SubElement(self.root, "assembly", errors="0", failed="0", skipped="0")
        self.collection = ET.SubElement(self.assembly, "collection")
        for method, count in CONTROLS.items():
            for index in range(count):
                self.add_test(method, f"control: {index}")
        for method, (filename, operators) in CORPORA.items():
            row = 'case\tquery\tdoc\tdoc' if filename == "production-cases.tsv" else (
                'case\tquery\tdoc\t{"range":{"date":{"gte":"2024-01-01"}}}' if not operators else 'case\told\tnew\tdoc')
            (self.folder / filename).write_text(row + "\n", encoding="utf-8")
            for operator, scoring in itertools.product(["DEFAULT", "AND", "OR"] if operators else [None], ["False", "True"]):
                self.add_test(method, f'id: "case", operatorName: "{operator}", scoring: {scoring}')
        self.assembly.set("total", str(len(list(self.root.iter("test")))))

    def add_test(self, method, arguments):
        ET.SubElement(self.collection, "test", type=CLASS, method=method, result="Pass", name=f"{CLASS}.{method}({arguments})")

    def test_complete_report(self):
        self.assertEqual(53, verify_report(self.root, self.folder))

    def test_assembly_error(self):
        self.assembly.set("errors", "1")
        with self.assertRaises(ValueError):
            verify_report(self.root, self.folder)

    def test_skipped_case(self):
        next(self.root.iter("test")).set("result", "Skip")
        with self.assertRaises(ValueError):
            verify_report(self.root, self.folder)

    def test_counter_mismatch(self):
        self.assembly.set("total", "999")
        with self.assertRaises(ValueError):
            verify_report(self.root, self.folder)

    def test_unknown_expected_document(self):
        (self.folder / "production-cases.tsv").write_text("case\tquery\tmissing\tdoc\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            verify_report(self.root, self.folder)

    def test_empty_report(self):
        with self.assertRaises(ValueError):
            verify_report(ET.Element("assemblies"), self.folder)


if __name__ == "__main__":
    unittest.main()
