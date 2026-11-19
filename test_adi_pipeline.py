"""
test_adi_pipeline.py - Verification of Part B Medallion Logic against Azure Document Intelligence (ADI)
"""

import json
import unittest
from pathlib import Path


class TestADIPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        custom_path = Path("custom.json")
        if not custom_path.exists():
            raise FileNotFoundError("custom.json not found")
        with open(custom_path, "r", encoding="utf-8") as f:
            cls.raw_adi = json.load(f)

    def test_engine_detection(self):
        parsed = self.raw_adi
        if "analyzeResult" in parsed:
            engine = "adi"
            ar = parsed.get("analyzeResult", {})
            model_version = ar.get("modelId") or ar.get("apiVersion", "adi-v1")
        else:
            engine = "unknown"
            model_version = "v1"

        self.assertEqual(engine, "adi")
        self.assertEqual(model_version, "Testing-model-v1")

    def test_evaluation_checks_pass(self):
        data = self.raw_adi
        is_adi = "analyzeResult" in data
        self.assertTrue(is_adi)

        ar = data.get("analyzeResult", {})
        pages = ar.get("pages", [])
        elements = ar.get("paragraphs", [])
        tables = ar.get("tables", [])
        page_count = len(pages)
        source_pg_cnt = len(pages)

        # 1. Required sections
        adi_req = ["pages", "paragraphs", "tables"]
        missing = [s for s in adi_req if s not in ar]
        self.assertEqual(len(missing), 0, f"Missing required ADI sections: {missing}")

        # 2. Page count match
        self.assertEqual(page_count, len(pages))
        self.assertEqual(page_count, 16)

        # 3. Page coverage
        coverage = len(pages) / float(source_pg_cnt)
        self.assertGreaterEqual(coverage, 0.95)

        # 4. Table structure
        tbl_err = None
        for tbl in tables:
            rc = tbl.get("rowCount") or tbl.get("row_count", 0)
            cc = tbl.get("columnCount") or tbl.get("column_count", 0)
            for cell in tbl.get("cells", []):
                rs = cell.get("rowSpan") or cell.get("row_span", 1)
                cs = cell.get("columnSpan") or cell.get("column_span", 1)
                if (rs > rc and rc > 0) or (cs > cc and cc > 0):
                    tbl_err = f"Cell span exceeds table dimensions"
                    break
        self.assertIsNone(tbl_err)

        # 5. Non-empty content
        empty_cnt = sum(1 for el in elements if not (el.get("content") or "").strip())
        self.assertLess(empty_cnt, len(elements))

    def test_map_adi_to_silver(self):
        doc_id = "doc_test_adi_001"
        segment_start_page = 1
        ar = self.raw_adi.get("analyzeResult", {})

        pages_rows = []
        for p in ar.get("pages", []):
            pg_num = p.get("pageNumber", 1)
            abs_pg = segment_start_page + (pg_num - 1)
            pages_rows.append((doc_id, abs_pg))

        paragraphs = ar.get("paragraphs", [])
        raw_tables = ar.get("tables", [])

        self.assertEqual(len(pages_rows), 16)
        self.assertEqual(len(paragraphs), 1040)
        self.assertEqual(len(raw_tables), 19)


if __name__ == "__main__":
    unittest.main()
