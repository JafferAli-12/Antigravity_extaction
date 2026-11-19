"""
test_extract.py - Unit tests for Part A Document Extractor
"""

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from extract import (
    detect_and_merge_cross_page_tables,
    generate_table_id,
    load_config,
    make_failed_json,
    process_single_file,
    run_extraction_batch,
)


class TestDocumentExtractor(unittest.TestCase):

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_extract_"))
        self.input_dir = self.test_dir / "input"
        self.output_dir = self.test_dir / "output"
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)
        self.config = load_config(None)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_make_failed_json(self):
        res = make_failed_json("test.pdf", "ENCRYPTED_PDF", "Password required")
        self.assertEqual(res["status"], "FAILED")
        self.assertEqual(res["error_class"], "ENCRYPTED_PDF")
        self.assertEqual(res["source_file"], "test.pdf")
        self.assertIn("detected_at", res)

    def test_generate_table_id(self):
        t1 = generate_table_id(1, 0, "docA")
        t2 = generate_table_id(1, 0, "docA")
        t3 = generate_table_id(1, 1, "docA")
        self.assertEqual(t1, t2)
        self.assertNotEqual(t1, t3)
        self.assertEqual(len(t1), 16)

    def test_cross_page_table_continuation_merge(self):
        table_page_1 = {
            "table_id": "tbl_p1",
            "start_page": 1,
            "end_page": 1,
            "is_cross_page": False,
            "row_count": 2,
            "column_count": 2,
            "caption": "Quarterly Figures",
            "cells": [
                {"row_index": 0, "column_index": 0, "row_span": 1, "column_span": 1, "text": "Q1"},
                {"row_index": 0, "column_index": 1, "row_span": 1, "column_span": 1, "text": "$100"},
                {"row_index": 1, "column_index": 0, "row_span": 1, "column_span": 1, "text": "Q2"},
                {"row_index": 1, "column_index": 1, "row_span": 1, "column_span": 1, "text": "$120"},
            ],
        }
        table_page_2 = {
            "table_id": "tbl_p2",
            "start_page": 2,
            "end_page": 2,
            "is_cross_page": False,
            "row_count": 2,
            "column_count": 2,
            "caption": None,
            "cells": [
                {"row_index": 0, "column_index": 0, "row_span": 1, "column_span": 1, "text": "Q3"},
                {"row_index": 0, "column_index": 1, "row_span": 1, "column_span": 1, "text": "$140"},
                {"row_index": 1, "column_index": 0, "row_span": 1, "column_span": 1, "text": "Q4"},
                {"row_index": 1, "column_index": 1, "row_span": 1, "column_span": 1, "text": "$190"},
            ],
        }

        warnings = []
        merged = detect_and_merge_cross_page_tables([table_page_1, table_page_2], warnings)

        self.assertEqual(len(merged), 1)
        self.assertTrue(merged[0]["is_cross_page"])
        self.assertEqual(merged[0]["start_page"], 1)
        self.assertEqual(merged[0]["end_page"], 2)
        self.assertEqual(merged[0]["row_count"], 4)
        self.assertEqual(len(merged[0]["cells"]), 8)
        # Check row offset in merged cells
        row_indices = [c["row_index"] for c in merged[0]["cells"]]
        self.assertEqual(sorted(list(set(row_indices))), [0, 1, 2, 3])

    def test_cross_page_table_header_keyword_no_merge(self):
        table_page_1 = {
            "table_id": "tbl_p1",
            "start_page": 1,
            "end_page": 1,
            "is_cross_page": False,
            "row_count": 2,
            "column_count": 2,
            "caption": "Table 1",
            "cells": [
                {"row_index": 0, "column_index": 0, "row_span": 1, "column_span": 1, "text": "Item"},
                {"row_index": 0, "column_index": 1, "row_span": 1, "column_span": 1, "text": "Price"},
            ],
        }
        table_page_2 = {
            "table_id": "tbl_p2",
            "start_page": 2,
            "end_page": 2,
            "is_cross_page": False,
            "row_count": 2,
            "column_count": 2,
            "caption": "Table 2",
            "cells": [
                # Row 0 contains header keyword "Item" and "Price", so it should NOT merge
                {"row_index": 0, "column_index": 0, "row_span": 1, "column_span": 1, "text": "Item Number"},
                {"row_index": 0, "column_index": 1, "row_span": 1, "column_span": 1, "text": "Price Total"},
            ],
        }

        warnings = []
        merged = detect_and_merge_cross_page_tables([table_page_1, table_page_2], warnings)
        self.assertEqual(len(merged), 2)
        self.assertFalse(merged[0]["is_cross_page"])
        self.assertFalse(merged[1]["is_cross_page"])

    def test_zero_byte_file_handling(self):
        empty_file = self.input_dir / "empty.pdf"
        empty_file.write_bytes(b"")

        log_entry = process_single_file(empty_file, self.output_dir, self.config)
        self.assertEqual(log_entry["status"], "FAILED")
        self.assertEqual(log_entry["error_class"], "EMPTY_FILE")

        out_json_path = self.output_dir / "empty.json"
        self.assertTrue(out_json_path.exists())
        with open(out_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(data["status"], "FAILED")
        self.assertEqual(data["error_class"], "EMPTY_FILE")

    def test_unsupported_format_handling(self):
        bad_file = self.input_dir / "binary.bin"
        bad_file.write_bytes(b"\x00\x01\x02\x03")

        log_entry = process_single_file(bad_file, self.output_dir, self.config)
        self.assertEqual(log_entry["status"], "FAILED")
        self.assertEqual(log_entry["error_class"], "UNSUPPORTED_FORMAT")

    def test_json_passthrough(self):
        valid_json_file = self.input_dir / "extracted.json"
        sample_data = {
            "status": "SUCCESS",
            "engine": "local",
            "pages": [],
            "elements": [],
            "tables": [],
        }
        with open(valid_json_file, "w", encoding="utf-8") as f:
            json.dump(sample_data, f)

        log_entry = process_single_file(valid_json_file, self.output_dir, self.config)
        self.assertEqual(log_entry["status"], "SUCCESS")

        out_path = self.output_dir / "extracted.json"
        self.assertTrue(out_path.exists())
        with open(out_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        self.assertEqual(loaded["engine"], "local")

    def test_idempotency_skip(self):
        valid_json_file = self.input_dir / "sample.json"
        with open(valid_json_file, "w", encoding="utf-8") as f:
            json.dump({"status": "SUCCESS"}, f)

        # First run
        first_entry = process_single_file(valid_json_file, self.output_dir, self.config)
        self.assertEqual(first_entry["status"], "SUCCESS")

        # Second run without overwrite
        second_entry = process_single_file(valid_json_file, self.output_dir, self.config, overwrite=False)
        self.assertEqual(second_entry["status"], "SKIPPED")


if __name__ == "__main__":
    unittest.main()
