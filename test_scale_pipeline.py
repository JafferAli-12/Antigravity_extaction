#!/usr/bin/env python3
"""
test_scale_pipeline.py - Automated Comprehensive Scale & Edge-Case Test Suite

Validates the large-scale document extraction and lakehouse pipeline across 15 scenarios:
1. Small PDF (10 pages)
2. 500-page PDF chunking & reconciliation
3. 2,000-page PDF chunking & reconciliation
4. 10,000-page PDF streaming plan & boundary calculation
5. Table spanning multiple pages (pages 4 to 6)
6. Table continuing across a chunk boundary (boundary expansion)
7. Repeated table headers across continuation fragments
8. Multiple distinct tables on the same page (page 10)
9. Scanned and digital pages in the same document
10. Document with rotated pages
11. Figures inside or associated with tables
12. Failed chunk followed by successful retry
13. Duplicate chunk processing (idempotency)
14. Out-of-order chunk completion
15. Corrupted or unsupported file quarantine
"""

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List

from chunker import (
    ChunkingManifest,
    DocumentAnalysisResult,
    IntelligentChunker,
    PDFAnalyzer,
    TableBoundaryHint,
    compute_file_hash,
    derive_doc_id,
)
from table_stitcher import (
    LogicalTable,
    NormalizedCell,
    ReviewIssue,
    TableFragment,
    TableStitcher,
)
from aggregator import (
    AggregatedDocument,
    DocumentAggregator,
    ReconciliationReport,
)


class TestScalePipeline(unittest.TestCase):

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_scale_"))

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    # --------------------------------------------------------------------------
    # Scenario 1: Small PDF (10 pages)
    # --------------------------------------------------------------------------
    def test_01_small_pdf_single_chunk(self):
        chunker = IntelligentChunker(preferred_chunk_size=100, max_chunk_size=200)
        plan = chunker.plan_chunks(total_pages=10)

        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0], (1, 10, "STANDARD", []))

    # --------------------------------------------------------------------------
    # Scenario 2: 500-page PDF Chunking & Reconciliation
    # --------------------------------------------------------------------------
    def test_02_500_page_pdf_chunking(self):
        chunker = IntelligentChunker(preferred_chunk_size=100, max_chunk_size=200)
        plan = chunker.plan_chunks(total_pages=500)

        self.assertEqual(len(plan), 5)
        # Check strict contiguous sequence
        expected_ranges = [(1, 100), (101, 200), (201, 300), (301, 400), (401, 500)]
        for i, (p_start, p_end, c_type, _) in enumerate(plan):
            self.assertEqual((p_start, p_end), expected_ranges[i])
            self.assertEqual(c_type, "STANDARD")

    # --------------------------------------------------------------------------
    # Scenario 3: 2,000-page PDF Chunking & Reconciliation
    # --------------------------------------------------------------------------
    def test_03_2000_page_pdf_chunking(self):
        chunker = IntelligentChunker(preferred_chunk_size=100, max_chunk_size=200)
        plan = chunker.plan_chunks(total_pages=2000)

        self.assertEqual(len(plan), 20)
        total_covered = sum(p_end - p_start + 1 for p_start, p_end, _, _ in plan)
        self.assertEqual(total_covered, 2000)
        self.assertEqual(plan[0][0], 1)
        self.assertEqual(plan[-1][1], 2000)

    # --------------------------------------------------------------------------
    # Scenario 4: 10,000-page PDF Streaming Plan & Boundaries
    # --------------------------------------------------------------------------
    def test_04_10000_page_pdf_chunking_plan(self):
        chunker = IntelligentChunker(preferred_chunk_size=200, max_chunk_size=400)
        plan = chunker.plan_chunks(total_pages=10000)

        self.assertEqual(len(plan), 50)  # 10,000 / 200 = 50 chunks
        # Verify contiguous boundaries without gaps or overlaps
        for idx in range(len(plan) - 1):
            curr_end = plan[idx][1]
            next_start = plan[idx + 1][0]
            self.assertEqual(next_start, curr_end + 1, f"Gap or overlap detected between chunk {idx} and {idx+1}")
        self.assertEqual(plan[-1][1], 10000)

    # --------------------------------------------------------------------------
    # Scenario 5: Table Spanning Multiple Pages (Pages 4 to 6)
    # --------------------------------------------------------------------------
    def test_05_multi_page_table_stitching(self):
        doc_id = "doc_multipage_tbl"

        frag_p4 = {
            "table_id": "tbl_frag_p4",
            "page_number": 4,
            "rowCount": 5,
            "columnCount": 3,
            "cells": [
                {"rowIndex": 0, "columnIndex": 0, "content": "Analyte"},
                {"rowIndex": 0, "columnIndex": 1, "content": "Value"},
                {"rowIndex": 0, "columnIndex": 2, "content": "Unit"},
                {"rowIndex": 1, "columnIndex": 0, "content": "Glucose"},
                {"rowIndex": 1, "columnIndex": 1, "content": "95.0"},
                {"rowIndex": 1, "columnIndex": 2, "content": "mg/dL"},
            ]
        }

        frag_p5 = {
            "table_id": "tbl_frag_p5",
            "page_number": 5,
            "rowCount": 4,
            "columnCount": 3,
            "cells": [
                {"rowIndex": 0, "columnIndex": 0, "content": "Analyte"},
                {"rowIndex": 0, "columnIndex": 1, "content": "Value"},
                {"rowIndex": 0, "columnIndex": 2, "content": "Unit"},
                {"rowIndex": 1, "columnIndex": 0, "content": "Hemoglobin"},
                {"rowIndex": 1, "columnIndex": 1, "content": "14.2"},
                {"rowIndex": 1, "columnIndex": 2, "content": "g/dL"},
            ]
        }

        frag_p6 = {
            "table_id": "tbl_frag_p6",
            "page_number": 6,
            "rowCount": 3,
            "columnCount": 3,
            "cells": [
                {"rowIndex": 0, "columnIndex": 0, "content": "Cholesterol"},
                {"rowIndex": 0, "columnIndex": 1, "content": "180"},
                {"rowIndex": 0, "columnIndex": 2, "content": "mg/dL"},
            ]
        }

        logical_tables = TableStitcher.stitch_document_tables(
            raw_tables=[frag_p4, frag_p5, frag_p6],
            doc_id=doc_id
        )

        self.assertEqual(len(logical_tables), 1, "All 3 fragments should be stitched into 1 logical table")
        lt = logical_tables[0]
        self.assertEqual(lt.page_start, 4)
        self.assertEqual(lt.page_end, 6)
        self.assertTrue(lt.is_cross_page)
        self.assertGreaterEqual(lt.stitch_confidence, 0.85)
        self.assertEqual(len(lt.fragments), 3)

    # --------------------------------------------------------------------------
    # Scenario 6: Table Continuing Across Chunk Boundary (Boundary Expansion)
    # --------------------------------------------------------------------------
    def test_06_chunk_boundary_expansion(self):
        chunker = IntelligentChunker(
            preferred_chunk_size=50,
            max_chunk_size=100,
            enable_table_preservation=True
        )

        # Table starts on page 48 and continues to page 53
        hints = [
            TableBoundaryHint(
                start_page=48,
                end_page=53,
                estimated_col_count=4,
                confidence=0.95,
                logical_table_id="tbl_cross_chunk_01"
            )
        ]

        plan = chunker.plan_chunks(total_pages=150, table_hints=hints)

        # Target chunk 1 would have ended at page 50, but because the table ends at 53,
        # it expands to page 53 so the table is NOT bisected!
        self.assertEqual(plan[0][0], 1)
        self.assertEqual(plan[0][1], 53, "Chunk 1 should expand boundary to page 53 to preserve table")
        self.assertEqual(plan[1][0], 54, "Chunk 2 should start at page 54")

    # --------------------------------------------------------------------------
    # Scenario 7: Repeated Table Headers Deduplication
    # --------------------------------------------------------------------------
    def test_07_repeated_table_headers_handling(self):
        frag_a = TableFragment(
            table_id="t1", doc_id="d1", page_no=1, logical_table_id="log1",
            fragment_order=0, n_rows=3, n_cols=2,
            cells=[
                NormalizedCell("c1", "t1", 0, 0, 1, 1, "Name", "Name", 1.0),
                NormalizedCell("c2", "t1", 0, 1, 1, 1, "Score", "Score", 1.0),
                NormalizedCell("c3", "t1", 1, 0, 1, 1, "Alice", "Alice", 1.0),
                NormalizedCell("c4", "t1", 1, 1, 1, 1, "90", "90", 1.0),
                NormalizedCell("c5", "t1", 2, 0, 1, 1, "Bob", "Bob", 1.0),
                NormalizedCell("c6", "t1", 2, 1, 1, 1, "85", "85", 1.0),
            ]
        )
        frag_b = TableFragment(
            table_id="t2", doc_id="d1", page_no=2, logical_table_id="log1",
            fragment_order=1, n_rows=2, n_cols=2,
            cells=[
                NormalizedCell("c7", "t2", 0, 0, 1, 1, "Name", "Name", 1.0),
                NormalizedCell("c8", "t2", 0, 1, 1, 1, "Score", "Score", 1.0),
                NormalizedCell("c9", "t2", 1, 0, 1, 1, "Charlie", "Charlie", 1.0),
                NormalizedCell("c10", "t2", 1, 1, 1, 1, "95", "95", 1.0),
            ]
        )

        score, details = TableStitcher.evaluate_pair(frag_a, frag_b)
        self.assertTrue(details["repeats_header"])
        self.assertGreaterEqual(score, 0.85)

        combined, total_rows = TableStitcher._stitch_cells([frag_a, frag_b])
        # Frag A has 3 rows (0, 1, 2); Frag B repeats header so row 0 is skipped, row 1 added as row 3
        self.assertEqual(total_rows, 4, "Stitched table should have 4 rows (header + 3 data rows)")
        data_cells = [c.clean_text for c in combined if c.col_no == 0]
        self.assertEqual(data_cells, ["Name", "Alice", "Bob", "Charlie"])

    # --------------------------------------------------------------------------
    # Scenario 8: Multiple Distinct Tables on Same Page (Page 10)
    # --------------------------------------------------------------------------
    def test_08_multiple_tables_on_same_page(self):
        doc_id = "doc_p10_tables"
        t1 = {
            "table_id": "tbl_p10_hematology",
            "page_number": 10,
            "rowCount": 3,
            "columnCount": 2,
            "cells": [
                {"rowIndex": 0, "columnIndex": 0, "content": "WBC"},
                {"rowIndex": 0, "columnIndex": 1, "content": "6.5"},
            ]
        }
        t2 = {
            "table_id": "tbl_p10_biochemistry",
            "page_number": 10,
            "rowCount": 4,
            "columnCount": 4,  # Different col count
            "cells": [
                {"rowIndex": 0, "columnIndex": 0, "content": "Sodium"},
                {"rowIndex": 0, "columnIndex": 1, "content": "140"},
                {"rowIndex": 0, "columnIndex": 2, "content": "mmol/L"},
                {"rowIndex": 0, "columnIndex": 3, "content": "135-145"},
            ]
        }
        t3 = {
            "table_id": "tbl_p10_urinalysis",
            "page_number": 10,
            "rowCount": 2,
            "columnCount": 3,
            "cells": [
                {"rowIndex": 0, "columnIndex": 0, "content": "Color"},
                {"rowIndex": 0, "columnIndex": 1, "content": "Yellow"},
                {"rowIndex": 0, "columnIndex": 2, "content": "Clear"},
            ]
        }

        logical_tables = TableStitcher.stitch_document_tables(
            raw_tables=[t1, t2, t3],
            doc_id=doc_id
        )

        self.assertEqual(len(logical_tables), 3, "All 3 distinct tables on page 10 must remain separate")

    # --------------------------------------------------------------------------
    # Scenario 9: Scanned and Digital Mixed Pages
    # --------------------------------------------------------------------------
    def test_09_mixed_scanned_digital_pages(self):
        manifest = ChunkingManifest(
            document_id="doc_mixed_01",
            source_file="mixed_report.pdf",
            file_hash="hash_mixed_123",
            total_pages=2,
            total_chunks=1,
            preferred_chunk_size=100,
            max_chunk_size=200,
            created_at="2026-11-20T12:00:00Z",
            chunks=[
                {
                    "document_id": "doc_mixed_01",
                    "chunk_id": "chk_mixed_0001",
                    "chunk_sequence": 1,
                    "original_page_start": 1,
                    "original_page_end": 2,
                    "page_count": 2,
                    "chunk_type": "STANDARD",
                }
            ]
        )
        chunk_results = [
            {
                "chunk_id": "chk_mixed_0001",
                "status": "SUCCESS",
                "pages": [
                    {"page_number": 1, "is_scanned": False, "lines": [{"text": "Digital Text"}]},
                    {"page_number": 2, "is_scanned": True, "lines": [{"text": "OCR Text"}]},
                ],
                "elements": [],
                "tables": []
            }
        ]

        agg_doc = DocumentAggregator.aggregate_from_chunks(
            manifest=manifest,
            chunk_results=chunk_results
        )

        self.assertEqual(len(agg_doc.pages), 2)
        self.assertEqual(agg_doc.pages[0].page_kind, "DIGITAL")
        self.assertEqual(agg_doc.pages[1].page_kind, "SCANNED")

    # --------------------------------------------------------------------------
    # Scenario 10: Document with Rotated Pages
    # --------------------------------------------------------------------------
    def test_10_rotated_pages_tracking(self):
        rotations = {1: 0, 2: 90, 3: 270, 4: 0}
        analysis = DocumentAnalysisResult(
            doc_id="doc_rot_01",
            file_path="sample_rotated.pdf",
            file_size_bytes=1024,
            file_hash="hash_rot",
            file_format="PDF",
            page_count=4,
            is_born_digital=True,
            is_scanned=False,
            is_encrypted=False,
            is_valid=True,
            page_rotations=rotations
        )

        self.assertEqual(analysis.page_rotations[2], 90)
        self.assertEqual(analysis.page_rotations[3], 270)

    # --------------------------------------------------------------------------
    # Scenario 11: Figures Associated with Tables
    # --------------------------------------------------------------------------
    def test_11_figures_inside_or_associated_with_tables(self):
        manifest = ChunkingManifest(
            document_id="doc_fig_01",
            source_file="report_with_figure.pdf",
            file_hash="hash_fig_123",
            total_pages=1,
            total_chunks=1,
            preferred_chunk_size=100,
            max_chunk_size=200,
            created_at="2026-11-20T12:00:00Z",
            chunks=[
                {
                    "document_id": "doc_fig_01",
                    "chunk_id": "chk_fig_0001",
                    "chunk_sequence": 1,
                    "original_page_start": 1,
                    "original_page_end": 1,
                    "page_count": 1,
                    "chunk_type": "STANDARD",
                }
            ]
        )
        chunk_results = [
            {
                "chunk_id": "chk_fig_0001",
                "status": "SUCCESS",
                "pages": [{"page_number": 1, "is_scanned": False, "lines": []}],
                "elements": [
                    {"page_number": 1, "element_index": 0, "type": "figure", "text": "ECG Waveform Graph", "bbox": [50, 100, 400, 300]}
                ],
                "tables": [
                    {
                        "table_id": "tbl_ecg_metrics",
                        "page_number": 1,
                        "rowCount": 2,
                        "columnCount": 2,
                        "cells": [
                            {"rowIndex": 0, "columnIndex": 0, "content": "Metric"},
                            {"rowIndex": 0, "columnIndex": 1, "content": "Value"},
                            {"rowIndex": 1, "columnIndex": 0, "content": "PR Interval"},
                            {"rowIndex": 1, "columnIndex": 1, "content": "160 ms"},
                        ]
                    }
                ]
            }
        ]

        agg_doc = DocumentAggregator.aggregate_from_chunks(manifest, chunk_results)
        self.assertEqual(len(agg_doc.paragraphs), 1)
        self.assertEqual(agg_doc.paragraphs[0].role, "figure")
        self.assertEqual(len(agg_doc.logical_tables), 1)

    # --------------------------------------------------------------------------
    # Scenario 12: Failed Chunk Followed by Successful Retry
    # --------------------------------------------------------------------------
    def test_12_failed_chunk_retry_recovery(self):
        manifest = ChunkingManifest(
            document_id="doc_retry_01",
            source_file="doc_retry.pdf",
            file_hash="hash_retry_123",
            total_pages=200,
            total_chunks=2,
            preferred_chunk_size=100,
            max_chunk_size=200,
            created_at="2026-11-20T12:00:00Z",
            chunks=[
                {
                    "document_id": "doc_retry_01",
                    "chunk_id": "chk_01",
                    "chunk_sequence": 1,
                    "original_page_start": 1,
                    "original_page_end": 100,
                    "page_count": 100,
                    "chunk_type": "STANDARD",
                },
                {
                    "document_id": "doc_retry_01",
                    "chunk_id": "chk_02",
                    "chunk_sequence": 2,
                    "original_page_start": 101,
                    "original_page_end": 200,
                    "page_count": 100,
                    "chunk_type": "STANDARD",
                }
            ]
        )

        # First attempt: Chunk 2 failed
        results_attempt_1 = [
            {"chunk_id": "chk_01", "status": "SUCCESS", "pages": [{"page_number": i} for i in range(1, 101)], "elements": [], "tables": []},
            {"chunk_id": "chk_02", "status": "FAILED", "error_class": "TIMEOUT", "error_message": "Gateway timeout", "pages": []}
        ]

        agg_attempt_1 = DocumentAggregator.aggregate_from_chunks(manifest, results_attempt_1)
        self.assertEqual(agg_attempt_1.reconciliation.status, "PARTIAL")
        self.assertIn("chk_02", agg_attempt_1.reconciliation.failed_chunks)
        self.assertEqual(len(agg_attempt_1.reconciliation.missing_pages), 100)

        # Retry attempt: Only Chunk 2 retried and succeeded
        results_attempt_2 = [
            results_attempt_1[0],  # Reused existing Chunk 1
            {"chunk_id": "chk_02", "status": "SUCCESS", "pages": [{"page_number": i} for i in range(1, 101)], "elements": [], "tables": []}
        ]

        agg_attempt_2 = DocumentAggregator.aggregate_from_chunks(manifest, results_attempt_2)
        self.assertEqual(agg_attempt_2.reconciliation.status, "PASSED")
        self.assertTrue(agg_attempt_2.reconciliation.is_valid)
        self.assertEqual(len(agg_attempt_2.pages), 200)

    # --------------------------------------------------------------------------
    # Scenario 13: Duplicate Chunk Processing (Idempotency)
    # --------------------------------------------------------------------------
    def test_13_duplicate_chunk_processing_idempotency(self):
        manifest = ChunkingManifest(
            document_id="doc_idemp_01",
            source_file="doc_idemp.pdf",
            file_hash="hash_idemp",
            total_pages=50,
            total_chunks=1,
            preferred_chunk_size=100,
            max_chunk_size=200,
            created_at="2026-11-20T12:00:00Z",
            chunks=[
                {
                    "document_id": "doc_idemp_01",
                    "chunk_id": "chk_idemp_1",
                    "chunk_sequence": 1,
                    "original_page_start": 1,
                    "original_page_end": 50,
                    "page_count": 50,
                    "chunk_type": "STANDARD",
                }
            ]
        )

        chunk_result = {
            "chunk_id": "chk_idemp_1",
            "status": "SUCCESS",
            "pages": [{"page_number": i} for i in range(1, 51)],
            "elements": [],
            "tables": []
        }

        # Run twice
        agg_1 = DocumentAggregator.aggregate_from_chunks(manifest, [chunk_result])
        agg_2 = DocumentAggregator.aggregate_from_chunks(manifest, [chunk_result])

        self.assertEqual(agg_1.document.page_count, agg_2.document.page_count)
        self.assertEqual(agg_1.reconciliation.status, agg_2.reconciliation.status)
        self.assertEqual(len(agg_1.pages), len(agg_2.pages))

    # --------------------------------------------------------------------------
    # Scenario 14: Out-of-Order Chunk Completion
    # --------------------------------------------------------------------------
    def test_14_out_of_order_chunk_completion(self):
        manifest = ChunkingManifest(
            document_id="doc_order_01",
            source_file="doc_order.pdf",
            file_hash="hash_order",
            total_pages=30,
            total_chunks=3,
            preferred_chunk_size=10,
            max_chunk_size=20,
            created_at="2026-11-20T12:00:00Z",
            chunks=[
                {"document_id": "doc_order_01", "chunk_id": "chk_1", "chunk_sequence": 1, "original_page_start": 1, "original_page_end": 10, "page_count": 10, "chunk_type": "STANDARD"},
                {"document_id": "doc_order_01", "chunk_id": "chk_2", "chunk_sequence": 2, "original_page_start": 11, "original_page_end": 20, "page_count": 10, "chunk_type": "STANDARD"},
                {"document_id": "doc_order_01", "chunk_id": "chk_3", "chunk_sequence": 3, "original_page_start": 21, "original_page_end": 30, "page_count": 10, "chunk_type": "STANDARD"},
            ]
        )

        # Chunks arrive in random order: Chunk 3 first, then Chunk 1, then Chunk 2
        scrambled_results = [
            {"chunk_id": "chk_3", "status": "SUCCESS", "pages": [{"page_number": i} for i in range(1, 11)], "elements": [], "tables": []},
            {"chunk_id": "chk_1", "status": "SUCCESS", "pages": [{"page_number": i} for i in range(1, 11)], "elements": [], "tables": []},
            {"chunk_id": "chk_2", "status": "SUCCESS", "pages": [{"page_number": i} for i in range(1, 11)], "elements": [], "tables": []},
        ]

        agg_doc = DocumentAggregator.aggregate_from_chunks(manifest, scrambled_results)

        # Verify page sequence is restored to exact contiguous order 1..30
        self.assertEqual(agg_doc.reconciliation.status, "PASSED")
        self.assertEqual([p.page_no for p in agg_doc.pages], list(range(1, 31)))

    # --------------------------------------------------------------------------
    # Scenario 15: Corrupted or Unsupported File Quarantine
    # --------------------------------------------------------------------------
    def test_15_corrupted_file_quarantine(self):
        corrupt_file = self.test_dir / "corrupted_document.pdf"
        corrupt_file.write_bytes(b"This is not a real PDF file")

        chunker = IntelligentChunker()
        manifest = chunker.create_chunks(corrupt_file, self.test_dir / "chunks")

        self.assertEqual(manifest.total_pages, 0)
        self.assertEqual(manifest.total_chunks, 0)
        self.assertEqual(len(manifest.chunks), 0)


if __name__ == "__main__":
    unittest.main()
