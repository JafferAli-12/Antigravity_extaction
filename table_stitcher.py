#!/usr/bin/env python3
"""
table_stitcher.py - Multi-Page Table Stitching Engine

Implements an automated, confidence-based stitching mechanism to reconstruct
multi-page tables split across pages or chunk boundaries into unified logical tables.

7-Point Confidence Scoring Metric:
1. Page Adjacency (weight = 0.25)
2. Column Count Match (weight = 0.20)
3. Column Alignment / Width Overlap (weight = 0.15)
4. Header Similarity / Repeated Header Detection (weight = 0.15)
5. Data Type Compatibility per Column (weight = 0.10)
6. Intervening Content / Body Paragraph Absence (weight = 0.10)
7. Continuation Markers (weight = 0.05)
"""

from __future__ import annotations

import datetime
import json
import logging
import re
import sys
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("table_stitcher")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

CONTINUATION_PATTERNS = [
    r"\(?continued\)?",
    r"\(?cont\.?\)?",
    r"table\s+.*\(?cont",
    r"continued\s+from\s+previous",
]


@dataclass
class NormalizedCell:
    cell_id: str
    table_id: str
    row_no: int
    col_no: int
    row_span: int
    col_span: int
    raw_text: str
    clean_text: str
    confidence: float
    bbox: Optional[List[float]] = None
    fix_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TableFragment:
    table_id: str
    doc_id: str
    page_no: int
    logical_table_id: str
    fragment_order: int
    n_rows: int
    n_cols: int
    caption: Optional[str] = None
    cells: List[NormalizedCell] = field(default_factory=list)
    bbox: Optional[List[float]] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["cells"] = [c.to_dict() for c in self.cells]
        return d


@dataclass
class ReviewIssue:
    issue_id: str
    doc_id: str
    cell_id: Optional[str]
    logical_table_id: Optional[str]
    issue_type: str  # "TABLE_STITCH_UNCERTAIN" | "REPEATED_HEADER" | "NUMERIC_PARSE_FAILURE"
    status: str = "OPEN"  # "OPEN" | "RESOLVED" | "IGNORED"
    severity: str = "MEDIUM"  # "LOW" | "MEDIUM" | "HIGH"
    raw_evidence: str = ""
    resolved_value: Optional[str] = None
    created_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LogicalTable:
    logical_table_id: str
    doc_id: str
    title: Optional[str]
    page_start: int
    page_end: int
    total_rows: int
    total_cols: int
    is_cross_page: bool
    stitch_confidence: float
    fragments: List[TableFragment] = field(default_factory=list)
    combined_cells: List[NormalizedCell] = field(default_factory=list)
    review_issues: List[ReviewIssue] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["fragments"] = [f.to_dict() for f in self.fragments]
        d["combined_cells"] = [c.to_dict() for c in self.combined_cells]
        d["review_issues"] = [i.to_dict() for i in self.review_issues]
        return d


class TableStitcher:
    """
    Evaluates candidate table fragments and merges them based on a 7-point confidence metric.
    """

    AUTO_STITCH_THRESHOLD = 0.85
    REVIEW_STITCH_THRESHOLD = 0.50

    @classmethod
    def evaluate_pair(
        cls,
        frag_a: TableFragment,
        frag_b: TableFragment,
        has_intervening_paragraphs: bool = False
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Computes 7-point confidence score between frag_a (earlier) and frag_b (subsequent).
        Returns (confidence, breakdown_details).
        """
        details: Dict[str, Any] = {}

        # 1. Page Adjacency (weight = 0.25)
        # Directly consecutive pages get 1.0; 2 pages apart get 0.5; others 0.0
        page_gap = frag_b.page_no - frag_a.page_no
        if page_gap == 1:
            s_adj = 1.0
        elif page_gap == 2:
            s_adj = 0.5
        else:
            s_adj = 0.0
        w_adj = 0.25
        details["s_adjacency"] = s_adj

        # 2. Column Count Match (weight = 0.20)
        s_col = 1.0 if frag_a.n_cols == frag_b.n_cols and frag_a.n_cols > 0 else 0.0
        w_col = 0.20
        details["s_column_count"] = s_col

        # 3. Column Alignment / Width Overlap (weight = 0.15)
        # Compare bounding boxes of cells in row 0
        s_align = cls._calculate_column_alignment(frag_a, frag_b)
        w_align = 0.15
        details["s_column_alignment"] = s_align

        # 4. Header Similarity / Repeated Header Detection (weight = 0.15)
        s_hdr, repeats_header = cls._evaluate_headers(frag_a, frag_b)
        w_hdr = 0.15
        details["s_header_match"] = s_hdr
        details["repeats_header"] = repeats_header

        # 5. Data Type Compatibility per Column (weight = 0.10)
        s_type = cls._evaluate_data_types(frag_a, frag_b)
        w_type = 0.10
        details["s_data_type"] = s_type

        # 6. Intervening Content (weight = 0.10)
        s_inter = 0.0 if has_intervening_paragraphs else 1.0
        w_inter = 0.10
        details["s_no_intervening_text"] = s_inter

        # 7. Continuation Markers (weight = 0.05)
        s_marker = cls._check_continuation_markers(frag_b.caption or "")
        w_marker = 0.05
        details["s_continuation_marker"] = s_marker

        # Total Weighted Score
        confidence = (
            w_adj * s_adj +
            w_col * s_col +
            w_align * s_align +
            w_hdr * s_hdr +
            w_type * s_type +
            w_inter * s_inter +
            w_marker * s_marker
        )

        details["total_confidence"] = round(confidence, 4)
        return round(confidence, 4), details

    @classmethod
    def stitch_document_tables(
        cls,
        raw_tables: List[Dict[str, Any]],
        doc_id: str,
        paragraphs_by_page: Optional[Dict[int, List[str]]] = None
    ) -> List[LogicalTable]:
        """
        Takes raw extracted table dictionaries (from ADI or local extractor),
        normalizes into fragments, and stitches them into LogicalTables.
        """
        if not raw_tables:
            return []

        paragraphs_by_page = paragraphs_by_page or {}

        # 1. Convert to Normalized TableFragments
        fragments: List[TableFragment] = []
        for t_idx, raw_t in enumerate(raw_tables):
            frag = cls._normalize_raw_table(raw_t, doc_id, t_idx)
            fragments.append(frag)

        # Sort fragments by start page and bounding box
        fragments.sort(key=lambda f: (f.page_no, f.table_id))

        logical_tables: List[LogicalTable] = []
        skip_frag_indices: Set[int] = set()

        for i, frag in enumerate(fragments):
            if i in skip_frag_indices:
                continue

            # Start a new logical table group with frag
            curr_log_id = frag.logical_table_id or f"log_tbl_{doc_id}_{frag.page_no}_{len(logical_tables)}"
            curr_fragments = [frag]
            accumulated_confidence = 1.0
            issues: List[ReviewIssue] = []

            # Look ahead for continuing fragments
            j = i + 1
            last_frag = frag

            while j < len(fragments):
                candidate_frag = fragments[j]

                # Check for intervening text between last_frag and candidate_frag
                has_intervening = False
                for pg in range(last_frag.page_no, candidate_frag.page_no + 1):
                    paras = paragraphs_by_page.get(pg, [])
                    # Consider substantial paragraphs (> 50 chars) as intervening
                    if len(paras) > 3:
                        has_intervening = True
                        break

                conf, details = cls.evaluate_pair(last_frag, candidate_frag, has_intervening)

                if conf >= cls.AUTO_STITCH_THRESHOLD:
                    # High confidence: Automatic merge
                    logger.info(
                        f"Auto-Stitching table on page {last_frag.page_no} with page {candidate_frag.page_no} "
                        f"(confidence: {conf})"
                    )
                    candidate_frag.fragment_order = len(curr_fragments)
                    candidate_frag.logical_table_id = curr_log_id
                    curr_fragments.append(candidate_frag)
                    skip_frag_indices.add(j)
                    accumulated_confidence = min(accumulated_confidence, conf)
                    last_frag = candidate_frag
                    j += 1
                elif conf >= cls.REVIEW_STITCH_THRESHOLD:
                    # Moderate confidence: Merge with Review Issue
                    logger.warning(
                        f"Uncertain Table Stitch: page {last_frag.page_no} to {candidate_frag.page_no} "
                        f"(confidence: {conf}). Creating review issue."
                    )
                    issue = ReviewIssue(
                        issue_id=f"rev_{uuid.uuid4().hex[:12]}",
                        doc_id=doc_id,
                        cell_id=None,
                        logical_table_id=curr_log_id,
                        issue_type="TABLE_STITCH_UNCERTAIN",
                        severity="MEDIUM",
                        raw_evidence=f"Table continuation from page {last_frag.page_no} to {candidate_frag.page_no} "
                                     f"scored {conf} (breakdown: {details})."
                    )
                    issues.append(issue)
                    candidate_frag.fragment_order = len(curr_fragments)
                    candidate_frag.logical_table_id = curr_log_id
                    curr_fragments.append(candidate_frag)
                    skip_frag_indices.add(j)
                    accumulated_confidence = min(accumulated_confidence, conf)
                    last_frag = candidate_frag
                    j += 1
                else:
                    # Score too low: Break chain (cannot continue further)
                    break

            # 2. Build Unified LogicalTable with re-indexed rows and cells
            start_p = min(f.page_no for f in curr_fragments)
            end_p = max(f.page_no for f in curr_fragments)
            is_cross = (start_p != end_p)

            combined_cells, total_rows = cls._stitch_cells(curr_fragments)

            log_table = LogicalTable(
                logical_table_id=curr_log_id,
                doc_id=doc_id,
                title=curr_fragments[0].caption,
                page_start=start_p,
                page_end=end_p,
                total_rows=total_rows,
                total_cols=curr_fragments[0].n_cols,
                is_cross_page=is_cross,
                stitch_confidence=round(accumulated_confidence, 4),
                fragments=curr_fragments,
                combined_cells=combined_cells,
                review_issues=issues,
            )
            logical_tables.append(log_table)

        return logical_tables

    # ==========================================================================
    # HELPER SCORING ALGORITHMS
    # ==========================================================================

    @staticmethod
    def _calculate_column_alignment(frag_a: TableFragment, frag_b: TableFragment) -> float:
        """Compares horizontal cell bounding box overlaps in row 0."""
        cells_a = [c for c in frag_a.cells if c.row_no == 0 and c.bbox]
        cells_b = [c for c in frag_b.cells if c.row_no == 0 and c.bbox]

        if not cells_a or not cells_b or len(cells_a) != len(cells_b):
            # Fallback to column count match if bboxes are missing
            return 1.0 if frag_a.n_cols == frag_b.n_cols and frag_a.n_cols > 0 else 0.5

        overlaps = []
        for ca, cb in zip(cells_a, cells_b):
            ax0, ax1 = ca.bbox[0], ca.bbox[2]
            bx0, bx1 = cb.bbox[0], cb.bbox[2]
            overlap_width = max(0.0, min(ax1, bx1) - max(ax0, bx0))
            union_width = max(ax1, bx1) - min(ax0, bx0)
            if union_width > 0:
                overlaps.append(overlap_width / union_width)

        return sum(overlaps) / len(overlaps) if overlaps else 0.8

    @staticmethod
    def _evaluate_headers(frag_a: TableFragment, frag_b: TableFragment) -> Tuple[float, bool]:
        """Evaluates whether fragment B repeats fragment A's headers or continues directly."""
        hdr_a = [c.clean_text.lower().strip() for c in frag_a.cells if c.row_no == 0]
        hdr_b = [c.clean_text.lower().strip() for c in frag_b.cells if c.row_no == 0]

        if not hdr_a or not hdr_b:
            return 0.5, False

        # Case 1: Fragment B repeats Fragment A's headers exactly
        if hdr_a == hdr_b:
            return 1.0, True

        # Case 2: Similar header keywords
        common = set(hdr_a).intersection(set(hdr_b))
        if len(common) / max(len(hdr_a), 1) >= 0.75:
            return 0.9, True

        # Case 3: Fragment B does NOT have headers (row 0 contains data values/numbers)
        has_numeric_b = any(re.search(r"\d", text) for text in hdr_b)
        has_numeric_a = any(re.search(r"\d", text) for text in hdr_a)
        if has_numeric_b and not has_numeric_a:
            # Fragment B is a direct row continuation without repeated headers
            return 1.0, False

        return 0.4, False

    @staticmethod
    def _evaluate_data_types(frag_a: TableFragment, frag_b: TableFragment) -> float:
        """Compares data type profile per column (numeric vs alphanumeric)."""
        if frag_a.n_cols != frag_b.n_cols or frag_a.n_cols == 0:
            return 0.0

        matches = 0
        for col_idx in range(frag_a.n_cols):
            col_a_cells = [c.clean_text for c in frag_a.cells if c.col_no == col_idx and c.row_no > 0]
            col_b_cells = [c.clean_text for c in frag_b.cells if c.col_no == col_idx]

            num_a = sum(1 for t in col_a_cells if re.match(r"^[-+]?[0.0-9,.]+$", t.strip()))
            num_b = sum(1 for t in col_b_cells if re.match(r"^[-+]?[0.0-9,.]+$", t.strip()))

            is_numeric_a = (num_a / max(len(col_a_cells), 1)) >= 0.5
            is_numeric_b = (num_b / max(len(col_b_cells), 1)) >= 0.5

            if is_numeric_a == is_numeric_b:
                matches += 1

        return matches / frag_a.n_cols

    @staticmethod
    def _check_continuation_markers(text: str) -> float:
        """Looks for explicit continuation captions."""
        if not text:
            return 0.0
        low = text.lower()
        for pat in CONTINUATION_PATTERNS:
            if re.search(pat, low):
                return 1.0
        return 0.0

    @classmethod
    def _normalize_raw_table(cls, raw: Dict[str, Any], doc_id: str, index: int) -> TableFragment:
        """Converts raw ADI or local table dictionary into standardized TableFragment."""
        # Support ADI (rowCount, columnCount, cells) or local (row_count, column_count, cells)
        n_rows = raw.get("rowCount") or raw.get("row_count") or 0
        n_cols = raw.get("columnCount") or raw.get("column_count") or 0
        t_id = raw.get("table_id") or f"tbl_{doc_id}_{index}"
        caption = raw.get("caption")
        if isinstance(caption, dict):
            caption = caption.get("content")

        # Page detection
        page_no = raw.get("page_number") or raw.get("start_page")
        if not page_no and "boundingRegions" in raw:
            br = raw.get("boundingRegions", [])
            page_no = br[0].get("pageNumber", 1) if br else 1
        page_no = page_no or 1

        cells_out: List[NormalizedCell] = []
        raw_cells = raw.get("cells", [])

        for c_idx, c in enumerate(raw_cells):
            r_idx = c.get("rowIndex") if c.get("rowIndex") is not None else c.get("row_index", 0)
            c_pos = c.get("columnIndex") if c.get("columnIndex") is not None else c.get("column_index", 0)
            r_span = c.get("rowSpan") if c.get("rowSpan") is not None else c.get("row_span", 1)
            c_span = c.get("columnSpan") if c.get("columnSpan") is not None else c.get("column_span", 1)
            text_val = c.get("content") if c.get("content") is not None else c.get("text", "")

            # Cleaning / normalization
            clean_text = text_val.strip()

            # Bounding box
            bbox = None
            if "boundingRegions" in c and c["boundingRegions"]:
                poly = c["boundingRegions"][0].get("polygon")
                if poly and len(poly) >= 4:
                    xs = poly[0::2]
                    ys = poly[1::2]
                    bbox = [min(xs), min(ys), max(xs), max(ys)]
            elif "bbox" in c:
                bbox = c["bbox"]

            cell_id = f"c_{doc_id}_{page_no}_{r_idx}_{c_pos}"
            cells_out.append(NormalizedCell(
                cell_id=cell_id,
                table_id=t_id,
                row_no=r_idx,
                col_no=c_pos,
                row_span=r_span,
                col_span=c_span,
                raw_text=text_val,
                clean_text=clean_text,
                confidence=float(c.get("confidence", 1.0)),
                bbox=bbox,
            ))

        return TableFragment(
            table_id=t_id,
            doc_id=doc_id,
            page_no=page_no,
            logical_table_id="",
            fragment_order=0,
            n_rows=n_rows,
            n_cols=n_cols,
            caption=caption,
            cells=cells_out,
        )

    @classmethod
    def _stitch_cells(cls, fragments: List[TableFragment]) -> Tuple[List[NormalizedCell], int]:
        """
        Combines cells across fragments into a contiguous coordinate grid.
        Strips repeated header rows on continuation fragments.
        """
        combined: List[NormalizedCell] = []
        current_row_offset = 0

        for f_idx, frag in enumerate(fragments):
            cells_by_row: Dict[int, List[NormalizedCell]] = {}
            for c in frag.cells:
                cells_by_row.setdefault(c.row_no, []).append(c)

            start_row = 0
            if f_idx > 0 and len(fragments) > 1:
                # Check if this fragment repeats the header from fragment 0
                _, is_repeated_header = cls._evaluate_headers(fragments[0], frag)
                if is_repeated_header:
                    # Skip row 0 (repeated header)
                    start_row = 1

            for r_idx in sorted(cells_by_row.keys()):
                if r_idx < start_row:
                    continue
                row_cells = cells_by_row[r_idx]
                target_row = current_row_offset + (r_idx - start_row)
                for cell in row_cells:
                    stitched_cell = NormalizedCell(
                        cell_id=f"{cell.cell_id}_st_{f_idx}",
                        table_id=frag.logical_table_id,
                        row_no=target_row,
                        col_no=cell.col_no,
                        row_span=cell.row_span,
                        col_span=cell.col_span,
                        raw_text=cell.raw_text,
                        clean_text=cell.clean_text,
                        confidence=cell.confidence,
                        bbox=cell.bbox,
                        fix_flags=cell.fix_flags,
                    )
                    combined.append(stitched_cell)

            actual_rows_added = max(0, frag.n_rows - start_row)
            current_row_offset += actual_rows_added

        return combined, current_row_offset
