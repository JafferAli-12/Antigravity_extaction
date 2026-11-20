#!/usr/bin/env python3
"""
aggregator.py - Extraction Aggregation & Reconciliation Layer

Collects and reconciles extracted chunk results into a unified, lossless document
representation. Implements:
- Strict 100% page continuity and sequence verification (1..N).
- Detection of missing chunks, failed chunks, duplicate pages, or sequence gaps.
- Absolute page number normalization across all elements and tables.
- Cross-chunk table stitching via TableStitcher.
- Medallion projection into Normalized (Silver) and Curated Lab (Gold) entities.
"""

from __future__ import annotations

import datetime
import json
import logging
import re
import sys
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from chunker import ChunkMetadata, ChunkingManifest
from table_stitcher import LogicalTable, NormalizedCell, ReviewIssue, TableFragment, TableStitcher

logger = logging.getLogger("document_aggregator")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ==============================================================================
# MEDALLION ENTITY MODELS
# ==============================================================================

@dataclass
class DocumentEntity:
    doc_id: str
    source_uri: str
    file_hash: str
    doc_type: str
    page_count: int
    total_tables: int
    status: str
    model_version: str
    processed_at: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PageEntity:
    page_id: str
    doc_id: str
    page_no: int
    page_kind: str  # "DIGITAL" | "SCANNED" | "MIXED"
    ocr_quality: float
    width: Optional[float] = None
    height: Optional[float] = None
    span_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ParagraphEntity:
    para_id: str
    doc_id: str
    page_no: int
    element_index: int
    role: str
    text: str
    bbox: Optional[List[float]] = None
    confidence: float = 1.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SampleEntity:
    sample_id: str
    doc_id: str
    sample_code: Optional[str]
    collected_on: Optional[str] = None
    patient_ref: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LabResultEntity:
    result_id: str
    sample_id: str
    doc_id: str
    source_cell_id: str
    analyte: str
    value_raw: str
    value_num: Optional[float]
    qualifier: Optional[str]  # "<", ">", "<=", ">="
    unit: Optional[str]
    reference_range: Optional[str]
    flag: Optional[str]  # "HIGH", "LOW", "NORMAL", "ABNORMAL"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ReconciliationReport:
    doc_id: str
    expected_pages: int
    actual_pages: int
    is_valid: bool
    status: str  # "PASSED" | "FAILED" | "PARTIAL"
    missing_pages: List[int] = field(default_factory=list)
    duplicate_pages: List[int] = field(default_factory=list)
    missing_chunks: List[str] = field(default_factory=list)
    failed_chunks: List[str] = field(default_factory=list)
    total_tables_reconstructed: int = 0
    total_lab_results: int = 0
    review_issues_count: int = 0
    reconciled_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AggregatedDocument:
    document: DocumentEntity
    reconciliation: ReconciliationReport
    pages: List[PageEntity] = field(default_factory=list)
    paragraphs: List[ParagraphEntity] = field(default_factory=list)
    logical_tables: List[LogicalTable] = field(default_factory=list)
    samples: List[SampleEntity] = field(default_factory=list)
    lab_results: List[LabResultEntity] = field(default_factory=list)
    review_issues: List[ReviewIssue] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "document": self.document.to_dict(),
            "reconciliation": self.reconciliation.to_dict(),
            "pages": [p.to_dict() for p in self.pages],
            "paragraphs": [p.to_dict() for p in self.paragraphs],
            "logical_tables": [t.to_dict() for t in self.logical_tables],
            "samples": [s.to_dict() for s in self.samples],
            "lab_results": [r.to_dict() for r in self.lab_results],
            "review_issues": [i.to_dict() for i in self.review_issues],
        }


# ==============================================================================
# AGGREGATOR & RECONCILIATION ENGINE
# ==============================================================================

class DocumentAggregator:
    """
    Coordinates aggregation, page reconciliation, table stitching, and entity generation.
    """

    @classmethod
    def aggregate_from_chunks(
        cls,
        manifest: ChunkingManifest,
        chunk_results: List[Dict[str, Any]],
        source_uri: str = ""
    ) -> AggregatedDocument:
        """
        Reconciles chunks against manifest, verifies page ordering, stitches tables,
        and builds clean Medallion entities.
        """
        doc_id = manifest.document_id
        expected_pages = manifest.total_pages
        logger.info(f"Starting aggregation for document {doc_id} ({expected_pages} expected pages)...")

        # 1. Chunk Presence & Status Audit
        chunk_map: Dict[str, Dict[str, Any]] = {}
        for res in chunk_results:
            c_id = res.get("chunk_id")
            if c_id:
                chunk_map[c_id] = res

        missing_chunks: List[str] = []
        failed_chunks: List[str] = []

        for chk in manifest.chunks:
            c_id = chk.chunk_id if hasattr(chk, "chunk_id") else chk.get("chunk_id")
            res = chunk_map.get(c_id)
            if not res:
                missing_chunks.append(c_id)
            elif res.get("status") == "FAILED":
                failed_chunks.append(c_id)

        # 2. Extract & Re-index Pages, Paragraphs, and Tables
        pages_by_num: Dict[int, PageEntity] = {}
        duplicate_pages: List[int] = []
        all_paragraphs: List[ParagraphEntity] = []
        all_raw_tables: List[Dict[str, Any]] = []
        model_versions: Set[str] = set()

        for chk in manifest.chunks:
            c_id = chk.chunk_id if hasattr(chk, "chunk_id") else chk.get("chunk_id")
            chunk_start_page = chk.original_page_start if hasattr(chk, "original_page_start") else chk.get("original_page_start", 1)
            res = chunk_map.get(c_id)
            if not res or res.get("status") == "FAILED":
                continue

            model_versions.add(res.get("model_version", "local-v1"))

            # Check if this is native ADI (analyzeResult) or local JSON schema
            if "analyzeResult" in res:
                ar = res["analyzeResult"]
                # 2.1 ADI Pages
                for p in ar.get("pages", []):
                    rel_p = p.get("pageNumber", 1)
                    abs_p = chunk_start_page + (rel_p - 1)
                    if abs_p in pages_by_num:
                        duplicate_pages.append(abs_p)
                    else:
                        pages_by_num[abs_p] = PageEntity(
                            page_id=f"p_{doc_id}_{abs_p}",
                            doc_id=doc_id,
                            page_no=abs_p,
                            page_kind="DIGITAL",
                            ocr_quality=1.0,
                            width=p.get("width"),
                            height=p.get("height"),
                            span_count=len(p.get("lines", []))
                        )

                # 2.2 ADI Paragraphs
                for idx, para in enumerate(ar.get("paragraphs", [])):
                    br = para.get("boundingRegions", [])
                    rel_p = br[0].get("pageNumber", 1) if br else 1
                    abs_p = chunk_start_page + (rel_p - 1)
                    bbox = None
                    if br and "polygon" in br[0]:
                        poly = br[0]["polygon"]
                        if len(poly) >= 4:
                            xs = poly[0::2]
                            ys = poly[1::2]
                            bbox = [min(xs), min(ys), max(xs), max(ys)]

                    all_paragraphs.append(ParagraphEntity(
                        para_id=f"para_{doc_id}_{abs_p}_{idx}",
                        doc_id=doc_id,
                        page_no=abs_p,
                        element_index=idx,
                        role=para.get("role", "text"),
                        text=para.get("content", ""),
                        bbox=bbox,
                    ))

                # 2.3 ADI Tables (adjust boundingRegions to absolute pages)
                for tbl in ar.get("tables", []):
                    tbl_copy = dict(tbl)
                    # Shift bounding regions to absolute page numbers
                    shifted_br = []
                    for b in tbl_copy.get("boundingRegions", []):
                        shifted_br.append({
                            "pageNumber": chunk_start_page + (b.get("pageNumber", 1) - 1),
                            "polygon": b.get("polygon")
                        })
                    tbl_copy["boundingRegions"] = shifted_br
                    all_raw_tables.append(tbl_copy)

            else:
                # Local Engine JSON format
                # 2.4 Local Pages
                for p in res.get("pages", []):
                    rel_p = p.get("page_number", 1)
                    abs_p = chunk_start_page + (rel_p - 1)
                    if abs_p in pages_by_num:
                        duplicate_pages.append(abs_p)
                    else:
                        pages_by_num[abs_p] = PageEntity(
                            page_id=f"p_{doc_id}_{abs_p}",
                            doc_id=doc_id,
                            page_no=abs_p,
                            page_kind="SCANNED" if p.get("is_scanned") else "DIGITAL",
                            ocr_quality=0.85 if p.get("is_scanned") else 1.0,
                            width=p.get("width"),
                            height=p.get("height"),
                            span_count=len(p.get("lines", []))
                        )

                # 2.5 Local Elements / Paragraphs
                for el in res.get("elements", []):
                    rel_p = el.get("page_number", 1)
                    abs_p = chunk_start_page + (rel_p - 1)
                    all_paragraphs.append(ParagraphEntity(
                        para_id=f"para_{doc_id}_{abs_p}_{el.get('element_index', 0)}",
                        doc_id=doc_id,
                        page_no=abs_p,
                        element_index=el.get("element_index", 0),
                        role=el.get("type", "text"),
                        text=el.get("text", ""),
                        bbox=el.get("bbox"),
                        confidence=el.get("confidence", 1.0)
                    ))

                # 2.6 Local Tables
                for tbl in res.get("tables", []):
                    tbl_copy = dict(tbl)
                    rel_start = tbl_copy.get("start_page", 1)
                    rel_end = tbl_copy.get("end_page", 1)
                    tbl_copy["start_page"] = chunk_start_page + (rel_start - 1)
                    tbl_copy["end_page"] = chunk_start_page + (rel_end - 1)
                    all_raw_tables.append(tbl_copy)

        # 3. Page Sequence Reconciliation Audit
        all_detected_pages = sorted(pages_by_num.keys())
        expected_page_set = set(range(1, expected_pages + 1))
        detected_page_set = set(all_detected_pages)
        missing_pages = sorted(list(expected_page_set - detected_page_set))

        is_reconciled = (
            len(missing_chunks) == 0 and
            len(failed_chunks) == 0 and
            len(missing_pages) == 0 and
            len(all_detected_pages) == expected_pages
        )

        status = "PASSED" if is_reconciled else ("PARTIAL" if len(all_detected_pages) > 0 else "FAILED")

        # 4. Multi-Page Table Stitching
        paragraphs_by_pg: Dict[int, List[str]] = {}
        for para in all_paragraphs:
            paragraphs_by_pg.setdefault(para.page_no, []).append(para.text)

        logical_tables = TableStitcher.stitch_document_tables(
            raw_tables=all_raw_tables,
            doc_id=doc_id,
            paragraphs_by_page=paragraphs_by_pg
        )

        # 5. Domain Extraction: Lab Results & Review Issues
        samples, lab_results, extra_issues = cls._extract_lab_results(logical_tables, doc_id)

        all_issues: List[ReviewIssue] = []
        for lt in logical_tables:
            all_issues.extend(lt.review_issues)
        all_issues.extend(extra_issues)

        # 6. Build Final Reconciliation Report & Document
        recon_report = ReconciliationReport(
            doc_id=doc_id,
            expected_pages=expected_pages,
            actual_pages=len(all_detected_pages),
            is_valid=is_reconciled,
            status=status,
            missing_pages=missing_pages,
            duplicate_pages=sorted(list(set(duplicate_pages))),
            missing_chunks=missing_chunks,
            failed_chunks=failed_chunks,
            total_tables_reconstructed=len(logical_tables),
            total_lab_results=len(lab_results),
            review_issues_count=len(all_issues),
        )

        doc_entity = DocumentEntity(
            doc_id=doc_id,
            source_uri=source_uri or manifest.source_file,
            file_hash=manifest.file_hash,
            doc_type="LAB_REPORT",
            page_count=len(all_detected_pages),
            total_tables=len(logical_tables),
            status=status,
            model_version=",".join(sorted(model_versions)) if model_versions else "v1",
            processed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        )

        sorted_pages = [pages_by_num[p] for p in sorted(pages_by_num.keys())]

        logger.info(
            f"Aggregation Complete for {doc_id}: Status={status}, "
            f"Pages={len(sorted_pages)}/{expected_pages}, Tables={len(logical_tables)}, "
            f"LabResults={len(lab_results)}, Issues={len(all_issues)}"
        )

        return AggregatedDocument(
            document=doc_entity,
            reconciliation=recon_report,
            pages=sorted_pages,
            paragraphs=all_paragraphs,
            logical_tables=logical_tables,
            samples=samples,
            lab_results=lab_results,
            review_issues=all_issues,
        )

    # ==========================================================================
    # DOMAIN PARSER: LAB REPORT RESULTS (CURATED LAYER)
    # ==========================================================================

    @classmethod
    def _extract_lab_results(
        cls,
        logical_tables: List[LogicalTable],
        doc_id: str
    ) -> Tuple[List[SampleEntity], List[LabResultEntity], List[ReviewIssue]]:
        """
        Parses structured clinical lab results from table cells.
        Extracts: Analyte, Value (raw + parsed numeric), Qualifier (<, >), Unit, Flag.
        Flags parsing anomalies as ReviewIssues.
        """
        samples: List[SampleEntity] = []
        lab_results: List[LabResultEntity] = []
        issues: List[ReviewIssue] = []

        # Standard sample container
        default_sample = SampleEntity(
            sample_id=f"smp_{doc_id}_01",
            doc_id=doc_id,
            sample_code="SMP-DEFAULT",
            collected_on=None
        )
        samples.append(default_sample)

        # Common clinical lab column keywords
        analyte_patterns = [r"test", r"analyte", r"parameter", r"investigation", r"description"]
        value_patterns = [r"result", r"value", r"observed", r"finding"]
        unit_patterns = [r"unit", r"uom"]
        ref_patterns = [r"ref", r"range", r"interval", r"biological"]
        flag_patterns = [r"flag", r"status", r"remark", r"abnormal"]

        for lt in logical_tables:
            # Group cells by row
            row_map: Dict[int, Dict[int, NormalizedCell]] = {}
            for c in lt.combined_cells:
                row_map.setdefault(c.row_no, {})[c.col_no] = c

            if not row_map or 0 not in row_map:
                continue

            # Identify column mappings from header row 0
            header_row = row_map[0]
            col_analyte: Optional[int] = None
            col_value: Optional[int] = None
            col_unit: Optional[int] = None
            col_ref: Optional[int] = None
            col_flag: Optional[int] = None

            for col_idx, cell in header_row.items():
                txt = cell.clean_text.lower()
                if any(re.search(p, txt) for p in analyte_patterns) and col_analyte is None:
                    col_analyte = col_idx
                elif any(re.search(p, txt) for p in value_patterns) and col_value is None:
                    col_value = col_idx
                elif any(re.search(p, txt) for p in unit_patterns) and col_unit is None:
                    col_unit = col_idx
                elif any(re.search(p, txt) for p in ref_patterns) and col_ref is None:
                    col_ref = col_idx
                elif any(re.search(p, txt) for p in flag_patterns) and col_flag is None:
                    col_flag = col_idx

            # If at least analyte and value columns are identified, parse data rows
            if col_analyte is not None and col_value is not None:
                for r_idx in sorted(row_map.keys()):
                    if r_idx == 0:
                        continue  # Skip header
                    row = row_map[r_idx]
                    analyte_cell = row.get(col_analyte)
                    value_cell = row.get(col_value)

                    if not analyte_cell or not value_cell:
                        continue

                    analyte_name = analyte_cell.clean_text.strip()
                    val_raw = value_cell.clean_text.strip()

                    if not analyte_name or not val_raw:
                        continue

                    # Parse numeric value & qualifier (e.g. "< 0.05", "14.2", ">= 100")
                    qualifier = None
                    val_num = None
                    q_match = re.match(r"^([<>]=?)\s*([0-9,.]+)", val_raw)
                    if q_match:
                        qualifier = q_match.group(1)
                        try:
                            val_num = float(q_match.group(2).replace(",", ""))
                        except ValueError:
                            val_num = None
                    else:
                        try:
                            val_num = float(val_raw.replace(",", ""))
                        except ValueError:
                            val_num = None

                    # If value contains unexpected characters without numeric match, raise review issue
                    if val_num is None and qualifier is None and any(char.isdigit() for char in val_raw):
                        issues.append(ReviewIssue(
                            issue_id=f"iss_{uuid.uuid4().hex[:12]}",
                            doc_id=doc_id,
                            cell_id=value_cell.cell_id,
                            logical_table_id=lt.logical_table_id,
                            issue_type="NUMERIC_PARSE_FAILURE",
                            severity="LOW",
                            raw_evidence=f"Value '{val_raw}' contains digits but could not be parsed to numeric value."
                        ))

                    unit_val = row.get(col_unit).clean_text.strip() if col_unit is not None and col_unit in row else None
                    ref_val = row.get(col_ref).clean_text.strip() if col_ref is not None and col_ref in row else None
                    flag_val = row.get(col_flag).clean_text.strip() if col_flag is not None and col_flag in row else None

                    # Infer abnormal flag if not explicitly present
                    if not flag_val and val_num is not None and ref_val:
                        # Attempt standard range parse: "10.0 - 20.0"
                        range_m = re.match(r"^([0-9.]+)\s*-\s*([0-9.]+)", ref_val)
                        if range_m:
                            low = float(range_m.group(1))
                            high = float(range_m.group(2))
                            if val_num < low:
                                flag_val = "LOW"
                            elif val_num > high:
                                flag_val = "HIGH"
                            else:
                                flag_val = "NORMAL"

                    lab_results.append(LabResultEntity(
                        result_id=f"res_{doc_id}_{r_idx}",
                        sample_id=default_sample.sample_id,
                        doc_id=doc_id,
                        source_cell_id=value_cell.cell_id,
                        analyte=analyte_name,
                        value_raw=val_raw,
                        value_num=val_num,
                        qualifier=qualifier,
                        unit=unit_val,
                        reference_range=ref_val,
                        flag=flag_val
                    ))

        return samples, lab_results, issues
