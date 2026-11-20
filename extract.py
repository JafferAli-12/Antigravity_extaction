#!/usr/bin/env python3
"""
extract.py - Standalone Document Extraction CLI (Part A)

Converts various document formats (.pdf, .docx, .xlsx, .xls, images, .json)
into a standardized, engine-shaped JSON schema mirroring output from ADI
(Azure Document Intelligence) or Unstructured, enabling offline, credential-free
development and downstream testing.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import logging
import os
import sys
import time
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Optional YAML support for config
try:
    import yaml
except ImportError:
    yaml = None

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("document_extractor")

# Supported file extensions
SUPPORTED_EXTENSIONS = {
    ".pdf": "PDF",
    ".png": "IMAGE",
    ".jpg": "IMAGE",
    ".jpeg": "IMAGE",
    ".tif": "IMAGE",
    ".tiff": "IMAGE",
    ".xlsx": "EXCEL",
    ".xls": "EXCEL",
    ".docx": "DOCX",
    ".json": "JSON_PASSTHROUGH",
}

HEADER_KEYWORDS = {
    "id", "name", "date", "description", "total", "amount", "qty", "quantity",
    "price", "item", "no", "number", "category", "status", "type", "code", "account"
}


# ==============================================================================
# DATA MODELS & SCHEMA
# ==============================================================================

@dataclass
class TextLine:
    text: str
    bbox: Optional[List[float]] = None
    confidence: Optional[float] = None


@dataclass
class Page:
    page_number: int
    width: Optional[float] = None
    height: Optional[float] = None
    is_scanned: bool = False
    lines: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class Element:
    page_number: int
    element_index: int
    type: str  # "heading" | "text" | "table" | "figure" | "unknown"
    text: str
    bbox: Optional[List[float]] = None
    confidence: Optional[float] = None


@dataclass
class TableCell:
    row_index: int
    column_index: int
    row_span: int
    column_span: int
    text: str


@dataclass
class Table:
    table_id: str
    start_page: int
    end_page: int
    is_cross_page: bool
    row_count: int
    column_count: int
    caption: Optional[str]
    cells: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class Figure:
    page_number: int
    bbox: Optional[List[float]]
    caption: Optional[str]
    image_ref: str


@dataclass
class ExtractionMetadata:
    page_count_in_source: int
    pages_with_text: int
    pages_without_text: int
    tables_detected: int
    figures_detected: int
    warnings: List[str] = field(default_factory=list)


@dataclass
class ExtractionResult:
    status: str
    source_file: str
    engine: str = "local"
    model_version: str = "local-v1"
    extracted_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    page_count: int = 0
    pages: List[Dict[str, Any]] = field(default_factory=list)
    elements: List[Dict[str, Any]] = field(default_factory=list)
    tables: List[Dict[str, Any]] = field(default_factory=list)
    figures: List[Dict[str, Any]] = field(default_factory=list)
    extraction_metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def make_failed_json(
    source_file: str,
    error_class: str,
    error_message: str,
) -> Dict[str, Any]:
    """Generates the standardized failure JSON contract for upstream/downstream tracking."""
    return {
        "status": "FAILED",
        "error_class": error_class,
        "error_message": error_message,
        "source_file": source_file,
        "detected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


def generate_table_id(page_num: int, table_idx: int, prefix: str = "") -> str:
    """Produces a deterministic table ID based on page number and index."""
    raw = f"{prefix}_p{page_num}_t{table_idx}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# ==============================================================================
# HEURISTIC CROSS-PAGE TABLE MERGER
# ==============================================================================

def detect_and_merge_cross_page_tables(
    tables: List[Dict[str, Any]],
    warnings: List[str],
    column_tolerance: int = 0,
    continuation_threshold: float = 0.6,
) -> List[Dict[str, Any]]:
    """
    Heuristically links tables split across consecutive pages.
    Compares the last row of table X on page N with the first row of table Y on page N+1.
    If column counts match and row 0 of Y is not a repetitive header, merges them.
    """
    if len(tables) < 2:
        return tables

    merged: List[Dict[str, Any]] = []
    skip_indices = set()

    for i in range(len(tables)):
        if i in skip_indices:
            continue

        curr = tables[i]

        # Look ahead for a candidate on page N+1
        if i + 1 < len(tables):
            nxt = tables[i + 1]
            if nxt["start_page"] == curr["end_page"] + 1:
                # Check column count
                col_diff = abs(curr["column_count"] - nxt["column_count"])
                if col_diff <= column_tolerance:
                    # Check first row of nxt for continuation
                    nxt_first_row = [
                        c["text"].strip().lower()
                        for c in nxt["cells"]
                        if c["row_index"] == 0
                    ]
                    # If first row contains strong header keywords, likely a new table
                    has_header_kw = any(
                        any(kw in cell_val for kw in HEADER_KEYWORDS)
                        for cell_val in nxt_first_row
                    )

                    if not has_header_kw:
                        # Perform merge
                        row_offset = curr["row_count"]
                        merged_cells = list(curr["cells"])

                        for cell in nxt["cells"]:
                            merged_cells.append({
                                "row_index": cell["row_index"] + row_offset,
                                "column_index": cell["column_index"],
                                "row_span": cell["row_span"],
                                "column_span": cell["column_span"],
                                "text": cell["text"],
                            })

                        new_table = {
                            "table_id": curr["table_id"],
                            "start_page": curr["start_page"],
                            "end_page": nxt["end_page"],
                            "is_cross_page": True,
                            "row_count": curr["row_count"] + nxt["row_count"],
                            "column_count": max(curr["column_count"], nxt["column_count"]),
                            "caption": curr.get("caption") or nxt.get("caption"),
                            "cells": merged_cells,
                        }
                        merged.append(new_table)
                        skip_indices.add(i + 1)
                        warnings.append(
                            f"Merged cross-page table {curr['table_id']} across pages "
                            f"{curr['start_page']} and {nxt['end_page']}."
                        )
                        continue
                    else:
                        warnings.append(
                            f"Table on page {nxt['start_page']} matches column count of table on "
                            f"page {curr['end_page']} but was kept separate due to header keywords."
                        )

        merged.append(curr)

    return merged


# ==============================================================================
# EXTRACTORS FOR SPECIFIC FILE TYPES
# ==============================================================================

def extract_pdf(
    file_path: Path,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extracts text, lines, tables, and figures from a PDF document.
    Uses PyPDF2/pypdf for encryption checking and pdfplumber for content.
    Falls back to pytesseract for OCR if enabled and needed.
    """
    source_name = file_path.name
    warnings: List[str] = []

    # 1. Encryption and validity check with pypdf / PyPDF2
    try:
        try:
            import pypdf
            reader = pypdf.PdfReader(str(file_path))
            if reader.is_encrypted:
                return make_failed_json(source_name, "ENCRYPTED_PDF", "PDF file is encrypted or password-protected.")
            page_count_reader = len(reader.pages)
        except ImportError:
            import PyPDF2
            with open(file_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                if reader.is_encrypted:
                    return make_failed_json(source_name, "ENCRYPTED_PDF", "PDF file is encrypted or password-protected.")
                page_count_reader = len(reader.pages)
    except Exception as e:
        err_msg = str(e).lower()
        if "encrypted" in err_msg or "password" in err_msg:
            return make_failed_json(source_name, "ENCRYPTED_PDF", f"Encrypted PDF detected: {e}")
        return make_failed_json(source_name, "CORRUPT_FILE", f"Failed to parse PDF headers: {e}")

    # 2. Extract using pdfplumber
    try:
        import pdfplumber
    except ImportError:
        return make_failed_json(source_name, "UNREADABLE", "pdfplumber is required for PDF extraction.")

    pages_output: List[Dict[str, Any]] = []
    elements_output: List[Dict[str, Any]] = []
    tables_output: List[Dict[str, Any]] = []
    figures_output: List[Dict[str, Any]] = []

    pages_with_text = 0
    pages_without_text = 0

    enable_ocr = config.get("enable_ocr", False)
    pytesseract_available = False
    if enable_ocr:
        try:
            import pytesseract
            from PIL import Image
            tess_cmd = config.get("tesseract_cmd")
            if tess_cmd:
                pytesseract.pytesseract.tesseract_cmd = tess_cmd
            pytesseract_available = True
        except Exception:
            pytesseract_available = False

    try:
        with pdfplumber.open(str(file_path)) as pdf:
            actual_page_count = len(pdf.pages)

            for page_idx, page in enumerate(pdf.pages):
                page_num = page_idx + 1
                width = float(page.width) if page.width else None
                height = float(page.height) if page.height else None
                lines: List[Dict[str, Any]] = []
                element_idx = 0
                is_scanned = False

                # Extract words/text
                text = page.extract_text() or ""
                words = page.extract_words()

                # Extract text lines
                if words:
                    # Group words by approximate y-coordinate to form lines
                    y_grouped: Dict[int, List[Dict[str, Any]]] = {}
                    for w in words:
                        y_key = int(round(w["top"] / 3.0) * 3)
                        y_grouped.setdefault(y_key, []).append(w)

                    for y_key in sorted(y_grouped.keys()):
                        line_words = sorted(y_grouped[y_key], key=lambda x: x["x0"])
                        line_text = " ".join(w["text"] for w in line_words).strip()
                        if line_text:
                            x0 = min(w["x0"] for w in line_words)
                            top = min(w["top"] for w in line_words)
                            x1 = max(w["x1"] for w in line_words)
                            bottom = max(w["bottom"] for w in line_words)
                            lines.append({
                                "text": line_text,
                                "bbox": [round(float(x0), 2), round(float(top), 2),
                                         round(float(x1), 2), round(float(bottom), 2)],
                                "confidence": 1.0,
                            })

                # Check if page has no extractable text
                if not text.strip():
                    if enable_ocr and pytesseract_available:
                        try:
                            # Render page to image for OCR
                            img = page.to_image(resolution=150).original
                            ocr_data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
                            ocr_lines: List[str] = []
                            n_boxes = len(ocr_data["text"])
                            for bi in range(n_boxes):
                                t = ocr_data["text"][bi].strip()
                                if t:
                                    ocr_lines.append(t)
                            if ocr_lines:
                                is_scanned = True
                                text = " ".join(ocr_lines)
                                lines.append({
                                    "text": text,
                                    "bbox": [0.0, 0.0, float(width or 0), float(height or 0)],
                                    "confidence": 0.85,
                                })
                        except Exception as ocr_err:
                            warnings.append(f"Page {page_num}: OCR attempt failed: {ocr_err}")

                    if not text.strip():
                        pages_without_text += 1
                        if not enable_ocr or not pytesseract_available:
                            warnings.append(f"Page {page_num}: no_text_layer_no_ocr")
                    else:
                        pages_with_text += 1
                else:
                    pages_with_text += 1

                # Append to pages
                pages_output.append({
                    "page_number": page_num,
                    "width": width,
                    "height": height,
                    "is_scanned": is_scanned,
                    "lines": lines,
                })

                # Extract Tables on this page
                try:
                    detected_tables = page.find_tables()
                    for t_idx, dt in enumerate(detected_tables):
                        extracted_table = dt.extract()
                        if not extracted_table:
                            continue

                        cells: List[Dict[str, Any]] = []
                        row_count = len(extracted_table)
                        col_count = max(len(row) for row in extracted_table) if row_count > 0 else 0

                        for r_idx, row in enumerate(extracted_table):
                            for c_idx, cell_value in enumerate(row):
                                cells.append({
                                    "row_index": r_idx,
                                    "column_index": c_idx,
                                    "row_span": 1,
                                    "column_span": 1,
                                    "text": str(cell_value or "").strip(),
                                })

                        t_bbox = [round(float(v), 2) for v in dt.bbox] if hasattr(dt, "bbox") else None
                        t_id = generate_table_id(page_num, t_idx, source_name)

                        tables_output.append({
                            "table_id": t_id,
                            "start_page": page_num,
                            "end_page": page_num,
                            "is_cross_page": False,
                            "row_count": row_count,
                            "column_count": col_count,
                            "caption": None,
                            "cells": cells,
                        })

                        elements_output.append({
                            "page_number": page_num,
                            "element_index": element_idx,
                            "type": "table",
                            "text": f"Table with {row_count} rows, {col_count} columns",
                            "bbox": t_bbox,
                            "confidence": 1.0,
                        })
                        element_idx += 1
                except Exception as tbl_err:
                    warnings.append(f"Page {page_num}: table extraction warning: {tbl_err}")

                # Extract Figures / Images on this page
                if hasattr(page, "images") and page.images:
                    for f_idx, img_meta in enumerate(page.images):
                        f_bbox = [
                            round(float(img_meta.get("x0", 0)), 2),
                            round(float(img_meta.get("top", 0)), 2),
                            round(float(img_meta.get("x1", 0)), 2),
                            round(float(img_meta.get("bottom", 0)), 2),
                        ]
                        figures_output.append({
                            "page_number": page_num,
                            "bbox": f_bbox,
                            "caption": None,
                            "image_ref": f"page_{page_num}_img_{f_idx}",
                        })
                        elements_output.append({
                            "page_number": page_num,
                            "element_index": element_idx,
                            "type": "figure",
                            "text": f"Embedded Image {f_idx}",
                            "bbox": f_bbox,
                            "confidence": 1.0,
                        })
                        element_idx += 1

                # Construct reading order text elements from remaining lines
                for line in lines:
                    line_text = line["text"]
                    is_heading = len(line_text) < 80 and (
                        line_text.isupper() or any(line_text.lower().startswith(h) for h in ["section", "chapter", "table of"])
                    )
                    elements_output.append({
                        "page_number": page_num,
                        "element_index": element_idx,
                        "type": "heading" if is_heading else "text",
                        "text": line_text,
                        "bbox": line["bbox"],
                        "confidence": line.get("confidence", 1.0),
                    })
                    element_idx += 1

    except Exception as e:
        return make_failed_json(source_name, "CORRUPT_FILE", f"Error during PDF extraction: {e}")

    # Heuristic cross-page table merge
    if config.get("cross_page_table_detection", True):
        tables_output = detect_and_merge_cross_page_tables(
            tables=tables_output,
            warnings=warnings,
            column_tolerance=config.get("cross_page_column_tolerance", 0),
            continuation_threshold=config.get("continuation_similarity_threshold", 0.6),
        )

    # Sort elements by (page_number, element_index)
    elements_output.sort(key=lambda el: (el["page_number"], el["element_index"]))

    res = ExtractionResult(
        status="SUCCESS",
        source_file=source_name,
        page_count=actual_page_count,
        pages=pages_output,
        elements=elements_output,
        tables=tables_output,
        figures=figures_output,
        extraction_metadata={
            "page_count_in_source": actual_page_count,
            "pages_with_text": pages_with_text,
            "pages_without_text": pages_without_text,
            "tables_detected": len(tables_output),
            "figures_detected": len(figures_output),
            "warnings": warnings,
        },
    )
    return res.to_dict()


def extract_image(
    file_path: Path,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extracts text and metadata from image files (.png, .jpg, .jpeg, .tif, .tiff).
    Treated as a single scanned page. If OCR is available, lines and text elements
    are generated; otherwise, creates a single figure element with a notice.
    """
    source_name = file_path.name
    warnings: List[str] = []

    try:
        from PIL import Image
        with Image.open(file_path) as img:
            width, height = img.size
    except Exception as e:
        return make_failed_json(source_name, "CORRUPT_FILE", f"Invalid or corrupt image: {e}")

    lines: List[Dict[str, Any]] = []
    elements: List[Dict[str, Any]] = []
    is_scanned = True
    has_text = False

    enable_ocr = config.get("enable_ocr", False)
    if enable_ocr:
        try:
            import pytesseract
            tess_cmd = config.get("tesseract_cmd")
            if tess_cmd:
                pytesseract.pytesseract.tesseract_cmd = tess_cmd

            with Image.open(file_path) as img:
                ocr_data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)

            n_boxes = len(ocr_data["text"])
            element_idx = 0
            for i in range(n_boxes):
                txt = ocr_data["text"][i].strip()
                if txt:
                    has_text = True
                    x = float(ocr_data["left"][i])
                    y = float(ocr_data["top"][i])
                    w = float(ocr_data["width"][i])
                    h = float(ocr_data["height"][i])
                    conf = float(ocr_data["conf"][i]) / 100.0 if ocr_data["conf"][i] >= 0 else None

                    bbox = [round(x, 2), round(y, 2), round(x + w, 2), round(y + h, 2)]
                    lines.append({"text": txt, "bbox": bbox, "confidence": conf})
                    elements.append({
                        "page_number": 1,
                        "element_index": element_idx,
                        "type": "text",
                        "text": txt,
                        "bbox": bbox,
                        "confidence": conf,
                    })
                    element_idx += 1
        except Exception as ocr_err:
            warnings.append(f"Image OCR failed: {ocr_err}")

    if not elements:
        elements.append({
            "page_number": 1,
            "element_index": 0,
            "type": "figure",
            "text": "Image file (OCR unavailable or disabled)",
            "bbox": [0.0, 0.0, float(width), float(height)],
            "confidence": 1.0,
        })
        warnings.append("OCR was unavailable or disabled; returned image metadata only.")

    figures = [{
        "page_number": 1,
        "bbox": [0.0, 0.0, float(width), float(height)],
        "caption": None,
        "image_ref": source_name,
    }]

    pages = [{
        "page_number": 1,
        "width": float(width),
        "height": float(height),
        "is_scanned": is_scanned,
        "lines": lines,
    }]

    res = ExtractionResult(
        status="SUCCESS",
        source_file=source_name,
        page_count=1,
        pages=pages,
        elements=elements,
        tables=[],
        figures=figures,
        extraction_metadata={
            "page_count_in_source": 1,
            "pages_with_text": 1 if has_text else 0,
            "pages_without_text": 0 if has_text else 1,
            "tables_detected": 0,
            "figures_detected": 1,
            "warnings": warnings,
        },
    )
    return res.to_dict()


def extract_excel(
    file_path: Path,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extracts tabular content from Excel spreadsheets (.xlsx, .xls).
    Models each sheet as a table with cells and merged ranges handled.
    page_count = 1, page_number = 1.
    """
    source_name = file_path.name
    warnings: List[str] = []

    try:
        import openpyxl
    except ImportError:
        return make_failed_json(source_name, "UNREADABLE", "openpyxl is required to extract Excel files.")

    try:
        # data_only=True evaluates formulas to cached values
        wb = openpyxl.load_workbook(str(file_path), data_only=True, read_only=False)
    except Exception as e:
        err_msg = str(e).lower()
        if "password" in err_msg or "encrypted" in err_msg:
            return make_failed_json(source_name, "ENCRYPTED_FILE", f"Password-protected Excel spreadsheet: {e}")
        return make_failed_json(source_name, "CORRUPT_FILE", f"Unable to read Excel workbook: {e}")

    tables: List[Dict[str, Any]] = []
    elements: List[Dict[str, Any]] = []
    lines: List[Dict[str, Any]] = []
    element_idx = 0

    for sheet_idx, sheet_name in enumerate(wb.sheetnames):
        sheet = wb[sheet_name]
        table_id = generate_table_id(1, sheet_idx, source_name)

        # Build merged cells map: (row, col) -> (row_span, col_span)
        merged_map: Dict[Tuple[int, int], Tuple[int, int]] = {}
        covered_cells: set = set()

        for m_range in sheet.merged_cells.ranges:
            min_col, min_row, max_col, max_row = m_range.bounds
            row_span = max_row - min_row + 1
            col_span = max_col - min_col + 1
            # 0-indexed indices
            top_left = (min_row - 1, min_col - 1)
            merged_map[top_left] = (row_span, col_span)

            for r in range(min_row - 1, max_row):
                for c in range(min_col - 1, max_col):
                    if (r, c) != top_left:
                        covered_cells.add((r, c))

        cells: List[Dict[str, Any]] = []
        max_r = sheet.max_row or 0
        max_c = sheet.max_column or 0

        # Scan non-empty rows
        row_count = 0
        for r_idx in range(max_r):
            row_cells_text = []
            has_data = False
            for c_idx in range(max_c):
                if (r_idx, c_idx) in covered_cells:
                    continue

                cell_obj = sheet.cell(row=r_idx + 1, column=c_idx + 1)
                val = cell_obj.value
                val_str = str(val).strip() if val is not None else ""

                if val_str:
                    has_data = True
                row_cells_text.append(val_str)

                row_span, col_span = merged_map.get((r_idx, c_idx), (1, 1))
                cells.append({
                    "row_index": r_idx,
                    "column_index": c_idx,
                    "row_span": row_span,
                    "column_span": col_span,
                    "text": val_str,
                })

            if has_data:
                row_count += 1
                row_summary = " | ".join(filter(bool, row_cells_text))
                lines.append({"text": row_summary, "bbox": None, "confidence": 1.0})
                elements.append({
                    "page_number": 1,
                    "element_index": element_idx,
                    "type": "table",
                    "text": f"[{sheet_name}] Row {r_idx + 1}: {row_summary}",
                    "bbox": None,
                    "confidence": 1.0,
                })
                element_idx += 1

        tables.append({
            "table_id": table_id,
            "start_page": 1,
            "end_page": 1,
            "is_cross_page": False,
            "row_count": max_r,
            "column_count": max_c,
            "caption": f"Sheet: {sheet_name}",
            "cells": cells,
        })

    pages = [{
        "page_number": 1,
        "width": None,
        "height": None,
        "is_scanned": False,
        "lines": lines,
    }]

    res = ExtractionResult(
        status="SUCCESS",
        source_file=source_name,
        page_count=1,
        pages=pages,
        elements=elements,
        tables=tables,
        figures=[],
        extraction_metadata={
            "page_count_in_source": 1,
            "pages_with_text": 1 if lines else 0,
            "pages_without_text": 0 if lines else 1,
            "tables_detected": len(tables),
            "figures_detected": 0,
            "warnings": warnings,
        },
    )
    return res.to_dict()


def extract_docx(
    file_path: Path,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extracts text paragraphs, tables, and embedded images from a Word document (.docx).
    Sets page_count = 1 with a warning that pagination is not reliable for DOCX.
    """
    source_name = file_path.name
    warnings: List[str] = ["pagination not reliable for DOCX"]

    try:
        import docx
    except ImportError:
        return make_failed_json(source_name, "UNREADABLE", "python-docx is required for DOCX extraction.")

    try:
        doc = docx.Document(str(file_path))
    except Exception as e:
        return make_failed_json(source_name, "CORRUPT_FILE", f"Failed to load DOCX document: {e}")

    elements: List[Dict[str, Any]] = []
    lines: List[Dict[str, Any]] = []
    tables: List[Dict[str, Any]] = []
    figures: List[Dict[str, Any]] = []
    element_idx = 0

    # 1. Paragraphs -> elements and lines
    for p in doc.paragraphs:
        txt = p.text.strip()
        if not txt:
            continue

        style_name = (p.style.name or "").lower()
        is_heading = style_name.startswith("heading") or "title" in style_name
        el_type = "heading" if is_heading else "text"

        lines.append({"text": txt, "bbox": None, "confidence": 1.0})
        elements.append({
            "page_number": 1,
            "element_index": element_idx,
            "type": el_type,
            "text": txt,
            "bbox": None,
            "confidence": 1.0,
        })
        element_idx += 1

    # 2. Tables -> tables and table elements
    for t_idx, tbl in enumerate(doc.tables):
        table_id = generate_table_id(1, t_idx, source_name)
        cells: List[Dict[str, Any]] = []
        row_count = len(tbl.rows)
        col_count = len(tbl.columns) if row_count > 0 else 0

        for r_idx, row in enumerate(tbl.rows):
            for c_idx, cell in enumerate(row.cells):
                cells.append({
                    "row_index": r_idx,
                    "column_index": c_idx,
                    "row_span": 1,
                    "column_span": 1,
                    "text": cell.text.strip(),
                })

        tables.append({
            "table_id": table_id,
            "start_page": 1,
            "end_page": 1,
            "is_cross_page": False,
            "row_count": row_count,
            "column_count": col_count,
            "caption": None,
            "cells": cells,
        })

        elements.append({
            "page_number": 1,
            "element_index": element_idx,
            "type": "table",
            "text": f"DOCX Table {t_idx + 1} with {row_count} rows, {col_count} columns",
            "bbox": None,
            "confidence": 1.0,
        })
        element_idx += 1

    # 3. Embedded images via zipfile inspect
    try:
        with zipfile.ZipFile(file_path, "r") as z:
            media_files = [f for f in z.namelist() if f.startswith("word/media/")]
            for f_idx, media_name in enumerate(media_files):
                figures.append({
                    "page_number": 1,
                    "bbox": None,
                    "caption": None,
                    "image_ref": media_name,
                })
                elements.append({
                    "page_number": 1,
                    "element_index": element_idx,
                    "type": "figure",
                    "text": f"Embedded Image: {Path(media_name).name}",
                    "bbox": None,
                    "confidence": 1.0,
                })
                element_idx += 1
    except Exception as img_err:
        warnings.append(f"Image inspection in docx package warning: {img_err}")

    pages = [{
        "page_number": 1,
        "width": None,
        "height": None,
        "is_scanned": False,
        "lines": lines,
    }]

    res = ExtractionResult(
        status="SUCCESS",
        source_file=source_name,
        page_count=1,
        pages=pages,
        elements=elements,
        tables=tables,
        figures=figures,
        extraction_metadata={
            "page_count_in_source": 1,
            "pages_with_text": 1 if lines else 0,
            "pages_without_text": 0 if lines else 1,
            "tables_detected": len(tables),
            "figures_detected": len(figures),
            "warnings": warnings,
        },
    )
    return res.to_dict()


def extract_json_passthrough(
    file_path: Path,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Validates and passes through an existing JSON file without modification.
    """
    source_name = file_path.name
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception as e:
        return make_failed_json(source_name, "CORRUPT_FILE", f"Invalid JSON format: {e}")


# ==============================================================================
# PIPELINE ROUTER & PROCESSOR
# ==============================================================================

def process_single_file(
    file_path: Path,
    output_dir: Path,
    config: Dict[str, Any],
    overwrite: bool = False,
) -> Dict[str, Any]:
    """
    Processes a single file, enforcing file size limits, zero-byte checks,
    idempotency, and format routing. Writes JSON output and returns execution metadata.
    """
    start_time = time.time()
    source_name = file_path.name
    output_file = output_dir / f"{file_path.stem}.json"
    max_mb = config.get("max_file_size_mb", 500)

    # 1. Idempotency check: Skip if output exists, is newer than source, and was a SUCCESS
    if not overwrite and output_file.exists():
        src_mtime = file_path.stat().st_mtime
        out_mtime = output_file.stat().st_mtime
        if src_mtime < out_mtime:
            is_prev_success = False
            try:
                with open(output_file, "r", encoding="utf-8") as f_prev:
                    prev_json = json.load(f_prev)
                if prev_json.get("status") == "SUCCESS":
                    is_prev_success = True
            except Exception:
                is_prev_success = False

            if is_prev_success:
                logger.info(f"Skipping '{source_name}' (successful output up-to-date).")
                return {
                    "source_file": source_name,
                    "output_file": str(output_file),
                    "status": "SKIPPED",
                    "duration_ms": 0,
                    "warnings": ["Output already up-to-date"],
                }
            else:
                logger.info(f"Re-extracting '{source_name}' because previous output was FAILED or corrupt.")

    # 2. File size checks
    try:
        file_size = file_path.stat().st_size
    except Exception as e:
        result = make_failed_json(source_name, "UNREADABLE", f"Cannot access file: {e}")
        write_json_output(output_file, result)
        return {
            "source_file": source_name,
            "output_file": str(output_file),
            "status": "FAILED",
            "duration_ms": int((time.time() - start_time) * 1000),
            "error_class": "UNREADABLE",
        }

    # Zero-byte file
    if file_size == 0:
        result = make_failed_json(source_name, "EMPTY_FILE", "Input file contains 0 bytes.")
        write_json_output(output_file, result)
        return {
            "source_file": source_name,
            "output_file": str(output_file),
            "status": "FAILED",
            "duration_ms": int((time.time() - start_time) * 1000),
            "error_class": "EMPTY_FILE",
        }

    # File size limit
    if file_size > max_mb * 1024 * 1024:
        msg = f"File size ({file_size / (1024 * 1024):.1f} MB) exceeds maximum allowed limit ({max_mb} MB)."
        result = make_failed_json(source_name, "CORRUPT_FILE", msg)
        write_json_output(output_file, result)
        return {
            "source_file": source_name,
            "output_file": str(output_file),
            "status": "FAILED",
            "duration_ms": int((time.time() - start_time) * 1000),
            "error_class": "CORRUPT_FILE",
            "warnings": [msg],
        }

    # 3. Route by extension
    ext = file_path.suffix.lower()
    doc_type = SUPPORTED_EXTENSIONS.get(ext)

    if not doc_type:
        result = make_failed_json(
            source_name,
            "UNSUPPORTED_FORMAT",
            f"Unsupported file extension '{ext}'. Supported: {list(SUPPORTED_EXTENSIONS.keys())}",
        )
    else:
        try:
            if doc_type == "PDF":
                result = extract_pdf(file_path, config)
            elif doc_type == "IMAGE":
                result = extract_image(file_path, config)
            elif doc_type == "EXCEL":
                result = extract_excel(file_path, config)
            elif doc_type == "DOCX":
                result = extract_docx(file_path, config)
            elif doc_type == "JSON_PASSTHROUGH":
                result = extract_json_passthrough(file_path, config)
            else:
                result = make_failed_json(source_name, "UNSUPPORTED_FORMAT", f"Unhandled category: {doc_type}")
        except Exception as unhandled_err:
            logger.exception(f"Unexpected exception processing '{source_name}': {unhandled_err}")
            result = make_failed_json(source_name, "UNREADABLE", f"Unhandled extraction exception: {unhandled_err}")

    # 4. Write output JSON
    write_json_output(output_file, result)

    duration_ms = int((time.time() - start_time) * 1000)
    status = result.get("status", "SUCCESS")
    error_class = result.get("error_class")
    warnings = result.get("extraction_metadata", {}).get("warnings", [])

    return {
        "source_file": source_name,
        "output_file": str(output_file),
        "status": status,
        "duration_ms": duration_ms,
        "error_class": error_class,
        "warnings": warnings,
    }


def write_json_output(target_path: Path, data: Dict[str, Any]) -> None:
    """Safely writes a dictionary to a JSON file."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target_path.with_suffix(".tmp")
    with open(temp_target, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    temp_target.replace(target_path)


# ==============================================================================
# MAIN BATCH DRIVER
# ==============================================================================

def run_extraction_batch(
    input_dir: Path,
    output_dir: Path,
    config: Dict[str, Any],
    overwrite: bool = False,
) -> Tuple[int, int, List[Dict[str, Any]]]:
    """
    Executes extraction across all files in the input directory.
    Writes sidecar log file extraction_log.jsonl and reports summary statistics.
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / config.get("log_filename", "extraction_log.jsonl")

    # Discover candidate files
    files = [p for p in input_dir.iterdir() if p.is_file()]
    logger.info(f"Discovered {len(files)} files in '{input_dir}'")

    succeeded = 0
    failed = 0
    failures_summary: List[Dict[str, Any]] = []

    with open(log_path, "a", encoding="utf-8") as log_file:
        for f in files:
            log_entry = process_single_file(f, output_dir, config, overwrite=overwrite)
            log_file.write(json.dumps(log_entry) + "\n")
            log_file.flush()

            if log_entry["status"] == "SUCCESS":
                succeeded += 1
                logger.info(f"[SUCCESS] {log_entry['source_file']} ({log_entry['duration_ms']} ms)")
            elif log_entry["status"] == "SKIPPED":
                logger.info(f"[SKIPPED] {log_entry['source_file']}")
            else:
                failed += 1
                failures_summary.append({
                    "file": log_entry["source_file"],
                    "error_class": log_entry.get("error_class"),
                })
                logger.warning(
                    f"[FAILED] {log_entry['source_file']} -> "
                    f"{log_entry.get('error_class')} ({log_entry['duration_ms']} ms)"
                )

    # Print summary table
    print("\n" + "=" * 60)
    print("DOCUMENT EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"Total Files Examined : {len(files)}")
    print(f"Successfully Extracted: {succeeded}")
    print(f"Failed / Quarantined  : {failed}")
    if failures_summary:
        print("\nFailures Detail:")
        for fail in failures_summary:
            print(f"  - {fail['file']} : {fail['error_class']}")
    print("=" * 60 + "\n")

    return succeeded, failed, failures_summary


def load_config(config_path: Optional[Path]) -> Dict[str, Any]:
    """Loads configuration options from a YAML file if supplied, or returns defaults."""
    default_config: Dict[str, Any] = {
        "enable_ocr": False,
        "tesseract_cmd": None,
        "tesseract_lang": "eng",
        "max_file_size_mb": 500,
        "timeout_seconds": 180,
        "cross_page_table_detection": True,
        "cross_page_column_tolerance": 0,
        "continuation_similarity_threshold": 0.6,
        "overwrite": False,
        "log_filename": "extraction_log.jsonl",
    }

    if not config_path:
        return default_config

    if not config_path.exists():
        logger.warning(f"Config path '{config_path}' not found. Using defaults.")
        return default_config

    if yaml is None:
        logger.warning("PyYAML not installed. Cannot load YAML config file. Using defaults.")
        return default_config

    with open(config_path, "r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f) or {}

    extraction_section = loaded.get("extraction", loaded)
    default_config.update(extraction_section)
    return default_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract documents to engine-shaped JSON without cloud credentials."
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        type=Path,
        help="Path to directory containing input documents",
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        type=Path,
        help="Path to directory where output JSON files will be stored",
    )
    parser.add_argument(
        "--config", "-c",
        type=Path,
        default=None,
        help="Optional path to config.yaml configuration file",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Force overwrite of existing JSON outputs even if up-to-date",
    )
    parser.add_argument(
        "--enable-ocr",
        action="store_true",
        help="Enable pytesseract OCR for scanned PDF pages and images",
    )

    args = parser.parse_args()

    config = load_config(args.config)
    if args.overwrite:
        config["overwrite"] = True
    if args.enable_ocr:
        config["enable_ocr"] = True

    try:
        run_extraction_batch(
            input_dir=args.input,
            output_dir=args.output,
            config=config,
            overwrite=config.get("overwrite", False),
        )
    except Exception as e:
        logger.error(f"Batch execution halted: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
