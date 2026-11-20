# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # === CELL 1: TITLE & WORKFLOW OVERVIEW ===
# MAGIC
# MAGIC # Document-to-JSON Local Extraction Pipeline (Part A)
# MAGIC
# MAGIC This notebook converts raw documents (`.pdf`, `.docx`, `.xlsx`, `.xls`, images, `.json`)
# MAGIC into standardized, engine-shaped JSON without requiring cloud API keys (e.g. ADI or Unstructured).
# MAGIC
# MAGIC ### Features:
# MAGIC 1. **Zero Cloud Dependencies**: Uses open-source libraries (`pdfplumber`, `PyPDF2`, `openpyxl`, `python-docx`, `Pillow`).
# MAGIC 2. **Standardized Engine-Shaped JSON**: Mirrors output structure from production engines (pages, elements, tables, figures, metadata).
# MAGIC 3. **Non-Crashing Error Handling**: Produces structured `FAILED` JSON records for locked PDFs, corrupt files, and unsupported formats.
# MAGIC 4. **Heuristic Cross-Page Table Merging**: Identifies and links tables split across page boundaries.
# MAGIC 5. **Idempotency & Sidecar Logging**: Tracks execution in `extraction_log.jsonl`.

# COMMAND ----------

# === CELL 2: CONFIGURATION & WIDGETS ===
import os

# Create widgets for interactive configuration
dbutils.widgets.text("input_path", "/Workspace/Users/shared/sample_docs", "Input Documents Path")
dbutils.widgets.text("output_path", "/Workspace/Users/shared/sample_json", "Output JSON Path")
dbutils.widgets.dropdown("enable_ocr", "False", ["True", "False"], "Enable OCR Fallback")
dbutils.widgets.dropdown("overwrite", "False", ["True", "False"], "Overwrite Existing Outputs")
dbutils.widgets.text("max_file_size_mb", "500", "Max File Size (MB)")

INPUT_PATH = dbutils.widgets.get("input_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")
ENABLE_OCR = dbutils.widgets.get("enable_ocr") == "True"
OVERWRITE = dbutils.widgets.get("overwrite") == "True"
MAX_FILE_SIZE_MB = int(dbutils.widgets.get("max_file_size_mb"))

print(f"Extraction Configuration:")
print(f"  Input Path       : {INPUT_PATH}")
print(f"  Output Path      : {OUTPUT_PATH}")
print(f"  Enable OCR       : {ENABLE_OCR}")
print(f"  Overwrite        : {OVERWRITE}")
print(f"  Max File Size MB : {MAX_FILE_SIZE_MB}")

# COMMAND ----------

# MAGIC %skip
# MAGIC # === CELL 3: DEPENDENCY INSTALLATION ===
# MAGIC %pip install --quiet pdfplumber pypdf PyPDF2 openpyxl python-docx Pillow pytesseract

# COMMAND ----------

# === CELL 4: EXTRACTION CORE ENGINE ===
import datetime
import hashlib
import json
import logging
import sys
import time
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("part_a_extractor")
logger.setLevel(logging.INFO)

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

def make_failed_json(source_file: str, error_class: str, error_message: str) -> Dict[str, Any]:
    return {
        "status": "FAILED",
        "error_class": error_class,
        "error_message": error_message,
        "source_file": source_file,
        "detected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

def generate_table_id(page_num: int, table_idx: int, prefix: str = "") -> str:
    raw = f"{prefix}_p{page_num}_t{table_idx}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

def detect_and_merge_cross_page_tables(tables: List[Dict[str, Any]], warnings: List[str]) -> List[Dict[str, Any]]:
    if len(tables) < 2:
        return tables
    merged = []
    skip_indices = set()
    for i in range(len(tables)):
        if i in skip_indices:
            continue
        curr = tables[i]
        if i + 1 < len(tables):
            nxt = tables[i + 1]
            if nxt["start_page"] == curr["end_page"] + 1 and curr["column_count"] == nxt["column_count"]:
                nxt_first_row = [c["text"].strip().lower() for c in nxt["cells"] if c["row_index"] == 0]
                has_header_kw = any(any(kw in val for kw in HEADER_KEYWORDS) for val in nxt_first_row)
                if not has_header_kw:
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
                        "column_count": curr["column_count"],
                        "caption": curr.get("caption") or nxt.get("caption"),
                        "cells": merged_cells,
                    }
                    merged.append(new_table)
                    skip_indices.add(i + 1)
                    warnings.append(f"Merged cross-page table {curr['table_id']} across pages {curr['start_page']}-{nxt['end_page']}.")
                    continue
        merged.append(curr)
    return merged

def extract_pdf_nb(file_path: Path, enable_ocr: bool) -> Dict[str, Any]:
    source_name = file_path.name
    warnings = []
    try:
        import pypdf
        reader = pypdf.PdfReader(str(file_path))
        if reader.is_encrypted:
            return make_failed_json(source_name, "ENCRYPTED_PDF", "PDF is encrypted/password-protected.")
    except Exception as e:
        err_str = str(e).lower()
        if "encrypted" in err_str or "password" in err_str:
            return make_failed_json(source_name, "ENCRYPTED_PDF", str(e))
        return make_failed_json(source_name, "CORRUPT_FILE", f"PDF header parse failed: {e}")

    try:
        import pdfplumber
        pages_output, elements_output, tables_output, figures_output = [], [], [], []
        pages_with_text, pages_without_text = 0, 0

        with pdfplumber.open(str(file_path)) as pdf:
            actual_page_count = len(pdf.pages)
            for page_idx, page in enumerate(pdf.pages):
                page_num = page_idx + 1
                width = float(page.width) if page.width else None
                height = float(page.height) if page.height else None
                lines = []
                element_idx = 0
                is_scanned = False

                text = page.extract_text() or ""
                words = page.extract_words()
                if words:
                    y_grouped = {}
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
                                "bbox": [round(float(x0), 2), round(float(top), 2), round(float(x1), 2), round(float(bottom), 2)],
                                "confidence": 1.0,
                            })

                if not text.strip():
                    pages_without_text += 1
                    warnings.append(f"Page {page_num}: no_text_layer_no_ocr")
                else:
                    pages_with_text += 1

                pages_output.append({
                    "page_number": page_num,
                    "width": width,
                    "height": height,
                    "is_scanned": is_scanned,
                    "lines": lines,
                })

                # Extract tables
                try:
                    for t_idx, dt in enumerate(page.find_tables()):
                        ext_tbl = dt.extract()
                        if not ext_tbl:
                            continue
                        cells = []
                        r_count = len(ext_tbl)
                        c_count = max(len(r) for r in ext_tbl) if r_count > 0 else 0
                        for r_idx, row in enumerate(ext_tbl):
                            for c_idx, c_val in enumerate(row):
                                cells.append({
                                    "row_index": r_idx, "column_index": c_idx,
                                    "row_span": 1, "column_span": 1,
                                    "text": str(c_val or "").strip()
                                })
                        t_id = generate_table_id(page_num, t_idx, source_name)
                        tables_output.append({
                            "table_id": t_id, "start_page": page_num, "end_page": page_num,
                            "is_cross_page": False, "row_count": r_count, "column_count": c_count,
                            "caption": None, "cells": cells
                        })
                        elements_output.append({
                            "page_number": page_num, "element_index": element_idx, "type": "table",
                            "text": f"Table with {r_count} rows, {c_count} cols", "bbox": None, "confidence": 1.0
                        })
                        element_idx += 1
                except Exception as te:
                    warnings.append(f"Page {page_num} table error: {te}")

                # Elements from lines
                for l in lines:
                    is_h = len(l["text"]) < 80 and (l["text"].isupper() or l["text"].lower().startswith("section"))
                    elements_output.append({
                        "page_number": page_num, "element_index": element_idx,
                        "type": "heading" if is_h else "text", "text": l["text"],
                        "bbox": l["bbox"], "confidence": l.get("confidence", 1.0)
                    })
                    element_idx += 1

        tables_output = detect_and_merge_cross_page_tables(tables_output, warnings)
        elements_output.sort(key=lambda e: (e["page_number"], e["element_index"]))

        return {
            "status": "SUCCESS", "source_file": source_name, "engine": "local", "model_version": "local-v1",
            "extracted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "page_count": actual_page_count, "pages": pages_output, "elements": elements_output,
            "tables": tables_output, "figures": figures_output,
            "extraction_metadata": {
                "page_count_in_source": actual_page_count, "pages_with_text": pages_with_text,
                "pages_without_text": pages_without_text, "tables_detected": len(tables_output),
                "figures_detected": len(figures_output), "warnings": warnings
            }
        }
    except Exception as e:
        return make_failed_json(source_name, "CORRUPT_FILE", f"PDF parse exception: {e}")

def extract_excel_nb(file_path: Path) -> Dict[str, Any]:
    source_name = file_path.name
    try:
        import openpyxl
        wb = openpyxl.load_workbook(str(file_path), data_only=True)
    except Exception as e:
        err = str(e).lower()
        if "password" in err or "encrypted" in err:
            return make_failed_json(source_name, "ENCRYPTED_FILE", str(e))
        return make_failed_json(source_name, "CORRUPT_FILE", f"Excel load error: {e}")

    tables, elements, lines = [], [], []
    el_idx = 0
    for s_idx, s_name in enumerate(wb.sheetnames):
        sheet = wb[s_name]
        t_id = generate_table_id(1, s_idx, source_name)
        cells = []
        max_r, max_c = sheet.max_row or 0, sheet.max_column or 0
        for r in range(max_r):
            row_vals = []
            for c in range(max_c):
                val = sheet.cell(row=r+1, column=c+1).value
                txt = str(val).strip() if val is not None else ""
                row_vals.append(txt)
                cells.append({"row_index": r, "column_index": c, "row_span": 1, "column_span": 1, "text": txt})
            if any(row_vals):
                row_summary = " | ".join(filter(bool, row_vals))
                lines.append({"text": row_summary, "bbox": None, "confidence": 1.0})
                elements.append({
                    "page_number": 1, "element_index": el_idx, "type": "table",
                    "text": f"[{s_name}] {row_summary}", "bbox": None, "confidence": 1.0
                })
                el_idx += 1
        tables.append({
            "table_id": t_id, "start_page": 1, "end_page": 1, "is_cross_page": False,
            "row_count": max_r, "column_count": max_c, "caption": f"Sheet: {s_name}", "cells": cells
        })

    return {
        "status": "SUCCESS", "source_file": source_name, "engine": "local", "model_version": "local-v1",
        "extracted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "page_count": 1, "pages": [{"page_number": 1, "width": None, "height": None, "is_scanned": False, "lines": lines}],
        "elements": elements, "tables": tables, "figures": [],
        "extraction_metadata": {
            "page_count_in_source": 1, "pages_with_text": 1 if lines else 0, "pages_without_text": 0 if lines else 1,
            "tables_detected": len(tables), "figures_detected": 0, "warnings": []
        }
    }

def extract_docx_nb(file_path: Path) -> Dict[str, Any]:
    source_name = file_path.name
    try:
        import docx
        doc = docx.Document(str(file_path))
    except Exception as e:
        return make_failed_json(source_name, "CORRUPT_FILE", f"DOCX error: {e}")

    elements, lines, tables = [], [], []
    el_idx = 0
    for p in doc.paragraphs:
        txt = p.text.strip()
        if not txt:
            continue
        is_h = (p.style.name or "").lower().startswith("heading")
        lines.append({"text": txt, "bbox": None, "confidence": 1.0})
        elements.append({
            "page_number": 1, "element_index": el_idx, "type": "heading" if is_h else "text",
            "text": txt, "bbox": None, "confidence": 1.0
        })
        el_idx += 1

    for t_idx, tbl in enumerate(doc.tables):
        t_id = generate_table_id(1, t_idx, source_name)
        cells = []
        for r_idx, row in enumerate(tbl.rows):
            for c_idx, cell in enumerate(row.cells):
                cells.append({
                    "row_index": r_idx, "column_index": c_idx, "row_span": 1, "column_span": 1,
                    "text": cell.text.strip()
                })
        tables.append({
            "table_id": t_id, "start_page": 1, "end_page": 1, "is_cross_page": False,
            "row_count": len(tbl.rows), "column_count": len(tbl.columns), "caption": None, "cells": cells
        })

    return {
        "status": "SUCCESS", "source_file": source_name, "engine": "local", "model_version": "local-v1",
        "extracted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "page_count": 1, "pages": [{"page_number": 1, "width": None, "height": None, "is_scanned": False, "lines": lines}],
        "elements": elements, "tables": tables, "figures": [],
        "extraction_metadata": {
            "page_count_in_source": 1, "pages_with_text": 1 if lines else 0, "pages_without_text": 0 if lines else 1,
            "tables_detected": len(tables), "figures_detected": 0, "warnings": ["pagination not reliable for DOCX"]
        }
    }

def extract_image_nb(file_path: Path, enable_ocr: bool) -> Dict[str, Any]:
    source_name = file_path.name
    warnings = []
    try:
        from PIL import Image
        with Image.open(file_path) as img:
            width, height = img.size
    except Exception as e:
        return make_failed_json(source_name, "CORRUPT_FILE", f"Invalid or corrupt image: {e}")

    lines, elements = [], []
    has_text = False

    if enable_ocr:
        try:
            import pytesseract
            with Image.open(file_path) as img:
                ocr_data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
            n_boxes = len(ocr_data["text"])
            el_idx = 0
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
                        "page_number": 1, "element_index": el_idx, "type": "text",
                        "text": txt, "bbox": bbox, "confidence": conf
                    })
                    el_idx += 1
        except Exception as ocr_err:
            warnings.append(f"Image OCR failed: {ocr_err}")

    if not elements:
        elements.append({
            "page_number": 1, "element_index": 0, "type": "figure",
            "text": "Image file (OCR unavailable or disabled)",
            "bbox": [0.0, 0.0, float(width), float(height)], "confidence": 1.0
        })
        warnings.append("OCR was unavailable or disabled; returned image metadata only.")

    return {
        "status": "SUCCESS", "source_file": source_name, "engine": "local", "model_version": "local-v1",
        "extracted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "page_count": 1,
        "pages": [{"page_number": 1, "width": float(width), "height": float(height), "is_scanned": True, "lines": lines}],
        "elements": elements, "tables": [],
        "figures": [{"page_number": 1, "bbox": [0.0, 0.0, float(width), float(height)], "caption": None, "image_ref": source_name}],
        "extraction_metadata": {
            "page_count_in_source": 1, "pages_with_text": 1 if has_text else 0, "pages_without_text": 0 if has_text else 1,
            "tables_detected": 0, "figures_detected": 1, "warnings": warnings
        }
    }

# COMMAND ----------

# === CELL 5: BATCH PROCESSING RUNNER ===
in_path = Path(INPUT_PATH)
out_path = Path(OUTPUT_PATH)
out_path.mkdir(parents=True, exist_ok=True)
log_file_path = out_path / "extraction_log.jsonl"

all_files = [p for p in in_path.iterdir() if p.is_file()] if in_path.exists() else []
print(f"Found {len(all_files)} files to extract.")

success_count, fail_count = 0, 0

with open(log_file_path, "a", encoding="utf-8") as log_f:
    for f in all_files:
        start_t = time.time()
        out_f = out_path / f"{f.stem}.json"
        
        # Idempotency
        if not OVERWRITE and out_f.exists() and f.stat().st_mtime < out_f.stat().st_mtime:
            continue
            
        # Size checks
        size = f.stat().st_size
        if size == 0:
            res = make_failed_json(f.name, "EMPTY_FILE", "Zero bytes")
        elif size > MAX_FILE_SIZE_MB * 1024 * 1024:
            res = make_failed_json(f.name, "CORRUPT_FILE", "Exceeds max file size limit")
        else:
            ext = f.suffix.lower()
            if ext == ".pdf":
                res = extract_pdf_nb(f, ENABLE_OCR)
            elif ext in [".xlsx", ".xls"]:
                res = extract_excel_nb(f)
            elif ext == ".docx":
                res = extract_docx_nb(f)
            elif ext in [".png", ".jpg", ".jpeg", ".tif", ".tiff"]:
                res = extract_image_nb(f, ENABLE_OCR)
            elif ext == ".json":
                try:
                    with open(f, "r", encoding="utf-8") as jf:
                        res = json.load(jf)
                except Exception as je:
                    res = make_failed_json(f.name, "CORRUPT_FILE", str(je))
            else:
                res = make_failed_json(f.name, "UNSUPPORTED_FORMAT", f"Unsupported extension {ext}")

        with open(out_f, "w", encoding="utf-8") as j_out:
            json.dump(res, j_out, indent=2)

        dur_ms = int((time.time() - start_t) * 1000)
        status = res.get("status", "SUCCESS")
        if status == "SUCCESS":
            success_count += 1
        else:
            fail_count += 1

        log_entry = {
            "source_file": f.name,
            "output_file": str(out_f),
            "status": status,
            "duration_ms": dur_ms,
            "error_class": res.get("error_class"),
            "warnings": res.get("extraction_metadata", {}).get("warnings", [])
        }
        log_f.write(json.dumps(log_entry) + "\n")

print(f"Batch completed: {success_count} succeeded, {fail_count} failed.")

# COMMAND ----------

# === CELL 6: INSPECT LOGS & SAMPLES ===
import pandas as pd

if log_file_path.exists():
    logs = [json.loads(line) for line in open(log_file_path, "r", encoding="utf-8")]
    df_logs = pd.DataFrame(logs)
    display(df_logs)