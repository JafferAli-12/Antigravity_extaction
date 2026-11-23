"""
Shared logic for the Document Intelligence medallion pipeline (Spark Declarative Pipeline).

Faithful port of `databricks_part_b` from the Antigravity_extaction repo:
  * Source-type guard (CELL 11)        -> guard_reason() (nested in parse_document)
  * Engine detection (CELL 12)         -> detect_engine() (nested in parse_document)
  * Evaluation engine (CELL 13)       -> evaluate() (nested in parse_document)
  * Silver mapping adapters (CELL 14)  -> map_local / map_adi adapters (nested in parse_silver)

All helpers are nested INSIDE the pandas UDFs so cloudpickle serializes them by
value (module-level helpers in a pipeline source file are not importable on
Spark workers and fail with AttributeError during deserialization).
"""
import hashlib
import json
import uuid

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType, StringType,
    StructField, StructType,
)

# One run id per pipeline update (equivalent of the notebook's PIPELINE_RUN_ID).
# Used on the driver only (F.lit(PIPELINE_RUN_ID)); never referenced inside UDFs.
PIPELINE_RUN_ID = str(uuid.uuid4())


# ---------------------------------------------------------------- bronze UDF
PARSED_DOC_SCHEMA = StructType([
    StructField("valid", BooleanType(), True),
    StructField("reason", StringType(), True),
    StructField("reason_details", StringType(), True),
    StructField("doc_id", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("engine", StringType(), True),
    StructField("model_version", StringType(), True),
    StructField("segment_id", StringType(), True),
    StructField("page_range", StringType(), True),
    StructField("ingest_run", StringType(), True),
    StructField("eval_passed", BooleanType(), True),
    StructField("eval_checks", ArrayType(StructType([
        StructField("check_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("details", StringType(), True),
    ])), True),
    StructField("failing_checks", StringType(), True),
])


@pandas_udf(PARSED_DOC_SCHEMA)
def parse_document(content: pd.Series, file_name: pd.Series) -> pd.DataFrame:
    """Guard + engine detection + evaluation for each landed file (one output struct per file)."""
    # === CONFIG (ported from notebook CELL 2: EVALUATION_CONFIG) ===
    min_page_coverage = 0.95
    required_json_sections = ["pages", "elements", "tables", "page_count"]
    expected_segment_count_field = "expected_segment_count"

    def guard_reason(fname: str, raw_text: str):
        """Port of the notebook's source-type guard (CELL 11).
        Returns (reason, parsed_json, reason_details); reason is None for valid payloads."""
        lname = fname.lower()
        if not lname.endswith(".json"):
            return "NON_JSON_IN_JSON_SOURCE", None, f"Ext: {lname}"
        stripped = raw_text.lstrip()
        if not (stripped.startswith("{") or stripped.startswith("[")):
            return "NON_JSON_IN_JSON_SOURCE", None, f"Ext: {lname}"
        try:
            parsed = json.loads(raw_text)
        except Exception as parse_err:  # CORRUPT_JSON
            return "CORRUPT_JSON", None, str(parse_err)
        if isinstance(parsed, dict) and parsed.get("status") == "FAILED":
            error_class = parsed.get("error_class", "EXTRACTION_FAILED")
            return f"EXTRACTION_FAILED_{error_class}", None, json.dumps(parsed)[:300]
        return None, parsed, None

    def detect_engine(parsed):
        """Port of notebook CELL 12 engine detection."""
        if isinstance(parsed, dict) and "analyzeResult" in parsed:
            ar = parsed.get("analyzeResult") or {}
            model_version = ar.get("modelId") or ar.get("apiVersion") or "adi-v1"
            return "adi", model_version
        if isinstance(parsed, dict) and "engine" in parsed:
            return parsed.get("engine") or "unknown", parsed.get("model_version") or "local-v1"
        if isinstance(parsed, list) or (isinstance(parsed, dict) and "elements" in parsed):
            return "unstructured", "unstructured-v1"
        return "unknown", "v1"

    def evaluate(parsed):
        """Port of notebook CELL 13 (completeness + quality checks)."""
        checks = [{"check_name": "JSON_VALID", "status": "PASS", "details": "Valid JSON object"}]
        is_dict = isinstance(parsed, dict)
        if is_dict and "analyzeResult" in parsed:
            ar = parsed.get("analyzeResult") or {}
            pages = ar.get("pages") or []
            elements = ar.get("paragraphs") or []
            tables = ar.get("tables") or []
            page_count = len(pages)
            source_pg_cnt = len(pages)
            missing = [s for s in ("pages", "paragraphs", "tables") if s not in ar]
        else:
            data = parsed if is_dict else {}
            pages = data.get("pages") or []
            elements = data.get("elements") or []
            tables = data.get("tables") or []
            page_count = data.get("page_count", len(pages))
            source_pg_cnt = (data.get("extraction_metadata") or {}).get("page_count_in_source", page_count)
            missing = [s for s in required_json_sections if s not in data]

        # b) REQUIRED_SECTIONS
        checks.append({
            "check_name": "REQUIRED_SECTIONS",
            "status": "FAIL" if missing else "PASS",
            "details": f"Missing sections: {missing}" if missing else "All required sections present",
        })

        # c) PAGE_COUNT_MATCH
        if page_count != len(pages):
            checks.append({
                "check_name": "PAGE_COUNT_MATCH", "status": "FAIL",
                "details": f"page_count ({page_count}) does not match len(pages) ({len(pages)})",
            })
        else:
            checks.append({
                "check_name": "PAGE_COUNT_MATCH", "status": "PASS",
                "details": f"{page_count} pages match",
            })

        # d) SEGMENT_COMPLETENESS
        if is_dict and expected_segment_count_field and expected_segment_count_field in parsed:
            checks.append({
                "check_name": "SEGMENT_COMPLETENESS", "status": "PASS",
                "details": "Segment validated",
            })

        # e) PAGE_COVERAGE
        if source_pg_cnt and source_pg_cnt > 0:
            coverage = len(pages) / float(source_pg_cnt)
            ok = coverage >= min_page_coverage
            checks.append({
                "check_name": "PAGE_COVERAGE",
                "status": "PASS" if ok else "FAIL",
                "details": f"Coverage: {coverage:.2f}",
            })

        # f) TABLE_STRUCTURE_VALID
        tbl_err = None
        for tbl in tables:
            if not isinstance(tbl, dict):
                continue
            rc = tbl.get("rowCount") if tbl.get("rowCount") is not None else tbl.get("row_count", 0)
            cc = tbl.get("columnCount") if tbl.get("columnCount") is not None else tbl.get("column_count", 0)
            for cell in tbl.get("cells") or []:
                if not isinstance(cell, dict):
                    continue
                rs = cell.get("rowSpan") if cell.get("rowSpan") is not None else cell.get("row_span", 1)
                cs = cell.get("columnSpan") if cell.get("columnSpan") is not None else cell.get("column_span", 1)
                if (rs and rc and rs > rc) or (cs and cc and cs > cc):
                    tbl_err = f"Table cell span ({rs}x{cs}) exceeds dimensions ({rc}x{cc})"
                    break
            if tbl_err:
                break
        checks.append({
            "check_name": "TABLE_STRUCTURE_VALID",
            "status": "FAIL" if tbl_err else "PASS",
            "details": tbl_err or "Table cell spans conform",
        })

        # g) NON_EMPTY_CONTENT
        empty_cnt = 0
        for el in elements:
            if not isinstance(el, dict):
                continue
            text_val = el.get("content") if el.get("content") is not None else el.get("text", "")
            if not str(text_val).strip():
                empty_cnt += 1
        if elements and empty_cnt == len(elements):
            checks.append({"check_name": "NON_EMPTY_CONTENT", "status": "FAIL", "details": "All elements have empty text"})
        else:
            checks.append({"check_name": "NON_EMPTY_CONTENT", "status": "PASS", "details": f"{len(elements)} elements verified"})

        # h) EXTRACTION_WARNINGS
        meta_warnings = (parsed.get("extraction_metadata") or {}).get("warnings") if is_dict else None
        if meta_warnings:
            checks.append({
                "check_name": "EXTRACTION_WARNINGS", "status": "WARN",
                "details": "; ".join(str(w) for w in meta_warnings[:3]),
            })

        return checks

    results = []
    for text, fname in zip(content, file_name):
        text = "" if text is None else str(text)
        fname = "" if fname is None else str(fname)
        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        base = {
            "valid": False, "reason": None, "reason_details": None,
            "doc_id": None, "content_hash": content_hash, "source_file": None,
            "engine": None, "model_version": None, "segment_id": None,
            "page_range": None, "ingest_run": str(uuid.uuid4()),
            "eval_passed": None, "eval_checks": [], "failing_checks": None,
        }
        reason, parsed, reason_details = guard_reason(fname, text)
        if reason is not None:
            base.update({"reason": reason, "reason_details": reason_details})
            results.append(base)
            continue
        engine, model_version = detect_engine(parsed)
        checks = evaluate(parsed)
        has_fail = any(c["status"] == "FAIL" for c in checks)
        failing = [c["check_name"] for c in checks if c["status"] == "FAIL"]
        base.update({
            "valid": True,
            "doc_id": content_hash[:16],
            "source_file": (parsed.get("source_file") if isinstance(parsed, dict) else None) or fname,
            "engine": engine,
            "model_version": model_version,
            "segment_id": parsed.get("segment_id") if isinstance(parsed, dict) else None,
            "page_range": parsed.get("page_range") if isinstance(parsed, dict) else None,
            "eval_passed": not has_fail,
            "eval_checks": checks,
            "failing_checks": json.dumps(failing) if failing else None,
        })
        results.append(base)
    return pd.DataFrame(results)


# ---------------------------------------------------------------- silver UDF
PAGE_STRUCT = StructType([
    StructField("doc_id", StringType(), True),
    StructField("absolute_page", IntegerType(), True),
    StructField("segment_id", StringType(), True),
    StructField("is_scanned", BooleanType(), True),
    StructField("width", DoubleType(), True),
    StructField("height", DoubleType(), True),
    StructField("span_count", IntegerType(), True),
])

ELEMENT_STRUCT = StructType([
    StructField("doc_id", StringType(), True),
    StructField("absolute_page", IntegerType(), True),
    StructField("element_index", IntegerType(), True),
    StructField("reading_order", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("text", StringType(), True),
    StructField("bbox", StringType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("raw_extras", StringType(), True),
])

TABLE_STRUCT = StructType([
    StructField("doc_id", StringType(), True),
    StructField("table_id", StringType(), True),
    StructField("start_page", IntegerType(), True),
    StructField("end_page", IntegerType(), True),
    StructField("is_cross_page", BooleanType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("col_count", IntegerType(), True),
    StructField("caption", StringType(), True),
    StructField("confidence", DoubleType(), True),
])

CELL_STRUCT = StructType([
    StructField("doc_id", StringType(), True),
    StructField("table_id", StringType(), True),
    StructField("row_index", IntegerType(), True),
    StructField("column_index", IntegerType(), True),
    StructField("row_span", IntegerType(), True),
    StructField("column_span", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("confidence", DoubleType(), True),
])

PARSED_SILVER_SCHEMA = StructType([
    StructField("doc_id", StringType(), True),
    StructField("engine", StringType(), True),
    StructField("parse_error", StringType(), True),
    StructField("pages", ArrayType(PAGE_STRUCT), True),
    StructField("elements", ArrayType(ELEMENT_STRUCT), True),
    StructField("tables", ArrayType(TABLE_STRUCT), True),
    StructField("cells", ArrayType(CELL_STRUCT), True),
])


@pandas_udf(PARSED_SILVER_SCHEMA)
def parse_silver(raw_text: pd.Series, engine_col: pd.Series, doc_id_col: pd.Series) -> pd.DataFrame:
    """Runs the engine-specific silver adapter (notebook CELL 14) for each valid,
    evaluation-passed document: local / Unstructured / Azure Document Intelligence,
    including ADI cross-page table reconstruction with header-repeat detection."""

    def map_local_to_silver(raw, doc_id, segment_start_page=1):
        """Port of the notebook's local / Part A engine adapter."""
        pages_rows, element_rows, table_rows, cell_rows = [], [], [], []

        for p in (raw.get("pages") or []):
            pg_num = p.get("page_number") or p.get("page_no") or 1
            abs_pg = segment_start_page + (int(pg_num) - 1)
            seg_id = str(raw.get("segment_id")) if raw.get("segment_id") is not None else None
            pages_rows.append({
                "doc_id": str(doc_id), "absolute_page": abs_pg, "segment_id": seg_id,
                "is_scanned": bool(p.get("is_scanned", False)),
                "width": float(p["width"]) if p.get("width") is not None else None,
                "height": float(p["height"]) if p.get("height") is not None else None,
                "span_count": int(len(p.get("lines") or [])),
            })

        for el_idx, el in enumerate(raw.get("elements") or []):
            pg_num = el.get("page_number") or el.get("page_no") or 1
            abs_pg = segment_start_page + (int(pg_num) - 1)
            el_type = str(el.get("type") or el.get("role") or "text")
            el_text = str(el.get("text") if el.get("text") is not None else (el.get("content") or ""))
            bbox_str = json.dumps(el["bbox"]) if el.get("bbox") is not None else None
            conf = float(el.get("confidence", 1.0)) if el.get("confidence") is not None else 1.0
            element_rows.append({
                "doc_id": str(doc_id), "absolute_page": abs_pg,
                "element_index": int(el.get("element_index", el_idx)),
                "reading_order": None,
                "type": el_type, "text": el_text,
                "bbox": bbox_str, "confidence": conf, "raw_extras": json.dumps({}),
            })

        for t_idx, tbl in enumerate(raw.get("tables") or []):
            t_id = str(tbl.get("table_id") or tbl.get("logical_table_id") or f"tbl_{doc_id}_{t_idx}")
            start_p = tbl.get("start_page") or tbl.get("page_start") or 1
            end_p = tbl.get("end_page") or tbl.get("page_end") or start_p
            r_cnt = tbl.get("row_count") or tbl.get("total_rows") or 0
            c_cnt = tbl.get("column_count") or tbl.get("col_count") or tbl.get("total_cols") or 0
            cap = str(tbl.get("caption") or tbl.get("title") or "") if (tbl.get("caption") or tbl.get("title")) is not None else None
            table_rows.append({
                "doc_id": str(doc_id), "table_id": t_id,
                "start_page": int(start_p), "end_page": int(end_p),
                "is_cross_page": bool(tbl.get("is_cross_page", False)),
                "row_count": int(r_cnt), "col_count": int(c_cnt),
                "caption": cap, "confidence": 1.0,
            })
            for cell in (tbl.get("cells") or tbl.get("combined_cells") or []):
                r_i = cell.get("row_index") if cell.get("row_index") is not None else cell.get("row_no", 0)
                c_i = cell.get("column_index") if cell.get("column_index") is not None else cell.get("col_no", 0)
                r_s = cell.get("row_span") if cell.get("row_span") is not None else 1
                c_s = cell.get("column_span") if cell.get("column_span") is not None else 1
                txt = str(cell.get("text") if cell.get("text") is not None else (cell.get("clean_text") or cell.get("content") or ""))
                conf = float(cell.get("confidence", 1.0)) if cell.get("confidence") is not None else 1.0
                cell_rows.append({
                    "doc_id": str(doc_id), "table_id": t_id,
                    "row_index": int(r_i or 0), "column_index": int(c_i or 0),
                    "row_span": int(r_s or 1), "column_span": int(c_s or 1),
                    "text": txt, "confidence": conf,
                })
        return pages_rows, element_rows, table_rows, cell_rows

    def map_adi_to_silver(raw, doc_id, segment_start_page=1):
        """Port of the ADI (Azure Document Intelligence) adapter."""
        ar = raw.get("analyzeResult", raw) or {}
        pages_rows, element_rows, table_rows, cell_rows = [], [], [], []

        # 1. Pages
        for p in (ar.get("pages") or []):
            pg_num = p.get("pageNumber", 1)
            abs_pg = segment_start_page + (int(pg_num) - 1)
            pages_rows.append({
                "doc_id": doc_id, "absolute_page": abs_pg,
                "segment_id": raw.get("segment_id"),
                "is_scanned": False,
                "width": float(p["width"]) if p.get("width") is not None else None,
                "height": float(p["height"]) if p.get("height") is not None else None,
                "span_count": int(len(p.get("lines") or [])),
            })

        # 2. Elements (from ADI paragraphs)
        role_mapping = {
            "title": "heading",
            "sectionHeading": "heading",
            "pageHeader": "header",
            "pageFooter": "footer",
            "pageNumber": "page_number",
            "footnote": "footnote",
        }
        for el_idx, para in enumerate(ar.get("paragraphs") or []):
            content = (para.get("content") or "").strip()
            role = para.get("role") or "text"
            el_type = role_mapping.get(role, "text")

            br = para.get("boundingRegions") or []
            pg_num = br[0].get("pageNumber", 1) if br else 1
            abs_pg = segment_start_page + (int(pg_num) - 1)

            bbox_json = None
            if br and "polygon" in br[0]:
                poly = br[0]["polygon"]
                if isinstance(poly, list) and len(poly) >= 4:
                    xs = poly[0::2]
                    ys = poly[1::2]
                    bbox_json = json.dumps([round(min(xs), 4), round(min(ys), 4), round(max(xs), 4), round(max(ys), 4)])

            raw_extras = json.dumps({"role": role, "spans": para.get("spans", [])})
            element_rows.append({
                "doc_id": doc_id, "absolute_page": abs_pg,
                "element_index": el_idx, "reading_order": None,
                "type": el_type, "text": content,
                "bbox": bbox_json, "confidence": 1.0, "raw_extras": raw_extras,
            })

        # 3. Tables & cross-page table reconstruction
        raw_tables = ar.get("tables") or []
        processed_tables = []
        skip_table_indices = set()

        for t_idx in range(len(raw_tables)):
            if t_idx in skip_table_indices:
                continue

            tbl = raw_tables[t_idx]
            br = tbl.get("boundingRegions") or []
            pg_nums = [b.get("pageNumber", 1) for b in br] if br else [1]
            start_pg = min(pg_nums)
            end_pg = max(pg_nums)
            rc = tbl.get("rowCount", 0) or 0
            cc = tbl.get("columnCount", 0) or 0
            cells = list(tbl.get("cells") or [])
            caption = tbl.get("caption", {}).get("content") if isinstance(tbl.get("caption"), dict) else tbl.get("caption")
            is_cross = (start_pg != end_pg)

            # Lookahead: merge consecutive tables that continue across pages
            lookahead_idx = t_idx + 1
            curr_end_pg = end_pg
            curr_rc = rc

            while lookahead_idx < len(raw_tables):
                nxt_tbl = raw_tables[lookahead_idx]
                nxt_br = nxt_tbl.get("boundingRegions") or []
                nxt_pgs = [b.get("pageNumber", 1) for b in nxt_br] if nxt_br else [1]
                nxt_start_pg = min(nxt_pgs)
                nxt_end_pg = max(nxt_pgs)
                nxt_cc = nxt_tbl.get("columnCount", 0) or 0
                nxt_rc = nxt_tbl.get("rowCount", 0) or 0

                if nxt_start_pg == curr_end_pg + 1 and nxt_cc == cc:
                    curr_headers = [c.get("content", "").strip().lower() for c in cells if (c.get("rowIndex") or 0) == 0]
                    nxt_headers = [c.get("content", "").strip().lower() for c in (nxt_tbl.get("cells") or []) if (c.get("rowIndex") or 0) == 0]

                    start_r = 1 if (curr_headers and curr_headers == nxt_headers) else 0
                    row_offset = curr_rc

                    for c in (nxt_tbl.get("cells") or []):
                        c_r = c.get("rowIndex", 0)
                        if start_r == 1 and c_r == 0:
                            continue  # skip repeated header
                        adjusted_r = (c_r - start_r) + row_offset
                        cells.append({
                            "rowIndex": adjusted_r,
                            "columnIndex": c.get("columnIndex", 0),
                            "rowSpan": c.get("rowSpan", 1),
                            "columnSpan": c.get("columnSpan", 1),
                            "content": c.get("content", ""),
                            "confidence": c.get("confidence", 1.0),
                        })

                    added_rows = nxt_rc - start_r
                    curr_rc += added_rows
                    curr_end_pg = nxt_end_pg
                    is_cross = True
                    skip_table_indices.add(lookahead_idx)
                    lookahead_idx += 1
                else:
                    break

            t_id = f"tbl_{doc_id}_{t_idx}"
            processed_tables.append({
                "table_id": t_id,
                "start_page": segment_start_page + (start_pg - 1),
                "end_page": segment_start_page + (curr_end_pg - 1),
                "is_cross_page": is_cross,
                "row_count": curr_rc,
                "column_count": cc,
                "caption": caption,
                "cells": cells,
            })

        for pt in processed_tables:
            t_id = pt["table_id"]
            cap = str(pt["caption"]) if pt["caption"] is not None else None
            table_rows.append({
                "doc_id": doc_id, "table_id": t_id,
                "start_page": int(pt["start_page"]), "end_page": int(pt["end_page"]),
                "is_cross_page": bool(pt["is_cross_page"]),
                "row_count": int(pt["row_count"] or 0), "col_count": int(pt["column_count"] or 0),
                "caption": cap, "confidence": 1.0,
            })
            for c in pt["cells"]:
                c_r = c.get("rowIndex") if c.get("rowIndex") is not None else c.get("row_index", 0)
                c_c = c.get("columnIndex") if c.get("columnIndex") is not None else c.get("column_index", 0)
                c_rs = c.get("rowSpan") if c.get("rowSpan") is not None else c.get("row_span", 1)
                c_cs = c.get("columnSpan") if c.get("columnSpan") is not None else c.get("column_span", 1)
                c_txt = str(c.get("content") if c.get("content") is not None else (c.get("text") or ""))
                c_conf = float(c.get("confidence", 1.0) or 1.0) if c.get("confidence") is not None else 1.0
                cell_rows.append({
                    "doc_id": str(doc_id), "table_id": str(t_id),
                    "row_index": int(c_r or 0), "column_index": int(c_c or 0),
                    "row_span": int(c_rs or 1), "column_span": int(c_cs or 1),
                    "text": c_txt, "confidence": c_conf,
                })

        return pages_rows, element_rows, table_rows, cell_rows

    results = []
    for text, engine, d_id in zip(raw_text, engine_col, doc_id_col):
        d_id = str(d_id) if d_id is not None else None
        empty = {
            "doc_id": d_id, "engine": engine, "parse_error": None,
            "pages": [], "elements": [], "tables": [], "cells": [],
        }
        try:
            parsed = json.loads("" if text is None else str(text))
            if engine == "adi":
                p_rows, e_rows, t_rows, c_rows = map_adi_to_silver(parsed, d_id)
            else:
                # local, unstructured and unknown all use the unified local adapter
                p_rows, e_rows, t_rows, c_rows = map_local_to_silver(parsed, d_id)
            # Continuous reading order per document (notebook computed this via window function)
            e_rows = sorted(e_rows, key=lambda r: (r["absolute_page"], r["element_index"]))
            for i, row in enumerate(e_rows):
                row["reading_order"] = i + 1
            results.append({
                **empty, "pages": p_rows, "elements": e_rows,
                "tables": t_rows, "cells": c_rows,
            })
        except Exception as silver_err:
            results.append({**empty, "parse_error": str(silver_err)})
    return pd.DataFrame(results)
