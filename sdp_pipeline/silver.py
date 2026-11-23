"""
Silver layer: engine adapters explode each valid, evaluation-passed document
into unified pages / elements / tables / table_cells tables.

Ported from `databricks_part_b` cell 14. The MERGE-into-silver logic of the
notebook becomes Auto CDC keyed exactly like the notebook's MERGE conditions,
which keeps silver idempotent under redeliveries.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from di_common import PIPELINE_RUN_ID, parse_silver


@dp.table(
    name="silver_parsed",
    comment="Per-document adapter output (pages/elements/tables/cells) before explosion.",
    private=True,
)
def silver_parsed():
    return (
        spark.readStream.table("bronze_documents_parsed")
        .filter("valid AND eval_passed")
        .select(
            "ingest_run", "file_mod_time",
            parse_silver(F.col("raw_text"), F.col("engine"), F.col("doc_id")).alias("s"),
        )
        .select("s.*", "ingest_run", "file_mod_time")
    )


# ---- silver_pages ----
@dp.temporary_view(name="silver_pages_changes")
def silver_pages_changes():
    return (
        spark.readStream.table("silver_parsed")
        .select("doc_id", "file_mod_time", F.explode("pages").alias("p"))
        .select(
            "doc_id", "p.absolute_page", "p.segment_id", "p.is_scanned",
            "p.width", "p.height", "p.span_count", "file_mod_time",
        )
    )


dp.create_streaming_table(
    name="silver_pages",
    comment="One row per document page (keyed doc_id + absolute_page via Auto CDC).",
)

dp.create_auto_cdc_flow(
    target="silver_pages",
    source="silver_pages_changes",
    keys=["doc_id", "absolute_page"],
    sequence_by="file_mod_time",
    stored_as_scd_type=1,
)


# ---- silver_elements ----
@dp.temporary_view(name="silver_elements_changes")
def silver_elements_changes():
    return (
        spark.readStream.table("silver_parsed")
        .select("doc_id", "file_mod_time", F.explode("elements").alias("e"))
        .select(
            "doc_id", "e.absolute_page", "e.element_index", "e.reading_order",
            "e.type", "e.text", "e.bbox", "e.confidence", "e.raw_extras", "file_mod_time",
        )
    )


dp.create_streaming_table(
    name="silver_elements",
    comment="One row per document element with continuous reading order (keyed doc_id + absolute_page + element_index via Auto CDC).",
)

dp.create_auto_cdc_flow(
    target="silver_elements",
    source="silver_elements_changes",
    keys=["doc_id", "absolute_page", "element_index"],
    sequence_by="file_mod_time",
    stored_as_scd_type=1,
)


# ---- silver_tables ----
@dp.temporary_view(name="silver_tables_changes")
def silver_tables_changes():
    return (
        spark.readStream.table("silver_parsed")
        .select("doc_id", "file_mod_time", F.explode("tables").alias("t"))
        .select(
            "doc_id", "t.table_id", "t.start_page", "t.end_page", "t.is_cross_page",
            "t.row_count", "t.col_count", "t.caption", "t.confidence", "file_mod_time",
        )
    )


dp.create_streaming_table(
    name="silver_tables",
    comment="One row per stitched (possibly cross-page) table (keyed doc_id + table_id via Auto CDC).",
)

dp.create_auto_cdc_flow(
    target="silver_tables",
    source="silver_tables_changes",
    keys=["doc_id", "table_id"],
    sequence_by="file_mod_time",
    stored_as_scd_type=1,
)


# ---- silver_table_cells ----
@dp.temporary_view(name="silver_table_cells_changes")
def silver_table_cells_changes():
    return (
        spark.readStream.table("silver_parsed")
        .select("doc_id", "file_mod_time", F.explode("cells").alias("c"))
        .select(
            "doc_id", "c.table_id", "c.row_index", "c.column_index",
            "c.row_span", "c.column_span", "c.text", "c.confidence", "file_mod_time",
        )
    )


dp.create_streaming_table(
    name="silver_table_cells",
    comment="One row per table cell (keyed doc_id + table_id + row_index + column_index via Auto CDC).",
)

dp.create_auto_cdc_flow(
    target="silver_table_cells",
    source="silver_table_cells_changes",
    keys=["doc_id", "table_id", "row_index", "column_index"],
    sequence_by="file_mod_time",
    stored_as_scd_type=1,
)


# ---- Silver audit events ----
@dp.append_flow(target="bronze_pipeline_events", name="events_silver")
def events_silver():
    src = spark.readStream.table("silver_parsed")
    ok = F.col("parse_error").isNull()
    return src.select(
        F.expr("uuid()").alias("event_id"),
        F.lit(PIPELINE_RUN_ID).alias("run_id"),
        F.col("doc_id"),
        F.lit("SILVER_PARSED").alias("stage"),
        F.when(ok, F.lit("SUCCESS")).otherwise(F.lit("FAILED")).alias("status"),
        F.when(ok, F.lit(None).cast("string")).otherwise(F.col("parse_error")).alias("error_message"),
        F.current_timestamp().alias("event_time"),
        F.to_json(F.create_map(F.lit("engine"), F.coalesce(F.col("engine"), F.lit("")))).alias("metadata"),
    )
