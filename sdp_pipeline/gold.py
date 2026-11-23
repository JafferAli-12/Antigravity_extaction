"""
Gold layer: consumer projections.

Ported from `databricks_part_b` cell 15. The notebook's MERGE-into-gold
statements become materialized views that recompute incrementally on
serverless pipelines.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_extracted_table_view",
    comment="Flat projection of stitched tables and their cells (highest-confidence cell per position, earliest table definition).",
)
def gold_extracted_table_view():
    cells = spark.read.table("silver_table_cells")
    tables = spark.read.table("silver_tables")

    cells_dedup = (
        cells.groupBy("doc_id", "table_id", "row_index", "column_index")
        .agg(F.max(F.struct(
            F.col("confidence"), F.col("row_span"), F.col("column_span"), F.col("text"),
        )).alias("m"))
        .select(
            "doc_id", "table_id", "row_index", "column_index",
            F.col("m.confidence").alias("confidence"),
            F.col("m.row_span").alias("row_span"),
            F.col("m.column_span").alias("column_span"),
            F.col("m.text").alias("text"),
        )
    )

    tables_dedup = (
        tables.groupBy("doc_id", "table_id")
        .agg(F.min(F.struct(F.col("start_page"), F.col("end_page"))).alias("m"))
        .select(
            "doc_id", "table_id",
            F.col("m.start_page").alias("start_page"),
            F.col("m.end_page").alias("end_page"),
        )
    )

    return (
        cells_dedup.join(tables_dedup, ["doc_id", "table_id"], "inner")
        .select(
            "doc_id", "table_id", "start_page", "end_page",
            "row_index", "column_index", "row_span", "column_span",
            "text", "confidence",
        )
    )


@dp.materialized_view(
    name="gold_document_summary",
    comment="One row per document: engine, page/table counts, failed pages, evaluation and processing status.",
)
def gold_document_summary():
    docs = spark.read.table("bronze_documents")

    pages = (
        spark.read.table("silver_pages")
        .groupBy("doc_id")
        .agg(
            F.count(F.lit(1)).alias("page_cnt"),
            F.sum(F.when(F.col("is_scanned"), F.lit(1)).otherwise(F.lit(0))).alias("scanned_cnt"),
        )
    )

    tables = (
        spark.read.table("silver_tables")
        .groupBy("doc_id")
        .agg(F.countDistinct("table_id").alias("tbl_cnt"))
    )

    quarantined = (
        spark.read.table("bronze_quarantine")
        .select("doc_id")
        .distinct()
        .withColumn("is_quarantined", F.lit(True))
    )

    return (
        docs
        .join(pages, "doc_id", "left")
        .join(tables, "doc_id", "left")
        .join(quarantined, "doc_id", "left")
        .select(
            "doc_id", "engine", "model_version",
            F.coalesce("page_cnt", F.lit(0)).cast("int").alias("page_count"),
            F.coalesce("tbl_cnt", F.lit(0)).cast("int").alias("table_count"),
            F.coalesce("scanned_cnt", F.lit(0)).cast("int").alias("failed_page_count"),
            F.when(F.col("is_quarantined"), F.lit("FAILED")).otherwise(F.lit("PASSED")).alias("evaluation_status"),
            F.lit("COMPLETED").alias("processing_status"),
            F.current_timestamp().alias("last_updated"),
        )
    )
