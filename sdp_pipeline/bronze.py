"""
Bronze layer: Auto Loader ingestion of engine-shaped JSON files, source-type
guard, raw payload store, document registry, evaluation results, quarantine
routing, and the audit-event log.

Ported from `databricks_part_b` cells 10-13 (source dispatcher, guard, bronze
ingestion, evaluation). Auto Loader replaces the custom source connectors and
the bronze_source_checkpoint table; Auto CDC replaces the registry /
redelivery MERGE logic.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from di_common import PIPELINE_RUN_ID, parse_document

LANDING_PATH = "/Volumes/ext_cat/doc_intel/landing"


@dp.table(
    name="bronze_landed_files",
    comment="Every file landed from the source volume (one row per file, whole-file text).",
    private=True,
)
def bronze_landed_files():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholeText", "true")
        .load(LANDING_PATH)
        .select(
            F.col("value").alias("raw_text"),
            F.col("_metadata.file_name").alias("file_name"),
            F.col("_metadata.file_path").alias("source"),
            F.col("_metadata.file_modification_time").alias("file_mod_time"),
        )
    )


@dp.table(
    name="bronze_documents_parsed",
    comment="Source guard + engine detection + evaluation for every landed document.",
    private=True,
)
def bronze_documents_parsed():
    return (
        spark.readStream.table("bronze_landed_files")
        .withColumn("parsed", parse_document(F.col("raw_text"), F.col("file_name")))
        .select("source", "file_name", "file_mod_time", "raw_text", "parsed.*")
    )


# ---- Valid documents: raw payload store (idempotent by doc_id via Auto CDC) ----
@dp.temporary_view(name="bronze_documents_changes")
def bronze_documents_changes():
    return (
        spark.readStream.table("bronze_documents_parsed")
        .filter("valid")
        .select(
            "doc_id", "content_hash", "source", "engine", "model_version",
            "segment_id", "page_range",
            F.col("raw_text").alias("raw_json"),
            F.col("file_mod_time").alias("ingested_at"),
            F.col("ingest_run").alias("run_id"),
            "eval_passed",
            "file_mod_time",
        )
    )


dp.create_streaming_table(
    name="bronze_documents",
    comment="Raw engine payload per document; redeliveries of identical content collapse onto the same doc_id (Auto CDC).",
)

dp.create_auto_cdc_flow(
    target="bronze_documents",
    source="bronze_documents_changes",
    keys=["doc_id"],
    sequence_by="file_mod_time",
    stored_as_scd_type=1,
)


# ---- Document registry (replaces the notebook's idempotency lookups) ----
@dp.temporary_view(name="bronze_document_registry_changes")
def bronze_document_registry_changes():
    return (
        spark.readStream.table("bronze_documents_parsed")
        .filter("valid")
        .select(
            "doc_id", "content_hash",
            F.col("file_mod_time").alias("first_seen"),
            F.lit("NEW").alias("status"),
            F.col("source_file").alias("logical_doc_id"),
            "file_mod_time",
        )
    )


dp.create_streaming_table(
    name="bronze_document_registry",
    comment="One registry row per document content hash (Auto CDC by doc_id).",
)

dp.create_auto_cdc_flow(
    target="bronze_document_registry",
    source="bronze_document_registry_changes",
    keys=["doc_id"],
    sequence_by="file_mod_time",
    stored_as_scd_type=1,
)


# ---- Evaluation results (one row per check) ----
@dp.table(
    name="bronze_evaluation_results",
    comment="Quality/completeness checks per document (JSON_VALID, REQUIRED_SECTIONS, PAGE_COUNT_MATCH, ...).",
)
def bronze_evaluation_results():
    return (
        spark.readStream.table("bronze_documents_parsed")
        .filter("valid")
        .select("doc_id", "ingest_run", "file_mod_time", F.explode("eval_checks").alias("chk"))
        .select(
            "doc_id",
            F.col("ingest_run").alias("run_id"),
            F.col("chk.check_name").alias("check_name"),
            F.col("chk.status").alias("status"),
            F.col("chk.details").alias("details"),
            F.col("file_mod_time").alias("evaluated_at"),
        )
    )


# ---- Quarantine: guard failures + evaluation failures ----
dp.create_streaming_table(
    name="bronze_quarantine",
    comment="Documents rejected by the source guard (NON_JSON_IN_JSON_SOURCE, CORRUPT_JSON, EXTRACTION_FAILED_*) or the evaluation gate (EVALUATION_FAILED).",
    schema="doc_id STRING, content_hash STRING, source STRING, reason STRING, raw_snippet STRING, quarantined_at TIMESTAMP",
)


@dp.append_flow(target="bronze_quarantine", name="guard_quarantine")
def guard_quarantine():
    return (
        spark.readStream.table("bronze_documents_parsed")
        .filter("NOT valid")
        .select(
            F.col("doc_id"),
            F.col("content_hash"),
            F.col("source"),
            F.col("reason"),
            F.coalesce(F.col("reason_details"), F.substring(F.col("raw_text"), 1, 500)).alias("raw_snippet"),
            F.current_timestamp().alias("quarantined_at"),
        )
    )


@dp.append_flow(target="bronze_quarantine", name="evaluation_quarantine")
def evaluation_quarantine():
    return (
        spark.readStream.table("bronze_documents_parsed")
        .filter("valid AND NOT eval_passed")
        .select(
            "doc_id", "content_hash", "source",
            F.lit("EVALUATION_FAILED").alias("reason"),
            F.concat(F.lit("Failing checks: "), F.coalesce(F.col("failing_checks"), F.lit(""))).alias("raw_snippet"),
            F.current_timestamp().alias("quarantined_at"),
        )
    )


# ---- Audit event log (replaces the notebook's bronze_pipeline_events writes) ----
dp.create_streaming_table(
    name="bronze_pipeline_events",
    comment="Audit trail: SOURCE_FETCH, SOURCE_GUARD, BRONZE_INGESTION, EVALUATION, SILVER_PARSED events.",
    schema="event_id STRING, run_id STRING, doc_id STRING, stage STRING, status STRING, error_message STRING, event_time TIMESTAMP, metadata STRING",
)


@dp.append_flow(target="bronze_pipeline_events", name="events_source_fetch")
def events_source_fetch():
    return spark.readStream.table("bronze_landed_files").select(
        F.expr("uuid()").alias("event_id"),
        F.lit(PIPELINE_RUN_ID).alias("run_id"),
        F.lit(None).cast("string").alias("doc_id"),
        F.lit("SOURCE_FETCH").alias("stage"),
        F.lit("LANDED").alias("status"),
        F.lit(None).cast("string").alias("error_message"),
        F.current_timestamp().alias("event_time"),
        F.to_json(F.create_map(
            F.lit("file_name"), F.col("file_name"),
            F.lit("size_bytes"), F.length(F.col("raw_text")).cast("string"),
        )).alias("metadata"),
    )


@dp.append_flow(target="bronze_pipeline_events", name="events_ingestion")
def events_ingestion():
    src = spark.readStream.table("bronze_documents_parsed")
    is_valid = F.col("valid")
    return src.select(
        F.expr("uuid()").alias("event_id"),
        F.lit(PIPELINE_RUN_ID).alias("run_id"),
        F.col("doc_id"),
        F.when(is_valid, F.lit("BRONZE_INGESTION")).otherwise(F.lit("SOURCE_GUARD")).alias("stage"),
        F.when(is_valid, F.lit("RAW_STORED")).otherwise(F.lit("QUARANTINED")).alias("status"),
        F.when(is_valid, F.lit(None).cast("string")).otherwise(F.col("reason")).alias("error_message"),
        F.current_timestamp().alias("event_time"),
        F.when(is_valid, F.to_json(F.create_map(
            F.lit("engine"), F.coalesce(F.col("engine"), F.lit("")),
            F.lit("model_version"), F.coalesce(F.col("model_version"), F.lit("")),
        ))).otherwise(F.to_json(F.create_map(
            F.lit("reason"), F.coalesce(F.col("reason"), F.lit("")),
        ))).alias("metadata"),
    )


@dp.append_flow(target="bronze_pipeline_events", name="events_evaluation")
def events_evaluation():
    src = spark.readStream.table("bronze_documents_parsed").filter("valid")
    passed = F.col("eval_passed")
    return src.select(
        F.expr("uuid()").alias("event_id"),
        F.lit(PIPELINE_RUN_ID).alias("run_id"),
        F.col("doc_id"),
        F.lit("EVALUATION").alias("stage"),
        F.when(passed, F.lit("SUCCESS")).otherwise(F.lit("QUARANTINED")).alias("status"),
        F.when(passed, F.lit(None).cast("string")).otherwise(F.col("failing_checks")).alias("error_message"),
        F.current_timestamp().alias("event_time"),
        F.lit(None).cast("string").alias("metadata"),
    )
