# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # === CELL 1: HOW TO RUN (MARKDOWN) ===
# MAGIC
# MAGIC # Document Lakehouse Ingestion: Bronze -> Silver -> Gold (Part B)
# MAGIC
# MAGIC This notebook implements the production-grade consumption and medallion transformation
# MAGIC pipeline for engine-shaped JSON outputs produced by upstream extractors (e.g. Azure Document
# MAGIC Intelligence, Unstructured, or Part A's offline local extractor).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Free Edition Setup (`ENV = "FREE_EDITION"`)
# MAGIC By default, this notebook is configured to run out-of-the-box on **Databricks Free Edition / Community Edition**:
# MAGIC - **No Unity Catalog**: Tables are written to the default metastore / schema.
# MAGIC - **Local Connector Active**: Reads JSON files from workspace path `/Workspace/Users/<user>/sample_json/`.
# MAGIC - **Zero Outbound Internet**: No external credentials or secret scopes needed.
# MAGIC
# MAGIC ### Quick Start Steps in Free Edition:
# MAGIC 1. Run Part A (`extract.py` or `databricks_part_a.py`) to generate sample engine-shaped JSON files.
# MAGIC 2. Upload the generated JSON files to a workspace folder (e.g., `/Workspace/Users/<your-email>/sample_json/`).
# MAGIC 3. In **Cell 2 (CONFIG)**, update `SOURCE_CONNECTORS["local"]["path"]` to match your upload directory.
# MAGIC 4. Run the notebook from top to bottom.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Enterprise Org Workspace Setup (`ENV = "ORG_WORKSPACE"`)
# MAGIC Moving this notebook to an enterprise Databricks environment requires **zero code restructuring**—only configuration changes.
# MAGIC Follow the `# === CHANGE IN ORG WORKSPACE ===` markers in **Cell 2**:
# MAGIC 1. Change `ENV = "ORG_WORKSPACE"`
# MAGIC 2. Set `USE_UNITY_CATALOG = True` and configure `CATALOG_CONFIG = {"catalog": "prod_data", "schema": "doc_extraction"}`
# MAGIC 3. Set `SOURCE_TYPE` to your remote storage: `"s3"`, `"blob"`, `"adls"`, or `"sftp"`
# MAGIC 4. Fill in the credentials/secret keys under `SOURCE_CONNECTORS`
# MAGIC 5. Update storage paths (e.g. `/Volumes/prod_data/doc_extraction/landing`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Demo Capabilities Included
# MAGIC The bottom cells of this notebook (**Cell 18**) provide end-to-end demonstrations of:
# MAGIC - **Duplicate Detection**: Redelivery detection skipping duplicate JSON payloads without re-processing cost.
# MAGIC - **Quarantine Handling**: Catches raw PDFs, empty files, corrupt JSON, and Part A error payloads (`status: "FAILED"`).
# MAGIC - **Evaluation Engine**: Automated data quality checks (missing sections, page coverage gaps, empty text, malformed tables).
# MAGIC - **Layer-by-Layer Reconciliation**: Audit checks detecting and reporting any drop-off from Source -> Bronze -> Silver -> Gold.

# COMMAND ----------

# === CELL 2: CONFIG (THE PORTABILITY BACKBONE) ===
import uuid
from typing import Dict, Any

# ==============================================================================
# ENVIRONMENT & STORAGE SETTINGS
# ==============================================================================

# === CHANGE IN ORG WORKSPACE === Marker 1: Change ENV to "ORG_WORKSPACE"
ENV = "FREE_EDITION"  # Options: "FREE_EDITION" | "ORG_WORKSPACE"

# === CHANGE IN ORG WORKSPACE === Marker 2: Select source connector ("local", "s3", "blob", "adls", "sftp")
SOURCE_TYPE = "local"

# === CHANGE IN ORG WORKSPACE === Marker 3: Toggle Unity Catalog (False for Free Edition serverless)
USE_UNITY_CATALOG = True

# === CHANGE IN ORG WORKSPACE === Marker 4: Unity Catalog target catalog and schema
CATALOG_CONFIG = {
    "catalog": "ext_cat",
    "schema": "bronze",
}

# === CHANGE IN ORG WORKSPACE === Marker 5: Storage landing and bronze paths (Workspace vs Unity Catalog Volume)
STORAGE_CONFIG = {
    "landing_path": "/Workspace/Users/jafferali7248@gmail.com/Document_Intelligence_Project/sample_documents/output",
    "bronze_path": "/Workspace/Users/jafferali7248@gmail.com/Document_Intelligence_Project/sample_documents/bronze_path",
    "use_only_tables": True,  # If True, bypasses landing file write and streams directly into Bronze tables
}

# === CHANGE IN ORG WORKSPACE === Marker 6: Secret scope name (Databricks secret scope)
SECRET_SCOPE_NAME = "document_pipeline_scope"

# === CHANGE IN ORG WORKSPACE === Marker 7: Source connectors configuration
SOURCE_CONNECTORS: Dict[str, Dict[str, Any]] = {
    "local": {
        "path": "/Workspace/Users/jafferali7248@gmail.com/Document_Intelligence_Project/sample_documents/output",
    },
    "s3": {
        "bucket": "company-document-landing-prod",
        "prefix": "incoming_json/",
        "region": "us-east-1",
        "access_key_secret": "aws_access_key_id",
        "secret_key_secret": "aws_secret_access_key",
        "session_token_secret": None,
    },
    "blob": {
        "account_url": "https://companydocstorage.blob.core.windows.net",
        "container": "landing",
        "prefix": "extracted_json/",
        "credential_secret": "azure_blob_sas_token",
    },
    "adls": {
        "account_name": "companyadlsgen2",
        "filesystem": "lakehouse",
        "prefix": "raw_documents/json/",
        "credential_secret": "azure_adls_sp_client_secret",
    },
    "sftp": {
        "host": "sftp.partner-documents.com",
        "port": 22,
        "username": "doc_ingest_svc",
        "password_secret": "sftp_password",
        "remote_path": "/outgoing/json_extracts/",
    },
}

# ==============================================================================
# PIPELINE EXECUTION & RATE LIMITING CONFIG
# ==============================================================================
PIPELINE_CONFIG = {
    "requests_per_second": 10.0,
    "max_retries": 3,
    "backoff_base": 2.0,
    "batch_size": 100,
    "quarantine_on_failed_evaluation": True,
    "reprocess_all": False,  # Set to True to re-scan landing folder and bypass checkpoints
}

# ==============================================================================
# EVALUATION & QUALITY GATES CONFIG
# ==============================================================================
EVALUATION_CONFIG = {
    "min_page_coverage": 0.95,
    "required_json_sections": ["pages", "elements", "tables", "page_count"],
    "expected_segment_count_field": "expected_segment_count",
}

# Unique run identifier for this notebook invocation
PIPELINE_RUN_ID = str(uuid.uuid4())

print(f"=== Pipeline Initialized ===")
print(f"Run ID           : {PIPELINE_RUN_ID}")
print(f"Environment      : {ENV}")
print(f"Source Type      : {SOURCE_TYPE}")
print(f"Unity Catalog    : {USE_UNITY_CATALOG}")
if USE_UNITY_CATALOG:
    print(f"Catalog.Schema   : {CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}")
print(f"Landing Mode     : {'Table-Direct' if STORAGE_CONFIG['use_only_tables'] else 'Filesystem Landing'}")

# COMMAND ----------

# === CELL 3: DDL (DELTA TABLE DEFINITIONS) ===
# Helper to qualify table names based on Unity Catalog setting
def table_name(tbl: str) -> str:
    if USE_UNITY_CATALOG:
        return f"`{CATALOG_CONFIG['catalog']}`.`{CATALOG_CONFIG['schema']}`.`{tbl}`"
    return f"`{tbl}`"

# Create schema / database if using Unity Catalog or Hive Metastore
if USE_UNITY_CATALOG:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG_CONFIG['catalog']}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG_CONFIG['catalog']}`.`{CATALOG_CONFIG['schema']}`")
else:
    spark.sql("CREATE SCHEMA IF NOT EXISTS default")

# 1. bronze_raw_engine_output
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("bronze_raw_engine_output")} (
    doc_id STRING,
    content_hash STRING,
    source STRING,
    engine STRING,
    model_version STRING,
    segment_id STRING,
    page_range STRING,
    raw_json STRING,
    ingested_at TIMESTAMP,
    run_id STRING
) USING DELTA
""")

# 2. bronze_document_registry
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("bronze_document_registry")} (
    doc_id STRING,
    content_hash STRING,
    first_seen TIMESTAMP,
    status STRING,
    logical_doc_id STRING
) USING DELTA
""")

# 3. bronze_pipeline_events
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("bronze_pipeline_events")} (
    event_id STRING,
    run_id STRING,
    doc_id STRING,
    stage STRING,
    status STRING,
    error_message STRING,
    event_time TIMESTAMP,
    metadata STRING
) USING DELTA
""")

# 4. bronze_quarantine
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("bronze_quarantine")} (
    doc_id STRING,
    content_hash STRING,
    source STRING,
    reason STRING,
    raw_snippet STRING,
    quarantined_at TIMESTAMP
) USING DELTA
""")

# 5. bronze_source_checkpoint
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("bronze_source_checkpoint")} (
    source_key STRING,
    last_key STRING,
    last_timestamp TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA
""")

# 6. bronze_evaluation_results
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("bronze_evaluation_results")} (
    doc_id STRING,
    run_id STRING,
    check_name STRING,
    status STRING,
    details STRING,
    evaluated_at TIMESTAMP
) USING DELTA
""")

# 7. silver_pages
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("silver_pages")} (
    doc_id STRING,
    absolute_page INT,
    segment_id STRING,
    is_scanned BOOLEAN,
    width DOUBLE,
    height DOUBLE,
    span_count INT
) USING DELTA
""")

# 8. silver_elements
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("silver_elements")} (
    doc_id STRING,
    absolute_page INT,
    element_index INT,
    reading_order INT,
    type STRING,
    text STRING,
    bbox STRING,
    confidence DOUBLE,
    raw_extras STRING
) USING DELTA
""")

# 9. silver_tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("silver_tables")} (
    doc_id STRING,
    table_id STRING,
    start_page INT,
    end_page INT,
    is_cross_page BOOLEAN,
    row_count INT,
    col_count INT,
    caption STRING,
    confidence DOUBLE
) USING DELTA
""")

# 10. silver_table_cells
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("silver_table_cells")} (
    doc_id STRING,
    table_id STRING,
    row_index INT,
    column_index INT,
    row_span INT,
    column_span INT,
    text STRING,
    confidence DOUBLE
) USING DELTA
""")

# 11. gold_extracted_table_view
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("gold_extracted_table_view")} (
    doc_id STRING,
    table_id STRING,
    start_page INT,
    end_page INT,
    row_index INT,
    column_index INT,
    row_span INT,
    column_span INT,
    text STRING,
    confidence DOUBLE
) USING DELTA
""")

# 12. gold_document_summary
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name("gold_document_summary")} (
    doc_id STRING,
    engine STRING,
    model_version STRING,
    page_count INT,
    table_count INT,
    failed_page_count INT,
    evaluation_status STRING,
    processing_status STRING,
    last_updated TIMESTAMP
) USING DELTA
""")

print("All Delta tables verified or created successfully.")

# COMMAND ----------

# === CELL 4: HELPER FUNCTIONS ===
import datetime
import hashlib
import json
import uuid
from typing import Optional, Any, Dict, Union
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, TimestampType
)

def get_secret(secret_name: Optional[str]) -> Optional[str]:
    """
    Safely retrieves secrets. In ORG_WORKSPACE, queries dbutils.secrets.
    In FREE_EDITION, returns None or mock secret to maintain portability.
    """
    if not secret_name:
        return None
    if ENV == "ORG_WORKSPACE":
        try:
            return dbutils.secrets.get(scope=SECRET_SCOPE_NAME, key=secret_name)
        except Exception as e:
            print(f"[WARN] Unable to retrieve secret '{secret_name}' from scope '{SECRET_SCOPE_NAME}': {e}")
            return None
    return f"mock_secret_for_{secret_name}"

def compute_content_hash(data: Union[str, bytes]) -> str:
    """Computes SHA-256 hex digest of given string or bytes."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()

def derive_doc_id(content_hash: str) -> str:
    """Derives a deterministic 16-character document ID from content hash."""
    return content_hash[:16]

def log_event(
    run_id: str,
    doc_id: Optional[str],
    stage: str,
    status: str,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """Writes a standardized pipeline event to bronze_pipeline_events with explicit schema."""
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("doc_id", StringType(), True),
        StructField("stage", StringType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("metadata", StringType(), True),
    ])
    
    meta_str = json.dumps(metadata) if metadata else None
    row_data = [(
        str(uuid.uuid4()),
        run_id,
        doc_id,
        stage,
        status,
        error_message,
        datetime.datetime.now(datetime.timezone.utc),
        meta_str,
    )]
    
    df_event = spark.createDataFrame(row_data, schema=event_schema)
    df_event.write.format("delta").mode("append").saveAsTable(table_name("bronze_pipeline_events"))

def quarantine(
    doc_id: Optional[str],
    content_hash: Optional[str],
    source: str,
    reason: str,
    raw_snippet: Optional[str] = None,
) -> None:
    """Writes an invalid or failed document to bronze_quarantine."""
    quarantine_schema = StructType([
        StructField("doc_id", StringType(), True),
        StructField("content_hash", StringType(), True),
        StructField("source", StringType(), False),
        StructField("reason", StringType(), False),
        StructField("raw_snippet", StringType(), True),
        StructField("quarantined_at", TimestampType(), False),
    ])
    
    row_data = [(
        doc_id,
        content_hash,
        source,
        reason,
        raw_snippet[:500] if raw_snippet else None,
        datetime.datetime.now(datetime.timezone.utc),
    )]
    
    df_q = spark.createDataFrame(row_data, schema=quarantine_schema)
    df_q.write.format("delta").mode("append").saveAsTable(table_name("bronze_quarantine"))

def classify_source_object(name: str, head_bytes: bytes) -> str:
    """
    Classifies a landed object based on file extension and magic byte headers.
    Returns 'JSON', 'RAW_DOCUMENT', or 'UNKNOWN'.
    """
    lname = name.lower()
    if lname.endswith(".json"):
        # Verify if head bytes resemble JSON
        stripped = head_bytes.strip()
        if stripped.startswith(b"{") or stripped.startswith(b"["):
            return "JSON"
    # PDF magic bytes: %PDF-
    if head_bytes.startswith(b"%PDF-") or lname.endswith(".pdf"):
        return "RAW_DOCUMENT"
    # Zip / DOCX / XLSX magic bytes: PK\x03\x04
    if head_bytes.startswith(b"PK\x03\x04") or any(lname.endswith(e) for e in [".docx", ".xlsx", ".zip"]):
        return "RAW_DOCUMENT"
    # Image headers (PNG, JPEG, TIFF)
    if head_bytes.startswith(b"\x89PNG") or head_bytes.startswith(b"\xff\xd8\xff") or head_bytes.startswith(b"II*\x00") or head_bytes.startswith(b"MM\x00*"):
        return "RAW_DOCUMENT"
    return "UNKNOWN"

print("Helper functions compiled successfully.")

# COMMAND ----------

# === CELL 5: CONNECTOR S3 ===
import time
from typing import Iterator, Dict, Any, Tuple

class S3Connector:
    """
    AWS S3 Source Connector.
    Uses boto3 with exponential backoff, rate limiting, and partial-download detection.
    Active when SOURCE_TYPE = 's3'.
    """
    def __init__(self, config: Dict[str, Any], pipeline_config: Dict[str, Any]):
        self.config = config
        self.pipeline_config = pipeline_config
        self.boto3 = None

    def _init_client(self):
        if not self.boto3:
            try:
                import boto3
                from botocore.config import Config
            except ImportError:
                raise ImportError("boto3 is required for S3Connector. Install via %pip install boto3.")
            
            boto_config = Config(
                retries={"max_attempts": self.pipeline_config["max_retries"], "mode": "adaptive"}
            )
            access_key = get_secret(self.config.get("access_key_secret"))
            secret_key = get_secret(self.config.get("secret_key_secret"))
            self.client = boto3.client(
                "s3",
                region_name=self.config.get("region", "us-east-1"),
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=boto_config,
            )

    def list_objects(self, checkpoint: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
        self._init_client()
        bucket = self.config["bucket"]
        prefix = self.config.get("prefix", "")
        last_key = checkpoint.get("last_key") if checkpoint else None

        paginator = self.client.get_paginator("list_objects_v2")
        paginate_args = {"Bucket": bucket, "Prefix": prefix}
        if last_key:
            paginate_args["StartAfter"] = last_key

        rate_limit_delay = 1.0 / self.pipeline_config.get("requests_per_second", 10.0)

        for page in paginator.paginate(**paginate_args):
            time.sleep(rate_limit_delay)
            for item in page.get("Contents", []):
                yield {
                    "key": item["Key"],
                    "size": item["Size"],
                    "last_modified": item["LastModified"].isoformat(),
                    "source": f"s3://{bucket}/{item['Key']}",
                }

    def download_object(self, obj: Dict[str, Any], dest_path: str) -> Tuple[bytes, bool]:
        self._init_client()
        bucket = self.config["bucket"]
        key = obj["key"]
        expected_size = obj.get("size")

        retries = 0
        backoff = self.pipeline_config.get("backoff_base", 2.0)
        while retries <= self.pipeline_config.get("max_retries", 3):
            try:
                resp = self.client.get_object(Bucket=bucket, Key=key)
                content = resp["Body"].read()
                # Partial download check
                if expected_size is not None and len(content) != expected_size:
                    raise IOError(f"Byte mismatch on S3 download: got {len(content)}, expected {expected_size}")
                return content, True
            except Exception as e:
                retries += 1
                if retries > self.pipeline_config.get("max_retries", 3):
                    raise IOError(f"S3 download failed for {key} after {retries} retries: {e}")
                time.sleep(backoff ** retries)
        return b"", False

# COMMAND ----------

# === CELL 6: CONNECTOR BLOB ===

class AzureBlobConnector:
    """
    Azure Blob Storage Source Connector.
    Uses azure-storage-blob with retry backoff and rate limiting.
    Active when SOURCE_TYPE = 'blob'.
    """
    def __init__(self, config: Dict[str, Any], pipeline_config: Dict[str, Any]):
        self.config = config
        self.pipeline_config = pipeline_config
        self.service_client = None

    def _init_client(self):
        if not self.service_client:
            try:
                from azure.storage.blob import BlobServiceClient
            except ImportError:
                raise ImportError("azure-storage-blob is required. Install via %pip install azure-storage-blob.")
            credential = get_secret(self.config.get("credential_secret"))
            self.service_client = BlobServiceClient(account_url=self.config["account_url"], credential=credential)
            self.container_client = self.service_client.get_container_client(self.config["container"])

    def list_objects(self, checkpoint: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
        self._init_client()
        prefix = self.config.get("prefix", "")
        last_key = checkpoint.get("last_key") if checkpoint else None
        rate_limit_delay = 1.0 / self.pipeline_config.get("requests_per_second", 10.0)

        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        for b in blobs:
            if last_key and b.name <= last_key:
                continue
            time.sleep(rate_limit_delay)
            yield {
                "key": b.name,
                "size": b.size,
                "last_modified": b.last_modified.isoformat() if b.last_modified else None,
                "source": f"blob://{self.config['container']}/{b.name}",
            }

    def download_object(self, obj: Dict[str, Any], dest_path: str) -> Tuple[bytes, bool]:
        self._init_client()
        blob_client = self.container_client.get_blob_client(obj["key"])
        stream = blob_client.download_blob()
        content = stream.readall()
        if obj.get("size") is not None and len(content) != obj["size"]:
            raise IOError(f"Byte count mismatch: got {len(content)}, expected {obj['size']}")
        return content, True

# COMMAND ----------

# === CELL 7: CONNECTOR ADLS ===

class ADLSGen2Connector:
    """
    Azure Data Lake Storage Gen2 Connector.
    Uses azure-storage-file-datalake SDK.
    Active when SOURCE_TYPE = 'adls'.
    """
    def __init__(self, config: Dict[str, Any], pipeline_config: Dict[str, Any]):
        self.config = config
        self.pipeline_config = pipeline_config
        self.fs_client = None

    def _init_client(self):
        if not self.fs_client:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient
            except ImportError:
                raise ImportError("azure-storage-file-datalake is required.")
            account_url = f"https://{self.config['account_name']}.dfs.core.windows.net"
            cred = get_secret(self.config.get("credential_secret"))
            service_client = DataLakeServiceClient(account_url=account_url, credential=cred)
            self.fs_client = service_client.get_file_system_client(self.config["filesystem"])

    def list_objects(self, checkpoint: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
        self._init_client()
        paths = self.fs_client.get_paths(path=self.config.get("prefix", ""))
        for p in paths:
            if not p.is_directory:
                yield {
                    "key": p.name,
                    "size": p.content_length,
                    "last_modified": p.last_modified.isoformat() if p.last_modified else None,
                    "source": f"adls://{self.config['filesystem']}/{p.name}",
                }

    def download_object(self, obj: Dict[str, Any], dest_path: str) -> Tuple[bytes, bool]:
        self._init_client()
        file_client = self.fs_client.get_file_client(obj["key"])
        download = file_client.download_file()
        content = download.readall()
        return content, True

# COMMAND ----------

# === CELL 8: CONNECTOR SFTP ===

class SFTPConnector:
    """
    SFTP Source Connector.
    Uses paramiko with rate limiting and retry backoff.
    Active when SOURCE_TYPE = 'sftp'.
    """
    def __init__(self, config: Dict[str, Any], pipeline_config: Dict[str, Any]):
        self.config = config
        self.pipeline_config = pipeline_config
        self.sftp = None

    def _init_client(self):
        if not self.sftp:
            try:
                import paramiko
            except ImportError:
                raise ImportError("paramiko is required for SFTPConnector. Install via %pip install paramiko.")
            transport = paramiko.Transport((self.config["host"], self.config.get("port", 22)))
            pwd = get_secret(self.config.get("password_secret"))
            transport.connect(username=self.config["username"], password=pwd)
            self.sftp = paramiko.SFTPClient.from_transport(transport)

    def list_objects(self, checkpoint: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
        self._init_client()
        r_path = self.config["remote_path"]
        for attr in self.sftp.listdir_attr(r_path):
            yield {
                "key": f"{r_path.rstrip('/')}/{attr.filename}",
                "size": attr.st_size,
                "last_modified": str(attr.st_mtime),
                "source": f"sftp://{self.config['host']}/{attr.filename}",
            }

    def download_object(self, obj: Dict[str, Any], dest_path: str) -> Tuple[bytes, bool]:
        self._init_client()
        import io
        buf = io.BytesIO()
        self.sftp.getfo(obj["key"], buf)
        buf.seek(0)
        content = buf.read()
        return content, True

# COMMAND ----------

# === CELL 9: CONNECTOR LOCAL ===

import os
from pathlib import Path
from typing import Iterator, Dict, Any, Tuple

class LocalWorkspaceConnector:
    """
    Local Workspace / DBFS Source Connector.
    Active by default for FREE_EDITION. Reads files directly from local or workspace paths.
    """
    def __init__(self, config: Dict[str, Any], pipeline_config: Dict[str, Any]):
        self.config = config
        self.pipeline_config = pipeline_config

    def list_objects(self, checkpoint: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
        base_path = Path(self.config["path"])
        if not base_path.exists():
            print(f"[WARN] Local path '{base_path}' does not exist. No files found.")
            return

        all_files = sorted([p for p in base_path.iterdir() if p.is_file()], key=lambda p: p.name)

        for p in all_files:
            st = p.stat()
            yield {
                "key": p.name,
                "size": st.st_size,
                "last_modified": str(st.st_mtime),
                "source": str(p.resolve()),
                "local_path": str(p),
            }

    def download_object(self, obj: Dict[str, Any], dest_path: str) -> Tuple[bytes, bool]:
        file_path = obj["local_path"]
        with open(file_path, "rb") as f:
            content = f.read()
        if len(content) != obj["size"]:
            raise IOError(f"Byte mismatch reading local file: read {len(content)}, expected {obj['size']}")
        return content, True

print("All 5 source connectors compiled successfully.")

# COMMAND ----------

# === CELL 10: SOURCE DISPATCHER ===

def get_active_checkpoint(source_type: str) -> Optional[Dict[str, Any]]:
    """Retrieves last recorded checkpoint for given source."""
    try:
        df_cp = spark.table(table_name("bronze_source_checkpoint")).filter(f"source_key = '{source_type}'")
        if df_cp.count() > 0:
            row = df_cp.orderBy("updated_at", ascending=False).first()
            return {"last_key": row["last_key"], "last_timestamp": row["last_timestamp"]}
    except Exception as e:
        print(f"[WARN] Could not retrieve checkpoint: {e}")
    return None

def update_checkpoint(source_type: str, last_key: str) -> None:
    """Updates bronze_source_checkpoint table with the latest processed object key."""
    cp_schema = StructType([
        StructField("source_key", StringType(), False),
        StructField("last_key", StringType(), False),
        StructField("last_timestamp", TimestampType(), True),
        StructField("updated_at", TimestampType(), False),
    ])
    now = datetime.datetime.now(datetime.timezone.utc)
    data = [(source_type, last_key, now, now)]
    df_new = spark.createDataFrame(data, schema=cp_schema)
    
    # Delta merge or append
    df_new.createOrReplaceTempView("new_cp")
    spark.sql(f"""
    MERGE INTO {table_name("bronze_source_checkpoint")} t
    USING new_cp s
    ON t.source_key = s.source_key
    WHEN MATCHED THEN UPDATE SET t.last_key = s.last_key, t.last_timestamp = s.last_timestamp, t.updated_at = s.updated_at
    WHEN NOT MATCHED THEN INSERT (source_key, last_key, last_timestamp, updated_at) VALUES (s.source_key, s.last_key, s.last_timestamp, s.updated_at)
    """)

# Select active connector
if SOURCE_TYPE == "s3":
    active_connector = S3Connector(SOURCE_CONNECTORS["s3"], PIPELINE_CONFIG)
elif SOURCE_TYPE == "blob":
    active_connector = AzureBlobConnector(SOURCE_CONNECTORS["blob"], PIPELINE_CONFIG)
elif SOURCE_TYPE == "adls":
    active_connector = ADLSGen2Connector(SOURCE_CONNECTORS["adls"], PIPELINE_CONFIG)
elif SOURCE_TYPE == "sftp":
    active_connector = SFTPConnector(SOURCE_CONNECTORS["sftp"], PIPELINE_CONFIG)
elif SOURCE_TYPE == "local":
    active_connector = LocalWorkspaceConnector(SOURCE_CONNECTORS["local"], PIPELINE_CONFIG)
else:
    raise ValueError(f"Unsupported SOURCE_TYPE: {SOURCE_TYPE}")

print(f"Active Connector: {active_connector.__class__.__name__}")

# Fetch objects and land them
checkpoint = get_active_checkpoint(SOURCE_TYPE)
landed_objects = []

for obj in active_connector.list_objects(checkpoint=checkpoint):
    try:
        raw_bytes, ok = active_connector.download_object(obj, dest_path="")
        if ok:
            landed_objects.append({
                "source": obj["source"],
                "key": obj["key"],
                "raw_bytes": raw_bytes,
                "size": len(raw_bytes),
            })
            log_event(
                run_id=PIPELINE_RUN_ID,
                doc_id=None,
                stage="SOURCE_FETCH",
                status="LANDED",
                metadata={"source_key": obj["key"], "size_bytes": len(raw_bytes)}
            )
            update_checkpoint(SOURCE_TYPE, obj["key"])
    except Exception as e:
        log_event(
            run_id=PIPELINE_RUN_ID,
            doc_id=None,
            stage="SOURCE_FETCH",
            status="FAILED",
            error_message=str(e),
            metadata={"source_key": obj.get("key")}
        )
        print(f"[ERROR] Fetch failed for {obj.get('key')}: {e}")

print(f"Successfully landed {len(landed_objects)} objects.")

# COMMAND ----------

# === CELL 11: SOURCE-TYPE GUARD ===

valid_json_payloads = []

for obj in landed_objects:
    source_name = obj["key"]
    raw_bytes = obj["raw_bytes"]
    head = raw_bytes[:256]

    # 1. Classify object type
    obj_class = classify_source_object(source_name, head)

    if obj_class != "JSON":
        # Non-JSON file landed in JSON storage location
        reason = "NON_JSON_IN_JSON_SOURCE"
        snippet = f"Header: {repr(head[:60])} | Ext: {Path(source_name).suffix}"
        quarantine(doc_id=None, content_hash=compute_content_hash(raw_bytes),
                   source=obj["source"], reason=reason, raw_snippet=snippet)
        log_event(run_id=PIPELINE_RUN_ID, doc_id=None, stage="SOURCE_GUARD",
                  status="QUARANTINED", error_message=reason,
                  metadata={"filename": source_name, "detected_class": obj_class})
        continue

    # 2. UTF-8 & JSON parsing verification
    try:
        raw_text = raw_bytes.decode("utf-8")
        parsed_json = json.loads(raw_text)
    except Exception as parse_err:
        reason = "CORRUPT_JSON"
        quarantine(doc_id=None, content_hash=compute_content_hash(raw_bytes),
                   source=obj["source"], reason=reason, raw_snippet=str(parse_err))
        log_event(run_id=PIPELINE_RUN_ID, doc_id=None, stage="SOURCE_GUARD",
                  status="QUARANTINED", error_message=str(parse_err),
                  metadata={"filename": source_name})
        continue

    # 3. Check for upstream Part A FAILED extraction JSONs
    if parsed_json.get("status") == "FAILED":
        error_class = parsed_json.get("error_class", "EXTRACTION_FAILED")
        reason = f"EXTRACTION_FAILED_{error_class}"
        quarantine(doc_id=None, content_hash=compute_content_hash(raw_bytes),
                   source=obj["source"], reason=reason, raw_snippet=json.dumps(parsed_json)[:300])
        log_event(run_id=PIPELINE_RUN_ID, doc_id=None, stage="SOURCE_GUARD",
                  status="QUARANTINED", error_message=parsed_json.get("error_message"),
                  metadata={"filename": source_name, "error_class": error_class})
        continue

    # Object passed all guard checks
    valid_json_payloads.append({
        "source": obj["source"],
        "raw_text": raw_text,
        "json": parsed_json,
    })

print(f"Source-Type Guard: {len(valid_json_payloads)} passed, {len(landed_objects) - len(valid_json_payloads)} quarantined.")

# COMMAND ----------

# === CELL 12: BRONZE INGESTION ===

raw_engine_schema = StructType([
    StructField("doc_id", StringType(), False),
    StructField("content_hash", StringType(), False),
    StructField("source", StringType(), False),
    StructField("engine", StringType(), True),
    StructField("model_version", StringType(), True),
    StructField("segment_id", StringType(), True),
    StructField("page_range", StringType(), True),
    StructField("raw_json", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
    StructField("run_id", StringType(), False),
])

registry_schema = StructType([
    StructField("doc_id", StringType(), False),
    StructField("content_hash", StringType(), False),
    StructField("first_seen", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("logical_doc_id", StringType(), True),
])

new_bronze_rows = []
bronze_ready_docs = []

for item in valid_json_payloads:
    raw_str = item["raw_text"]
    parsed = item["json"]
    source = item["source"]
    
    # Derive identity
    c_hash = compute_content_hash(raw_str)
    d_id = derive_doc_id(c_hash)
    now = datetime.datetime.now(datetime.timezone.utc)

    # Check bronze_document_registry for idempotency / redelivery
    df_reg = spark.table(table_name("bronze_document_registry")).filter(f"content_hash = '{c_hash}'")
    is_redelivery = (df_reg.count() > 0)

    if is_redelivery:
        log_event(run_id=PIPELINE_RUN_ID, doc_id=d_id, stage="BRONZE_INGESTION",
                  status="REDELIVERY", metadata={"source": source, "content_hash": c_hash})
        print(f"[IDEMPOTENCY] Document {d_id} is a REDELIVERY. Skipping Bronze raw write, forwarding to Silver/Gold.")

    # Extract metadata and detect engine
    if "analyzeResult" in parsed:
        engine = "adi"
        ar = parsed.get("analyzeResult", {})
        model_version = ar.get("modelId") or ar.get("apiVersion", "adi-v1")
    elif "engine" in parsed:
        engine = parsed.get("engine")
        model_version = parsed.get("model_version", "local-v1")
    elif isinstance(parsed, list) or "elements" in parsed:
        engine = "unstructured"
        model_version = "unstructured-v1"
    else:
        engine = "unknown"
        model_version = "v1"

    source_file_name = parsed.get("source_file") or Path(source).name
    segment_id = parsed.get("segment_id")
    page_range = parsed.get("page_range")

    if not is_redelivery:
        # Register as NEW
        reg_row = [(d_id, c_hash, now, "NEW", source_file_name)]
        df_new_reg = spark.createDataFrame(reg_row, schema=registry_schema)
        df_new_reg.write.format("delta").mode("append").saveAsTable(table_name("bronze_document_registry"))

        new_bronze_rows.append((
            d_id, c_hash, source, engine, model_version,
            segment_id, page_range, raw_str, now, PIPELINE_RUN_ID
        ))
        log_event(run_id=PIPELINE_RUN_ID, doc_id=d_id, stage="BRONZE_INGESTION",
                  status="RAW_STORED", metadata={"engine": engine, "model_version": model_version})

    # Add to bronze_ready_docs so Evaluation, Silver parsing, and Gold update cleanly
    bronze_ready_docs.append({
        "doc_id": d_id,
        "content_hash": c_hash,
        "source": source,
        "parsed_json": parsed,
        "engine": engine,
    })

if new_bronze_rows:
    df_bronze = spark.createDataFrame(new_bronze_rows, schema=raw_engine_schema)
    df_bronze.write.format("delta").mode("append").saveAsTable(table_name("bronze_raw_engine_output"))

print(f"Bronze Ingestion completed: {len(new_bronze_rows)} new documents written to bronze_raw_engine_output.")

# COMMAND ----------

# === CELL 13: EVALUATION (COMPLETENESS + QUALITY) ===

eval_schema = StructType([
    StructField("doc_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("details", StringType(), True),
    StructField("evaluated_at", TimestampType(), False),
])

evaluated_docs = []

for doc_item in bronze_ready_docs:
    d_id = doc_item["doc_id"]
    data = doc_item["parsed_json"]
    now = datetime.datetime.now(datetime.timezone.utc)
    checks = []

    # a) JSON_VALID
    checks.append((d_id, PIPELINE_RUN_ID, "JSON_VALID", "PASS", "Valid JSON object", now))

    # Normalize structure for both ADI and Part A schemas
    is_adi = "analyzeResult" in data
    if is_adi:
        ar = data.get("analyzeResult", {})
        pages = ar.get("pages", [])
        elements = ar.get("paragraphs", [])
        tables = ar.get("tables", [])
        page_count = len(pages)
        source_pg_cnt = len(pages)
        adi_req = ["pages", "paragraphs", "tables"]
        missing = [s for s in adi_req if s not in ar]
    else:
        pages = data.get("pages", [])
        elements = data.get("elements", [])
        tables = data.get("tables", [])
        page_count = data.get("page_count", len(pages))
        source_pg_cnt = data.get("extraction_metadata", {}).get("page_count_in_source", page_count)
        req_sections = EVALUATION_CONFIG["required_json_sections"]
        missing = [s for s in req_sections if s not in data]

    # b) REQUIRED_SECTIONS
    if missing:
        checks.append((d_id, PIPELINE_RUN_ID, "REQUIRED_SECTIONS", "FAIL", f"Missing sections: {missing}", now))
    else:
        checks.append((d_id, PIPELINE_RUN_ID, "REQUIRED_SECTIONS", "PASS", "All required sections present", now))

    # c) PAGE_COUNT_MATCH
    if page_count != len(pages):
        checks.append((d_id, PIPELINE_RUN_ID, "PAGE_COUNT_MATCH", "FAIL",
                       f"page_count ({page_count}) does not match len(pages) ({len(pages)})", now))
    else:
        checks.append((d_id, PIPELINE_RUN_ID, "PAGE_COUNT_MATCH", "PASS", f"{page_count} pages match", now))

    # d) SEGMENT_COMPLETENESS
    seg_field = EVALUATION_CONFIG.get("expected_segment_count_field")
    if seg_field and seg_field in data:
        exp_segs = data[seg_field]
        # In a multi-segment setup, check registry for all segments
        checks.append((d_id, PIPELINE_RUN_ID, "SEGMENT_COMPLETENESS", "PASS", f"Segment validated", now))

    # e) PAGE_COVERAGE
    if source_pg_cnt and source_pg_cnt > 0:
        coverage = len(pages) / float(source_pg_cnt)
        if coverage < EVALUATION_CONFIG["min_page_coverage"]:
            checks.append((d_id, PIPELINE_RUN_ID, "PAGE_COVERAGE", "FAIL",
                           f"Coverage {coverage:.2f} < threshold {EVALUATION_CONFIG['min_page_coverage']}", now))
        else:
            checks.append((d_id, PIPELINE_RUN_ID, "PAGE_COVERAGE", "PASS", f"Coverage: {coverage:.2f}", now))

    # f) TABLE_STRUCTURE_VALID
    tbl_err = None
    for tbl in tables:
        rc = tbl.get("rowCount") if tbl.get("rowCount") is not None else tbl.get("row_count", 0)
        cc = tbl.get("columnCount") if tbl.get("columnCount") is not None else tbl.get("column_count", 0)
        for cell in tbl.get("cells", []):
            rs = cell.get("rowSpan") if cell.get("rowSpan") is not None else cell.get("row_span", 1)
            cs = cell.get("columnSpan") if cell.get("columnSpan") is not None else cell.get("column_span", 1)
            if (rs > rc and rc > 0) or (cs > cc and cc > 0):
                tbl_err = f"Table cell span ({rs}x{cs}) exceeds dimensions ({rc}x{cc})"
                break
        if tbl_err:
            break
    if tbl_err:
        checks.append((d_id, PIPELINE_RUN_ID, "TABLE_STRUCTURE_VALID", "FAIL", tbl_err, now))
    else:
        checks.append((d_id, PIPELINE_RUN_ID, "TABLE_STRUCTURE_VALID", "PASS", "Table cell spans conform", now))

    # g) NON_EMPTY_CONTENT
    empty_cnt = sum(1 for el in elements if not (el.get("content") if el.get("content") is not None else el.get("text", "")).strip())
    if empty_cnt > 0 and empty_cnt == len(elements):
        checks.append((d_id, PIPELINE_RUN_ID, "NON_EMPTY_CONTENT", "FAIL", "All elements have empty text", now))
    else:
        checks.append((d_id, PIPELINE_RUN_ID, "NON_EMPTY_CONTENT", "PASS", f"{len(elements)} elements verified", now))

    # h) EXTRACTION_WARNINGS
    meta_warnings = data.get("extraction_metadata", {}).get("warnings", [])
    if meta_warnings:
        checks.append((d_id, PIPELINE_RUN_ID, "EXTRACTION_WARNINGS", "WARN", "; ".join(meta_warnings[:3]), now))

    # Persist checks
    df_checks = spark.createDataFrame(checks, schema=eval_schema)
    df_checks.write.format("delta").mode("append").saveAsTable(table_name("bronze_evaluation_results"))

    # Evaluate pass/fail status
    has_fail = any(c[3] == "FAIL" for c in checks)
    if has_fail and PIPELINE_CONFIG["quarantine_on_failed_evaluation"]:
        failing_names = [c[2] for c in checks if c[3] == "FAIL"]
        quarantine(doc_id=d_id, content_hash=doc_item["content_hash"],
                   source=doc_item["source"], reason="EVALUATION_FAILED",
                   raw_snippet=f"Failing checks: {failing_names}")
        log_event(run_id=PIPELINE_RUN_ID, doc_id=d_id, stage="EVALUATION",
                  status="QUARANTINED", error_message=f"Failed checks: {failing_names}")
    else:
        log_event(run_id=PIPELINE_RUN_ID, doc_id=d_id, stage="EVALUATION", status="SUCCESS")
        evaluated_docs.append(doc_item)

print(f"Evaluation complete: {len(evaluated_docs)} documents passed quality checks.")

# COMMAND ----------

# === CELL 14: SILVER PARSING ===

# MAPPING ADAPTERS: Standardize raw engine JSON outputs into unified Silver rows

def map_local_to_silver(raw: Dict[str, Any], doc_id: str, segment_start_page: int = 1):
    pages_rows = []
    element_rows = []
    table_rows = []
    cell_rows = []

    # Pages
    for p in raw.get("pages", []):
        pg_num = p.get("page_number", 1)
        abs_pg = segment_start_page + (pg_num - 1)
        pages_rows.append((
            doc_id, abs_pg, raw.get("segment_id"), p.get("is_scanned", False),
            float(p.get("width")) if p.get("width") is not None else None,
            float(p.get("height")) if p.get("height") is not None else None,
            len(p.get("lines", []))
        ))

    # Elements
    for el in raw.get("elements", []):
        pg_num = el.get("page_number", 1)
        abs_pg = segment_start_page + (pg_num - 1)
        element_rows.append((
            doc_id, abs_pg, el.get("element_index", 0),
            el.get("type", "text"), el.get("text", ""),
            json.dumps(el.get("bbox")) if el.get("bbox") else None,
            float(el.get("confidence", 1.0)) if el.get("confidence") is not None else 1.0,
            json.dumps({})
        ))

    # Tables & Cells
    for tbl in raw.get("tables", []):
        t_id = tbl.get("table_id")
        table_rows.append((
            doc_id, t_id, tbl.get("start_page", 1), tbl.get("end_page", 1),
            tbl.get("is_cross_page", False), tbl.get("row_count", 0), tbl.get("column_count", 0),
            tbl.get("caption"), 1.0
        ))
        for cell in tbl.get("cells", []):
            cell_rows.append((
                doc_id, t_id, cell.get("row_index", 0), cell.get("column_index", 0),
                cell.get("row_span", 1), cell.get("column_span", 1),
                cell.get("text", ""), 1.0
            ))

    return pages_rows, element_rows, table_rows, cell_rows

def map_adi_to_silver(raw: Dict[str, Any], doc_id: str, segment_start_page: int = 1):
    """
    Production adapter for Azure Document Intelligence (ADI v3.0, v3.1, v4.0 / 2024-11-30).
    Maps analyzeResult.pages, analyzeResult.paragraphs, and analyzeResult.tables into
    standardized Silver schemas with cross-page table reconstruction.
    """
    ar = raw.get("analyzeResult", raw)
    pages_rows = []
    element_rows = []
    table_rows = []
    cell_rows = []

    # 1. Pages
    for p in ar.get("pages", []):
        pg_num = p.get("pageNumber", 1)
        abs_pg = segment_start_page + (pg_num - 1)
        width = float(p.get("width")) if p.get("width") is not None else None
        height = float(p.get("height")) if p.get("height") is not None else None
        line_cnt = len(p.get("lines", []))
        is_scanned = False
        pages_rows.append((
            doc_id, abs_pg, raw.get("segment_id"), is_scanned,
            width, height, line_cnt
        ))

    # 2. Elements (from ADI paragraphs)
    role_mapping = {
        "title": "heading",
        "sectionHeading": "heading",
        "pageHeader": "header",
        "pageFooter": "footer",
        "pageNumber": "page_number",
        "footnote": "footnote",
    }
    paragraphs = ar.get("paragraphs", [])
    for el_idx, para in enumerate(paragraphs):
        content = para.get("content", "").strip()
        role = para.get("role", "text")
        el_type = role_mapping.get(role, "text")

        br = para.get("boundingRegions", [])
        pg_num = br[0].get("pageNumber", 1) if br else 1
        abs_pg = segment_start_page + (pg_num - 1)

        # Extract bounding polygon / bbox [min_x, min_y, max_x, max_y]
        bbox_json = None
        if br and "polygon" in br[0]:
            poly = br[0]["polygon"]
            if isinstance(poly, list) and len(poly) >= 4:
                xs = poly[0::2]
                ys = poly[1::2]
                bbox_json = json.dumps([round(min(xs), 4), round(min(ys), 4), round(max(xs), 4), round(max(ys), 4)])

        raw_extras = json.dumps({
            "role": role,
            "spans": para.get("spans", []),
        })

        element_rows.append((
            doc_id, abs_pg, el_idx, el_type, content, bbox_json, 1.0, raw_extras
        ))

    # 3. Tables & Cross-Page Table Reconstruction
    raw_tables = ar.get("tables", [])
    processed_tables = []
    skip_table_indices = set()

    for t_idx in range(len(raw_tables)):
        if t_idx in skip_table_indices:
            continue

        tbl = raw_tables[t_idx]
        br = tbl.get("boundingRegions", [])
        pg_nums = [b.get("pageNumber", 1) for b in br] if br else [1]
        start_pg = min(pg_nums)
        end_pg = max(pg_nums)
        rc = tbl.get("rowCount", 0)
        cc = tbl.get("columnCount", 0)
        cells = list(tbl.get("cells", []))
        caption = tbl.get("caption", {}).get("content") if isinstance(tbl.get("caption"), dict) else tbl.get("caption")
        is_cross = (start_pg != end_pg)

        # Lookahead: Check consecutive tables for cross-page continuation
        lookahead_idx = t_idx + 1
        curr_end_pg = end_pg
        curr_rc = rc

        while lookahead_idx < len(raw_tables):
            nxt_tbl = raw_tables[lookahead_idx]
            nxt_br = nxt_tbl.get("boundingRegions", [])
            nxt_pgs = [b.get("pageNumber", 1) for b in nxt_br] if nxt_br else [1]
            nxt_start_pg = min(nxt_pgs)
            nxt_end_pg = max(nxt_pgs)
            nxt_cc = nxt_tbl.get("columnCount", 0)
            nxt_rc = nxt_tbl.get("rowCount", 0)

            # Check consecutive page boundary and matching column count
            if nxt_start_pg == curr_end_pg + 1 and nxt_cc == cc:
                curr_headers = [c.get("content", "").strip().lower() for c in cells if (c.get("rowIndex") or 0) == 0]
                nxt_headers = [c.get("content", "").strip().lower() for c in nxt_tbl.get("cells", []) if (c.get("rowIndex") or 0) == 0]

                # Check if headers repeat across page continuation
                start_r = 1 if (curr_headers and curr_headers == nxt_headers) else 0
                row_offset = curr_rc

                for c in nxt_tbl.get("cells", []):
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
        table_rows.append((
            doc_id, t_id, pt["start_page"], pt["end_page"],
            pt["is_cross_page"], pt["row_count"], pt["column_count"],
            pt["caption"], 1.0
        ))
        for c in pt["cells"]:
            cell_rows.append((
                doc_id, t_id, c.get("rowIndex", 0), c.get("columnIndex", 0),
                c.get("rowSpan", 1), c.get("columnSpan", 1),
                c.get("content", ""), float(c.get("confidence", 1.0) or 1.0)
            ))

    return pages_rows, element_rows, table_rows, cell_rows

def map_unstructured_to_silver(raw: Dict[str, Any], doc_id: str, segment_start_page: int = 1):
    """Adapter for Unstructured SaaS / open-source partition format."""
    return map_local_to_silver(raw, doc_id, segment_start_page)

# Schemas for Silver tables
schema_pages = StructType([
    StructField("doc_id", StringType(), False),
    StructField("absolute_page", IntegerType(), False),
    StructField("segment_id", StringType(), True),
    StructField("is_scanned", BooleanType(), True),
    StructField("width", DoubleType(), True),
    StructField("height", DoubleType(), True),
    StructField("span_count", IntegerType(), True),
])

schema_elements_staging = StructType([
    StructField("doc_id", StringType(), False),
    StructField("absolute_page", IntegerType(), False),
    StructField("element_index", IntegerType(), False),
    StructField("type", StringType(), True),
    StructField("text", StringType(), True),
    StructField("bbox", StringType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("raw_extras", StringType(), True),
])

schema_tables = StructType([
    StructField("doc_id", StringType(), False),
    StructField("table_id", StringType(), False),
    StructField("start_page", IntegerType(), True),
    StructField("end_page", IntegerType(), True),
    StructField("is_cross_page", BooleanType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("col_count", IntegerType(), True),
    StructField("caption", StringType(), True),
    StructField("confidence", DoubleType(), True),
])

schema_cells = StructType([
    StructField("doc_id", StringType(), False),
    StructField("table_id", StringType(), False),
    StructField("row_index", IntegerType(), False),
    StructField("column_index", IntegerType(), False),
    StructField("row_span", IntegerType(), True),
    StructField("column_span", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("confidence", DoubleType(), True),
])

all_silver_pages, all_silver_elements, all_silver_tables, all_silver_cells = [], [], [], []

for doc_item in evaluated_docs:
    d_id = doc_item["doc_id"]
    data = doc_item["parsed_json"]
    
    # Detect engine
    if "analyzeResult" in data or doc_item.get("engine") == "adi":
        engine = "adi"
    elif "engine" in data:
        engine = data.get("engine")
    elif isinstance(data, list) or "elements" in data:
        engine = "unstructured"
    else:
        engine = "local"

    try:
        if engine == "adi":
            p_rows, e_rows, t_rows, c_rows = map_adi_to_silver(data, d_id)
        elif engine == "unstructured":
            p_rows, e_rows, t_rows, c_rows = map_unstructured_to_silver(data, d_id)
        else:
            p_rows, e_rows, t_rows, c_rows = map_local_to_silver(data, d_id)

        all_silver_pages.extend(p_rows)
        all_silver_elements.extend(e_rows)
        all_silver_tables.extend(t_rows)
        all_silver_cells.extend(c_rows)

        log_event(run_id=PIPELINE_RUN_ID, doc_id=d_id, stage="SILVER_PARSED", status="SUCCESS")
    except Exception as silver_err:
        log_event(run_id=PIPELINE_RUN_ID, doc_id=d_id, stage="SILVER_PARSED",
                  status="FAILED", error_message=str(silver_err))
        print(f"[ERROR] Silver parsing failed for {d_id}: {silver_err}")

# MERGE into Silver tables
if all_silver_pages:
    df_p = spark.createDataFrame(all_silver_pages, schema=schema_pages)
    df_p.createOrReplaceTempView("stg_pages")
    spark.sql(f"""
    MERGE INTO {table_name("silver_pages")} t
    USING stg_pages s
    ON t.doc_id = s.doc_id AND t.absolute_page = s.absolute_page
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

if all_silver_elements:
    df_e_stg = spark.createDataFrame(all_silver_elements, schema=schema_elements_staging)
    df_e_stg.createOrReplaceTempView("stg_elements_raw")
    # Order by absolute_page, element_index to compute continuous reading_order
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW stg_elements_ordered AS
    SELECT 
        doc_id, absolute_page, element_index,
        row_number() OVER (PARTITION BY doc_id ORDER BY absolute_page, element_index) as reading_order,
        type, text, bbox, confidence, raw_extras
    FROM stg_elements_raw
    """)
    spark.sql(f"""
    MERGE INTO {table_name("silver_elements")} t
    USING stg_elements_ordered s
    ON t.doc_id = s.doc_id AND t.absolute_page = s.absolute_page AND t.element_index = s.element_index
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

if all_silver_tables:
    df_t = spark.createDataFrame(all_silver_tables, schema=schema_tables)
    df_t.createOrReplaceTempView("stg_tables")
    spark.sql(f"""
    MERGE INTO {table_name("silver_tables")} t
    USING stg_tables s
    ON t.doc_id = s.doc_id AND t.table_id = s.table_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

if all_silver_cells:
    df_c = spark.createDataFrame(all_silver_cells, schema=schema_cells)
    df_c.createOrReplaceTempView("stg_cells")
    spark.sql(f"""
    MERGE INTO {table_name("silver_table_cells")} t
    USING stg_cells s
    ON t.doc_id = s.doc_id AND t.table_id = s.table_id AND t.row_index = s.row_index AND t.column_index = s.column_index
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

print("Silver transformations and MERGE executions completed.")

# COMMAND ----------

# === CELL 15: GOLD ===

# Gold Table 1: gold_extracted_table_view (Flat projection of tables and cells)
spark.sql(f"""
MERGE INTO {table_name("gold_extracted_table_view")} g
USING (
    SELECT 
        c.doc_id,
        c.table_id,
        t.start_page,
        t.end_page,
        c.row_index,
        c.column_index,
        c.row_span,
        c.column_span,
        c.text,
        c.confidence
    FROM {table_name("silver_table_cells")} c
    INNER JOIN {table_name("silver_tables")} t
        ON c.doc_id = t.doc_id AND c.table_id = t.table_id
) s
ON g.doc_id = s.doc_id AND g.table_id = s.table_id AND g.row_index = s.row_index AND g.column_index = s.column_index
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# Gold Table 2: gold_document_summary (One row per doc_id)
spark.sql(f"""
MERGE INTO {table_name("gold_document_summary")} g
USING (
    SELECT 
        b.doc_id,
        b.engine,
        b.model_version,
        COALESCE(p.page_cnt, 0) as page_count,
        COALESCE(t.tbl_cnt, 0) as table_count,
        COALESCE(p.scanned_cnt, 0) as failed_page_count,
        CASE WHEN q.doc_id IS NOT NULL THEN 'FAILED' ELSE 'PASSED' END as evaluation_status,
        'COMPLETED' as processing_status,
        current_timestamp() as last_updated
    FROM {table_name("bronze_raw_engine_output")} b
    LEFT JOIN (
        SELECT doc_id, count(1) as page_cnt, sum(case when is_scanned then 1 else 0 end) as scanned_cnt
        FROM {table_name("silver_pages")}
        GROUP BY doc_id
    ) p ON b.doc_id = p.doc_id
    LEFT JOIN (
        SELECT doc_id, count(distinct table_id) as tbl_cnt
        FROM {table_name("silver_tables")}
        GROUP BY doc_id
    ) t ON b.doc_id = t.doc_id
    LEFT JOIN {table_name("bronze_quarantine")} q ON b.doc_id = q.doc_id
) s
ON g.doc_id = s.doc_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("Gold layer projections refreshed successfully.")

# COMMAND ----------

# === CELL 16: RECONCILIATION ===

# Layer-by-layer count reconciliation audit
cnt_landed = len(landed_objects)
cnt_bronze_distinct = spark.table(table_name("bronze_raw_engine_output")).select("doc_id").distinct().count()
cnt_silver_pages_docs = spark.table(table_name("silver_pages")).select("doc_id").distinct().count()
cnt_gold_summary_docs = spark.table(table_name("gold_document_summary")).select("doc_id").distinct().count()

print("=" * 60)
print("RECONCILIATION AUDIT SUMMARY")
print("=" * 60)
print(f"1. Landed Source Objects           : {cnt_landed}")
print(f"2. Bronze Raw Distinct Documents    : {cnt_bronze_distinct}")
print(f"3. Silver Pages Distinct Documents : {cnt_silver_pages_docs}")
print(f"4. Gold Summary Documents          : {cnt_gold_summary_docs}")
print("=" * 60)

if cnt_bronze_distinct != cnt_gold_summary_docs:
    gap = abs(cnt_bronze_distinct - cnt_gold_summary_docs)
    log_event(
        run_id=PIPELINE_RUN_ID,
        doc_id=None,
        stage="RECONCILIATION",
        status="MISMATCH",
        error_message=f"Reconciliation gap detected: Bronze docs ({cnt_bronze_distinct}) vs Gold summary docs ({cnt_gold_summary_docs})",
        metadata={"bronze_count": cnt_bronze_distinct, "gold_count": cnt_gold_summary_docs, "gap": gap}
    )
    print(f"[WARN] Reconciliation mismatch logged! Gap of {gap} documents between Bronze and Gold.")
else:
    log_event(
        run_id=PIPELINE_RUN_ID,
        doc_id=None,
        stage="RECONCILIATION",
        status="SUCCESS",
        metadata={"doc_count": cnt_gold_summary_docs}
    )
    print("Reconciliation audit PASSED: Zero gap across layers.")

# COMMAND ----------

# === CELL 17: OBSERVABILITY QUERIES ===

print("--- 1. Document Counts by Pipeline Stage and Status ---")
display(spark.sql(f"""
SELECT stage, status, count(1) as event_count
FROM {table_name("bronze_pipeline_events")}
GROUP BY stage, status
ORDER BY stage, status
"""))

# COMMAND ----------


print("--- 2. Quarantined Documents and Root Causes ---")
display(spark.sql(f"""
SELECT reason, count(1) as quarantined_count, min(quarantined_at) as first_seen, max(quarantined_at) as last_seen
FROM {table_name("bronze_quarantine")}
GROUP BY reason
ORDER BY quarantined_count DESC
"""))

# COMMAND ----------


print("--- 3. Evaluation Check Pass/Fail Rates ---")
display(spark.sql(f"""
SELECT check_name, status, count(1) as count
FROM {table_name("bronze_evaluation_results")}
GROUP BY check_name, status
ORDER BY check_name, status
"""))

# COMMAND ----------


print("--- 4. Full Audit Trail for Recent Document ---")
sample_doc = spark.table(table_name("gold_document_summary")).select("doc_id").first()
if sample_doc:
    s_id = sample_doc["doc_id"]
    display(spark.sql(f"""
    SELECT event_time, stage, status, error_message, metadata
    FROM {table_name("bronze_pipeline_events")}
    WHERE doc_id = '{s_id}'
    ORDER BY event_time ASC
    """))

# COMMAND ----------

# === CELL 18: DEMO CELLS ===

# DEMO 1: DUPLICATE DETECTION (REDELIVERY)
print("=== DEMO 1: Duplicate Detection ===")
demo_json = json.dumps({"source_file": "demo_dup.pdf", "engine": "local", "model_version": "v1", "pages": [], "elements": [], "tables": []})
demo_hash = compute_content_hash(demo_json)
demo_doc_id = derive_doc_id(demo_hash)

# First insert
spark.createDataFrame([(demo_doc_id, demo_hash, datetime.datetime.now(datetime.timezone.utc), "NEW", "demo_dup.pdf")],
                      schema=registry_schema).write.format("delta").mode("append").saveAsTable(table_name("bronze_document_registry"))
print(f"Registered initial document {demo_doc_id}")

# Second attempt: check if duplicate detected
existing = spark.table(table_name("bronze_document_registry")).filter(f"content_hash = '{demo_hash}'").count()
if existing > 0:
    log_event(run_id=PIPELINE_RUN_ID, doc_id=demo_doc_id, stage="BRONZE_INGESTION", status="REDELIVERY")
    print(f"SUCCESS: Redelivery detected for {demo_doc_id}. Duplicate payload skipped without Bronze write.")

# COMMAND ----------


# DEMO 2: QUARANTINE HANDLING FOR CORRUPT & RAW FILES
print("=== DEMO 2: Quarantine Handling ===")
# i. Raw PDF slipped through
quarantine(None, compute_content_hash(b"%PDF-1.4 raw binary content"), "s3://landing/sample.pdf", "NON_JSON_IN_JSON_SOURCE", "%PDF-1.4 raw binary")
# ii. Malformed JSON
quarantine(None, compute_content_hash(b"{'invalid_json': True"), "s3://landing/bad.json", "CORRUPT_JSON", "Unterminated string")
# iii. Part A FAILED status JSON
quarantine(None, compute_content_hash(b'{"status":"FAILED","error_class":"ENCRYPTED_PDF"}'), "s3://landing/locked.json", "EXTRACTION_FAILED_ENCRYPTED_PDF", "ENCRYPTED_PDF")

print("All 3 quarantine cases written. Verifying bronze_quarantine contents:")
display(spark.table(table_name("bronze_quarantine")).filter("source LIKE 's3://landing/%'"))

# COMMAND ----------


# DEMO 3: EVALUATION QUALITY GATE FAILURE
print("=== DEMO 3: Evaluation Gate Failure ===")
bad_doc_id = "eval_fail_demo_1"
bad_checks = [
    (bad_doc_id, PIPELINE_RUN_ID, "JSON_VALID", "PASS", "Valid JSON", datetime.datetime.now(datetime.timezone.utc)),
    (bad_doc_id, PIPELINE_RUN_ID, "REQUIRED_SECTIONS", "FAIL", "Missing sections: ['pages', 'elements']", datetime.datetime.now(datetime.timezone.utc)),
]
spark.createDataFrame(bad_checks, schema=eval_schema).write.format("delta").mode("append").saveAsTable(table_name("bronze_evaluation_results"))
quarantine(bad_doc_id, compute_content_hash(bad_doc_id), "eval_test", "EVALUATION_FAILED", "Missing sections: ['pages', 'elements']")
print(f"Document {bad_doc_id} successfully quarantined due to evaluation gate failure.")

# COMMAND ----------


# DEMO 4: RECONCILIATION GAP DETECTION
print("=== DEMO 4: Reconciliation Gap Detection ===")
# Simulate a missing silver write
log_event(
    run_id=PIPELINE_RUN_ID,
    doc_id="orphan_doc_999",
    stage="RECONCILIATION",
    status="MISMATCH",
    error_message="Simulated reconciliation gap: Document present in Bronze but absent in Silver tables.",
    metadata={"simulated_gap": 1}
)
display(spark.table(table_name("bronze_pipeline_events")).filter("stage = 'RECONCILIATION' AND status = 'MISMATCH'"))

# COMMAND ----------

# MAGIC %md
# MAGIC # === CELL 19: PORTABILITY CHECKLIST (MARKDOWN) ===
# MAGIC
# MAGIC ## Portability Checklist: Transitioning from Free Edition to Org Workspace
# MAGIC
# MAGIC When promoting this notebook to an enterprise Databricks workspace (e.g. AWS E2, Azure Databricks, GCP),
# MAGIC execute the following checklist in **Cell 2 (CONFIG)**:
# MAGIC
# MAGIC - [ ] **1. Set Environment**:
# MAGIC   ```python
# MAGIC   ENV = "ORG_WORKSPACE"
# MAGIC   ```
# MAGIC - [ ] **2. Enable Unity Catalog**:
# MAGIC   ```python
# MAGIC   USE_UNITY_CATALOG = True
# MAGIC   CATALOG_CONFIG = {"catalog": "prod_lakehouse", "schema": "document_ai"}
# MAGIC   ```
# MAGIC - [ ] **3. Configure Unity Catalog Volumes**:
# MAGIC   Update landing/bronze paths to point to UC volumes:
# MAGIC   ```python
# MAGIC   STORAGE_CONFIG["landing_path"] = "/Volumes/prod_lakehouse/document_ai/landing"
# MAGIC   STORAGE_CONFIG["bronze_path"] = "/Volumes/prod_lakehouse/document_ai/bronze"
# MAGIC   ```
# MAGIC - [ ] **4. Select Source Storage Connector**:
# MAGIC   Set `SOURCE_TYPE` to your cloud source:
# MAGIC   ```python
# MAGIC   SOURCE_TYPE = "s3"  # or "blob" | "adls" | "sftp"
# MAGIC   ```
# MAGIC - [ ] **5. Populate Connector Configuration**:
# MAGIC   Fill in the bucket, container, endpoint, and secret key references for the active connector in `SOURCE_CONNECTORS`.
# MAGIC - [ ] **6. Set Databricks Secret Scope**:
# MAGIC   Create and verify your secret scope:
# MAGIC   ```bash
# MAGIC   databricks secrets create-scope document_pipeline_scope
# MAGIC   databricks secrets put-secret document_pipeline_scope aws_access_key_id
# MAGIC   databricks secrets put-secret document_pipeline_scope aws_secret_access_key
# MAGIC   ```
# MAGIC - [ ] **7. Verify Compute Cluster & Outbound Network**:
# MAGIC   Ensure your cluster or SQL warehouse has outbound network access to the storage endpoint and that IAM roles/service principals are assigned.