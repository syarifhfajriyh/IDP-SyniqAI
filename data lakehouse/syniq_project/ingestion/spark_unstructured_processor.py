"""
Spark Unstructured Processor
==============================
Orchestrates the full unstructured-media pipeline:

  MongoDB (media.assets)    AWS S3 (syniq-raw-media)
          │                          │
          └──────────┬───────────────┘
                     │  raw file metadata + URIs
                     ▼
       ┌──────────────────────────────┐
       │  Source Connectors           │
       │  MongoDBConnector            │
       │  S3Connector                 │
       └──────────────┬───────────────┘
                      │  list[dict]  (file metadata + URIs)
                      ▼
       ┌──────────────────────────────────────────┐
       │  spark_unstructured_processor.py          │
       │                                          │
       │  1. Merge asset records from MongoDB/S3  │
       │  2. Stage files into MinIO Bronze        │
       │  3. Call SilverTransformerSpark           │
       │     ├─ ImageTransformer  (resize, EXIF)  │
       │     ├─ VideoTransformer  (thumbs, meta)  │
       │     ├─ AudioTransformer  (volume, ID3)   │
       │     └─ DocumentTransformer (OCR, chunks) │
       └──────────────┬───────────────────────────┘
                      │  enriched DataFrames
                      ▼
       ╔══════════════════════════════════════════╗
       ║  Silver Iceberg Tables (MinIO)           ║
       ║  silver.unstructured.images              ║
       ║  silver.unstructured.videos              ║
       ║  silver.unstructured.audio               ║
       ║  silver.unstructured.documents           ║
       ╚══════════════════════════════════════════╝

Entry point used by ``unstructured_router.py``:

    from spark_unstructured_processor import UnstructuredProcessor
    processor = UnstructuredProcessor(config)
    result = processor.run(media_type="image", domain="media")
"""

from __future__ import annotations

import io
import logging
import os
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path bootstrap so sub-imports resolve regardless of working directory
# ---------------------------------------------------------------------------
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE / "ingestion"))
sys.path.insert(0, str(_HERE / "connectors"))


# ---------------------------------------------------------------------------
# Supported media types ↔ transform parameter names
# ---------------------------------------------------------------------------
MEDIA_TYPES = ("image", "video", "audio", "document")

_TRANSFORM_PARAM_MAP = {
    "image":    "image_transforms",
    "video":    "video_transforms",
    "audio":    "audio_transforms",
    "document": "pdf_transforms",    # documents include PDF + text
    "text":     "text_transforms",
    "pdf":      "pdf_transforms",
}

# Unified unstructured_type value for the Silver transformer
_SILVER_TYPE_MAP = {
    "image":    "image",
    "video":    "video",
    "audio":    "audio",
    "document": "pdf",
    "text":     "text",
    "pdf":      "pdf",
}


# ===========================================================================
# UnstructuredProcessor
# ===========================================================================
class UnstructuredProcessor:
    """
    Full pipeline orchestrator for unstructured-media silver transformation.

    Parameters
    ----------
    config : dict
        Top-level config containing optional sub-dicts:

        ``mongodb`` – MongoDBConnector config (host, port, database …)
        ``s3``      – S3Connector config (bucket, aws_access_key_id …)
        ``minio``   – MinIO config (endpoint, access_key, secret_key)
        ``spark``   – Spark tuning overrides (optional)

    Example::

        processor = UnstructuredProcessor({
            "mongodb": {"host": "localhost", "port": 27017, "database": "media"},
            "s3": {"bucket": "syniq-raw-media", "aws_access_key_id": "...", ...},
            "minio": {"endpoint": "http://localhost:9000",
                      "access_key": "admin", "secret_key": "password123"},
        })
        result = processor.run(
            media_type="image",
            domain="media",
            entity="product_images",
            transforms={"resize": True, "resizeWidth": 512, "resizeHeight": 512},
        )
    """

    BRONZE_BUCKET = "syniqai-bronze"

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._minio_config = config.get("minio", self._default_minio_config())
        self._mongo_config = config.get("mongodb")
        self._s3_config = config.get("s3")
        self._minio_client = None  # lazy init

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        media_type: str,
        domain: str = "media",
        entity: str = "assets",
        source_override: Optional[str] = None,
        transforms: Optional[Dict[str, Any]] = None,
        execution_mode: str = "full",
        rules: Optional[List[Dict[str, Any]]] = None,
        limit: int = 10_000,
        stage_to_bronze: bool = True,
    ) -> Dict[str, Any]:
        """
        Run the full unstructured media pipeline.

        Parameters
        ----------
        media_type  : ``"image"`` | ``"video"`` | ``"audio"`` | ``"document"``
        domain      : Silver namespace prefix (e.g. ``"media"``)
        entity      : Collection / prefix name (e.g. ``"product_images"``)
        source_override : Force a single connector ``"s3"`` or ``"mongodb"``
        transforms  : Transform option dict forwarded to the Silver transformer
        execution_mode : ``"full"`` or ``"incremental"``
        rules       : List of data-quality rule dicts
        limit       : Maximum number of source assets to discover
        stage_to_bronze : When True, copy discovered files into MinIO Bronze
                          before running Spark.  Set False if files are already
                          present in Bronze.

        Returns
        -------
        dict with keys ``job_id``, ``status``, ``metrics``, ``silver_table``,
        ``assets_discovered``, ``assets_staged``, ``error`` (if failed).
        """
        job_id = f"unstructured_{domain}_{entity}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        start = datetime.now()

        logger.info(f"🚀 UnstructuredProcessor.run() | job={job_id}")
        logger.info(f"   media_type={media_type} domain={domain} entity={entity}")

        try:
            # ── 1. Discover assets from source connectors ───────────────────
            assets: List[Dict[str, Any]] = []

            if source_override in (None, "mongodb") and self._mongo_config:
                mongo_assets = self._collect_from_mongodb(
                    collection=entity, media_type=media_type, limit=limit
                )
                assets.extend(mongo_assets)
                logger.info(f"   MongoDB: {len(mongo_assets)} assets found")

            if source_override in (None, "s3") and self._s3_config:
                s3_assets = self._collect_from_s3(
                    media_type=media_type,
                    prefix=self._s3_config.get("prefix", ""),
                    limit=limit - len(assets),
                )
                assets.extend(s3_assets)
                logger.info(f"   S3: {len(s3_assets)} assets found")

            if not assets and stage_to_bronze:
                logger.warning("No source connectors configured or no assets found; "
                               "assuming files are already in Bronze.")

            assets_discovered = len(assets)

            # ── 2. Stage files to MinIO Bronze ──────────────────────────────
            assets_staged = 0
            if stage_to_bronze and assets:
                assets_staged = self._stage_to_bronze(
                    assets=assets,
                    domain=domain,
                    entity=entity,
                )
                logger.info(f"   Staged {assets_staged}/{assets_discovered} files to Bronze")

            # ── 3. Call SilverTransformerSpark ──────────────────────────────
            silver_type = _SILVER_TYPE_MAP.get(media_type, media_type)
            transform_param = _TRANSFORM_PARAM_MAP.get(media_type, "pdf_transforms")
            transform_kwargs: Dict[str, Any] = {transform_param: transforms or {}}

            result = self._run_spark_transform(
                source=source_override or self._infer_source(),
                entity=entity,
                domain=domain,
                silver_type=silver_type,
                execution_mode=execution_mode,
                rules=rules or [],
                transform_kwargs=transform_kwargs,
            )

            end = datetime.now()
            result["job_id"] = job_id
            result["assets_discovered"] = assets_discovered
            result["assets_staged"] = assets_staged
            result["total_duration_seconds"] = (end - start).total_seconds()
            return result

        except Exception as exc:
            logger.error(f"❌ UnstructuredProcessor failed: {exc}", exc_info=True)
            return {
                "job_id": job_id,
                "status": "failed",
                "error": str(exc),
                "assets_discovered": 0,
                "assets_staged": 0,
                "total_duration_seconds": (datetime.now() - start).total_seconds(),
            }

    def get_source_stats(self) -> Dict[str, Any]:
        """Return asset counts from configured source connectors."""
        stats: Dict[str, Any] = {}

        if self._mongo_config:
            try:
                from connectors.mongodb_connector import MongoDBConnector
                conn = MongoDBConnector(self._mongo_config)
                conn.connect()
                db = conn.db
                stats["mongodb"] = {
                    "collections": db.list_collection_names(),
                    "assets_collection": db["assets"].count_documents({})
                    if "assets" in db.list_collection_names()
                    else 0,
                }
                conn.client.close()
            except Exception as exc:
                stats["mongodb"] = {"error": str(exc)}

        if self._s3_config:
            try:
                from connectors.s3_connector import S3Connector
                conn = S3Connector(self._s3_config)
                conn.connect()
                stats["s3"] = conn.get_bucket_stats()
                conn.disconnect()
            except Exception as exc:
                stats["s3"] = {"error": str(exc)}

        return stats

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _default_minio_config(self) -> Dict[str, Any]:
        return {
            "endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            "access_key": os.getenv("MINIO_ACCESS_KEY", ""),
            "secret_key": os.getenv("MINIO_SECRET_KEY", ""),
        }

    def _infer_source(self) -> str:
        """Infer source label for Iceberg table naming."""
        if self._s3_config:
            return "s3"
        if self._mongo_config:
            return "mongodb"
        return "unstructured"

    # ── Source connector helpers ────────────────────────────────────────────

    def _collect_from_mongodb(
        self,
        collection: str,
        media_type: Optional[str],
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Fetch asset metadata records from MongoDB media.assets."""
        try:
            from connectors.mongodb_connector import MongoDBConnector
        except ImportError:
            from mongodb_connector import MongoDBConnector  # type: ignore

        connector = MongoDBConnector(self._mongo_config)
        try:
            connector.connect()
        except Exception as conn_err:
            logger.warning(f"MongoDB connection failed – skipping MongoDB source: {conn_err}")
            return []

        query_filter: Dict[str, Any] = {}
        if media_type:
            query_filter["media_type"] = media_type

        records: List[Dict[str, Any]] = []
        for batch in connector.extract(
            {
                "entity": collection,
                "mode": "full",
                "batch_size": min(limit, 1_000),
                "query_filter": query_filter,
                "flatten_nested": True,
                "extract_binary": False,   # we want URIs only
            }
        ):
            df = batch["data"]
            for row in df.to_dict(orient="records"):
                row["_source_connector"] = "mongodb"
                records.append(row)
                if len(records) >= limit:
                    break
            if len(records) >= limit:
                break

        connector.client.close()
        return records

    def _collect_from_s3(
        self,
        media_type: Optional[str],
        prefix: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """List raw media files from AWS S3 syniq-raw-media bucket."""
        try:
            from connectors.s3_connector import S3Connector
        except ImportError:
            from s3_connector import S3Connector  # type: ignore

        connector = S3Connector(self._s3_config)
        if not connector.connect():
            logger.error("S3Connector.connect() failed — skipping S3 assets")
            return []

        assets = connector.list_assets(
            media_type=media_type,
            prefix=prefix or None,
            limit=limit,
            generate_presigned=False,  # not needed for Bronze staging
        )
        connector.disconnect()
        return assets

    # ── Bronze staging ──────────────────────────────────────────────────────

    def _get_minio_client(self):
        """Lazy-init MinIO client."""
        if self._minio_client is None:
            from minio import Minio

            endpoint = (
                self._minio_config["endpoint"]
                .replace("http://", "")
                .replace("https://", "")
            )
            self._minio_client = Minio(
                endpoint,
                access_key=self._minio_config["access_key"],
                secret_key=self._minio_config["secret_key"],
                secure=self._minio_config.get("secure", False),
            )

            # Ensure Bronze bucket exists
            if not self._minio_client.bucket_exists(self.BRONZE_BUCKET):
                self._minio_client.make_bucket(self.BRONZE_BUCKET)
                logger.info(f"Created Bronze bucket: {self.BRONZE_BUCKET}")

        return self._minio_client

    def _stage_to_bronze(
        self,
        assets: List[Dict[str, Any]],
        domain: str,
        entity: str,
    ) -> int:
        """
        Download files from S3 / MongoDB GridFS URIs and upload to MinIO Bronze.

        Bronze path: ``syniqai-bronze/{domain}/{source}/{entity}/{file_name}``

        Returns number of successfully staged files.
        """
        import boto3  # type: ignore

        minio = self._get_minio_client()
        staged = 0

        # Build S3 client if we have S3 assets
        s3_client = None
        if self._s3_config:
            import boto3 as _boto3
            session = _boto3.Session(
                aws_access_key_id=self._s3_config.get("aws_access_key_id"),
                aws_secret_access_key=self._s3_config.get("aws_secret_access_key"),
                region_name=self._s3_config.get("region_name", "us-east-1"),
            )
            s3_client = session.client("s3")

        for asset in assets:
            try:
                uri: str = asset.get("uri", "")
                file_name: str = asset.get("file_name", "")
                if not file_name:
                    continue

                source_label = "s3" if uri.startswith("s3://") else "mongodb"
                bronze_object_name = f"{domain}/{source_label}/{entity}/{file_name}"

                # ── Download from S3 ────────────────────────────────────────
                if uri.startswith("s3://") and s3_client:
                    s3_key: str = asset.get("s3_key", "")
                    bucket: str = asset.get("bucket", self._s3_config.get("bucket", ""))

                    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
                    data: bytes = response["Body"].read()

                    minio.put_object(
                        bucket_name=self.BRONZE_BUCKET,
                        object_name=bronze_object_name,
                        data=io.BytesIO(data),
                        length=len(data),
                        content_type=_guess_content_type(file_name),
                    )
                    staged += 1

                # ── MongoDB GridFS / presigned URL ──────────────────────────
                elif asset.get("presigned_url"):
                    import urllib.request
                    with urllib.request.urlopen(asset["presigned_url"]) as resp:
                        data = resp.read()
                    minio.put_object(
                        bucket_name=self.BRONZE_BUCKET,
                        object_name=bronze_object_name,
                        data=io.BytesIO(data),
                        length=len(data),
                        content_type=_guess_content_type(file_name),
                    )
                    staged += 1

                else:
                    logger.debug(f"Skipping asset {file_name}: no download mechanism available")

            except Exception as exc:
                logger.warning(f"Failed to stage {asset.get('file_name', '?')}: {exc}")

        return staged

    # ── Spark transform ─────────────────────────────────────────────────────

    def _run_spark_transform(
        self,
        source: str,
        entity: str,
        domain: str,
        silver_type: str,
        execution_mode: str,
        rules: List[Dict[str, Any]],
        transform_kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Delegate to SilverTransformerSpark for the Bronze→Silver step."""
        try:
            from ingestion.silver_transformer_spark import SilverTransformerSpark
        except ImportError:
            try:
                from silver_transformer_spark import SilverTransformerSpark  # type: ignore
            except ImportError as err:
                raise ImportError(
                    "Cannot import SilverTransformerSpark. "
                    "Ensure data lakehouse/syniq_project/ingestion/ is on PYTHONPATH."
                ) from err

        logger.info(f"🔥 Launching SilverTransformerSpark for {domain}.{entity} ({silver_type})")
        transformer = SilverTransformerSpark(minio_config=self._minio_config)

        try:
            result = transformer.transform(
                source=source,
                entity=entity,
                domain=domain,
                execution_mode=execution_mode,
                rules=rules,
                data_type="unstructured",
                unstructured_type=silver_type,
                **transform_kwargs,
            )
        finally:
            transformer.cleanup()

        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _guess_content_type(file_name: str) -> str:
    import mimetypes
    ct, _ = mimetypes.guess_type(file_name)
    return ct or "application/octet-stream"
