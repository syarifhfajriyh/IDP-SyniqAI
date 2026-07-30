import pandas as pd
import json
from datetime import datetime
from typing import Dict, Any, Optional

class BronzeIngestor:
    """
    Handles ingestion of raw data into the Bronze layer, with metadata enrichment,
    validation, and integration with catalog, quality, and lineage modules.
    """
    def __init__(self, minio_client, catalog=None, profiler=None, lineage=None,
                 enable_catalog=True, enable_profiling=True, enable_lineage=True):
        self.minio_client = minio_client
        self.catalog = catalog
        self.profiler = profiler
        self.lineage = lineage
        self.enable_catalog = enable_catalog
        self.enable_profiling = enable_profiling
        self.enable_lineage = enable_lineage

    def ingest_batch(self, df: pd.DataFrame, source: str, entity: str, bucket: str = "bronze",
                     partition: Optional[Dict[str, Any]] = None, extra_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Ingests a DataFrame into the Bronze layer, writes Parquet and metadata, and updates catalog, quality, and lineage.
        """
        now = datetime.now()
        partition = partition or {
            "year": now.year,
            "month": f"{now.month:02d}",
            "day": f"{now.day:02d}"
        }
        # Build MinIO path
        base_path = f"{bucket}/{source}/{entity}/year={partition['year']}/month={partition['month']}/day={partition['day']}"
        parquet_file = f"{base_path}/part-{now.strftime('%Y%m%d_%H%M%S')}.parquet"
        metadata_file = f"{base_path}/_metadata.json"

        # Add basic metadata columns
        df = df.copy()
        df["_ingestion_timestamp"] = now.isoformat()
        df["_source_system"] = source
        if extra_metadata:
            for k, v in extra_metadata.items():
                df[f"_{k}"] = v

        # Write Parquet to MinIO
        import io
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        self.minio_client.put_object(
            bucket_name=bucket,
            object_name=parquet_file[len(bucket)+1:],
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )

        # Prepare metadata
        metadata = {
            "source": source,
            "entity": entity,
            "rows": len(df),
            "columns": list(df.columns),
            "ingestion_time": now.isoformat(),
            "parquet_file": parquet_file
        }

        # Register in catalog
        catalog_version = None
        if self.enable_catalog and self.catalog:
            schema = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
            catalog_entry = self.catalog.register_table(
                source=source,
                entity=entity,
                schema=schema,
                location=parquet_file,
                row_count=len(df),
                tags={"layer": "bronze"}
            )
            catalog_version = catalog_entry.get("version")
            metadata["catalog_version"] = catalog_version

        # Profile data quality
        quality_score = None
        validation_report = None
        if self.enable_profiling and self.profiler:
            profile = self.profiler.profile_dataframe(df, source, entity)
            quality_score = profile.get("quality_score")
            validation_report = profile
            metadata["quality_score"] = quality_score

        # Record lineage
        if self.enable_lineage and self.lineage:
            self.lineage.record_transformation(
                source_layer="bronze",
                source_location=parquet_file,
                target_layer="bronze",
                target_location=parquet_file,
                transformation="raw_ingestion",
                row_count=len(df),
                columns_used=list(df.columns)
            )

        # Write metadata to MinIO
        meta_buffer = io.BytesIO(json.dumps(metadata, indent=2).encode("utf-8"))
        self.minio_client.put_object(
            bucket_name=bucket,
            object_name=metadata_file[len(bucket)+1:],
            data=meta_buffer,
            length=meta_buffer.getbuffer().nbytes,
            content_type="application/json"
        )

        return {
            "success": True,
            "rows_written": len(df),
            "file_path": parquet_file,
            "catalog_version": catalog_version,
            "quality_score": quality_score,
            "validation_report": validation_report
        }

# Example usage (not run directly):
# from minio import Minio
# minio_client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
# ingestor = BronzeIngestor(minio_client)
# df = pd.read_csv("some_data.csv")
# result = ingestor.ingest_batch(df, "postgres_prod", "customers")
# print(result)
