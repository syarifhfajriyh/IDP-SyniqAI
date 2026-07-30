"""
ERD (Entity-Relationship Diagram) Routes
Introspects a live source database's real primary/foreign key constraints
(via SQLAlchemy's Inspector, same approach as data ingestion/Connector/
postgres_connector.py) and returns a node/edge graph the frontend can render.

Nothing in the codebase discovered FK relationships before this — the only
prior "PK/FK" signals (gold_eda_service.py's schema-structure heuristic) were
guessed from column-name/uniqueness patterns, not real constraints.
"""
import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/erd", tags=["erd"])


def _build_engine(
    host: Optional[str], port: Optional[int], database: Optional[str],
    user: Optional[str], password: Optional[str],
) -> Engine:
    """Build a SQLAlchemy engine for schema introspection only (no pooling
    needed — this connects, reads catalog metadata, and disconnects)."""
    host = host or os.getenv("POSTGRES_HOST", "localhost")
    port = port or int(os.getenv("POSTGRES_PORT", "5432"))
    database = database or os.getenv("POSTGRES_DB", "postgres")
    user = user or os.getenv("POSTGRES_USER", "postgres")
    password = password if password is not None else os.getenv("POSTGRES_PASSWORD", "")

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(conn_str, pool_size=1, max_overflow=0, connect_args={"connect_timeout": 10})


def _introspect_schema(engine: Engine, schema: str) -> Dict[str, Any]:
    inspector = inspect(engine)
    table_names = inspector.get_table_names(schema=schema)

    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []

    for table_name in table_names:
        columns = inspector.get_columns(table_name, schema=schema)
        pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
        pk_columns = set(pk_constraint.get("constrained_columns") or [])

        node_columns = [
            {
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "is_primary_key": col["name"] in pk_columns,
            }
            for col in columns
        ]

        nodes.append({
            "table": table_name,
            "schema": schema,
            "columns": node_columns,
            "primary_key": sorted(pk_columns),
        })

        for fk in inspector.get_foreign_keys(table_name, schema=schema):
            referred_table = fk.get("referred_table")
            constrained_columns = fk.get("constrained_columns") or []
            referred_columns = fk.get("referred_columns") or []
            if not referred_table or not constrained_columns:
                continue
            for source_col, target_col in zip(constrained_columns, referred_columns or constrained_columns):
                edges.append({
                    "id": fk.get("name") or f"{table_name}.{source_col}->{referred_table}.{target_col}",
                    "source_table": table_name,
                    "source_column": source_col,
                    "target_table": referred_table,
                    "target_column": target_col,
                })

    return {
        "schema": schema,
        "nodes": nodes,
        "edges": edges,
        "table_count": len(nodes),
        "relationship_count": len(edges),
    }


@router.get("/schema")
def get_schema_erd(
    host: Optional[str] = Query(None, description="Source DB host (defaults to POSTGRES_HOST env)"),
    port: Optional[int] = Query(None, description="Source DB port (defaults to POSTGRES_PORT env)"),
    database: Optional[str] = Query(None, description="Source database name (defaults to POSTGRES_DB env)"),
    user: Optional[str] = Query(None, description="Source DB user (defaults to POSTGRES_USER env)"),
    password: Optional[str] = Query(None, description="Source DB password (defaults to POSTGRES_PASSWORD env)"),
    schema: str = Query("public", description="Database schema to introspect"),
):
    """
    Discover real table/column/PK/FK structure from a live Postgres source
    and return it as a node/edge graph for ERD rendering.
    """
    engine = _build_engine(host, port, database, user, password)
    try:
        with engine.connect():
            pass  # fail fast with a clear error if the connection is bad
        result = _introspect_schema(engine, schema)
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"ERD introspection failed: {e}")
        raise HTTPException(status_code=500, detail=f"ERD introspection failed: {e}")
    finally:
        engine.dispose()
