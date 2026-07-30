"""
MongoDB CDC Routes for Airflow Integration
Provides endpoints for MongoDB Change Data Capture orchestration
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
import json
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

router = APIRouter()

# Watermark file location
WATERMARK_DIR = Path(__file__).parent.parent.parent / "Kafka Integration" / "watermarks"
WATERMARK_DIR.mkdir(parents=True, exist_ok=True)
WATERMARK_FILE = WATERMARK_DIR / "mongodb_watermarks.json"

# MongoDB CDC configuration
MONGODB_CONFIG_FILE = Path(__file__).parent.parent.parent / "Kafka Integration" / ".env.mongodb"


class MongoDBConfigRequest(BaseModel):
    """MongoDB connection configuration"""
    uri: str
    database: str
    collections: Optional[str] = None  # comma-separated list


class MongoDBHealthRequest(BaseModel):
    """MongoDB health check request"""
    uri: str
    database: str


@router.get("/mongodb/cdc/watermarks")
async def get_mongodb_watermarks():
    """Get current MongoDB CDC watermarks"""
    try:
        if WATERMARK_FILE.exists():
            with open(WATERMARK_FILE, 'r') as f:
                watermarks = json.load(f)
            
            return {
                "success": True,
                "watermarks": watermarks,
                "count": len(watermarks)
            }
        else:
            return {
                "success": True,
                "watermarks": {},
                "count": 0,
                "message": "No watermarks found yet. Run the DAG to create watermarks."
            }
    except Exception as e:
        logger.error(f"Failed to read watermarks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mongodb/cdc/watermarks/reset")
async def reset_mongodb_watermarks(collection: Optional[str] = None):
    """Reset MongoDB CDC watermarks (full or specific collection)"""
    try:
        if collection:
            # Reset specific collection
            if WATERMARK_FILE.exists():
                with open(WATERMARK_FILE, 'r') as f:
                    watermarks = json.load(f)
                
                if collection in watermarks:
                    # Backup before reset
                    backup_file = WATERMARK_DIR / f"watermarks_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    with open(backup_file, 'w') as f:
                        json.dump(watermarks, f, indent=2)
                    
                    # Reset specific collection
                    del watermarks[collection]
                    
                    with open(WATERMARK_FILE, 'w') as f:
                        json.dump(watermarks, f, indent=2)
                    
                    return {
                        "success": True,
                        "message": f"Watermark reset for collection: {collection}",
                        "backup": str(backup_file)
                    }
                else:
                    return {
                        "success": False,
                        "message": f"Collection not found: {collection}"
                    }
            else:
                return {
                    "success": False,
                    "message": "No watermarks file found"
                }
        else:
            # Reset all watermarks
            if WATERMARK_FILE.exists():
                # Backup before reset
                backup_file = WATERMARK_DIR / f"watermarks_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(WATERMARK_FILE, 'r') as f:
                    watermarks = json.load(f)
                
                with open(backup_file, 'w') as f:
                    json.dump(watermarks, f, indent=2)
                
                # Reset all
                WATERMARK_FILE.unlink()
                
                return {
                    "success": True,
                    "message": "All watermarks reset",
                    "backup": str(backup_file)
                }
            else:
                return {
                    "success": True,
                    "message": "No watermarks to reset"
                }
    except Exception as e:
        logger.error(f"Failed to reset watermarks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mongodb/health")
async def check_mongodb_health(request: MongoDBHealthRequest):
    """Check MongoDB connection health"""
    try:
        from pymongo import MongoClient
        from pymongo.errors import ConnectionFailure, OperationFailure
        
        # Create MongoDB client
        client = MongoClient(request.uri, serverSelectionTimeoutMS=5000)
        
        # Test connection
        client.admin.command('ping')
        
        # Get database info
        db = client[request.database]
        collections = db.list_collection_names()
        
        # Get collection stats
        total_documents = 0
        for coll_name in collections:
            try:
                total_documents += db[coll_name].estimated_document_count()
            except:
                pass
        
        client.close()
        
        return {
            "status": "success",
            "message": f"Connected to MongoDB: {request.database}",
            "database": request.database,
            "collections_count": len(collections),
            "collections": collections,
            "total_documents": total_documents
        }
    except ConnectionFailure as e:
        logger.error(f"MongoDB connection failed: {e}")
        return {
            "status": "error",
            "message": f"Failed to connect to MongoDB: {str(e)}"
        }
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        return {
            "status": "error",
            "message": f"MongoDB authentication failed: {str(e)}"
        }
    except Exception as e:
        logger.error(f"MongoDB health check failed: {e}")
        return {
            "status": "error",
            "message": f"MongoDB health check failed: {str(e)}"
        }


@router.post("/mongodb/cdc/config")
async def update_mongodb_config(config: MongoDBConfigRequest):
    """Update MongoDB CDC configuration"""
    try:
        # Read existing .env.mongodb
        config_lines = []
        if MONGODB_CONFIG_FILE.exists():
            with open(MONGODB_CONFIG_FILE, 'r') as f:
                config_lines = f.readlines()
        
        # Update configuration
        new_lines = []
        uri_updated = False
        db_updated = False
        coll_updated = False
        
        for line in config_lines:
            if line.startswith('MONGODB_URI='):
                new_lines.append(f'MONGODB_URI={config.uri}\n')
                uri_updated = True
            elif line.startswith('MONGODB_DATABASE='):
                new_lines.append(f'MONGODB_DATABASE={config.database}\n')
                db_updated = True
            elif line.startswith('MONGODB_COLLECTIONS=') and config.collections:
                new_lines.append(f'MONGODB_COLLECTIONS={config.collections}\n')
                coll_updated = True
            else:
                new_lines.append(line)
        
        # Add missing configurations
        if not uri_updated:
            new_lines.append(f'MONGODB_URI={config.uri}\n')
        if not db_updated:
            new_lines.append(f'MONGODB_DATABASE={config.database}\n')
        if config.collections and not coll_updated:
            new_lines.append(f'MONGODB_COLLECTIONS={config.collections}\n')
        
        # Write back to file
        with open(MONGODB_CONFIG_FILE, 'w') as f:
            f.writelines(new_lines)
        
        return {
            "success": True,
            "message": "MongoDB configuration updated",
            "config_file": str(MONGODB_CONFIG_FILE)
        }
    except Exception as e:
        logger.error(f"Failed to update MongoDB config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mongodb/cdc/config")
async def get_mongodb_config():
    """Get current MongoDB CDC configuration"""
    try:
        if not MONGODB_CONFIG_FILE.exists():
            return {
                "success": False,
                "message": "MongoDB configuration file not found"
            }
        
        # Parse .env.mongodb file
        config = {}
        with open(MONGODB_CONFIG_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    config[key] = value
        
        return {
            "success": True,
            "config": config
        }
    except Exception as e:
        logger.error(f"Failed to read MongoDB config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mongodb/collections")
async def list_mongodb_collections(uri: str, database: str):
    """List all collections in MongoDB database"""
    try:
        from pymongo import MongoClient
        
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[database]
        collections = db.list_collection_names()
        
        # Get collection stats
        collection_info = []
        for coll_name in collections:
            try:
                coll = db[coll_name]
                stats = {
                    "name": coll_name,
                    "document_count": coll.estimated_document_count(),
                    "indexes": len(list(coll.list_indexes()))
                }
                collection_info.append(stats)
            except Exception as e:
                logger.warning(f"Failed to get stats for {coll_name}: {e}")
                collection_info.append({
                    "name": coll_name,
                    "document_count": 0,
                    "indexes": 0
                })
        
        client.close()
        
        return {
            "success": True,
            "database": database,
            "collections": collection_info,
            "count": len(collection_info)
        }
    except Exception as e:
        logger.error(f"Failed to list MongoDB collections: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mongodb/cdc/extraction/status")
async def get_extraction_status():
    """Get MongoDB CDC extraction status (from watermarks)"""
    try:
        if not WATERMARK_FILE.exists():
            return {
                "success": True,
                "status": "not_started",
                "message": "No extractions have been performed yet"
            }
        
        with open(WATERMARK_FILE, 'r') as f:
            watermarks = json.load(f)
        
        # Calculate status for each collection
        collection_status = []
        for collection, watermark in watermarks.items():
            if watermark:
                # Convert ObjectId timestamp to datetime
                try:
                    from bson import ObjectId
                    obj_id = ObjectId(watermark)
                    timestamp = obj_id.generation_time
                    
                    collection_status.append({
                        "collection": collection,
                        "last_extracted": timestamp.isoformat(),
                        "watermark": watermark
                    })
                except:
                    collection_status.append({
                        "collection": collection,
                        "last_extracted": watermark,
                        "watermark": watermark
                    })
            else:
                collection_status.append({
                    "collection": collection,
                    "last_extracted": None,
                    "watermark": None
                })
        
        return {
            "success": True,
            "status": "active",
            "collections": collection_status,
            "total_collections": len(collection_status)
        }
    except Exception as e:
        logger.error(f"Failed to get extraction status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
