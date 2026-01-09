"""
Azure Function to create a new asset record
"""
import json
import uuid
from datetime import datetime, timezone
import azure.functions as func

from ..shared.auth import require_api_key, AuthenticationError
from ..shared.logging_utils import create_logger
from ..shared.storage import (
    create_sas_token,
    get_storage_container,
    build_blob_url
)
from ..shared.sql_client import execute_update
from ..shared.cosmos_client import upsert_asset_doc


logger = create_logger(__name__)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handle POST /api/assets - Create a new asset
    
    Request body:
        {
            "fileName": "example.jpg",
            "fileType": "image/jpeg",
            "fileSize": 12345
        }
    
    Returns:
        {
            "id": "uuid",
            "blobUrl": "https://...",
            "uploadUrl": "https://...?sas_token"
        }
    """
    # Authenticate request
    try:
        api_key = req.headers.get('x-api-key')
        require_api_key(api_key)
    except AuthenticationError:
        logger.warning("Unauthorized access attempt")
        return func.HttpResponse(
            'Unauthorized',
            status_code=401,
            mimetype='text/plain'
        )
    
    # Parse request body
    try:
        request_data = req.get_json()
    except ValueError:
        logger.error("Invalid JSON in request body")
        return func.HttpResponse(
            'Invalid JSON format',
            status_code=400,
            mimetype='text/plain'
        )
    
    # Validate required fields
    file_name = request_data.get('fileName')
    file_type = request_data.get('fileType')
    file_size = request_data.get('fileSize')
    
    if not all([file_name, file_type, file_size]):
        logger.error("Missing required fields in request")
        return func.HttpResponse(
            'Missing required fields: fileName, fileType, fileSize',
            status_code=400,
            mimetype='text/plain'
        )
    
    try:
        file_size = int(file_size)
    except (ValueError, TypeError):
        logger.error(f"Invalid fileSize: {file_size}")
        return func.HttpResponse(
            'fileSize must be a valid integer',
            status_code=400,
            mimetype='text/plain'
        )
    
    # Generate unique asset ID
    asset_id = str(uuid.uuid4())
    container_name = get_storage_container()
    blob_path = f"{asset_id}/{file_name}"
    
    # Generate SAS token for upload
    sas_token = create_sas_token(container_name, blob_path)
    blob_url = build_blob_url(container_name, blob_path)
    upload_url = f"{blob_url}?{sas_token}"
    
    # Save to Cosmos DB
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        cosmos_doc = {
            'id': asset_id,
            'fileName': file_name,
            'fileType': file_type,
            'uploadDate': now_iso,
            'fileSize': file_size,
            'blobUrl': blob_url,
        }
        upsert_asset_doc(cosmos_doc)
    except Exception as e:
        logger.warning(f"Failed to save to Cosmos DB: {e}")
        # Continue with SQL save
    
    # Save to SQL Database
    try:
        execute_update(
            """
            INSERT INTO file_metadata 
            (id, user_id, file_name, file_type, file_size, blob_url, status, created_at)
            VALUES (:id, NULL, :file_name, :file_type, :file_size, :blob_url, 'pending', SYSUTCDATETIME())
            """,
            {
                'id': asset_id,
                'file_name': file_name,
                'file_type': file_type,
                'file_size': file_size,
                'blob_url': blob_url,
            }
        )
    except Exception as e:
        logger.error(f"Failed to save to SQL Database: {e}")
        return func.HttpResponse(
            'Failed to save asset metadata',
            status_code=500,
            mimetype='text/plain'
        )
    
    # Prepare response
    response_data = {
        'id': asset_id,
        'blobUrl': blob_url,
        'uploadUrl': upload_url,
    }
    
    logger.info(f"Created asset {asset_id} for file {file_name}")
    
    return func.HttpResponse(
        json.dumps(response_data),
        status_code=201,
        mimetype='application/json'
    )

