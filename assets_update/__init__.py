"""
Azure Function to update an existing asset
"""
import json
import azure.functions as func

from ..shared.auth import require_api_key, AuthenticationError
from ..shared.logging_utils import create_logger
from ..shared.sql_client import execute_update, fetch_all_records
from ..shared.cosmos_client import get_asset_doc, upsert_asset_doc


logger = create_logger(__name__)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handle PUT /api/assets/{id} - Update asset metadata
    
    Request body can contain:
        - fileName
        - fileType
        - fileSize
        - blobUrl
        - status
    
    Returns:
        Updated asset object
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
    
    # Get asset ID from route
    asset_id = req.route_params.get('id')
    if not asset_id:
        logger.error("Missing asset ID in route")
        return func.HttpResponse(
            'Asset ID is required',
            status_code=400,
            mimetype='text/plain'
        )
    
    # Parse request body
    try:
        update_data = req.get_json() or {}
    except ValueError:
        logger.error("Invalid JSON in request body")
        return func.HttpResponse(
            'Invalid JSON format',
            status_code=400,
            mimetype='text/plain'
        )
    
    # Allowed fields for update
    allowed_fields = ['fileName', 'fileType', 'fileSize', 'blobUrl', 'status']
    fields_to_update = {k: v for k, v in update_data.items() if k in allowed_fields}
    
    if not fields_to_update:
        logger.warning(f"No valid fields to update for asset {asset_id}")
        return func.HttpResponse(
            'No valid fields provided for update',
            status_code=400,
            mimetype='text/plain'
        )
    
    # Update Cosmos DB
    try:
        doc = get_asset_doc(asset_id) or {"id": asset_id}
        doc.update(fields_to_update)
        upsert_asset_doc(doc)
    except Exception as e:
        logger.warning(f"Failed to update Cosmos DB: {e}")
        # Continue with SQL update
    
    # Update SQL Database
    try:
        # Build SQL update statement dynamically
        sql_updates = []
        sql_params = {'id': asset_id}
        
        field_mapping = {
            'fileName': 'file_name',
            'fileType': 'file_type',
            'fileSize': 'file_size',
            'blobUrl': 'blob_url',
        }
        
        for field, value in fields_to_update.items():
            if field in field_mapping:
                sql_field = field_mapping[field]
                sql_updates.append(f"{sql_field} = :{sql_field}")
                sql_params[sql_field] = value
            elif field == 'status':
                sql_updates.append("status = :status")
                sql_params['status'] = value
        
        if sql_updates:
            update_query = f"UPDATE file_metadata SET {', '.join(sql_updates)} WHERE id = :id"
            execute_update(update_query, sql_params)
        else:
            logger.warning(f"No SQL fields to update for asset {asset_id}")
    except Exception as e:
        logger.error(f"Failed to update SQL Database: {e}")
        return func.HttpResponse(
            'Failed to update asset',
            status_code=500,
            mimetype='text/plain'
        )
    
    # Fetch updated record to return (merge Cosmos and SQL data)
    try:
        # Try to get from Cosmos DB first
        cosmos_doc = get_asset_doc(asset_id)
        
        # Get from SQL Database
        sql_results = fetch_all_records(
            """
            SELECT 
                id,
                file_name AS fileName,
                file_type AS fileType,
                file_size AS fileSize,
                blob_url AS blobUrl,
                status,
                created_at AS uploadDate
            FROM file_metadata
            WHERE id = :id
            """,
            {'id': asset_id}
        )
        
        # Merge Cosmos and SQL data (Cosmos takes precedence)
        updated_doc = cosmos_doc or {}
        if sql_results:
            updated_doc.update(sql_results[0])
        if not updated_doc:
            updated_doc = {'id': asset_id, **fields_to_update}
    except Exception as e:
        logger.warning(f"Failed to fetch updated record: {e}")
        updated_doc = {'id': asset_id, **fields_to_update}
    
    logger.info(f"Updated asset {asset_id} with fields: {list(fields_to_update.keys())}")
    
    return func.HttpResponse(
        json.dumps(updated_doc),
        status_code=200,
        mimetype='application/json'
    )

