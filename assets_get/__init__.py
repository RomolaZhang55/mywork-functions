"""
Azure Function to get a single asset by ID
"""
import json
import azure.functions as func

from ..shared.auth import require_api_key, AuthenticationError
from ..shared.logging_utils import create_logger
from ..shared.sql_client import fetch_all_records
from ..shared.cosmos_client import get_asset_doc


logger = create_logger(__name__)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handle GET /api/assets/{id} - Get asset by ID
    
    Returns:
        Asset object or 404 if not found
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
    
    # Get asset ID from route parameters
    asset_id = req.route_params.get('id')
    if not asset_id:
        logger.error("Missing asset ID in route")
        return func.HttpResponse(
            'Asset ID is required',
            status_code=400,
            mimetype='text/plain'
        )
    
    # Get from Cosmos DB and SQL Database (merge results)
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
        result = cosmos_doc or {}
        if sql_results:
            result.update(sql_results[0])
        
        if not result:
            logger.info(f"Asset {asset_id} not found")
            return func.HttpResponse(
                'Not Found',
                status_code=404,
                mimetype='text/plain'
            )
    except Exception as e:
        logger.error(f"Failed to query databases: {e}")
        return func.HttpResponse(
            'Internal server error',
            status_code=500,
            mimetype='text/plain'
        )
    
    logger.info(f"Retrieved asset {asset_id}")
    
    return func.HttpResponse(
        json.dumps(result),
        status_code=200,
        mimetype='application/json'
    )

