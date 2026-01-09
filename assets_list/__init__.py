"""
Azure Function to list all assets
"""
import json
import azure.functions as func

from ..shared.auth import require_api_key, AuthenticationError
from ..shared.logging_utils import create_logger
from ..shared.sql_client import fetch_all_records


logger = create_logger(__name__)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handle GET /api/assets - List all assets
    
    Returns:
        Array of asset objects
    """
    # Authenticate request
    try:
        api_key = req.headers.get('x-api-key')
        require_api_key(api_key)
    except AuthenticationError:
        logger.warning("Unauthorized access attempt to list assets")
        return func.HttpResponse(
            'Unauthorized',
            status_code=401,
            mimetype='text/plain'
        )
    
    # Query all assets from SQL Database
    try:
        assets = fetch_all_records(
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
            ORDER BY created_at DESC
            """
        )
    except Exception as e:
        logger.error(f"Failed to fetch assets from database: {e}")
        return func.HttpResponse(
            'Internal server error',
            status_code=500,
            mimetype='text/plain'
        )
    
    logger.info(f"Retrieved {len(assets)} assets")
    
    return func.HttpResponse(
        json.dumps(assets),
        status_code=200,
        mimetype='application/json'
    )

