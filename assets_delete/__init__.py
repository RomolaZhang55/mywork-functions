"""
Azure Function to delete an asset
"""
import json
import azure.functions as func

from ..shared.auth import require_api_key, AuthenticationError
from ..shared.logging_utils import create_logger
from ..shared.sql_client import execute_update
from ..shared.storage import create_blob_client, get_storage_container
from ..shared.cosmos_client import delete_asset_doc


logger = create_logger(__name__)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handle DELETE /api/assets/{id} - Delete asset and associated blob
    
    Returns:
        {"deleted": true, "id": "uuid"}
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
    
    # Delete blob files from storage
    try:
        blob_client = create_blob_client()
        container_name = get_storage_container()
        container_client = blob_client.get_container_client(container_name)
        
        # Find all blobs with this asset ID prefix
        blobs_to_delete = [
            blob.name 
            for blob in container_client.list_blobs(name_starts_with=f"{asset_id}/")
        ]
        
        # Delete each blob
        for blob_name in blobs_to_delete:
            try:
                container_client.delete_blob(blob_name)
                logger.info(f"Deleted blob: {blob_name}")
            except Exception as e:
                logger.warning(f"Failed to delete blob {blob_name}: {e}")
    except Exception as e:
        logger.error(f"Failed to delete blobs from storage: {e}")
        # Continue with database cleanup
    
    # Delete from SQL Database
    try:
        execute_update(
            "DELETE FROM file_metadata WHERE id = :id",
            {'id': asset_id}
        )
        logger.info(f"Deleted asset {asset_id} from SQL Database")
    except Exception as e:
        logger.error(f"Failed to delete from SQL Database: {e}")
        return func.HttpResponse(
            'Failed to delete asset from database',
            status_code=500,
            mimetype='text/plain'
        )
    
    # Delete from Cosmos DB
    try:
        delete_asset_doc(asset_id)
        logger.info(f"Deleted asset {asset_id} from Cosmos DB")
    except Exception as e:
        logger.warning(f"Failed to delete from Cosmos DB: {e}")
        # Continue even if Cosmos delete fails
    
    response_data = {
        'deleted': True,
        'id': asset_id
    }
    
    logger.info(f"Successfully deleted asset {asset_id}")
    
    return func.HttpResponse(
        json.dumps(response_data),
        status_code=200,
        mimetype='application/json'
    )

