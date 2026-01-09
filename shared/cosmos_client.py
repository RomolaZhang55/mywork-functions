import os
from typing import Dict, Any, Optional
from azure.cosmos import CosmosClient, PartitionKey, exceptions


_CONTAINER = None

def get_container():
    global _CONTAINER
    if _CONTAINER is None:
        try:
            endpoint = os.getenv('COSMOS_ENDPOINT')
            key = os.getenv('COSMOS_KEY')
            if not endpoint or not key:
                raise ValueError("COSMOS_ENDPOINT and COSMOS_KEY must be set")
            db_name = os.getenv('COSMOS_DB_NAME', 'media-platform')
            container_name = os.getenv('COSMOS_CONTAINER', 'assets')
            client = CosmosClient(endpoint, key)
            db = client.create_database_if_not_exists(db_name)
            _CONTAINER = db.create_container_if_not_exists(
                id=container_name,
                partition_key=PartitionKey(path="/id"),
                offer_throughput=400,
            )
        except Exception as e:
            import logging
            logging.error(f"Failed to initialize Cosmos DB container: {e}")
            raise
    return _CONTAINER


def upsert_asset_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    container = get_container()
    return container.upsert_item(doc)


def get_asset_doc(asset_id: str) -> Optional[Dict[str, Any]]:
    container = get_container()
    try:
        return container.read_item(item=asset_id, partition_key=asset_id)
    except exceptions.CosmosResourceNotFoundError:
        return None


def delete_asset_doc(asset_id: str) -> None:
    container = get_container()
    try:
        container.delete_item(item=asset_id, partition_key=asset_id)
    except exceptions.CosmosResourceNotFoundError:
        pass



