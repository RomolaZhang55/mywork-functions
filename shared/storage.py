"""
Azure Blob Storage operations module
"""
import os
from datetime import datetime, timedelta
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
)


def create_blob_client() -> BlobServiceClient:
    """
    Create and return a BlobServiceClient instance
    
    Returns:
        BlobServiceClient configured with connection string
    """
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is required")
    return BlobServiceClient.from_connection_string(connection_string)


def get_storage_container() -> str:
    """
    Get the storage container name from environment
    
    Returns:
        Container name, defaults to 'assets'
    """
    return os.getenv('AZURE_STORAGE_CONTAINER', 'assets')


def create_sas_token(container: str, blob_name: str, expiration_hours: int = 2) -> str:
    """
    Generate a SAS token for blob upload
    
    Args:
        container: Container name
        blob_name: Blob name/path
        expiration_hours: Hours until token expires
        
    Returns:
        SAS token string
    """
    account_name = os.getenv('AZURE_STORAGE_ACCOUNT')
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
    account_key = _parse_connection_string(connection_string).get('AccountKey', '')
    
    if not account_name or not account_key:
        raise ValueError("Storage account name and key are required for SAS generation")
    
    expiry_time = datetime.utcnow() + timedelta(hours=expiration_hours)
    
    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(create=True, write=True, add=True),
        expiry=expiry_time,
    )
    
    return sas_token


def build_blob_url(container: str, blob_name: str) -> str:
    """
    Build the full URL for a blob
    
    Args:
        container: Container name
        blob_name: Blob name/path
        
    Returns:
        Full blob URL
    """
    account_name = os.getenv('AZURE_STORAGE_ACCOUNT')
    if not account_name:
        raise ValueError("AZURE_STORAGE_ACCOUNT environment variable is required")
    return f"https://{account_name}.blob.core.windows.net/{container}/{blob_name}"


def _parse_connection_string(conn_str: str) -> dict:
    """
    Parse Azure Storage connection string into key-value pairs
    
    Args:
        conn_str: Connection string
        
    Returns:
        Dictionary of connection string components
    """
    if not conn_str:
        return {}
    
    parts = {}
    for item in conn_str.split(';'):
        if '=' in item:
            key, value = item.split('=', 1)
            parts[key.strip()] = value.strip()
    
    return parts

