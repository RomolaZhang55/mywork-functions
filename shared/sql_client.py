"""
SQL Database operations module using SQLAlchemy
"""
import os
from typing import Any, Dict, List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# Global engine instance (singleton)
_db_engine: Optional[Engine] = None


def _construct_connection_string() -> str:
    """
    Build SQL Server connection string from environment variables
    
    Returns:
        SQLAlchemy connection string
    """
    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    encrypt = os.getenv('SQL_ENCRYPT', 'true')
    driver = os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
    
    if not all([server, database, username, password]):
        raise ValueError("SQL connection parameters are incomplete")
    
    # Format driver name for URL (replace spaces with +)
    driver_encoded = driver.replace(' ', '+')
    
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}"
        f"?driver={driver_encoded}&Encrypt={encrypt}&TrustServerCertificate=no"
    )
    
    return connection_string


def get_database_engine() -> Engine:
    """
    Get or create the database engine (singleton pattern)
    
    Returns:
        SQLAlchemy Engine instance
    """
    global _db_engine
    
    if _db_engine is None:
        try:
            conn_str = _construct_connection_string()
            _db_engine = create_engine(
                conn_str,
                pool_pre_ping=True,  # Verify connections before using
                echo=False
            )
        except Exception as e:
            import logging
            logging.error(f"Failed to initialize SQL database engine: {e}")
            raise
    
    return _db_engine


def fetch_all_records(query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute a SELECT query and return all results as dictionaries
    
    Args:
        query: SQL SELECT query string
        parameters: Optional query parameters
        
    Returns:
        List of dictionaries, one per row
    """
    engine = get_database_engine()
    with engine.connect() as connection:
        result = connection.execute(text(query), parameters or {})
        column_names = result.keys()
        rows = result.fetchall()
        
        # Convert rows to dictionaries
        return [dict(zip(column_names, row)) for row in rows]


def execute_update(query: str, parameters: Optional[Dict[str, Any]] = None) -> None:
    """
    Execute an INSERT, UPDATE, or DELETE query
    
    Args:
        query: SQL query string
        parameters: Optional query parameters
    """
    engine = get_database_engine()
    with engine.begin() as connection:
        connection.execute(text(query), parameters or {})

