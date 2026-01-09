"""
Logging utilities with Application Insights integration
"""
import logging
import os

# Try to import Application Insights dependencies
try:
    from opencensus.ext.azure.log_exporter import AzureLogHandler
    from opencensus.ext.azure.trace_exporter import AzureExporter
    from opencensus.trace import config_integration
    from opencensus.trace.samplers import ProbabilitySampler
    from opencensus.trace.tracer import Tracer
    
    # Configure integrations
    config_integration.trace_integrations(['requests', 'logging'])
    HAS_APP_INSIGHTS = True
except ImportError:
    HAS_APP_INSIGHTS = False


def create_logger(module_name: str) -> logging.Logger:
    """
    Create a logger with console and Application Insights handlers
    
    Args:
        module_name: Name of the module/logger
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(module_name)
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    # Console handler with formatting
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Application Insights handler (if available)
    if HAS_APP_INSIGHTS:
        app_insights_connection = (
            os.getenv('APPLICATIONINSIGHTS_CONNECTION_STRING') or
            os.getenv('APPINSIGHTS_INSTRUMENTATIONKEY')
        )
        
        if app_insights_connection:
            try:
                ai_handler = AzureLogHandler(connection_string=app_insights_connection)
                logger.addHandler(ai_handler)
            except Exception as e:
                logging.warning(f"Could not initialize Application Insights: {e}")
    
    # Set log level from environment
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    return logger


def create_tracer(service_name: str = 'media-platform') -> Optional[object]:
    """
    Create a distributed tracing tracer for Application Insights
    
    Args:
        service_name: Name of the service
        
    Returns:
        Tracer instance or None if not available
    """
    if not HAS_APP_INSIGHTS:
        return None
    
    app_insights_connection = (
        os.getenv('APPLICATIONINSIGHTS_CONNECTION_STRING') or
        os.getenv('APPINSIGHTS_INSTRUMENTATIONKEY')
    )
    
    if not app_insights_connection:
        return None
    
    try:
        exporter = AzureExporter(connection_string=app_insights_connection)
        sampler = ProbabilitySampler(rate=1.0)  # Sample all traces
        tracer = Tracer(exporter=exporter, sampler=sampler)
        return tracer
    except Exception:
        return None

