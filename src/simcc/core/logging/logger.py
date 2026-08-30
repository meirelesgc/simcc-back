import structlog

from simcc.core.logging.config import configure_logging

# Configure logging immediately upon module load
configure_logging()

# Expose the configured structlog bound logger
logger = structlog.get_logger()
