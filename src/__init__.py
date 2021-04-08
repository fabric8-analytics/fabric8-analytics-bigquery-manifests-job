"""The source code for big query manifest jo."""
import logging
from src.config.settings import SETTINGS

# Set root logger format for uniform log format.
logging.basicConfig(level=SETTINGS.logging_level,
                    format='[%(asctime)s] %(levelname)s in %(pathname)s:%(lineno)d: %(message)s')
