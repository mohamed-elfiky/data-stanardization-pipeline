import os
import tomli
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def load_config(config_path: str, section: Optional[str] = None) -> Dict[str, Any]:
    try:
        with open(config_path, "rb") as f:
            config = tomli.load(f)

        if section is not None:
            if section not in config:
                logger.warning(
                    f"Section {section} not found in config, returning empty dict"
                )
                return {}
            return config[section]

        return config
    except Exception as e:
        logger.error(f"Error loading config from {config_path}: {str(e)}")
        return {}
