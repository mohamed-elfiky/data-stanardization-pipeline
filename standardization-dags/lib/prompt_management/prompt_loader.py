import importlib
import logging
from string import Template
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def load_prompt(prompt_type: str, version: str) -> Optional[str]:
    try:
        module_path = get_prompt_path(prompt_type, version)

        module = importlib.import_module(module_path)

        return module.PROMPT
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to load prompt {prompt_type} version {version}: {str(e)}")
        return None


def load_prompt_with_params(
    prompt_type: str, version: str, params: Dict[str, Any]
) -> Optional[str]:
    prompt = load_prompt(prompt_type, version)

    if prompt is None:
        return None

    try:
        template = Template(prompt)
        return template.substitute(params)
    except KeyError as e:
        logger.error(f"Missing parameter in prompt formatting: {str(e)}")
        return None
    except ValueError as e:
        logger.error(f"Error in template formatting: {str(e)}")
        return None


def get_prompt_path(path: str, version: str) -> str:
    return f"{path}.{version}"
