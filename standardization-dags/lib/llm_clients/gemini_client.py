import logging
import os
from typing import Dict, Any, Type
from google import genai
from google.api_core.exceptions import GoogleAPIError
from google.genai import types

logger = logging.getLogger("gemini_client")


class GeminiClient:
    def __init__(self, config: Dict[str, Any] = None):
        self.generation_config = config.get("generation_config", {})
        self.model = config.get("model", "gemini-2.5-flash-preview-04-17")

        logger.info(f"Initialized Gemini client with model: {self.model}")

        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")

        self.model_client = genai.Client(api_key=api_key)

    def process(self, prompt: str, schema: Type = None) -> str:
        try:
            generation_config = types.GenerateContentConfig(
                temperature=self.generation_config.get("temperature", 0.2),
                top_p=self.generation_config.get("top_p", 0.95),
                top_k=self.generation_config.get("top_k", 40),
                max_output_tokens=self.generation_config.get(
                    "max_output_tokens", 65536
                ),
                response_mime_type=self.generation_config.get(
                    "response_mime_type", "text/plain"
                ),
            )

            if self.generation_config.get("thinking_budget", None) is not None:
                generation_config.thinking_config = types.ThinkingConfig(
                    thinking_budget=self.generation_config.get("thinking_budget")
                )

            response = self.model_client.models.generate_content(
                model=self.model, contents=prompt, config=generation_config
            )

            logger.info(f"Generated response: {response.text}")
            return response.text

        except GoogleAPIError as e:
            logger.error(f"Gemini API error: {str(e)}")
            raise e
        except Exception as e:
            logger.error(f"Error processing prompt: {str(e)}")
            raise e
