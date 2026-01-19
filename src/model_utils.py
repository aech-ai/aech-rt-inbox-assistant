"""Model string parsing utilities for reasoning effort / thinking budget configuration.

Custom syntax for passing model settings inline with the model name:
  provider:model@key=value@key2=value2

Examples:
  openai-responses:gpt-5.2@reasoning_effort=low
  anthropic:claude-sonnet-4@thinking=true
  openai-responses:gpt-5-mini  (no settings, uses defaults)
"""
from typing import Any


def parse_model_string(model_string: str) -> tuple[str, dict[str, Any]]:
    """Parse model string with optional settings.

    Format: provider:model@key=value@key2=value2

    Returns:
        (model_name, settings_dict)
    """
    if "@" not in model_string:
        return model_string, {}

    parts = model_string.split("@")
    model_name = parts[0]
    settings = {}

    for part in parts[1:]:
        if "=" in part:
            key, value = part.split("=", 1)
            # Parse common value types
            if value.lower() == "true":
                settings[key] = True
            elif value.lower() == "false":
                settings[key] = False
            elif value.isdigit():
                settings[key] = int(value)
            else:
                settings[key] = value

    return model_name, settings


def get_model_settings(model_string: str):
    """Get pydantic-ai model_settings from parsed model string.

    Translates our @key=value syntax to pydantic-ai ModelSettings.
    Returns None if no settings specified.
    """
    model_name, settings = parse_model_string(model_string)

    if not settings:
        return None

    # Detect provider and build appropriate settings
    # Setting names = pydantic-ai param names (without provider prefix)

    if model_name.startswith("openai-responses:"):
        from pydantic_ai.models.openai import OpenAIResponsesModelSettings
        kwargs = {}
        if "reasoning_effort" in settings:
            kwargs["openai_reasoning_effort"] = settings["reasoning_effort"]
        if "reasoning_summary" in settings:
            kwargs["openai_reasoning_summary"] = settings["reasoning_summary"]
        return OpenAIResponsesModelSettings(**kwargs) if kwargs else None

    elif model_name.startswith("anthropic:"):
        from pydantic_ai.models.anthropic import AnthropicModelSettings
        kwargs = {}
        if "thinking" in settings:
            thinking_val = settings["thinking"]
            if thinking_val is True:
                kwargs["anthropic_thinking"] = {"type": "enabled", "budget_tokens": 10000}
            elif isinstance(thinking_val, int) and thinking_val > 0:
                kwargs["anthropic_thinking"] = {"type": "enabled", "budget_tokens": thinking_val}
            # False means don't enable thinking (leave unset)
        return AnthropicModelSettings(**kwargs) if kwargs else None

    return None
