from pydantic import BaseModel, Field
from typing import Literal


class ChatRequest(BaseModel):
    """
    Raw user input received from API / UI.
    """
    message: str = Field(
        ...,
        min_length=1,
        description="Raw user message"
    )


class ToolDecision(BaseModel):
    """
    Output of the LLM router.

    tool meanings:
    - chat         → greetings / small talk (direct LLM reply)
    - adx          → database-related query
    - out_of_scope → non-DB but meaningful question (formatter reply)
    """

    tool: Literal["chat", "adx", "out_of_scope"] = Field(
        ...,
        description="Routing decision"
    )

    query_goal: str = Field(
        ...,
        description=(
            "Clean query intent for ADX. "
            "Must be empty for chat or out_of_scope."
        )
    )
