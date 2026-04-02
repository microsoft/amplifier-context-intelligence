"""Event handlers for the context-intelligence server.

Three handlers are registered in this package:

- DefaultHandler    — creates Event nodes for ALL events (unconditional)
- SessionHandler    — enricher: owns Session node lifecycle (start/fork/end)
- ToolCallHandler   — enricher: owns ToolCall lifecycle (tool:pre/post/error)
"""

from context_intelligence_server.handlers.data_layer_1.default import DefaultHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.handlers.tool_call import ToolCallHandler

__all__ = [
    "DefaultHandler",
    "SessionHandler",
    "ToolCallHandler",
]
