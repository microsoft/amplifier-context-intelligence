"""Event handlers for the context-intelligence server.

Three handlers are registered in this package:

- DefaultHandler    — creates Event nodes for ALL events, unconditional
- SessionHandler    — enricher: owns Session node lifecycle for start/fork/end
- ToolCallHandler   — enricher: owns ToolCall lifecycle for tool:pre/post/error
                      (not imported yet — added in Phase 2 Task 6)
"""

from context_intelligence_server.handlers.default import DefaultHandler
from context_intelligence_server.handlers.session import SessionHandler

__all__ = [
    "DefaultHandler",
    "SessionHandler",
]
