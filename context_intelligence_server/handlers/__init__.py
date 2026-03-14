"""Event handlers for the context-intelligence server.

Seven handlers are registered in this package:

- DefaultHandler        — catches all unclaimed, non-excluded events
- OrchestratorRunHandler — owns orchestrator_run lifecycle events
- RecipeHandler         — owns recipe lifecycle events
- SessionHandler        — owns Session node lifecycle events (start/fork/end)
- StepHandler           — owns step lifecycle events
- SystemEventHandler    — owns known system events (compaction, cancellation)
- ToolExecutionHandler  — owns tool_execution lifecycle events
"""

from context_intelligence_server.handlers.default import DefaultHandler
from context_intelligence_server.handlers.event import SystemEventHandler
from context_intelligence_server.handlers.orchestrator_run import OrchestratorRunHandler
from context_intelligence_server.handlers.recipe import RecipeHandler
from context_intelligence_server.handlers.session import SessionHandler
from context_intelligence_server.handlers.step import StepHandler
from context_intelligence_server.handlers.tool_execution import ToolExecutionHandler

__all__ = [
    "DefaultHandler",
    "OrchestratorRunHandler",
    "RecipeHandler",
    "SessionHandler",
    "StepHandler",
    "SystemEventHandler",
    "ToolExecutionHandler",
]
