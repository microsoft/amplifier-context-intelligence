"""Event handlers for the context-intelligence server.

Handlers are organised by architectural layer:

- ``data_layer_1/`` — raw event capture: DefaultHandler and all FieldLifter classes
- ``data_layer_2/`` — semantic enrichment: SessionHandler, ToolCallHandler, and
  all future enrichers

The shared ``EventHandler`` protocol and ``HookResult`` live at
``context_intelligence_server.protocol`` (not inside this package).

New event handlers always land in ``data_layer_2/``, never at this package root.
"""
