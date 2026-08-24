"""Out-of-band graph rectification tools.

Nothing in this package is imported by the server at startup or on the
request path -- see ``run.py`` for the CORE PRINCIPLE this package exists to
honor (migrations run OUT-OF-BAND, never in the server's critical path).
"""
