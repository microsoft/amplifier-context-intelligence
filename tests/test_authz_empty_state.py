"""TB-7 (adversarial review) — capability-route boot guard + empty-state pins.

The "third option" adjudicated by the council: rather than the full
``_is_write_capable`` semantic change (deferred to Step 3 — it touches the M2
matrix, ~1200 tests), this file provides

  1. Coverage of the NEW structural boot guard
     ``_assert_capability_routes_not_exempt`` (main.py), which refuses to boot if
     any ``require_read``/``require_write`` route is auth-exempt (those deps fail
     OPEN on unpopulated request state).

  2. Characterization / PIN tests of the ACTUAL current empty-state authz matrix,
     so the fail-open behaviour is DOCUMENTED, not assumed:
       - ``require_admin`` default-DENIES on empty scope state (403) — asserted
         directly (passes today).
       - ``require_read`` / ``require_write`` default-ALLOW on empty scope state
         today. These are written as STRICT xfail tests asserting the DESIRED
         (fail-closed) behaviour, so when Step 3 tightens ``_is_write_capable``
         they flip XFAIL -> XPASS and FAIL the suite — a tripwire that forces
         removal of the xfail marker and makes the Step-3 fix un-skippable.

Isolation-only: no server, no Azure, no Neo4j, no network.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import Depends, FastAPI, HTTPException

from context_intelligence_server.authz import (
    require_read,
    require_write,
)
from context_intelligence_server.routers.admin import require_admin


# ---------------------------------------------------------------------------
# 1. Boot guard: _assert_capability_routes_not_exempt
# ---------------------------------------------------------------------------


def test_boot_guard_passes_on_real_app() -> None:
    """POSITIVE: the guard must not raise on the real, correctly-wired app.

    ``create_asgi_app`` calls ``_assert_capability_routes_not_exempt`` during
    construction; if any require_read/require_write route were exempt today it
    would raise here. (conftest sets allow_unauthenticated=True so the app boots
    without credentials.)
    """
    from context_intelligence_server.main import create_asgi_app

    # No exception == guard passed. Constructing the app exercises the guard.
    create_asgi_app()


def test_boot_guard_raises_when_capability_route_is_exempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NEGATIVE: a require_read route placed in the active exempt set -> RuntimeError.

    We do NOT weaken the real exempt sets. Instead we build a synthetic FastAPI
    app with a single ``/leaky`` route gated by ``require_read``, monkeypatch
    ``main._app`` reference used by the guard to point at it, and monkeypatch the
    auth module's exempt set to include ``/leaky``. The guard must raise and name
    the route, the dependency, and the exempt match.
    """
    import context_intelligence_server.auth as auth_module
    import context_intelligence_server.main as main_module
    from context_intelligence_server.config import Settings

    synthetic = FastAPI()

    @synthetic.get("/leaky", dependencies=[Depends(require_read)])
    async def _leaky() -> dict[str, str]:  # pragma: no cover - never called
        return {"ok": "no"}

    # Point the guard at the synthetic app and make /leaky exempt.
    monkeypatch.setattr(main_module, "app", synthetic)
    monkeypatch.setattr(
        auth_module,
        "_EXEMPT_PATHS",
        frozenset({"/leaky"}),
    )

    settings = Settings(web_ui_enabled=True, allow_unauthenticated=True)

    with pytest.raises(RuntimeError) as exc_info:
        main_module._assert_capability_routes_not_exempt(settings)

    msg = str(exc_info.value)
    assert "/leaky" in msg
    assert "require_read" in msg
    assert "TB-7" in msg


def test_boot_guard_raises_when_capability_route_matched_by_exempt_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NEGATIVE (prefix arm): a require_write route swallowed by an exempt PREFIX."""
    import context_intelligence_server.auth as auth_module
    import context_intelligence_server.main as main_module
    from context_intelligence_server.config import Settings

    synthetic = FastAPI()

    @synthetic.post("/static/danger", dependencies=[Depends(require_write)])
    async def _danger() -> dict[str, str]:  # pragma: no cover - never called
        return {"ok": "no"}

    monkeypatch.setattr(main_module, "app", synthetic)
    # /static/ is a real exempt prefix; the synthetic route lives under it.
    settings = Settings(web_ui_enabled=True, allow_unauthenticated=True)

    with pytest.raises(RuntimeError) as exc_info:
        main_module._assert_capability_routes_not_exempt(settings)

    msg = str(exc_info.value)
    assert "/static/danger" in msg
    assert "require_write" in msg
    assert "/static/" in msg
    # Sanity: the real prefix tuple was used, untouched.
    assert "/static/" in auth_module._EXEMPT_PREFIXES


# ---------------------------------------------------------------------------
# 2. Characterization / PIN tests of the empty-state authz matrix
# ---------------------------------------------------------------------------


def _fake_request(state: dict[str, Any] | None, app_state: Any) -> Any:
    """Build a minimal object exposing ``.scope`` and ``.app.state`` for the deps.

    When ``state`` is None the ``state`` key is absent from scope entirely (the
    strongest "unpopulated" case); otherwise it is set to the given dict.
    """
    scope: dict[str, Any] = {} if state is None else {"state": state}
    return SimpleNamespace(scope=scope, app=SimpleNamespace(state=app_state))


# Entra-mode app.state with roles configured — represents a fully-configured
# server, so a DENY on empty scope state is purely the default-deny property,
# not a missing-config artifact.
_ENTRA_APP_STATE = SimpleNamespace(
    auth_mode="entra",
    entra_admin_role="IdentityAdmin",
    service_data_role="Contributor",
    reader_role="Reader",
    admin_api_key_configured=False,
)


def test_require_admin_denies_on_empty_scope_state() -> None:
    """PIN (passes today): require_admin default-DENIES (403) on empty state.

    This is the desired fail-closed property; require_admin already has it
    because is_admin defaults to False.
    """
    req = _fake_request({}, _ENTRA_APP_STATE)
    with pytest.raises(HTTPException) as exc_info:
        require_admin(req)
    assert exc_info.value.status_code == 403


def test_require_admin_denies_on_absent_state_key() -> None:
    """PIN: same, but the ``state`` key is absent from scope entirely."""
    req = _fake_request(None, _ENTRA_APP_STATE)
    with pytest.raises(HTTPException) as exc_info:
        require_admin(req)
    assert exc_info.value.status_code == 403


@pytest.mark.xfail(
    reason=(
        "TB-7: require_write fails OPEN on unpopulated scope state "
        "(_is_write_capable defaults is_service->False->write-capable); "
        "fail-closed tightening deferred to Step 3 (see docs/14). When Step 3 "
        "tightens _is_write_capable this flips XFAIL->XPASS and FAILs the suite, "
        "forcing removal of this marker."
    ),
    strict=True,
)
def test_require_write_should_deny_on_empty_scope_state() -> None:
    """DESIRED (fails today -> XFAIL): require_write should DENY on empty state."""
    req = _fake_request({}, _ENTRA_APP_STATE)
    with pytest.raises(HTTPException):
        require_write(req)


@pytest.mark.xfail(
    reason=(
        "TB-7: require_read fails OPEN on unpopulated scope state "
        "(_is_write_capable defaults is_service->False->write-capable); "
        "fail-closed tightening deferred to Step 3 (see docs/14). When Step 3 "
        "tightens _is_write_capable this flips XFAIL->XPASS and FAILs the suite, "
        "forcing removal of this marker."
    ),
    strict=True,
)
def test_require_read_should_deny_on_empty_scope_state() -> None:
    """DESIRED (fails today -> XFAIL): require_read should DENY on empty state."""
    req = _fake_request({}, _ENTRA_APP_STATE)
    with pytest.raises(HTTPException):
        require_read(req)
