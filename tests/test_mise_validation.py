"""Local HTTP contract tests, not configured-image or token-authenticity proof."""

import base64
import threading
import time
import traceback
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection, HTTPResponse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from context_intelligence_server import mise_validation as m

URI = "https://service.example.test/events?part=%2F&part=two"
IDENTITY = [
    ("Subject-Token-Claim-tid", "tenant"),
    ("Subject-Token-Claim-oid", "subject"),
]


class Sidecar:
    def __init__(self) -> None:
        self.status = 200
        self.headers = list(IDENTITY)
        self.body = b""
        self.ready_statuses = [200]
        self.ready_headers: list[tuple[str, str]] = []
        self.negative_status = 401
        self.negative_headers: list[tuple[str, str]] = []
        self.requests: list[tuple[str, str, list[tuple[str, str]], bytes]] = []
        self.trickle = False
        self.trickle_body = False
        self.stall_rejection = False
        self.stop = threading.Event()
        self.peer_closed = threading.Event()
        self.connections: list[HTTPConnection] = []
        self.responses: list[HTTPResponse] = []


@pytest.fixture
def sidecar(monkeypatch: pytest.MonkeyPatch) -> Iterator[Sidecar]:
    state = Sidecar()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any) -> None:
            pass

        def do_GET(self) -> None:
            self.respond()

        def do_POST(self) -> None:
            self.respond()

        def respond(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            state.requests.append(
                (self.command, self.path, list(self.headers.items()), body)
            )
            if self.path == "/readyz":
                status = state.ready_statuses[0]
                if len(state.ready_statuses) > 1:
                    state.ready_statuses.pop(0)
                headers, payload = state.ready_headers, b""
            elif self.headers.get("Authorization") == "Bearer not-a-jwt":
                status = state.negative_status
                headers, payload = state.negative_headers, b""
            else:
                status, headers, payload = state.status, state.headers, state.body
            try:
                if state.trickle and self.path == "/ValidateRequest":
                    for byte in b"HTTP/1.1 200 OK\r\nX-Slow: never-finished":
                        self.connection.sendall(bytes([byte]))
                        if state.stop.wait(0.04):
                            return
                    return
                self.send_response(status)
                for name, value in headers:
                    self.send_header(name, value)
                if state.trickle_body and status == 200:
                    self.send_header("Connection", "close")
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    # No newline: read(1) waits for a chunk-size line while
                    # getresponse() has already detached connection.sock.
                    while not state.stop.is_set():
                        self.connection.sendall(b"1")
                        if state.stop.wait(0.04):
                            return
                    return
                self.send_header(
                    "Content-Length",
                    "1000000000"
                    if state.stall_rejection and status != 200
                    else str(len(payload)),
                )
                self.end_headers()
                self.wfile.write(payload)
                self.wfile.flush()
                if state.stall_rejection and status != 200:
                    self.connection.settimeout(2)
                    if self.connection.recv(1) == b"":
                        state.peer_closed.set()
            except (OSError, ValueError):
                state.peer_closed.set()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
    thread.start()

    class Connection(HTTPConnection):
        def getresponse(self) -> HTTPResponse:
            response = super().getresponse()
            state.responses.append(response)
            return response

    def connect(host: str, port: int, *, timeout: float) -> HTTPConnection:
        assert (host, port) == ("127.0.0.1", 5000)
        assert 0 < timeout <= 1
        connection = Connection("127.0.0.1", server.server_port, timeout=timeout)
        state.connections.append(connection)
        return connection

    monkeypatch.setattr(m, "HTTPConnection", connect, raising=False)
    try:
        yield state
    finally:
        state.stop.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert all(connection.sock is None for connection in state.connections)
        assert all(response.isclosed() for response in state.responses)


def validate(
    validator: m.MiseTokenValidator, token: str = "opaque-token"
) -> dict[str, Any]:
    return validator.validate(token, original_uri=URI, original_method="PATCH")


def assert_sanitized(error: BaseException, message: str) -> None:
    assert str(error) == message
    assert error.__cause__ is None
    assert error.__suppress_context__
    text = "".join(traceback.format_exception(error))
    assert "opaque-token" not in text
    assert URI not in text
    assert "private diagnostic" not in text


def test_startup_checks_ready_then_negative_contract_once(sidecar: Sidecar) -> None:
    validator = m.MiseTokenValidator()
    assert [(method, path) for method, path, _, _ in sidecar.requests] == [
        ("GET", "/readyz"),
        ("POST", "/ValidateRequest"),
    ]
    negative = dict(sidecar.requests[1][2])
    assert negative["Authorization"] == "Bearer not-a-jwt"
    assert negative["original-uri"] == "http://127.0.0.1:8000/events"
    assert negative["original-method"] == "POST"
    assert validate(validator) == {"tid": "tenant", "oid": "subject"}
    assert len(sidecar.requests) == 3
    assert len(sidecar.connections) == 3


def test_positive_wire_exact_context_and_empty_body(
    sidecar: Sidecar, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://unreachable.example.test:1")
    monkeypatch.setenv("ALL_PROXY", "http://unreachable.example.test:1")
    monkeypatch.setenv("MISE_ENDPOINT", "http://unreachable.example.test:1")
    validator = m.MiseTokenValidator()
    validate(validator)
    method, path, pairs, body = sidecar.requests[-1]
    headers = dict(pairs)
    assert (method, path, body) == ("POST", "/ValidateRequest", b"")
    assert headers["Authorization"] == "Bearer opaque-token"
    assert headers["original-uri"] == URI
    assert headers["original-method"] == "PATCH"
    assert headers["return-all-subject-token-claims"] == "1"
    assert headers["Content-Length"] == "0"
    assert headers["Connection"] == "close"
    assert "Cookie" not in headers


@pytest.mark.parametrize(
    "status", [201, 204, 299, 301, 302, 307, 400, 401, 403, 500, 503]
)
def test_positive_accepts_only_exact_200_no_retry_or_redirect(
    sidecar: Sidecar, status: int
) -> None:
    validator = m.MiseTokenValidator()
    sidecar.status = status
    sidecar.headers.append(("Location", "http://unreachable.example.test/steal"))
    with pytest.raises(m.TokenValidationError) as error:
        validate(validator)
    assert_sanitized(error.value, "Token validation failed")
    assert len(sidecar.requests) == 3


def test_decodes_repeated_plain_and_encoded_subject_claims(sidecar: Sidecar) -> None:
    validator = m.MiseTokenValidator()
    encoded = base64.b64encode("café,東京".encode()).decode("ascii")
    sidecar.headers = [
        ("subject-token-claim-TID", "tenant"),
        ("Subject-Token-Encoded-Claim-OID", "c3ViamVjdA=="),
        ("Subject-Token-Claim-Roles", "Reader,Contributor"),
        ("subject-token-claim-roles", "Reader"),
        ("Subject-Token-Claim-roles", "Reader"),
        ("Subject-Token-Claim-scp", ""),
        ("Subject-Token-Claim-idtyp", ""),
        ("Subject-Token-Claim-APPID", "application"),
        ("Subject-Token-Claim-AZP", "authorized"),
        ("Subject-Token-Encoded-Claim-DisplayName", encoded),
        ("Subject-Token-Claim-custom", ""),
        ("Subject-Token-Claim-custom", "second"),
        ("Actor-Token-Claim-oid", "wrong-identity"),
        ("WWW-Authenticate", "private diagnostic"),
        ("error-description", "private diagnostic"),
    ]
    assert validate(validator) == {
        "tid": "tenant",
        "oid": "subject",
        "roles": ["Reader,Contributor", "Reader", "Reader"],
        "scp": "",
        "idtyp": "",
        "appid": "application",
        "azp": "authorized",
        "DisplayName": "café,東京",
        "custom": ["", "second"],
    }


@pytest.mark.parametrize("claim", ["tid", "oid", "scp", "idtyp", "appid", "azp"])
@pytest.mark.parametrize("encoded", [False, True])
def test_duplicate_singletons_fail_even_if_equal(
    sidecar: Sidecar, claim: str, encoded: bool
) -> None:
    validator = m.MiseTokenValidator()
    value = {"tid": "tenant", "oid": "subject"}.get(claim, "")
    if claim not in {"tid", "oid"}:
        sidecar.headers.append((f"Subject-Token-Claim-{claim}", value))
    prefix = "Subject-Token-Encoded-Claim-" if encoded else "Subject-Token-Claim-"
    duplicate = base64.b64encode(value.encode()).decode() if encoded else value
    sidecar.headers.append((prefix + claim.upper(), duplicate))
    with pytest.raises(m.TokenValidationError):
        validate(validator)


@pytest.mark.parametrize(
    "headers",
    [
        [],
        [("Actor-Token-Claim-oid", "subject")],
        [("Subject-Token-Claim-tid", "tenant")],
        [("Subject-Token-Claim-oid", "subject")],
        [("Subject-Token-Claim-tid", " "), ("Subject-Token-Claim-oid", "subject")],
        [*IDENTITY, ("Subject-Token-Claim-", "")],
        [
            *IDENTITY,
            ("Subject-Token-Claim-Custom", "x"),
            ("Subject-Token-Claim-custom", "x"),
        ],
        [*IDENTITY, ("Subject-Token-Encoded-Claim-extra", "bad padding")],
        [*IDENTITY, ("Subject-Token-Encoded-Claim-extra", "/w==")],
        [*IDENTITY, ("Subject-Token-Encoded-Claim-extra", "YWJj\n")],
        [*IDENTITY, ("Subject-Token-Claim-extra", "one\r\n two")],
        [*IDENTITY, ("Subject-Token-Claim-extra", "one\x7ftwo")],
    ],
)
def test_malformed_or_missing_claims_fail_closed(
    sidecar: Sidecar, headers: list[tuple[str, str]]
) -> None:
    validator = m.MiseTokenValidator()
    sidecar.headers = headers
    with pytest.raises(m.TokenValidationError) as error:
        validate(validator)
    assert_sanitized(error.value, "Token validation failed")


def test_values_are_not_trimmed_or_comma_split(sidecar: Sidecar) -> None:
    validator = m.MiseTokenValidator()
    sidecar.headers = [
        ("Subject-Token-Encoded-Claim-tid", base64.b64encode(b" tenant ").decode()),
        ("Subject-Token-Encoded-Claim-oid", base64.b64encode(b" subject ").decode()),
        ("Subject-Token-Claim-roles", "Reader,Contributor"),
    ]
    assert validate(validator) == {
        "tid": " tenant ",
        "oid": " subject ",
        "roles": ["Reader,Contributor"],
    }


@pytest.mark.parametrize("claim", ["scp", "roles", "oid"])
@pytest.mark.parametrize("control", [chr(code) for code in [*range(32), 127]])
def test_encoded_claim_controls_fail_closed(
    sidecar: Sidecar, claim: str, control: str
) -> None:
    validator = m.MiseTokenValidator()
    value = {"scp": "access_as_user", "roles": "Reader", "oid": "subject"}[claim]
    encoded = base64.b64encode((value + control).encode()).decode("ascii")
    sidecar.headers = [
        (name, value)
        for name, value in IDENTITY
        if name != f"Subject-Token-Claim-{claim}"
    ] + [(f"Subject-Token-Encoded-Claim-{claim}", encoded)]
    with pytest.raises(m.TokenValidationError) as error:
        validate(validator)
    assert_sanitized(error.value, "Token validation failed")
    assert encoded not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize("claim", ["scp", "roles", "oid"])
def test_encoded_claims_preserve_unicode(sidecar: Sidecar, claim: str) -> None:
    validator = m.MiseTokenValidator()
    encoded = base64.b64encode("café,東京".encode()).decode("ascii")
    sidecar.headers = [
        (name, value)
        for name, value in IDENTITY
        if name != f"Subject-Token-Claim-{claim}"
    ] + [(f"Subject-Token-Encoded-Claim-{claim}", encoded)]
    expected: dict[str, Any] = {"tid": "tenant", "oid": "subject"}
    expected[claim] = ["café,東京"] if claim == "roles" else "café,東京"
    assert validate(validator) == expected


def test_positive_body_is_rejected(sidecar: Sidecar) -> None:
    validator = m.MiseTokenValidator()
    sidecar.body = b"not claims"
    with pytest.raises(m.TokenValidationError):
        validate(validator)


def test_response_header_aggregate_limit(sidecar: Sidecar) -> None:
    validator = m.MiseTokenValidator()
    sidecar.headers += [(f"X-Extra-{i}", "x" * 4000) for i in range(17)]
    with pytest.raises(m.TokenValidationError):
        validate(validator)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("bearer", ""),
        ("bearer", "a b"),
        ("bearer", "a\tb"),
        ("bearer", "a\r\nb"),
        ("bearer", "é"),
        ("bearer", "\x7f"),
        ("bearer", "x" * (32 * 1024 + 1)),
        ("bearer", None),
        ("original_uri", ""),
        ("original_uri", "https://test/\r\nX: injected"),
        ("original_uri", "\x00"),
        ("original_uri", "東京"),
        ("original_uri", "u" * (16 * 1024 + 1)),
        ("original_uri", None),
        ("original_method", ""),
        ("original_method", "P OST"),
        ("original_method", "GET\r\nX"),
        ("original_method", "x" * 33),
        ("original_method", None),
    ],
)
def test_invalid_inputs_rejected_before_connection(
    sidecar: Sidecar, field: str, value: Any
) -> None:
    validator = m.MiseTokenValidator()
    kwargs = {"bearer": "opaque-token", "original_uri": URI, "original_method": "PATCH"}
    kwargs[field] = value
    with pytest.raises(m.TokenValidationError):
        validator.validate(**kwargs)
    assert len(sidecar.requests) == 2


@pytest.mark.parametrize(
    ("ready", "negative", "headers"),
    [
        (302, 401, []),
        (204, 401, []),
        (200, 200, []),
        (200, 403, []),
        (200, 401, [("Subject-Token-Claim-oid", "subject")]),
        (200, 401, [("Subject-Token-Encoded-Claim-", "")]),
    ],
)
def test_startup_contract_failure_is_immediate_and_sanitized(
    sidecar: Sidecar, ready: int, negative: int, headers: list[tuple[str, str]]
) -> None:
    sidecar.ready_statuses = [ready]
    sidecar.negative_status = negative
    sidecar.negative_headers = headers
    with pytest.raises(RuntimeError) as error:
        m.MiseTokenValidator()
    assert_sanitized(error.value, "MISE initialization failed")
    assert len(sidecar.requests) == (2 if ready == 200 else 1)


def test_startup_retries_503_until_ready(sidecar: Sidecar) -> None:
    sidecar.ready_statuses = [503, 503, 200]
    m.MiseTokenValidator()
    assert len(sidecar.requests) == 4


def test_startup_refusal_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts: list[float] = []

    def refused(*args: Any, **kwargs: Any) -> HTTPConnection:
        attempts.append(time.monotonic())
        raise ConnectionRefusedError("private diagnostic")

    monkeypatch.setattr(m, "HTTPConnection", refused, raising=False)
    monkeypatch.setattr(m, "_STARTUP_TIMEOUT", 0.15, raising=False)
    monkeypatch.setattr(m, "_POLL_INTERVAL", 0.02, raising=False)
    start = time.monotonic()
    with pytest.raises(RuntimeError) as error:
        m.MiseTokenValidator()
    assert_sanitized(error.value, "MISE initialization failed")
    assert 2 <= len(attempts) < 20
    assert time.monotonic() - start < 0.5


@pytest.mark.parametrize(
    "body", [False, True], ids=["headers", "detached-response-body"]
)
def test_absolute_deadline_stops_trickling_response(
    sidecar: Sidecar, monkeypatch: pytest.MonkeyPatch, body: bool
) -> None:
    validator = m.MiseTokenValidator()
    monkeypatch.setattr(m, "_HTTP_TIMEOUT", 0.15, raising=False)
    sidecar.trickle = not body
    sidecar.trickle_body = body
    previous_threads = set(threading.enumerate())
    start = time.monotonic()
    with pytest.raises(m.TokenValidationError):
        validate(validator)
    assert time.monotonic() - start < 0.6
    assert sidecar.peer_closed.wait(1)
    assert not [
        thread
        for thread in threading.enumerate()
        if isinstance(thread, threading.Timer) and thread not in previous_threads
    ]


def test_denial_closes_without_reading_body(sidecar: Sidecar) -> None:
    validator = m.MiseTokenValidator()
    sidecar.status = 401
    sidecar.stall_rejection = True
    with pytest.raises(m.TokenValidationError):
        validate(validator)
    assert sidecar.peer_closed.wait(1)


@pytest.mark.parametrize("failure", ["start", "cancel", "join"])
@pytest.mark.parametrize("startup", [False, True], ids=["validation", "startup"])
def test_timer_failure_closes_socket_and_response(
    sidecar: Sidecar,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    startup: bool,
) -> None:
    validator = m.MiseTokenValidator()
    timers: list[threading.Timer] = []

    class FaultyTimer(threading.Timer):
        def start(self) -> None:
            timers.append(self)
            if failure == "start":
                raise RuntimeError("private diagnostic")
            super().start()

        def cancel(self) -> None:
            if failure == "cancel":
                raise RuntimeError("private diagnostic")
            super().cancel()

        def join(self, timeout: float | None = None) -> None:
            super().join(timeout)
            if failure == "join":
                raise RuntimeError("private diagnostic")

    monkeypatch.setattr(m, "Timer", FaultyTimer)
    monkeypatch.setattr(m, "_HTTP_TIMEOUT", 0.1)
    try:
        error_type = RuntimeError if startup else m.TokenValidationError
        with pytest.raises(error_type) as error:
            if startup:
                m.MiseTokenValidator()
            else:
                validate(validator)
        message = "MISE initialization failed" if startup else "Token validation failed"
        assert_sanitized(error.value, message)
        assert all(connection.sock is None for connection in sidecar.connections)
        assert all(response.isclosed() for response in sidecar.responses)
        assert all(not timer.is_alive() for timer in timers)
    finally:
        # Keep red runs resource-safe without concealing the assertions above.
        for timer in timers:
            threading.Timer.cancel(timer)
            if timer.ident is not None:
                threading.Timer.join(timer, timeout=2)
            assert not timer.is_alive()
        for response in sidecar.responses:
            response.close()
        for connection in sidecar.connections:
            connection.close()


def test_parallel_requests_have_isolated_connections_and_context(
    sidecar: Sidecar,
) -> None:
    validator = m.MiseTokenValidator()

    def request(index: int) -> dict[str, Any]:
        return validator.validate(
            f"token-{index}", original_uri=f"{URI}&index={index}", original_method="GET"
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(request, range(12)))
    assert results == [{"tid": "tenant", "oid": "subject"}] * 12
    assert len(sidecar.requests) == 14
    contexts = [dict(pairs) for _, _, pairs, _ in sidecar.requests[2:]]
    assert {(h["Authorization"], h["original-uri"]) for h in contexts} == {
        (f"Bearer token-{i}", f"{URI}&index={i}") for i in range(12)
    }


def test_exchange_failure_has_sanitized_traceback(
    sidecar: Sidecar, monkeypatch: pytest.MonkeyPatch
) -> None:
    validator = m.MiseTokenValidator()

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise TimeoutError("private diagnostic opaque-token " + URI)

    monkeypatch.setattr(m, "HTTPConnection", fail)
    with pytest.raises(m.TokenValidationError) as error:
        validate(validator)
    assert_sanitized(error.value, "Token validation failed")


@pytest.mark.parametrize(
    "failure", [ConnectionRefusedError, ConnectionResetError, TimeoutError]
)
def test_startup_retries_only_transient_transport_failures(
    sidecar: Sidecar, monkeypatch: pytest.MonkeyPatch, failure: type[Exception]
) -> None:
    connect = m.HTTPConnection
    attempts: list[bool] = []

    def initially_unavailable(
        host: str, port: int, *, timeout: float
    ) -> HTTPConnection:
        attempts.append(True)
        if len(attempts) == 1:
            raise failure("private diagnostic")
        return connect(host, port, timeout=timeout)

    monkeypatch.setattr(m, "HTTPConnection", initially_unavailable)
    monkeypatch.setattr(m, "_POLL_INTERVAL", 0.001)
    m.MiseTokenValidator()
    assert len(attempts) == 3
    assert len(sidecar.requests) == 2


def test_startup_persistent_503_obeys_overall_deadline(
    sidecar: Sidecar, monkeypatch: pytest.MonkeyPatch
) -> None:
    sidecar.ready_statuses = [503]
    monkeypatch.setattr(m, "_STARTUP_TIMEOUT", 0.15)
    monkeypatch.setattr(m, "_POLL_INTERVAL", 0.02)
    start = time.monotonic()
    with pytest.raises(RuntimeError):
        m.MiseTokenValidator()
    assert 2 <= len(sidecar.requests) < 20
    assert time.monotonic() - start < 0.5


def test_input_byte_limits_allow_exact_boundary(sidecar: Sidecar) -> None:
    validator = m.MiseTokenValidator()
    assert validator.validate(
        "x" * (32 * 1024),
        original_uri="u" * (16 * 1024),
        original_method="x" * 32,
    ) == {"tid": "tenant", "oid": "subject"}


def test_successful_validation_is_not_cached(sidecar: Sidecar) -> None:
    validator = m.MiseTokenValidator()
    assert validate(validator)["oid"] == "subject"
    sidecar.status = 401
    with pytest.raises(m.TokenValidationError):
        validate(validator)
    assert len(sidecar.requests) == len(sidecar.connections) == 4


def test_negative_claims_on_503_fail_immediately(
    sidecar: Sidecar, monkeypatch: pytest.MonkeyPatch
) -> None:
    sidecar.negative_status = 503
    sidecar.negative_headers = [("Subject-Token-Claim-oid", "subject")]
    monkeypatch.setattr(m, "_STARTUP_TIMEOUT", 0.12)
    monkeypatch.setattr(m, "_POLL_INTERVAL", 0.01)
    with pytest.raises(RuntimeError):
        m.MiseTokenValidator()
    assert len(sidecar.requests) == 2


@pytest.mark.parametrize(
    "headers",
    [
        [("X-Folded", "value\r\n continuation")],
        [("X-Control", "value\x7f")],
        [(f"X-Large-{i}", "x" * 4000) for i in range(17)],
    ],
)
def test_malformed_startup_headers_are_not_retried(
    sidecar: Sidecar, headers: list[tuple[str, str]]
) -> None:
    sidecar.ready_headers = headers
    with pytest.raises(RuntimeError) as error:
        m.MiseTokenValidator()
    assert_sanitized(error.value, "MISE initialization failed")
    assert len(sidecar.requests) == 1


def test_startup_denial_body_and_actor_claims_are_ignored(sidecar: Sidecar) -> None:
    sidecar.negative_headers = [
        ("Actor-Token-Claim-oid", "actor"),
        ("WWW-Authenticate", "private diagnostic"),
        ("error-description", "private diagnostic"),
    ]
    sidecar.stall_rejection = True
    m.MiseTokenValidator()
    assert sidecar.peer_closed.wait(1)


def test_stdlib_header_count_limit_is_preserved(sidecar: Sidecar) -> None:
    validator = m.MiseTokenValidator()
    sidecar.headers += [(f"X-Extra-{i}", "x") for i in range(101)]
    with pytest.raises(m.TokenValidationError):
        validate(validator)


@pytest.mark.parametrize(
    "value",
    ["_w==", "abc", "YWJj=", "YWJj==", "YQ===", "YQ", "YWI", "YR==", "YWJ=", "é"],
)
def test_nonstandard_base64_fails_closed(sidecar: Sidecar, value: str) -> None:
    validator = m.MiseTokenValidator()
    sidecar.headers.append(("Subject-Token-Encoded-Claim-extra", value))
    with pytest.raises(m.TokenValidationError) as error:
        validate(validator)
    assert_sanitized(error.value, "Token validation failed")


@pytest.mark.parametrize(
    ("value", "expected"), [("", ""), ("YQ==", "a"), ("YWI=", "ab"), ("YWJj", "abc")]
)
def test_canonical_base64_preserves_values(
    sidecar: Sidecar, value: str, expected: str
) -> None:
    validator = m.MiseTokenValidator()
    sidecar.headers.append(("Subject-Token-Encoded-Claim-extra", value))
    assert validate(validator) == {"tid": "tenant", "oid": "subject", "extra": expected}
