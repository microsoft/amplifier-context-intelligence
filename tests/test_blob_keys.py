"""Pure tests for ``blob_keys`` -- the single logical-name -> physical-name map.

Every expectation below is a hand-derived literal.  The digests were produced
with an independent ``hashlib`` one-liner over
``b"ci-blob-event-key:v1\\x00" + name.encode("utf-8")``; none of them is
computed by the module under test.
"""

from __future__ import annotations

import dataclasses

import pytest

from context_intelligence_server import blob_keys
from context_intelligence_server.blob_keys import (
    BlobAddress,
    address_from_filename,
    component,
    legacy_address,
    parse_address,
    parse_uri,
    read_candidates,
    write_address,
)
from context_intelligence_server.utils import fold_name

ZERO = "0" * 64
DIGEST_A_SLASH_B = "5998a4c7b7280b4eb36c9f88878ed97c34b2cbeb7f138372a02365515c97bde9"
DIGEST_A_BACKSLASH_B = (
    "f3af24d320ab49ad3ecaddd0d48f69cb7c393eb657ffbf1b80416af26a1346b4"
)
DIGEST_A_NEWLINE_B = "97acd576bb4912bc9586a183710a1ee2753dacc4d562fae69f46a9523cc79a93"
DIGEST_A_NUL_B = "73d03db243d6111678a74f7b4a9c4378f9d628339e36b2f55a569d0f64860bd7"
DIGEST_A_DEL_B = "1ff7ea36424f80b894b2ddb54b7c3cb6ce34971b3c68afc560dea4d6cbd54f53"
DIGEST_RAW_MARKER = "4ed35e725865da2e0d3fa8f8369fbc02ab78e80f4a05e1eaaacbb994e9e01ca9"
DIGEST_UPPER = (
    "a5a85a56601cf40815bcece818dd41807a5706cd99f652d9d8918fb139131259"  # "A/B"
)
DIGEST_MIXED = (
    "7c147c66ba3a35e155497eaad43b1db9ca6e8c385719f9c852870a0f8fe273e9"  # "a/B"
)
DIGEST_NFC = "50745a03ff5cffd2b323008ff7aa430e47cd6c9ca0f411f3fc95b0aa28c2544a"
DIGEST_NFD = "07378cf49acb973550204755868f31fa3f07d65f9dacfdd9ca0865ed2ef76377"
DIGEST_NL_K300 = "28850d7540b100a97c4ec4f8fab47e485b826bd1e7185d2942a18e2e44db21a9"
MARKER = "~ci-key-v1"


# --- write_address: ordinary names are unchanged -----------------------------


@pytest.mark.parametrize(
    "name",
    [
        "ordinary",
        "k_1",
        "a%b",
        "a?b",
        "a#b",
        "100%",
        "q?x=1#frag",
        "a%2fb",
        "a%5cb",
        "a%00b",
        "%?#",
    ],
)
def test_ordinary_names_keep_their_exact_uri_and_filename(name: str) -> None:
    addr = write_address("sess", name)

    assert addr == BlobAddress("sess", name)
    assert addr.mapped is False
    assert addr.uri == f"ci-blob://sess/{name}"
    assert addr.filename == f"{name}.json"


def test_a_marker_looking_but_safe_name_stays_ordinary() -> None:
    name = MARKER + "~" + "a" * 64

    addr = write_address("sess", name)

    assert addr.mapped is False
    assert addr.filename == f"{name}.json"


def test_a_name_ending_in_dot_blob_is_an_ordinary_json_file() -> None:
    addr = write_address("sess", "foo.blob")

    assert addr.filename == "foo.blob.json"


# --- write_address: separators and controls map to the flat digest name ------


@pytest.mark.parametrize(
    ("name", "digest"),
    [
        ("a/b", DIGEST_A_SLASH_B),
        ("a\\b", DIGEST_A_BACKSLASH_B),
        ("a\nb", DIGEST_A_NEWLINE_B),
        ("a\x00b", DIGEST_A_NUL_B),
        ("a\x7fb", DIGEST_A_DEL_B),
    ],
)
def test_separator_and_control_names_map_to_a_flat_digest_address(
    name: str, digest: str
) -> None:
    addr = write_address("sess", name)

    assert addr == BlobAddress("sess", digest, mapped=True)
    assert addr.uri == f"ci-blob://sess/{MARKER}/{digest}"
    assert addr.filename == f"{MARKER}~{digest}.blob"
    assert "/" not in addr.filename


def test_mapping_is_deterministic_and_case_sensitive() -> None:
    assert write_address("s", "A/B") == write_address("s", "A/B")
    assert write_address("s", "A/B").name == DIGEST_UPPER
    assert write_address("s", "a/B").name == DIGEST_MIXED
    assert write_address("s", "a/b").name != write_address("s", "A/B").name


def test_mapping_hashes_utf8_without_unicode_normalisation() -> None:
    assert write_address("s", "caf\u00e9/x").name == DIGEST_NFC
    assert write_address("s", "cafe\u0301/x").name == DIGEST_NFD


def test_percent_encoded_separators_are_not_decoded_into_mapping() -> None:
    assert write_address("s", "a%2fb").mapped is False
    assert write_address("s", "a%5cb").mapped is False


def test_a_raw_logical_name_that_looks_like_a_mapped_handle_is_hashed_again() -> None:
    addr = write_address("s", f"{MARKER}/{ZERO}")

    assert addr.mapped is True
    assert addr.name == DIGEST_RAW_MARKER
    assert addr.name != ZERO  # never impersonates the canonical target for ZERO


# --- write_address: length budgets ------------------------------------------


def test_long_ordinary_names_fold_to_the_existing_242_byte_form() -> None:
    addr = write_address("s", "k" * 300)

    assert addr.mapped is False
    assert addr.name == "k" * 225 + "~" + "17b16d8ef494060f"
    assert len(addr.name.encode()) == 242
    assert len(f"{addr.name}.tmp-suffix".encode()) <= 255  # 13-byte temp suffix budget


def test_long_names_with_backslash_keep_the_existing_underscore_fold() -> None:
    addr = write_address("s", "a\\" + "x" * 400)

    assert addr.mapped is False
    assert addr.name == "a_" + "x" * 223 + "~" + "67c29909eca707fd"


def test_long_names_whose_fold_is_still_unsafe_map_to_the_digest() -> None:
    addr = write_address("s", "\n" + "k" * 300)

    assert addr.mapped is True
    assert addr.name == DIGEST_NL_K300


@pytest.mark.parametrize(
    ("logical", "expected_name"),
    [
        # A separator/NUL inside the kept 225-byte prefix is replaced by "_" by the
        # existing fold, so the fold is safe and the name stays ordinary.
        ("a/" + "x" * 400, "a_" + "x" * 223 + "~4d42d46a6b95ff4c"),
        ("a\x00" + "x" * 400, "a_" + "x" * 223 + "~7c10d2023276eb90"),
        # A separator/newline PAST the cut is simply dropped by the fold.
        ("k" * 300 + "/z", "k" * 225 + "~5842fdd431096743"),
        ("k" * 300 + "\n", "k" * 225 + "~5656f61b9f78cb46"),
    ],
    ids=[
        "slash-in-kept-prefix",
        "nul-in-kept-prefix",
        "slash-past-the-cut",
        "newline-past-the-cut",
    ],
)
def test_long_names_whose_fold_is_safe_keep_the_pre_mapping_ordinary_fold(
    logical: str, expected_name: str
) -> None:
    addr = write_address("s", logical)

    assert addr.mapped is False
    assert addr.name == fold_name(logical, 242)  # the unchanged pre-mapping fold
    assert addr.name == expected_name  # independently derived literal
    assert len(addr.name.encode("utf-8")) <= 242
    assert addr.uri == f"ci-blob://s/{expected_name}"
    assert addr.filename == f"{expected_name}.json"


def test_a_mapped_address_leaves_room_for_the_temporary_suffix() -> None:
    addr = write_address("s", "a/b")

    assert len(addr.filename.encode()) == 80
    assert len(f"{MARKER}~{addr.name}.{'t' * 8}.tmp".encode()) == 88


def test_long_session_ids_fold_at_255_bytes() -> None:
    addr = write_address("sess-" + "q" * 500, "k")

    assert len(addr.session.encode()) == 255
    assert "~" in addr.session


# --- write_address: rejection ------------------------------------------------


@pytest.mark.parametrize(
    "bad", ["", ".", "..", ".ci-blob-retention.lock", ".ci-blob-retention"]
)
def test_empty_dot_and_reserved_keys_are_invalid_not_mapped(bad: str) -> None:
    with pytest.raises(ValueError):
        write_address("s", bad)


@pytest.mark.parametrize(
    "bad",
    [
        "",
        ".",
        "..",
        "a/b",
        "a\\b",
        "a\nb",
        "a\x00b",
        "a\x7fb",
        ".ci-blob-retention.lock",
        ".ci-blob-retention",
    ],
)
def test_unsafe_sessions_are_rejected_for_every_key(bad: str) -> None:
    with pytest.raises(ValueError):
        write_address(bad, "key")
    with pytest.raises(ValueError):
        write_address(bad, "a/b")


def test_a_lone_surrogate_key_still_raises_a_utf8_error() -> None:
    with pytest.raises(UnicodeEncodeError):
        write_address("s", "a\ud800/b")


# --- BlobAddress -------------------------------------------------------------


def test_blob_address_is_immutable() -> None:
    addr = BlobAddress("s", "k")

    with pytest.raises(dataclasses.FrozenInstanceError):
        addr.name = "other"  # type: ignore[misc]


def test_blob_address_formats_without_folding() -> None:
    addr = BlobAddress("s", "k" * 300)

    assert addr.name == "k" * 300
    assert addr.filename == "k" * 300 + ".json"


@pytest.mark.parametrize(
    "args",
    [
        ("a/b", "k", False),
        ("", "k", False),
        ("s", "a/b", False),
        ("s", "", False),
        ("s", "..", False),
        ("s", "a\nb", False),
        ("s", ".ci-blob-retention.lock", False),
        ("s", "not-hex", True),
        ("s", "A" * 64, True),
        ("s", "a" * 63, True),
    ],
)
def test_blob_address_rejects_unsafe_parts(args: tuple[str, str, bool]) -> None:
    with pytest.raises(ValueError):
        BlobAddress(*args)


# --- component / parse_uri / compatibility exports ---------------------------


@pytest.mark.parametrize("ok", ["a%b", "a?b", "a#b", "%2f", "a b"])
def test_component_allows_percent_question_and_hash(ok: str) -> None:
    assert component(ok) == ok


@pytest.mark.parametrize("bad", ["", ".", "..", "a/b", "a\\b", "a\x1fb", "a\x7fb"])
def test_component_still_rejects_paths_and_controls(bad: str) -> None:
    with pytest.raises(ValueError):
        component(bad)


def test_retention_module_re_exports_the_same_helpers() -> None:
    from context_intelligence_server import blob_retention

    assert blob_retention.component is blob_keys.component
    assert blob_retention.parse_uri is blob_keys.parse_uri
    assert blob_retention.LOCK == ".ci-blob-retention.lock"
    assert blob_retention.METADATA == ".ci-blob-retention"


def test_parse_uri_returns_ordinary_name_or_mapped_handle() -> None:
    assert parse_uri("ci-blob://s/k%?#") == ("s", "k%?#")
    assert parse_uri(f"ci-blob://s/{MARKER}/{ZERO}") == ("s", f"{MARKER}/{ZERO}")


# --- parse_address ------------------------------------------------------------


def test_parse_address_accepts_exactly_two_ordinary_parts() -> None:
    assert parse_address("ci-blob://s/k") == BlobAddress("s", "k")
    assert parse_address("ci-blob://s%1?2#3/k%2f..%2fx") == BlobAddress(
        "s%1?2#3", "k%2f..%2fx"
    )


def test_parse_address_accepts_the_exact_three_part_mapped_form() -> None:
    assert parse_address(f"ci-blob://s/{MARKER}/{DIGEST_A_SLASH_B}") == BlobAddress(
        "s", DIGEST_A_SLASH_B, mapped=True
    )


@pytest.mark.parametrize(
    "uri",
    [
        "http://s/k",
        "ci-blob://",
        "ci-blob://s",
        "ci-blob://s/",
        "ci-blob:///k",
        "ci-blob://s/a/b",
        f"ci-blob://s/{MARKER}/{DIGEST_A_SLASH_B.upper()}",
        f"ci-blob://s/{MARKER}/{'a' * 63}",
        f"ci-blob://s/{MARKER}/{'a' * 65}",
        f"ci-blob://s/{MARKER}/",
        f"ci-blob://s/other/{ZERO}",
        f"ci-blob://s/{MARKER}/{ZERO}/x",
        f"ci-blob://s/{MARKER}x/{ZERO}",
        "ci-blob://../k",
        "ci-blob://./k",
        "ci-blob://s/..",
        "ci-blob://s/.",
        "ci-blob://s/.ci-blob-retention.lock",
        "ci-blob://.ci-blob-retention/k",
        "ci-blob://s\x00/k",
        "ci-blob://s\nx/k",
        "ci-blob://s\\x/k",
        "ci-blob://s/a\nb",
        "ci-blob://s/a\x00b",
    ],
)
def test_parse_address_rejects_malformed_and_unsafe_uris(uri: str) -> None:
    with pytest.raises(ValueError):
        parse_address(uri)


def test_parse_address_folds_long_ordinary_names_like_the_store() -> None:
    folded = parse_address("ci-blob://s/" + "k" * 300)

    assert folded.name == "k" * 225 + "~" + "17b16d8ef494060f"
    assert parse_address(folded.uri) == folded  # idempotent


def test_parse_address_folds_long_session_ids() -> None:
    addr = parse_address("ci-blob://" + "sess-" + "q" * 500 + "/k")

    assert len(addr.session.encode()) == 255


def test_the_ordinary_and_mapped_uris_of_a_marker_look_alike_never_alias() -> None:
    ordinary = write_address("s", f"{MARKER}~{ZERO}")
    mapped = BlobAddress("s", ZERO, mapped=True)

    assert ordinary.uri == f"ci-blob://s/{MARKER}~{ZERO}"
    assert mapped.uri == f"ci-blob://s/{MARKER}/{ZERO}"
    assert ordinary.filename == f"{MARKER}~{ZERO}.json"
    assert mapped.filename == f"{MARKER}~{ZERO}.blob"
    assert parse_address(ordinary.uri) == ordinary
    assert parse_address(mapped.uri) == mapped
    assert ordinary != mapped


# --- legacy POSIX backslash names ----------------------------------------------


def test_parse_address_accepts_a_legacy_backslash_name_on_posix() -> None:
    assert parse_address("ci-blob://s/a\\b") == BlobAddress("s", "a\\b")


def test_legacy_backslash_is_refused_when_the_platform_has_an_alt_separator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(blob_keys, "_legacy_backslash_supported", lambda: False)

    with pytest.raises(ValueError):
        parse_address("ci-blob://s/a\\b")
    with pytest.raises(ValueError):
        address_from_filename("s", "a\\b.json")
    with pytest.raises(ValueError):
        BlobAddress("s", "a\\b")
    assert legacy_address("s", "a\\b") is None


def test_legacy_backslash_is_still_an_unsafe_session() -> None:
    with pytest.raises(ValueError):
        parse_address("ci-blob://s\\x/k")


# --- address_from_filename ------------------------------------------------------


def test_address_from_filename_recognises_ordinary_json() -> None:
    assert address_from_filename("s", "k%?#.json") == BlobAddress("s", "k%?#")
    assert address_from_filename("s", "foo.blob.json") == BlobAddress("s", "foo.blob")


def test_address_from_filename_recognises_exact_mapped_blob() -> None:
    assert address_from_filename(
        "s", f"{MARKER}~{DIGEST_A_SLASH_B}.blob"
    ) == BlobAddress("s", DIGEST_A_SLASH_B, mapped=True)


def test_address_from_filename_never_folds_or_remaps_the_disk_name() -> None:
    addr = address_from_filename("s", "k" * 300 + ".json")

    assert addr == BlobAddress("s", "k" * 300)
    assert addr is not None and addr.filename == "k" * 300 + ".json"


def test_address_from_filename_accepts_legacy_posix_backslash_names() -> None:
    assert address_from_filename("s", "a\\b.json") == BlobAddress("s", "a\\b")


@pytest.mark.parametrize(
    "filename",
    [
        "readme",
        "k.tmp",
        "k.txt",
        "x.blob",
        f"{MARKER}~{'a' * 63}.blob",
        f"{MARKER}~{'A' * 64}.blob",
        f"{MARKER}~{ZERO}.blob.tmp",
        f"{MARKER}x~{ZERO}.blob",
        f"{MARKER}~{ZERO}.json.blob",
        f"{MARKER}~{ZERO}.{'t' * 8}.tmp",
    ],
)
def test_address_from_filename_returns_none_for_unknown_files(filename: str) -> None:
    assert address_from_filename("s", filename) is None


@pytest.mark.parametrize(
    "filename",
    [
        ".json",
        "..json",
        ".ci-blob-retention.lock.json",
        ".ci-blob-retention.json",
        "a\nb.json",
    ],
)
def test_address_from_filename_raises_for_recognised_but_invalid_ordinary_names(
    filename: str,
) -> None:
    with pytest.raises(ValueError):
        address_from_filename("s", filename)


# --- legacy_address ------------------------------------------------------------------


def test_legacy_address_matches_the_canonical_one_for_safe_names() -> None:
    assert legacy_address("s", "plain%?#") == write_address("s", "plain%?#")
    assert legacy_address("s", "k" * 300) == write_address("s", "k" * 300)
    assert legacy_address("s", "a\\" + "x" * 400) == write_address(
        "s", "a\\" + "x" * 400
    )


def test_legacy_address_is_the_literal_backslash_name_on_posix() -> None:
    assert legacy_address("s", "a\\b") == BlobAddress("s", "a\\b")
    assert legacy_address("s", "a\\b") != write_address("s", "a\\b")


@pytest.mark.parametrize(
    "name",
    [
        "a/b",
        "a\nb",
        "a\x00b",
        "a\x7fb",
        "",
        ".",
        "..",
        ".ci-blob-retention.lock",
        ".ci-blob-retention",
    ],
)
def test_legacy_address_is_none_when_the_old_name_was_never_creatable(
    name: str,
) -> None:
    assert legacy_address("s", name) is None


def test_legacy_address_rejects_an_unsafe_session() -> None:
    with pytest.raises(ValueError):
        legacy_address("a/b", "k")


# --- read_candidates -------------------------------------------------------------------


def test_a_safe_ordinary_uri_has_only_its_own_candidate() -> None:
    assert read_candidates("ci-blob://s/k%?#") == (BlobAddress("s", "k%?#"),)


def test_a_mapped_uri_has_only_its_own_candidate() -> None:
    uri = f"ci-blob://s/{MARKER}/{DIGEST_A_BACKSLASH_B}"

    assert read_candidates(uri) == (
        BlobAddress("s", DIGEST_A_BACKSLASH_B, mapped=True),
    )


def test_a_legacy_backslash_uri_tries_the_old_file_then_the_canonical_one() -> None:
    assert read_candidates("ci-blob://s/a\\b") == (
        BlobAddress("s", "a\\b"),
        BlobAddress("s", DIGEST_A_BACKSLASH_B, mapped=True),
    )


def test_a_long_safe_name_with_underscore_fold_is_not_a_legacy_candidate_pair() -> None:
    # The folded form carries no backslash, so it has exactly one candidate.
    uri = write_address("s", "a\\" + "x" * 400).uri

    assert len(read_candidates(uri)) == 1


def test_candidates_are_unique() -> None:
    for uri in ("ci-blob://s/k", "ci-blob://s/a\\b", f"ci-blob://s/{MARKER}/{ZERO}"):
        got = read_candidates(uri)
        assert len(set(got)) == len(got)
