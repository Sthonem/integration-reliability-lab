import time

import pytest

from irlab.signing import HEADER_SIGNATURE, HEADER_TIMESTAMP, SignatureError, sign_headers, verify

SECRET = "test-secret-not-real"
BODY = b'{"id":"evt_1","amount":100}'


def _verify(headers, body=BODY, secrets=(SECRET,), now=None):
    return verify(headers, body, list(secrets), tolerance_s=300, now=now)


def test_roundtrip_returns_message_id():
    headers = sign_headers(SECRET, "evt_1", BODY)
    assert _verify(headers) == "evt_1"


def test_signature_covers_body():
    headers = sign_headers(SECRET, "evt_1", BODY)
    with pytest.raises(SignatureError) as exc:
        _verify(headers, body=BODY.replace(b"100", b"999"))
    assert (exc.value.code, exc.value.http_status) == ("invalid_signature", 401)


def test_signature_covers_message_id_and_timestamp():
    headers = sign_headers(SECRET, "evt_1", BODY)
    with pytest.raises(SignatureError):
        _verify({**headers, "webhook-id": "evt_2"})
    with pytest.raises(SignatureError):
        _verify({**headers, HEADER_TIMESTAMP: str(int(headers[HEADER_TIMESTAMP]) + 1)})


def test_wrong_secret_rejected():
    headers = sign_headers("another-secret", "evt_1", BODY)
    with pytest.raises(SignatureError) as exc:
        _verify(headers)
    assert exc.value.code == "invalid_signature"


@pytest.mark.parametrize("skew", [-301, 301])
def test_timestamp_outside_tolerance_rejected(skew):
    ts = int(time.time())
    headers = sign_headers(SECRET, "evt_1", BODY, timestamp=ts)
    with pytest.raises(SignatureError) as exc:
        _verify(headers, now=ts + skew)
    assert (exc.value.code, exc.value.http_status) == ("timestamp_out_of_tolerance", 401)


def test_timestamp_inside_tolerance_accepted():
    ts = int(time.time())
    headers = sign_headers(SECRET, "evt_1", BODY, timestamp=ts)
    assert _verify(headers, now=ts + 299) == "evt_1"


def test_rotation_any_configured_secret_verifies():
    old_signed = sign_headers("old-secret", "evt_1", BODY)
    new_signed = sign_headers("new-secret", "evt_1", BODY)
    assert _verify(old_signed, secrets=["new-secret", "old-secret"]) == "evt_1"
    assert _verify(new_signed, secrets=["new-secret", "old-secret"]) == "evt_1"


def test_dual_signed_header_verifies_with_either_secret():
    headers = sign_headers(["new-secret", "old-secret"], "evt_1", BODY)
    assert len(headers[HEADER_SIGNATURE].split()) == 2
    assert _verify(headers, secrets=["old-secret"]) == "evt_1"
    assert _verify(headers, secrets=["new-secret"]) == "evt_1"


@pytest.mark.parametrize(
    "mutate, code",
    [
        (lambda h: h.pop("webhook-id"), "missing_signature_headers"),
        (lambda h: h.pop(HEADER_TIMESTAMP), "missing_signature_headers"),
        (lambda h: h.pop(HEADER_SIGNATURE), "missing_signature_headers"),
        (lambda h: h.__setitem__(HEADER_TIMESTAMP, "yesterday"), "invalid_timestamp"),
        (lambda h: h.__setitem__(HEADER_SIGNATURE, "v0,abc"), "invalid_signature"),
        (lambda h: h.__setitem__(HEADER_SIGNATURE, "v1,ğüşçö-non-ascii"), "invalid_signature"),
    ],
)
def test_malformed_headers(mutate, code):
    headers = sign_headers(SECRET, "evt_1", BODY)
    mutate(headers)
    with pytest.raises(SignatureError) as exc:
        _verify(headers)
    assert exc.value.code == code


def test_no_secrets_configured_never_verifies():
    headers = sign_headers(SECRET, "evt_1", BODY)
    with pytest.raises(SignatureError):
        _verify(headers, secrets=[])
