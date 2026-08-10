from __future__ import annotations

from x_virality.logging import redact_secrets


def test_redaction_covers_nested_keys_bearer_values_and_url_credentials() -> None:
    result = redact_secrets(
        None,  # type: ignore[arg-type]
        "info",
        {
            "token": "abc",
            "nested": {"authorization": "Bearer abc.def"},
            "error": (
                "failed postgresql+psycopg://user:password@db:5432/app with Bearer another-token"
            ),
        },
    )

    assert result["token"] == "[redacted]"
    assert result["nested"] == {"authorization": "[redacted]"}
    assert result["error"] == (
        "failed postgresql+psycopg://user:[redacted]@db:5432/app with Bearer [redacted]"
    )
