from __future__ import annotations

from decimal import Decimal

import pytest

from x_virality.services.snapshots import classify_lookup_error
from x_virality.services.topics import (
    TOPIC_ASSIGNMENT_VERSION,
    TopicPostView,
    canonicalize_external_url,
    deterministic_topic_evidence,
)


@pytest.mark.parametrize(
    "value",
    [
        "https://x.com/example/status/1?utm_source=test",
        "https://mobile.twitter.com/example/status/1",
        "https://user:secret@example.com/article",
        "ftp://example.com/article",
        "https://example.com:invalid/article",
    ],
)
def test_canonicalize_external_url_rejects_non_external_or_ambiguous_identity(
    value: str,
) -> None:
    assert canonicalize_external_url(value) is None


def test_canonicalize_external_url_normalizes_identity_and_removes_tracking() -> None:
    value = "HTTP://WWW.Exämple.com:80/a/./b/../c/?utm_source=x&b=2&fbclid=ignored&a=1#fragment"

    assert canonicalize_external_url(value) == "https://xn--exmple-cua.com/a/c?a=1&b=2"


def test_deterministic_topic_evidence_uses_repost_root_and_canonical_external_url() -> None:
    post = TopicPostView(
        id="post-1",
        text="example",
        conversation_id="conversation-1",
        reply_to_post_id=None,
        quoted_post_id="quote-1",
        reposted_post_id="original-1",
        normalized_text_hash="hash",
        entities={
            "urls": [
                {
                    "unwound_url": "https://x.com/self/status/1",
                    "expanded_url": "https://www.example.com/story?utm_campaign=x&id=7",
                }
            ],
            "hashtags": [{"tag": "AI"}, {"tag": "Agents"}],
        },
        context_annotations=(),
    )

    evidence = deterministic_topic_evidence(post)
    by_kind = {item.kind: item for item in evidence}

    assert [item.strength for item in evidence] == sorted(
        (item.strength for item in evidence), reverse=True
    )
    assert by_kind["original_post"].value == "original-1"
    assert by_kind["original_post"].details == {"source": "reposted_post_id"}
    assert by_kind["external_url"].value == "https://example.com/story?id=7"
    assert by_kind["external_url"].provider == TOPIC_ASSIGNMENT_VERSION
    assert by_kind["entity_signature"].strength == Decimal("0.650000")


def test_single_hashtag_is_not_specific_topic_identity() -> None:
    post = TopicPostView(
        id="reply-1",
        text="example",
        conversation_id=None,
        reply_to_post_id="parent-1",
        quoted_post_id=None,
        reposted_post_id=None,
        normalized_text_hash="hash",
        entities={"hashtags": [{"tag": "AI"}]},
        context_annotations=(),
    )

    assert deterministic_topic_evidence(post) == ()


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        ({"title": "Post is protected"}, "protected"),
        ({"detail": "Tombstone: deleted by author"}, "deleted"),
        ({"title": "Not Found", "detail": "No post exists"}, "unavailable"),
        ({}, "unavailable"),
    ],
)
def test_lookup_error_classification_requires_explicit_terminal_fact(
    error: dict[str, str], expected: str
) -> None:
    assert classify_lookup_error(error) == expected
