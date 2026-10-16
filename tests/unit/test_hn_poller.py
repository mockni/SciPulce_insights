"""Tests du poller Hacker News (TP 3, partie B)."""

from unittest.mock import MagicMock, patch

from src.ingestion import hn_poller
from src.ingestion.hn_poller import classify_post, extract_arxiv_id, prepare_hn_doc, url_domain


def test_extract_arxiv_id_strips_version():
    assert extract_arxiv_id("https://arxiv.org/abs/2301.07041v2") == "2301.07041"
    assert extract_arxiv_id("https://example.com") is None
    assert extract_arxiv_id(None) is None


def test_classify_post():
    assert classify_post({"type": "story", "title": "Show HN: my project", "url": "x"}) == "show_hn"
    assert classify_post({"type": "story", "title": "Ask HN: what do you read?"}) == "ask_hn"
    assert classify_post({"type": "story", "title": "A paper", "url": "https://arxiv.org"}) == "article"
    assert classify_post({"type": "story", "title": "Some text"}) == "text"
    assert classify_post({"type": "job", "title": "We are hiring"}) == "job"


def test_url_domain():
    assert url_domain("https://www.arxiv.org/abs/1") == "arxiv.org"
    assert url_domain(None) is None


def test_prepare_story(hn_story):
    doc = prepare_hn_doc(hn_story)
    assert doc["hn_id"] == "40000001"
    assert doc["relation"] == {"name": "story"}
    assert doc["story_id"] == "40000001"
    assert doc["arxiv_id_linked"] == "1706.03762"
    assert doc["domain"] == "arxiv.org"
    assert doc["score"] == 120
    assert "kids" not in doc


def test_prepare_comment_with_escaped_arxiv_link(hn_comment):
    doc = prepare_hn_doc(hn_comment, story_id="40000001")
    assert doc["relation"] == {"name": "comment", "parent": "40000001"}
    assert doc["parent"] == "40000001"
    assert "score" not in doc                  # un commentaire n'a pas de score
    assert doc["arxiv_id_linked"] == "2005.14165"
    assert doc["_link_source"] == "text"


def test_index_batch_routes_comments_and_creates_links(hn_story, hn_comment):
    es = MagicMock()
    es.indices.exists.return_value = True
    es.mget.return_value = {"docs": [{"_id": "1706.03762", "found": True}, {"_id": "2005.14165", "found": False}]}

    with patch.object(hn_poller.helpers, "bulk", return_value=(4, [])) as bulk:
        count = hn_poller.index_batch(es, [hn_story, hn_comment], story_ids={40000002: "40000001"})

    actions = bulk.call_args[0][1]
    hn_actions = [a for a in actions if a["_index"] == "hn-items"]
    link_actions = [a for a in actions if a["_index"] == "arxiv-hn-links"]
    assert count == 2
    assert len(hn_actions) == 2 and len(link_actions) == 2

    comment_action = next(a for a in hn_actions if a["_id"] == "40000002")
    assert comment_action["_routing"] == "40000001"
    assert "_link_source" not in comment_action["_source"]

    story_link = next(a for a in link_actions if a["_source"]["hn_item_id"] == "40000001")
    assert story_link["_id"] == "1706.03762_40000001"
    assert story_link["_source"]["in_corpus"] is True
    assert story_link["_source"]["link_source"] == "url"


def test_poll_once_skips_already_indexed(hn_story):
    es = MagicMock()
    with patch.object(hn_poller, "fetch_new_story_ids", return_value=[40000001, 40000009]), \
         patch.object(hn_poller, "existing_ids", return_value={"40000009"}), \
         patch.object(hn_poller, "fetch_items", return_value=[hn_story]) as fetch, \
         patch.object(hn_poller, "index_batch", return_value=1) as index_batch:
        total = hn_poller.poll_once(es, limit=10)

    assert total == 1
    fetch.assert_called_once_with([40000001])
    index_batch.assert_called_once()
