"""Tests de l'enrichissement (TP 5, partie C) et de la détection de liens."""

from src.enrichment.detect_arxiv_links import find_links
from src.enrichment.enrich_arxiv import aggregate_links, build_update_actions, compute_keywords

CORPUS = [
    "Graph neural networks learn node representations for node classification on citation graphs.",
    "We train graph neural networks with message passing for molecular property prediction.",
    "Reinforcement learning agents learn policies for robotic manipulation tasks in simulation.",
    "Deep reinforcement learning for robotic grasping with sparse rewards and simulation.",
    "Diffusion models generate high resolution images from text prompts.",
    "Text to image diffusion models with classifier free guidance generate images.",
    "Large language models follow instructions after reinforcement learning from human feedback.",
    "Instruction tuning of large language models improves zero shot generalization.",
    "Convolutional networks for image classification on ImageNet.",
    "Vision transformers for image classification without convolutions.",
]


def test_compute_keywords_returns_relevant_terms():
    ids = [f"id{i}" for i in range(len(CORPUS))]
    keywords = compute_keywords(ids, CORPUS, top_n=5, min_df=1)
    assert set(keywords) == set(ids)
    assert all(len(v) <= 5 for v in keywords.values())
    assert any("graph" in k for k in keywords["id0"])
    # chaque mot de chaque mot-clé provient bien du texte de l'article
    # (les bigrammes sautent les mots vides : « text to image » → « text image »)
    for i, text in enumerate(CORPUS):
        assert all(w in text.lower() for k in keywords[f"id{i}"] for w in k.split())
    # les mots vides scientifiques sont exclus
    assert not any(k in {"we", "the", "paper"} for kws in keywords.values() for k in kws)


def test_aggregate_links_sums_and_deduplicates():
    links = [
        {"arxiv_id": "1706.03762", "hn_item_id": "1", "hn_score": 100, "hn_comments": 10},
        {"arxiv_id": "1706.03762", "hn_item_id": "2", "hn_score": 50, "hn_comments": 5},
        {"arxiv_id": "1706.03762", "hn_item_id": "2", "hn_score": 50, "hn_comments": 5},  # doublon
        {"arxiv_id": "2005.14165", "hn_item_id": "3", "hn_score": None, "hn_comments": None},
        {"hn_item_id": "4"},
    ]
    agg = aggregate_links(links)
    assert agg["1706.03762"] == {"hn_score": 150, "hn_comments_count": 15, "hn_item_ids": ["1", "2"]}
    assert agg["2005.14165"]["hn_score"] == 0
    assert len(agg) == 2


def test_build_update_actions_are_partial_updates():
    actions = list(build_update_actions(
        {"a": ["graph", "node"], "b": []},
        {"a": {"hn_score": 3, "hn_comments_count": 1, "hn_item_ids": ["9"]}},
        index="arxiv-papers",
    ))
    assert len(actions) == 1                         # "b" n'a rien à mettre à jour
    action = actions[0]
    assert action["_op_type"] == "update" and action["doc_as_upsert"] is False
    assert action["doc"] == {"keywords": ["graph", "node"], "hn_score": 3,
                             "hn_comments_count": 1, "hn_item_ids": ["9"]}


def test_find_links_in_url_and_escaped_text():
    src = {"url": "https://arxiv.org/abs/2301.07041",
           "text": "related: https:&#x2F;&#x2F;arxiv.org&#x2F;abs&#x2F;2005.14165 and arxiv.org/abs/2301.07041"}
    assert find_links(src) == [("2301.07041", "url"), ("2005.14165", "text")]
    assert find_links({"url": None, "text": None}) == []
