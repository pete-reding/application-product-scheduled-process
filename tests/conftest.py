"""
conftest.py
-----------
Shared pytest fixtures and mocks for the product_normalizer test suite.

All tests run entirely offline — no MotherDuck connection required.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


# ── Environment stub ──────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Inject a fake MOTHERDUCK_TOKEN so Settings() can be instantiated."""
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "test_token_fixture")
    monkeypatch.setenv("FUZZY_THRESHOLD", "72")
    monkeypatch.setenv("MIN_TOKEN_LENGTH", "2")


# ── Catalog / reference data stubs ───────────────────────────────────────────

SAMPLE_CATALOG = [
    {
        "product_id": "P001",
        "product_name": "Roundup PowerMAX",
        "category": "herbicide",
        "npk_analysis": None,
    },
    {
        "product_id": "P002",
        "product_name": "Atrazine 90DF",
        "category": "herbicide",
        "npk_analysis": None,
    },
    {
        "product_id": "P003",
        "product_name": "Headline AMP",
        "category": "fungicide",
        "npk_analysis": None,
    },
    {
        "product_id": "P004",
        "product_name": "UAN 28-0-0",
        "category": "fertilizer",
        "npk_analysis": "28-0-0",
    },
    {
        "product_id": "P005",
        "product_name": "Warrior II",
        "category": "insecticide",
        "npk_analysis": None,
    },
]

SAMPLE_ABBREVIATIONS = {
    "RU": "Roundup",
    "ATR": "Atrazine",
    "HEADAMP": "Headline AMP",
    "UAN": "UAN 28-0-0",
}

SAMPLE_EXACT_MAP = {
    "ROUNDUP": {
        "raw_text": "roundup",
        "product_id": "P001",
        "normalized_name": "Roundup PowerMAX",
        "category": "herbicide",
    },
}

SAMPLE_CUSTOM_RULES = [
    {
        "pattern": r"(?i)^roundup\s+[\d\.]+\s*(oz|qt|pt|gal)?$",
        "normalized_name": "Roundup PowerMAX",
        "product_id": "P001",
        "category": "herbicide",
        "notes": "Rate-embedded roundup",
    }
]


@pytest.fixture()
def patch_reference_data():
    """
    Patch the private reference-data globals in matchers so tests never
    touch MotherDuck.
    """
    with (
        patch("product_normalizer.matchers._catalog", SAMPLE_CATALOG),
        patch("product_normalizer.matchers._abbreviations", SAMPLE_ABBREVIATIONS),
        patch("product_normalizer.matchers._exact_map", SAMPLE_EXACT_MAP),
        patch("product_normalizer.matchers._custom_rules", SAMPLE_CUSTOM_RULES),
    ):
        yield


@pytest.fixture()
def mock_db_query():
    """Patch db.query to return an empty list by default."""
    with patch("product_normalizer.db.query", return_value=[]) as m:
        yield m


@pytest.fixture()
def mock_db_execute():
    """Patch db.execute to be a no-op."""
    with patch("product_normalizer.db.execute", return_value=MagicMock()) as m:
        yield m
