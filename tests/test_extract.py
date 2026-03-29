"""
test_extract.py
---------------
Unit tests for the CDC extraction and JSON parsing logic in extract.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from product_normalizer.extract import (
    _parse_candidates,
    extract_product_candidates,
    max_published_at,
)


# ── _parse_candidates ─────────────────────────────────────────────────────────

class TestParseCandidates:
    def test_top_level_product(self):
        raw = '{"product": "Roundup PowerMAX"}'
        result = _parse_candidates(raw)
        assert result == ["Roundup PowerMAX"]

    def test_tank_mix_array(self):
        raw = '{"tankMix": [{"name": "Roundup"}, {"name": "Atrazine"}]}'
        result = _parse_candidates(raw)
        assert result == ["Roundup", "Atrazine"]

    def test_combined_product_and_tank_mix(self):
        raw = '{"product": "Prefix", "tankMix": [{"name": "Roundup"}, {"name": "Atrazine"}]}'
        result = _parse_candidates(raw)
        assert "Prefix" in result
        assert "Roundup" in result
        assert "Atrazine" in result
        assert len(result) == 3

    def test_empty_product_string_skipped(self):
        raw = '{"product": "   "}'
        result = _parse_candidates(raw)
        assert result == []

    def test_empty_tank_mix_name_skipped(self):
        raw = '{"tankMix": [{"name": ""}, {"name": "Roundup"}]}'
        result = _parse_candidates(raw)
        assert result == ["Roundup"]

    def test_invalid_json_returns_empty(self):
        result = _parse_candidates("not valid json {{")
        assert result == []

    def test_null_input_returns_empty(self):
        result = _parse_candidates(None)  # type: ignore[arg-type]
        assert result == []

    def test_tank_mix_missing_name_key_skipped(self):
        raw = '{"tankMix": [{"product_name": "Roundup"}, {"name": "Atrazine"}]}'
        result = _parse_candidates(raw)
        assert result == ["Atrazine"]

    def test_no_product_no_tank_mix(self):
        raw = '{"application_rate": 1.5, "unit": "gal/acre"}'
        result = _parse_candidates(raw)
        assert result == []

    def test_whitespace_stripped(self):
        raw = '{"product": "  Roundup  "}'
        result = _parse_candidates(raw)
        assert result == ["Roundup"]


# ── extract_product_candidates ────────────────────────────────────────────────

class TestExtractProductCandidates:
    def _make_row(self, feature_id, ts_str, features_json):
        return {
            "feature_id": feature_id,
            "flow_published_at": datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc),
            "raw_features_json": features_json,
        }

    def test_single_product_row(self):
        fake_rows = [
            self._make_row("f1", "2024-05-01T12:00:00", '{"product": "Roundup"}'),
        ]
        watermark = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch("product_normalizer.extract.extract_raw_since", return_value=fake_rows):
            result = extract_product_candidates(watermark)
        assert len(result) == 1
        assert result[0]["raw_product_name"] == "Roundup"
        assert result[0]["feature_id"] == "f1"

    def test_tank_mix_expands_to_multiple_candidates(self):
        fake_rows = [
            self._make_row(
                "f2",
                "2024-05-01T12:00:00",
                '{"tankMix": [{"name": "Roundup"}, {"name": "Atrazine"}]}',
            ),
        ]
        watermark = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch("product_normalizer.extract.extract_raw_since", return_value=fake_rows):
            result = extract_product_candidates(watermark)
        assert len(result) == 2
        names = {r["raw_product_name"] for r in result}
        assert names == {"Roundup", "Atrazine"}

    def test_duplicate_same_feature_id_and_name_deduplicated(self):
        fake_rows = [
            self._make_row("f3", "2024-05-01T12:00:00", '{"product": "Roundup"}'),
            self._make_row("f3", "2024-05-01T12:00:00", '{"product": "Roundup"}'),
        ]
        watermark = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch("product_normalizer.extract.extract_raw_since", return_value=fake_rows):
            result = extract_product_candidates(watermark)
        assert len(result) == 1

    def test_empty_source_returns_empty(self):
        watermark = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch("product_normalizer.extract.extract_raw_since", return_value=[]):
            result = extract_product_candidates(watermark)
        assert result == []


# ── max_published_at ──────────────────────────────────────────────────────────

class TestMaxPublishedAt:
    def test_returns_max_timestamp(self):
        ts1 = datetime(2024, 5, 1, tzinfo=timezone.utc)
        ts2 = datetime(2024, 6, 15, tzinfo=timezone.utc)
        ts3 = datetime(2024, 3, 10, tzinfo=timezone.utc)
        candidates = [
            {"flow_published_at": ts1},
            {"flow_published_at": ts2},
            {"flow_published_at": ts3},
        ]
        assert max_published_at(candidates) == ts2

    def test_empty_list_returns_none(self):
        assert max_published_at([]) is None

    def test_single_element(self):
        ts = datetime(2024, 5, 1, tzinfo=timezone.utc)
        assert max_published_at([{"flow_published_at": ts}]) == ts
