"""
test_matchers.py
----------------
Unit tests for the 9-step matching cascade in matchers.py.

All tests are fully offline — reference data is injected via conftest fixtures.
"""

from __future__ import annotations

import pytest

from product_normalizer.matchers import MatchMethod, match


# ── Step 1: Junk filter ───────────────────────────────────────────────────────

class TestJunkFilter:
    @pytest.mark.parametrize("name", [
        "",
        "   ",
        "0",
        "123",
        "999.5",
        "-",
        "--",
        "N/A",
        "n/a",
        "na",
        "none",
        "NULL",
        "test",
        "unknown",
        "unk",
        "tbd",
        "a",
        "X",
    ])
    def test_junk_inputs_are_filtered(self, name, patch_reference_data):
        result = match(name)
        assert result.method == MatchMethod.JUNK, f"Expected JUNK for: {repr(name)}"

    def test_valid_name_not_filtered(self, patch_reference_data):
        result = match("Roundup PowerMAX")
        assert result.method != MatchMethod.JUNK


# ── Step 2: Exact mapping ─────────────────────────────────────────────────────

class TestExactMapping:
    def test_exact_map_hit_case_insensitive(self, patch_reference_data):
        result = match("roundup")
        assert result.method == MatchMethod.EXACT_MAP
        assert result.normalized_name == "Roundup PowerMAX"
        assert result.category == "herbicide"

    def test_exact_map_uppercase(self, patch_reference_data):
        result = match("ROUNDUP")
        assert result.method == MatchMethod.EXACT_MAP

    def test_exact_map_miss_falls_through(self, patch_reference_data):
        result = match("Headline AMP")
        assert result.method != MatchMethod.EXACT_MAP


# ── Step 3: Catalog exact ─────────────────────────────────────────────────────

class TestCatalogExact:
    def test_catalog_hit(self, patch_reference_data):
        result = match("Roundup PowerMAX")
        assert result.method == MatchMethod.CATALOG_EXACT
        assert result.product_id == "P001"

    def test_catalog_case_insensitive(self, patch_reference_data):
        result = match("roundup powermax")
        assert result.method == MatchMethod.CATALOG_EXACT

    def test_catalog_miss_falls_through(self, patch_reference_data):
        result = match("SomeMadeUpProductXYZ999")
        assert result.method not in (MatchMethod.CATALOG_EXACT,)


# ── Step 4: Abbreviation expansion ───────────────────────────────────────────

class TestAbbreviationExpansion:
    def test_abbreviation_expands_and_matches(self, patch_reference_data):
        # "UAN" → "UAN 28-0-0" which is in catalog
        result = match("UAN")
        assert result.method == MatchMethod.ABBREVIATION
        assert result.normalized_name == "UAN 28-0-0"

    def test_no_expansion_no_false_match(self, patch_reference_data):
        # Word not in abbreviation dict — should not hit ABBREVIATION step
        result = match("Warrior II")
        assert result.method != MatchMethod.ABBREVIATION


# ── Step 5: NPK regex ─────────────────────────────────────────────────────────

class TestNPKRegex:
    @pytest.mark.parametrize("name,expected_npk", [
        ("28-0-0",   "28-0-0"),
        ("18-46-0",  "18-46-0"),
        ("0-0-60",   "0-0-60"),
        ("11-52-0",  "11-52-0"),
        ("46-0-0",   "46-0-0"),
        ("10-34-0",  "10-34-0"),
        ("32-0-0",   "32-0-0"),
    ])
    def test_npk_detected(self, name, expected_npk, patch_reference_data):
        result = match(name)
        assert result.method == MatchMethod.NPK
        assert result.npk_analysis == expected_npk
        assert result.category == "fertilizer"

    def test_npk_not_triggered_for_non_ratio(self, patch_reference_data):
        result = match("Roundup PowerMAX")
        assert result.method != MatchMethod.NPK

    def test_npk_partial_does_not_match(self, patch_reference_data):
        # "28-0" — only 2 segments, not a valid N-P-K ratio
        result = match("28-0")
        assert result.method != MatchMethod.NPK


# ── Step 6: 2,4-D variants ───────────────────────────────────────────────────

class TestTwoFourDVariants:
    @pytest.mark.parametrize("name", [
        "2,4-D",
        "2,4D",
        "24D",
        "2-4-D",
        "2 4 D",
        "2,4 d amine",
    ])
    def test_two_four_d_variants_detected(self, name, patch_reference_data):
        result = match(name)
        assert result.method == MatchMethod.TWO_FOUR_D, f"Expected TWO_FOUR_D for: {repr(name)}"
        assert result.category == "herbicide"


# ── Step 7: Custom rules ──────────────────────────────────────────────────────

class TestCustomRules:
    def test_rate_embedded_roundup(self, patch_reference_data):
        result = match("Roundup 22oz")
        assert result.method == MatchMethod.CUSTOM_RULE
        assert result.normalized_name == "Roundup PowerMAX"

    def test_rate_embedded_roundup_qt(self, patch_reference_data):
        result = match("roundup 1.5qt")
        assert result.method == MatchMethod.CUSTOM_RULE


# ── Step 8: Fuzzy matching ────────────────────────────────────────────────────

class TestFuzzyMatching:
    def test_fuzzy_match_on_misspelling(self, patch_reference_data):
        # "Warroir II" — typo for "Warrior II"
        result = match("Warroir II")
        assert result.method == MatchMethod.FUZZY
        assert result.normalized_name == "Warrior II"

    def test_fuzzy_confidence_is_fraction(self, patch_reference_data):
        result = match("Warroir II")
        assert 0 < result.confidence <= 1.0

    def test_below_threshold_falls_to_no_match(self, patch_reference_data):
        # Completely random string
        result = match("zzzzzzzzqqqqqq")
        assert result.method == MatchMethod.NO_MATCH


# ── Step 9: No match ─────────────────────────────────────────────────────────

class TestNoMatch:
    def test_unknown_product_queued(self, patch_reference_data):
        result = match("XYZ Unknown Product 9999 Alpha")
        assert result.method == MatchMethod.NO_MATCH
        assert result.normalized_name is None

    def test_no_match_is_not_resolved(self, patch_reference_data):
        result = match("completely made up string abc123xyz")
        assert not result.is_resolved


# ── MatchResult properties ────────────────────────────────────────────────────

class TestMatchResultProperties:
    def test_resolved_methods_are_resolved(self, patch_reference_data):
        result = match("Roundup PowerMAX")
        assert result.is_resolved

    def test_junk_is_not_resolved(self, patch_reference_data):
        result = match("n/a")
        assert not result.is_resolved
