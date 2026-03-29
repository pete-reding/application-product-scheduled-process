"""
matchers.py
-----------
Nine-step deterministic matching cascade for raw agricultural product names.

Steps (in order)
----------------
1.  Junk filter         — blank, numeric-only, single-char, known garbage
2.  Exact mapping       — pre-built exact_mapping lookup table
3.  Catalog exact       — case-insensitive match against products catalog
4.  Abbreviation dict   — expand abbreviations then re-match catalog
5.  NPK regex           — detect fertilizer NPK ratio strings (e.g. 28-0-0)
6.  2,4-D variants      — normalise 2,4-D / 2,4D / 24D / 2-4D etc.
7.  Custom rules        — regex-based rules loaded from DB
8.  Fuzzy token overlap — RapidFuzz token_set_ratio ≥ threshold
9.  No match            — enqueue for human review

Each step returns a ``MatchResult`` or ``None`` to fall through to the next.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from rapidfuzz import fuzz

from .config import settings
from .db import query

logger = logging.getLogger(__name__)

# ── Data models ───────────────────────────────────────────────────────────────


class MatchMethod(str, Enum):
    JUNK = "junk"
    EXACT_MAP = "exact_map"
    CATALOG_EXACT = "catalog_exact"
    ABBREVIATION = "abbreviation"
    NPK = "npk"
    TWO_FOUR_D = "two_four_d"
    CUSTOM_RULE = "custom_rule"
    FUZZY = "fuzzy"
    NO_MATCH = "no_match"


@dataclass
class MatchResult:
    raw_product_name: str
    method: MatchMethod
    normalized_name: str | None = None
    product_id: str | None = None
    category: str | None = None
    npk_analysis: str | None = None
    confidence: float = 1.0
    notes: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def is_resolved(self) -> bool:
        return self.method not in (MatchMethod.NO_MATCH, MatchMethod.JUNK)


# ── Reference data (lazy-loaded per run) ─────────────────────────────────────

_catalog: list[dict] | None = None
_abbreviations: dict[str, str] | None = None
_exact_map: dict[str, dict] | None = None
_custom_rules: list[dict] | None = None

_JUNK_PATTERN = re.compile(
    r"^\s*$"                    # blank
    r"|^[\d\s\.\-\/]+$"         # all digits/punctuation
    r"|^[a-zA-Z]$"              # single character
    r"|^(n/?a|none|null|test|unknown|unk|tbd|na|--|-)$",
    re.IGNORECASE,
)

_NPK_PATTERN = re.compile(
    r"^(\d{1,3})-(\d{1,3})-(\d{1,3})(?:-(\d{1,3}))?(?:\s+\w+)?$"
)

_TWO_FOUR_D_VARIANTS = re.compile(
    r"\b2[,\-\s]?4[,\-\s]?d\b", re.IGNORECASE
)


def _load_reference_data() -> None:
    global _catalog, _abbreviations, _exact_map, _custom_rules  # noqa: PLW0603

    logger.debug("Loading reference data from MotherDuck…")

    _catalog = query(
        f"""
        SELECT product_id, product_name, category, npk_analysis
        FROM   {settings.catalog_table}
        """
    )

    abbrev_rows = query(
        f"SELECT abbreviation, expansion FROM {settings.abbreviations_table}"
    )
    _abbreviations = {r["abbreviation"].upper(): r["expansion"] for r in abbrev_rows}

    exact_rows = query(
        f"""
        SELECT raw_text, product_id, normalized_name, category
        FROM   {settings.exact_map_table}
        """
    )
    _exact_map = {r["raw_text"].upper(): r for r in exact_rows}

    _custom_rules = query(
        f"""
        SELECT pattern, normalized_name, product_id, category, notes
        FROM   {settings.custom_rules_table}
        WHERE  is_active = TRUE
        ORDER  BY priority
        """
    )

    logger.info(
        "Reference data loaded — catalog=%d  abbrevs=%d  exact_map=%d  rules=%d",
        len(_catalog),
        len(_abbreviations),
        len(_exact_map),
        len(_custom_rules),
    )


def reload_reference_data() -> None:
    """Force reload of all reference tables (call at start of each run)."""
    global _catalog, _abbreviations, _exact_map, _custom_rules  # noqa: PLW0603
    _catalog = _abbreviations = _exact_map = _custom_rules = None
    _load_reference_data()


def _ensure_loaded() -> None:
    if _catalog is None:
        _load_reference_data()


# ── Individual matchers ───────────────────────────────────────────────────────


def _step1_junk(name: str) -> MatchResult | None:
    if _JUNK_PATTERN.match(name.strip()):
        return MatchResult(
            raw_product_name=name, method=MatchMethod.JUNK, notes="Filtered as junk"
        )
    return None


def _step2_exact_map(name: str) -> MatchResult | None:
    _ensure_loaded()
    row = _exact_map.get(name.strip().upper())  # type: ignore[union-attr]
    if row:
        return MatchResult(
            raw_product_name=name,
            method=MatchMethod.EXACT_MAP,
            normalized_name=row["normalized_name"],
            product_id=row["product_id"],
            category=row["category"],
        )
    return None


def _step3_catalog_exact(name: str) -> MatchResult | None:
    _ensure_loaded()
    needle = name.strip().upper()
    for row in _catalog:  # type: ignore[union-attr]
        if row["product_name"].upper() == needle:
            return MatchResult(
                raw_product_name=name,
                method=MatchMethod.CATALOG_EXACT,
                normalized_name=row["product_name"],
                product_id=row["product_id"],
                category=row["category"],
                npk_analysis=row.get("npk_analysis"),
            )
    return None


def _step4_abbreviation(name: str) -> MatchResult | None:
    _ensure_loaded()
    tokens = name.strip().upper().split()
    expanded = " ".join(_abbreviations.get(t, t) for t in tokens)  # type: ignore[union-attr]
    if expanded.upper() == name.strip().upper():
        return None  # No expansion happened
    result = _step3_catalog_exact(expanded)
    if result:
        result.method = MatchMethod.ABBREVIATION
        result.notes = f"Expanded '{name}' → '{expanded}'"
    return result


def _step5_npk(name: str) -> MatchResult | None:
    m = _NPK_PATTERN.match(name.strip())
    if not m:
        return None
    n, p, k = m.group(1), m.group(2), m.group(3)
    npk = f"{n}-{p}-{k}"
    return MatchResult(
        raw_product_name=name,
        method=MatchMethod.NPK,
        normalized_name=f"Fertilizer {npk}",
        category="fertilizer",
        npk_analysis=npk,
        notes=f"NPK ratio detected: {npk}",
    )


def _step6_two_four_d(name: str) -> MatchResult | None:
    if _TWO_FOUR_D_VARIANTS.search(name):
        # Attempt catalog lookup after canonical substitution
        canonical = _TWO_FOUR_D_VARIANTS.sub("2,4-D", name)
        result = _step3_catalog_exact(canonical)
        if result:
            result.method = MatchMethod.TWO_FOUR_D
            result.notes = f"2,4-D variant normalised from '{name}'"
            return result
        # Return partial resolution even without catalog hit
        return MatchResult(
            raw_product_name=name,
            method=MatchMethod.TWO_FOUR_D,
            normalized_name=canonical,
            category="herbicide",
            notes=f"2,4-D variant; canonical form: '{canonical}'",
            confidence=0.85,
        )
    return None


def _step7_custom_rules(name: str) -> MatchResult | None:
    _ensure_loaded()
    for rule in _custom_rules:  # type: ignore[union-attr]
        try:
            if re.search(rule["pattern"], name, re.IGNORECASE):
                return MatchResult(
                    raw_product_name=name,
                    method=MatchMethod.CUSTOM_RULE,
                    normalized_name=rule["normalized_name"],
                    product_id=rule.get("product_id"),
                    category=rule.get("category"),
                    notes=rule.get("notes"),
                )
        except re.error as exc:
            logger.warning("Invalid custom rule pattern '%s': %s", rule["pattern"], exc)
    return None


def _step8_fuzzy(name: str) -> MatchResult | None:
    _ensure_loaded()
    best_score = 0
    best_row: dict | None = None

    needle = name.strip()
    for row in _catalog:  # type: ignore[union-attr]
        score = fuzz.token_set_ratio(needle, row["product_name"])
        if score > best_score:
            best_score = score
            best_row = row

    if best_row and best_score >= settings.fuzzy_threshold:
        return MatchResult(
            raw_product_name=name,
            method=MatchMethod.FUZZY,
            normalized_name=best_row["product_name"],
            product_id=best_row["product_id"],
            category=best_row["category"],
            npk_analysis=best_row.get("npk_analysis"),
            confidence=round(best_score / 100, 4),
            notes=f"Fuzzy score: {best_score}",
        )
    return None


# ── Cascade entry point ───────────────────────────────────────────────────────

_STEPS = [
    _step1_junk,
    _step2_exact_map,
    _step3_catalog_exact,
    _step4_abbreviation,
    _step5_npk,
    _step6_two_four_d,
    _step7_custom_rules,
    _step8_fuzzy,
]


def match(raw_name: str) -> MatchResult:
    """
    Run the 9-step cascade and return the first successful ``MatchResult``.

    If no step resolves the name, returns a NO_MATCH result so the
    candidate is queued for human review.
    """
    for step_fn in _STEPS:
        result = step_fn(raw_name)
        if result is not None:
            logger.debug(
                "%-30s → %-20s  %s",
                raw_name[:30],
                result.method.value,
                result.normalized_name or "",
            )
            return result

    # Step 9 — no match
    logger.debug("NO MATCH for '%s'", raw_name[:60])
    return MatchResult(
        raw_product_name=raw_name,
        method=MatchMethod.NO_MATCH,
        notes="Queued for human review",
    )


def match_batch(candidates: list[dict]) -> list[MatchResult]:
    """Match a list of candidate dicts (must have ``raw_product_name`` key)."""
    return [match(c["raw_product_name"]) for c in candidates]
