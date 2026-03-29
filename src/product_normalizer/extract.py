"""
extract.py
----------
CDC extraction layer.

Pulls new records from ``agmri.agmri.base_feature`` using
``flow_published_at`` as the watermark.  Both extraction paths are
supported:

  * Top-level ``features.product``  — single product string
  * ``features.tankMix[*].name``    — multi-product tank-mix array
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from .config import settings
from .db import execute, query, scalar

logger = logging.getLogger(__name__)

# ── Watermark helpers ─────────────────────────────────────────────────────────


def get_watermark() -> datetime:
    """Return the last successfully processed ``flow_published_at`` timestamp."""
    ts = scalar(
        f"""
        SELECT watermark_ts
        FROM   {settings.watermark_table}
        WHERE  pipeline_name = 'product_normalizer'
        ORDER  BY updated_at DESC
        LIMIT  1
        """
    )
    if ts is None:
        # First run — start from epoch
        logger.info("No watermark found — performing full initial load.")
        return datetime(2000, 1, 1, tzinfo=timezone.utc)
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def set_watermark(ts: datetime) -> None:
    """Upsert the watermark for the current run."""
    execute(
        f"""
        INSERT INTO {settings.watermark_table}
            (pipeline_name, watermark_ts, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        """,
        ["product_normalizer", ts],
    )
    logger.debug("Watermark set to %s", ts.isoformat())


# ── Raw extraction ────────────────────────────────────────────────────────────


def extract_raw_since(watermark: datetime) -> list[dict]:
    """
    Pull all ``base_feature`` rows published after *watermark*.

    Returns a list of dicts with keys:
        feature_id, flow_published_at, raw_features_json
    """
    rows = query(
        f"""
        SELECT
            id                          AS feature_id,
            flow_published_at,
            features::VARCHAR           AS raw_features_json
        FROM   {settings.source_table}
        WHERE  flow_published_at > ?
          AND  features IS NOT NULL
        ORDER  BY flow_published_at
        """,
        [watermark],
    )
    logger.info("Extracted %d raw rows from source.", len(rows))
    return rows


# ── Product candidate parsing ─────────────────────────────────────────────────


def _parse_candidates(raw_json: str) -> list[str]:
    """
    Extract product name strings from a ``features`` JSON blob.

    Handles:
      { "product": "Roundup" }
      { "tankMix": [{ "name": "Roundup" }, { "name": "Prefix" }] }
      Combined top-level + tankMix
    """
    candidates: list[str] = []
    try:
        obj = json.loads(raw_json)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Could not parse features JSON: %.80s", raw_json)
        return candidates

    # Path 1 — top-level product string
    if isinstance(obj.get("product"), str):
        name = obj["product"].strip()
        if name:
            candidates.append(name)

    # Path 2 — tankMix array
    for mix_item in obj.get("tankMix", []):
        if isinstance(mix_item, dict):
            name = mix_item.get("name", "").strip()
            if name:
                candidates.append(name)

    return candidates


# ── Public entry point ────────────────────────────────────────────────────────


def extract_product_candidates(watermark: datetime) -> list[dict]:
    """
    Full extraction: pull raw rows, parse product candidates, deduplicate.

    Returns a list of dicts:
        feature_id, flow_published_at, raw_product_name
    """
    raw_rows = extract_raw_since(watermark)
    candidates: list[dict] = []
    seen: set[tuple] = set()

    for row in raw_rows:
        names = _parse_candidates(row["raw_features_json"])
        for name in names:
            key = (row["feature_id"], name)
            if key not in seen:
                seen.add(key)
                candidates.append(
                    {
                        "feature_id": row["feature_id"],
                        "flow_published_at": row["flow_published_at"],
                        "raw_product_name": name,
                    }
                )

    logger.info(
        "Parsed %d product candidates from %d source rows.",
        len(candidates),
        len(raw_rows),
    )
    return candidates


def max_published_at(candidates: list[dict]) -> datetime | None:
    """Return the maximum ``flow_published_at`` from a candidate list."""
    if not candidates:
        return None
    return max(c["flow_published_at"] for c in candidates)
