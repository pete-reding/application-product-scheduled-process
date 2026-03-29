"""
writer.py
---------
Append-only decision ingestion into MotherDuck.

All writes go to ``my_db.product_normalization.*``.  The append-only
pattern preserves full audit history — no rows are ever updated or deleted.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from .config import settings
from .db import execute, transaction
from .matchers import MatchMethod, MatchResult

logger = logging.getLogger(__name__)


# ── Decisions ─────────────────────────────────────────────────────────────────


def write_decisions(
    candidates: list[dict],
    results: list[MatchResult],
    run_id: str,
) -> int:
    """
    Insert one row per candidate into ``normalization_decisions``.

    Parameters
    ----------
    candidates:
        Original candidate dicts (must contain ``feature_id`` and
        ``flow_published_at``).
    results:
        Corresponding MatchResult objects (same length as candidates).
    run_id:
        Identifier for the current pipeline run.

    Returns
    -------
    int
        Number of rows written.
    """
    if not candidates:
        return 0

    assert len(candidates) == len(results), "candidates and results must be same length"

    rows_written = 0
    now = datetime.now(timezone.utc)

    with transaction() as conn:
        for cand, result in zip(candidates, results):
            conn.execute(
                f"""
                INSERT INTO {settings.decisions_table} (
                    feature_id,
                    flow_published_at,
                    raw_product_name,
                    match_method,
                    normalized_name,
                    product_id,
                    category,
                    npk_analysis,
                    confidence,
                    notes,
                    run_id,
                    decided_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    cand["feature_id"],
                    cand["flow_published_at"],
                    result.raw_product_name,
                    result.method.value,
                    result.normalized_name,
                    result.product_id,
                    result.category,
                    result.npk_analysis,
                    result.confidence,
                    result.notes,
                    run_id,
                    now,
                ],
            )
            rows_written += 1

    logger.info("Wrote %d decision rows (run_id=%s).", rows_written, run_id)
    return rows_written


# ── Review queue ──────────────────────────────────────────────────────────────


def write_review_queue(
    candidates: list[dict],
    results: list[MatchResult],
    run_id: str,
) -> int:
    """
    Append NO_MATCH candidates to the review queue table.

    Returns the number of rows queued.
    """
    no_match_pairs = [
        (c, r)
        for c, r in zip(candidates, results)
        if r.method == MatchMethod.NO_MATCH
    ]

    if not no_match_pairs:
        logger.info("No candidates require review.")
        return 0

    now = datetime.now(timezone.utc)
    rows_queued = 0

    with transaction() as conn:
        for cand, result in no_match_pairs:
            conn.execute(
                f"""
                INSERT INTO {settings.review_queue_table} (
                    feature_id,
                    flow_published_at,
                    raw_product_name,
                    run_id,
                    queued_at,
                    resolved
                ) VALUES (?, ?, ?, ?, ?, FALSE)
                """,
                [
                    cand["feature_id"],
                    cand["flow_published_at"],
                    result.raw_product_name,
                    run_id,
                    now,
                ],
            )
            rows_queued += 1

    logger.info("Queued %d entries for review (run_id=%s).", rows_queued, run_id)
    return rows_queued


# ── Run log ───────────────────────────────────────────────────────────────────


def write_run_log(
    run_id: str,
    watermark_start: datetime,
    watermark_end: datetime | None,
    total_candidates: int,
    resolved: int,
    queued_for_review: int,
    duration_seconds: float,
    status: str = "success",
    error_message: str | None = None,
) -> None:
    """Append a summary row to the run_log table."""
    execute(
        f"""
        INSERT INTO {settings.run_log_table} (
            run_id,
            watermark_start,
            watermark_end,
            total_candidates,
            resolved,
            queued_for_review,
            duration_seconds,
            status,
            error_message,
            logged_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            run_id,
            watermark_start,
            watermark_end,
            total_candidates,
            resolved,
            queued_for_review,
            round(duration_seconds, 3),
            status,
            error_message,
        ],
    )
    logger.info(
        "Run log written — run_id=%s  status=%s  resolved=%d  queued=%d  duration=%.1fs",
        run_id,
        status,
        resolved,
        queued_for_review,
        duration_seconds,
    )
