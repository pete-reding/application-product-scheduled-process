"""
test_writer.py
--------------
Unit tests for the append-only decision ingestion logic in writer.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from product_normalizer.matchers import MatchMethod, MatchResult
from product_normalizer.writer import write_decisions, write_review_queue, write_run_log


def _make_candidate(feature_id="f1", ts=None):
    return {
        "feature_id": feature_id,
        "flow_published_at": ts or datetime(2024, 5, 1, tzinfo=timezone.utc),
    }


def _make_result(method=MatchMethod.CATALOG_EXACT, name="Roundup PowerMAX"):
    return MatchResult(
        raw_product_name="roundup",
        method=method,
        normalized_name=name,
        category="herbicide",
    )


# ── write_decisions ───────────────────────────────────────────────────────────

class TestWriteDecisions:
    def test_returns_zero_for_empty_input(self):
        count = write_decisions([], [], "run_001")
        assert count == 0

    def test_writes_correct_number_of_rows(self, mock_db_execute):
        mock_conn = MagicMock()
        candidates = [_make_candidate(f"f{i}") for i in range(3)]
        results = [_make_result() for _ in range(3)]

        with patch("product_normalizer.writer.transaction") as mock_tx:
            mock_tx.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_tx.return_value.__exit__ = MagicMock(return_value=False)
            count = write_decisions(candidates, results, "run_test")

        assert count == 3
        assert mock_conn.execute.call_count == 3

    def test_raises_on_length_mismatch(self):
        candidates = [_make_candidate("f1"), _make_candidate("f2")]
        results = [_make_result()]
        with pytest.raises(AssertionError):
            write_decisions(candidates, results, "run_bad")

    def test_method_value_written_as_string(self):
        mock_conn = MagicMock()
        candidates = [_make_candidate("f1")]
        results = [_make_result(method=MatchMethod.FUZZY, name="Roundup PowerMAX")]

        with patch("product_normalizer.writer.transaction") as mock_tx:
            mock_tx.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_tx.return_value.__exit__ = MagicMock(return_value=False)
            write_decisions(candidates, results, "run_test")

        call_args = mock_conn.execute.call_args_list[0]
        params = call_args[0][1]  # positional args[1] = params list
        # match_method is index 3 in the INSERT params
        assert params[3] == "fuzzy"


# ── write_review_queue ────────────────────────────────────────────────────────

class TestWriteReviewQueue:
    def test_only_no_match_rows_queued(self):
        mock_conn = MagicMock()
        candidates = [_make_candidate(f"f{i}") for i in range(4)]
        results = [
            _make_result(MatchMethod.CATALOG_EXACT),
            _make_result(MatchMethod.NO_MATCH),
            _make_result(MatchMethod.FUZZY),
            _make_result(MatchMethod.NO_MATCH),
        ]
        with patch("product_normalizer.writer.transaction") as mock_tx:
            mock_tx.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_tx.return_value.__exit__ = MagicMock(return_value=False)
            count = write_review_queue(candidates, results, "run_test")

        assert count == 2
        assert mock_conn.execute.call_count == 2

    def test_empty_queue_when_all_resolved(self):
        candidates = [_make_candidate("f1")]
        results = [_make_result(MatchMethod.EXACT_MAP)]
        count = write_review_queue(candidates, results, "run_test")
        assert count == 0

    def test_returns_zero_for_empty_input(self):
        count = write_review_queue([], [], "run_test")
        assert count == 0


# ── write_run_log ─────────────────────────────────────────────────────────────

class TestWriteRunLog:
    def test_run_log_inserts_row(self, mock_db_execute):
        ts = datetime(2024, 5, 1, tzinfo=timezone.utc)
        write_run_log(
            run_id="run_001",
            watermark_start=ts,
            watermark_end=ts,
            total_candidates=100,
            resolved=90,
            queued_for_review=10,
            duration_seconds=3.14,
            status="success",
        )
        mock_db_execute.assert_called_once()

    def test_error_status_accepted(self, mock_db_execute):
        ts = datetime(2024, 5, 1, tzinfo=timezone.utc)
        write_run_log(
            run_id="run_err",
            watermark_start=ts,
            watermark_end=None,
            total_candidates=0,
            resolved=0,
            queued_for_review=0,
            duration_seconds=0.5,
            status="error",
            error_message="Connection refused",
        )
        call_params = mock_db_execute.call_args[0][1]
        assert "error" in call_params
        assert "Connection refused" in call_params
