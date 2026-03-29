"""
pipeline.py
-----------
Daily pipeline orchestrator.

Execution order
---------------
1. Reload reference data
2. Read CDC watermark
3. Extract product candidates
4. Run 9-step matching cascade
5. Write decisions to MotherDuck
6. Write NO_MATCH entries to review queue
7. Advance watermark
8. Write run log
9. Generate review HTML
10. Send macOS notification
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.table import Table

from . import __version__
from .config import settings
from .db import close_conn
from .extract import extract_product_candidates, get_watermark, max_published_at, set_watermark
from .matchers import MatchMethod, match_batch, reload_reference_data
from .notify import notify_run_complete, notify_run_failed
from .review_ui import generate_review_html
from .writer import write_decisions, write_review_queue, write_run_log

logger = logging.getLogger(__name__)
console = Console()


def run_pipeline(dry_run: bool = False) -> dict:
    """
    Execute the full daily normalization pipeline.

    Parameters
    ----------
    dry_run:
        If True, skip all writes (useful for local testing).

    Returns
    -------
    dict
        Summary statistics for the run.
    """
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start_ts = time.monotonic()

    console.rule(f"[bold green]Product Normalizer v{__version__}[/bold green]")
    console.print(f"Run ID : [cyan]{run_id}[/cyan]")
    console.print(f"Dry run: [yellow]{dry_run}[/yellow]\n")

    watermark_start = datetime.now(timezone.utc)  # fallback
    watermark_end: datetime | None = None
    total_candidates = 0
    resolved = 0
    queued = 0
    review_path: str | None = None

    try:
        # ── 1. Reference data ─────────────────────────────────────────
        console.print("[bold]1/9[/bold] Loading reference data…")
        reload_reference_data()

        # ── 2. Watermark ──────────────────────────────────────────────
        console.print("[bold]2/9[/bold] Reading CDC watermark…")
        watermark_start = get_watermark()
        console.print(f"      Watermark: [cyan]{watermark_start.isoformat()}[/cyan]")

        # ── 3. Extract ────────────────────────────────────────────────
        console.print("[bold]3/9[/bold] Extracting product candidates…")
        candidates = extract_product_candidates(watermark_start)
        total_candidates = len(candidates)
        console.print(f"      Candidates: [cyan]{total_candidates}[/cyan]")

        if not candidates:
            console.print("[yellow]No new records — pipeline complete.[/yellow]")
            duration = time.monotonic() - start_ts
            if not dry_run:
                write_run_log(
                    run_id=run_id,
                    watermark_start=watermark_start,
                    watermark_end=None,
                    total_candidates=0,
                    resolved=0,
                    queued_for_review=0,
                    duration_seconds=duration,
                    status="no_new_data",
                )
            notify_run_complete(resolved=0, queued=0, duration_seconds=duration)
            return {"run_id": run_id, "total": 0, "resolved": 0, "queued": 0}

        # ── 4. Match ──────────────────────────────────────────────────
        console.print("[bold]4/9[/bold] Running matching cascade…")
        results = match_batch(candidates)

        # ── 5. Decisions ──────────────────────────────────────────────
        console.print("[bold]5/9[/bold] Writing decisions…")
        if not dry_run:
            write_decisions(candidates, results, run_id)

        # ── 6. Review queue ───────────────────────────────────────────
        console.print("[bold]6/9[/bold] Writing review queue…")
        if not dry_run:
            queued = write_review_queue(candidates, results, run_id)
        else:
            queued = sum(1 for r in results if r.method == MatchMethod.NO_MATCH)

        resolved = total_candidates - queued

        # ── 7. Advance watermark ──────────────────────────────────────
        console.print("[bold]7/9[/bold] Advancing watermark…")
        watermark_end = max_published_at(candidates)
        if watermark_end and not dry_run:
            set_watermark(watermark_end)

        # ── 8. Run log ────────────────────────────────────────────────
        duration = time.monotonic() - start_ts
        console.print("[bold]8/9[/bold] Writing run log…")
        if not dry_run:
            write_run_log(
                run_id=run_id,
                watermark_start=watermark_start,
                watermark_end=watermark_end,
                total_candidates=total_candidates,
                resolved=resolved,
                queued_for_review=queued,
                duration_seconds=duration,
            )

        # ── 9. Review HTML ────────────────────────────────────────────
        if queued > 0:
            console.print("[bold]9/9[/bold] Generating review UI…")
            output_dir = Path(__file__).resolve().parents[3] / "output"
            html_path = generate_review_html(run_id=run_id, output_dir=output_dir)
            if html_path:
                review_path = str(html_path)
                console.print(f"      Review UI: [link={review_path}]{review_path}[/link]")
        else:
            console.print("[bold]9/9[/bold] No items for review — skipping HTML generation.")

        # ── Summary table ─────────────────────────────────────────────
        _print_summary(run_id, total_candidates, resolved, queued, duration)
        notify_run_complete(resolved, queued, duration, review_path)

        return {
            "run_id": run_id,
            "total": total_candidates,
            "resolved": resolved,
            "queued": queued,
            "duration_seconds": round(duration, 2),
        }

    except Exception as exc:  # noqa: BLE001
        duration = time.monotonic() - start_ts
        logger.exception("Pipeline failed: %s", exc)
        if not dry_run:
            try:
                write_run_log(
                    run_id=run_id,
                    watermark_start=watermark_start,
                    watermark_end=watermark_end,
                    total_candidates=total_candidates,
                    resolved=resolved,
                    queued_for_review=queued,
                    duration_seconds=duration,
                    status="error",
                    error_message=str(exc),
                )
            except Exception:  # noqa: BLE001
                pass
        notify_run_failed(str(exc))
        raise
    finally:
        close_conn()


def _print_summary(
    run_id: str,
    total: int,
    resolved: int,
    queued: int,
    duration: float,
) -> None:
    table = Table(title="Run Summary", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="dim", width=22)
    table.add_column("Value", justify="right")

    table.add_row("Run ID", run_id)
    table.add_row("Total candidates", str(total))
    table.add_row("Resolved", f"[green]{resolved}[/green]")
    table.add_row("Queued for review", f"[yellow]{queued}[/yellow]")
    table.add_row("Match rate", f"{resolved / total * 100:.1f}%" if total else "—")
    table.add_row("Duration", f"{duration:.2f}s")

    console.print(table)
