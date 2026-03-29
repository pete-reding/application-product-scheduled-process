"""
cli.py
------
Typer-based CLI.  Registered as the ``normalize`` entry point.

Commands
--------
  normalize run          Run the daily pipeline
  normalize status       Show watermark and recent run log
  normalize review       Open the latest review HTML in the browser
  normalize seed         (Re)seed reference tables from SQL files
"""

from __future__ import annotations

import logging
import subprocess
import webbrowser
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .config import PROJECT_ROOT, settings
from .db import query

app = typer.Typer(
    name="normalize",
    help="Agricultural product name normalization pipeline.",
    add_completion=False,
)
console = Console()


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else getattr(logging, settings.log_level)
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(settings.log_dir / "pipeline.log"),
        ],
    )


@app.command()
def run(
    dry_run: bool = typer.Option(False, "--dry-run", "-n", help="Skip all writes."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging."),
) -> None:
    """Run the daily product normalization pipeline."""
    _configure_logging(verbose)
    from .pipeline import run_pipeline

    run_pipeline(dry_run=dry_run)


@app.command()
def status(
    n: int = typer.Option(10, "--last", "-n", help="Number of recent runs to show."),
) -> None:
    """Display watermark and recent run history."""
    _configure_logging(False)

    # Watermark
    wm = query(
        f"""
        SELECT watermark_ts
        FROM   {settings.watermark_table}
        WHERE  pipeline_name = 'product_normalizer'
        ORDER  BY updated_at DESC
        LIMIT  1
        """
    )
    console.print(
        f"\nWatermark: [cyan]{wm[0]['watermark_ts'] if wm else 'none (first run)'}[/cyan]\n"
    )

    # Run log
    runs = query(
        f"""
        SELECT run_id, status, total_candidates, resolved, queued_for_review,
               round(duration_seconds,1) AS duration_s, logged_at
        FROM   {settings.run_log_table}
        ORDER  BY logged_at DESC
        LIMIT  {n}
        """
    )

    if not runs:
        console.print("[yellow]No runs recorded yet.[/yellow]")
        return

    table = Table(title=f"Last {n} Runs", show_header=True, header_style="bold blue")
    for col in ["run_id", "status", "total", "resolved", "queued", "duration_s", "logged_at"]:
        table.add_column(col)

    for r in runs:
        status_color = "green" if r["status"] == "success" else "red"
        table.add_row(
            r["run_id"],
            f"[{status_color}]{r['status']}[/{status_color}]",
            str(r["total_candidates"]),
            str(r["resolved"]),
            str(r["queued_for_review"]),
            str(r["duration_s"]),
            str(r["logged_at"]),
        )
    console.print(table)


@app.command()
def review(
    run_id: str = typer.Option("", "--run-id", help="Specific run ID to open."),
) -> None:
    """Open the latest review HTML in the default browser."""
    output_dir = PROJECT_ROOT / "output"
    if not output_dir.exists():
        console.print("[red]No output directory found. Run the pipeline first.[/red]")
        raise typer.Exit(1)

    html_files = sorted(output_dir.glob("review_*.html"), reverse=True)
    if not html_files:
        console.print("[yellow]No review files found.[/yellow]")
        raise typer.Exit(0)

    target = html_files[0]
    if run_id:
        matches = [f for f in html_files if run_id in f.name]
        if not matches:
            console.print(f"[red]No review file for run_id '{run_id}'.[/red]")
            raise typer.Exit(1)
        target = matches[0]

    console.print(f"Opening: [link={target}]{target}[/link]")
    webbrowser.open(f"file://{target}")


@app.command()
def seed(
    force: bool = typer.Option(False, "--force", help="Drop and recreate tables."),
) -> None:
    """(Re)seed pipeline infrastructure tables from SQL files."""
    _configure_logging(False)
    script = PROJECT_ROOT / "scripts" / "setup.py"
    if not script.exists():
        console.print(f"[red]Setup script not found: {script}[/red]")
        raise typer.Exit(1)
    args = ["python", str(script)]
    if force:
        args.append("--force")
    subprocess.run(args, check=True)


if __name__ == "__main__":
    app()
