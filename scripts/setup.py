#!/usr/bin/env python3
"""
scripts/setup.py
----------------
One-time infrastructure setup for the product normalization pipeline.

What this does
--------------
1. Verifies MotherDuck connectivity
2. Confirms read-only access to agmri and catalog shares
3. Confirms write access to my_db
4. Creates the product_normalization schema
5. Creates sequences
6. Creates all 7 pipeline tables
7. Seeds abbreviation_dictionary, exact_mapping, and custom_rules

Usage
-----
    python scripts/setup.py              # idempotent — safe to re-run
    python scripts/setup.py --force      # drop and recreate all tables
    python scripts/setup.py --verify     # connectivity check only
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from rich.console import Console
from rich.panel import Panel

console = Console()
logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# Ordered list of SQL files to execute
SQL_FILES = [
    "001_create_schema.sql",
    "003_create_sequences.sql",   # sequences before tables
    "002_create_tables.sql",
    "004_seed_data.sql",
]


def _load_sql(filename: str) -> str:
    path = SQL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def verify_connectivity(conn) -> bool:
    """Quick smoke tests for all three databases."""
    checks = [
        ("MotherDuck version",   "SELECT version()"),
        ("agmri source read",    "SELECT COUNT(*) FROM agmri.agmri.base_feature LIMIT 1"),
        ("catalog read",         "SELECT COUNT(*) FROM product_normalization_table.main.product_catalog LIMIT 1"),
        ("my_db write probe",    "CREATE SCHEMA IF NOT EXISTS product_normalization"),
    ]

    all_ok = True
    for label, sql in checks:
        try:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(stmt)
            console.print(f"  [green]✓[/green]  {label}")
        except Exception as exc:  # noqa: BLE001
            console.print(f"  [red]✗[/red]  {label} — {exc}")
            all_ok = False

    return all_ok


def drop_all(conn) -> None:
    """Drop all pipeline tables and sequences (--force mode)."""
    objects = [
        "TABLE  product_normalization.normalization_decisions",
        "TABLE  product_normalization.review_queue",
        "TABLE  product_normalization.abbreviation_dictionary",
        "TABLE  product_normalization.exact_mapping",
        "TABLE  product_normalization.custom_rules",
        "TABLE  product_normalization.run_log",
        "TABLE  product_normalization.pipeline_watermark",
        "SEQUENCE product_normalization.decisions_seq",
        "SEQUENCE product_normalization.review_seq",
        "SEQUENCE product_normalization.abbrev_seq",
        "SEQUENCE product_normalization.exact_seq",
        "SEQUENCE product_normalization.rules_seq",
        "SEQUENCE product_normalization.runlog_seq",
    ]
    for obj in objects:
        try:
            conn.execute(f"DROP {obj} IF EXISTS")
            console.print(f"  [yellow]dropped[/yellow]  {obj.split()[-1]}")
        except Exception as exc:  # noqa: BLE001
            console.print(f"  [red]error[/red]    {obj.split()[-1]} — {exc}")


def run_sql_file(conn, filename: str) -> bool:
    """Execute all statements in a SQL file. Returns True on success."""
    sql = _load_sql(filename)
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    ok = True
    for stmt in statements:
        try:
            conn.execute(stmt)
        except Exception as exc:  # noqa: BLE001
            # Ignore "already exists" during idempotent runs
            msg = str(exc).lower()
            if "already exists" in msg or "duplicate" in msg:
                logger.debug("Skipped (already exists): %.80s", stmt[:80])
            else:
                console.print(f"  [red]SQL error[/red]: {exc}\n  Statement: {stmt[:100]}")
                ok = False
    return ok


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Set up product normalization pipeline infrastructure in MotherDuck."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Drop and recreate all tables (destructive).",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Connectivity check only — make no changes.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Debug logging.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-8s  %(message)s",
    )

    console.print(Panel.fit(
        "[bold green]Product Normalizer — Setup[/bold green]",
        subtitle="MotherDuck infrastructure",
    ))

    # Import here so .env is loaded first
    try:
        from product_normalizer.config import settings
        from product_normalizer.db import get_conn, close_conn
    except ImportError as exc:
        console.print(f"[red]Import error:[/red] {exc}")
        console.print("Make sure you have run: pip install -e .[dev]")
        return 1

    conn = get_conn()

    # ── Verify connectivity ───────────────────────────────────────────────────
    console.rule("[bold]Connectivity checks[/bold]")
    ok = verify_connectivity(conn)
    if not ok:
        console.print("\n[red]One or more connectivity checks failed. Aborting.[/red]")
        return 1

    if args.verify:
        console.print("\n[green]--verify passed. No changes made.[/green]")
        return 0

    # ── Force mode — drop everything first ───────────────────────────────────
    if args.force:
        console.rule("[bold yellow]DROP mode (--force)[/bold yellow]")
        console.print("[yellow]Dropping all pipeline tables and sequences…[/yellow]")
        drop_all(conn)

    # ── Create schema + tables + seed data ───────────────────────────────────
    console.rule("[bold]Creating infrastructure[/bold]")
    all_ok = True
    for sql_file in SQL_FILES:
        console.print(f"  Running [cyan]{sql_file}[/cyan]…", end=" ")
        success = run_sql_file(conn, sql_file)
        if success:
            console.print("[green]OK[/green]")
        else:
            console.print("[red]FAILED[/red]")
            all_ok = False

    # ── Verify tables exist ───────────────────────────────────────────────────
    console.rule("[bold]Verifying tables[/bold]")
    expected_tables = [
        "pipeline_watermark",
        "normalization_decisions",
        "review_queue",
        "abbreviation_dictionary",
        "exact_mapping",
        "custom_rules",
        "run_log",
    ]
    for tbl in expected_tables:
        try:
            result = conn.execute(
                f"SELECT COUNT(*) FROM product_normalization.{tbl}"
            ).fetchone()
            count = result[0] if result else 0
            console.print(f"  [green]✓[/green]  {tbl:<35} ({count} rows)")
        except Exception as exc:  # noqa: BLE001
            console.print(f"  [red]✗[/red]  {tbl:<35} — {exc}")
            all_ok = False

    close_conn()

    if all_ok:
        console.print(Panel.fit(
            "[bold green]✅ Setup complete![/bold green]\n"
            "Run the pipeline with:  [cyan]normalize run[/cyan]",
        ))
        return 0
    else:
        console.print(Panel.fit(
            "[bold red]⚠️  Setup completed with errors.[/bold red]\n"
            "Review output above and re-run with [cyan]--verbose[/cyan] for details.",
        ))
        return 1


if __name__ == "__main__":
    sys.exit(main())
