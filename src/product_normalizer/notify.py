"""
notify.py
---------
macOS desktop notification helper.

Uses the ``osascript`` command so no third-party dependencies are needed.
Silently no-ops on non-macOS platforms.
"""

from __future__ import annotations

import logging
import platform
import subprocess

from .config import settings

logger = logging.getLogger(__name__)


def send_notification(
    title: str,
    message: str,
    subtitle: str = "",
    sound: str | None = None,
) -> None:
    """
    Send a macOS Notification Center alert.

    Parameters
    ----------
    title:
        Bold header text.
    message:
        Body text.
    subtitle:
        Optional secondary line.
    sound:
        Sound name (defaults to ``settings.macos_sound``).
    """
    if platform.system() != "Darwin":
        logger.debug("Non-macOS platform — skipping notification.")
        return

    sound = sound or settings.macos_sound
    subtitle_clause = f'subtitle "{subtitle}"' if subtitle else ""
    sound_clause = f'sound name "{sound}"' if sound else ""

    script = (
        f'display notification "{message}" '
        f'with title "{title}" '
        f"{subtitle_clause} "
        f"{sound_clause}"
    ).strip()

    try:
        subprocess.run(
            ["osascript", "-e", script],
            check=True,
            capture_output=True,
            timeout=10,
        )
        logger.debug("Notification sent: %s — %s", title, message)
    except subprocess.SubprocessError as exc:
        logger.warning("Failed to send notification: %s", exc)


def notify_run_complete(
    resolved: int,
    queued: int,
    duration_seconds: float,
    review_path: str | None = None,
) -> None:
    """Send the standard pipeline completion notification."""
    lines = [
        f"✅ {resolved} resolved  🔎 {queued} need review",
        f"⏱ {duration_seconds:.1f}s",
    ]
    if review_path:
        lines.append(f"Review: {review_path}")

    send_notification(
        title="Product Normalizer",
        subtitle="Daily run complete",
        message="  |  ".join(lines),
    )


def notify_run_failed(error: str) -> None:
    """Send a failure alert."""
    send_notification(
        title="Product Normalizer ❌",
        subtitle="Pipeline failed",
        message=error[:200],
        sound="Basso",
    )
