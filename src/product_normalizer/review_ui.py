"""
review_ui.py
------------
Generates a self-contained HTML review interface for NO_MATCH candidates.

No server required — the HTML file opens directly in any browser and
POSTs decisions back via the ``sendDecision()`` JS function, which
writes to a local ``decisions.json`` sidecar that the ingestion step
picks up on the next run.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from .config import settings
from .db import query

logger = logging.getLogger(__name__)

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Product Review — {run_id}</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         background:#f5f5f5;color:#222;padding:24px}}
    h1{{font-size:1.4rem;margin-bottom:4px;color:#1a1a2e}}
    .meta{{font-size:.85rem;color:#666;margin-bottom:24px}}
    .card{{background:#fff;border-radius:10px;padding:20px;margin-bottom:16px;
           box-shadow:0 1px 4px rgba(0,0,0,.08)}}
    .raw{{font-size:1.1rem;font-weight:600;margin-bottom:12px;color:#1a1a2e}}
    .badge{{display:inline-block;font-size:.75rem;padding:2px 8px;border-radius:12px;
            font-weight:600;margin-right:6px}}
    .badge-no-match{{background:#ffe4e4;color:#c0392b}}
    label{{font-size:.85rem;font-weight:500;display:block;margin:10px 0 4px}}
    input,select{{width:100%;padding:8px 10px;border:1px solid #ddd;
                  border-radius:6px;font-size:.9rem}}
    .actions{{display:flex;gap:10px;margin-top:14px;flex-wrap:wrap}}
    button{{padding:9px 18px;border:none;border-radius:6px;font-size:.85rem;
            font-weight:600;cursor:pointer}}
    .btn-accept{{background:#27ae60;color:#fff}}
    .btn-skip{{background:#95a5a6;color:#fff}}
    .btn-junk{{background:#e74c3c;color:#fff}}
    .done-banner{{display:none;background:#27ae60;color:#fff;padding:16px;
                  border-radius:10px;font-size:1rem;font-weight:600;text-align:center;
                  margin-top:24px}}
    .progress{{font-size:.85rem;color:#666;margin-bottom:20px}}
    .resolved{{opacity:.4;pointer-events:none}}
    .resolved .card-inner{{text-decoration:line-through}}
  </style>
</head>
<body>
  <h1>🌱 Product Name Review</h1>
  <p class="meta">Run ID: <strong>{run_id}</strong> &nbsp;|&nbsp; Generated: {generated_at}</p>
  <p class="progress" id="progress"></p>
  <div id="items"></div>
  <div class="done-banner" id="done">✅ All items reviewed — save decisions.json and re-run the pipeline.</div>

  <script>
    const ITEMS = {items_json};
    const decisions = {{}};
    let resolved = 0;

    function updateProgress() {{
      document.getElementById('progress').textContent =
        `${{resolved}} / ${{ITEMS.length}} reviewed`;
      if (resolved === ITEMS.length) {{
        document.getElementById('done').style.display = 'block';
      }}
    }}

    function sendDecision(idx, action) {{
      const item = ITEMS[idx];
      const norm = document.getElementById(`norm_${{idx}}`).value.trim();
      const cat  = document.getElementById(`cat_${{idx}}`).value;
      if (action === 'accept' && !norm) {{ alert('Enter a normalized name first.'); return; }}
      decisions[item.feature_id + '|' + item.raw_product_name] = {{
        feature_id: item.feature_id,
        raw_product_name: item.raw_product_name,
        action,
        normalized_name: action === 'accept' ? norm : null,
        category: action === 'accept' ? cat : null,
        reviewed_at: new Date().toISOString()
      }};
      document.getElementById(`card_${{idx}}`).classList.add('resolved');
      resolved++;
      updateProgress();
      // Download updated decisions.json
      const blob = new Blob([JSON.stringify(Object.values(decisions), null, 2)],
        {{type:'application/json'}});
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'decisions.json';
      a.click();
    }}

    function render() {{
      const container = document.getElementById('items');
      ITEMS.forEach((item, idx) => {{
        container.innerHTML += `
          <div id="card_${{idx}}" class="card">
            <div class="card-inner">
              <div class="raw"><span class="badge badge-no-match">NO MATCH</span>${{item.raw_product_name}}</div>
              <label>Normalized Name</label>
              <input id="norm_${{idx}}" type="text" placeholder="e.g. Roundup PowerMAX" />
              <label>Category</label>
              <select id="cat_${{idx}}">
                <option value="">— select —</option>
                <option>herbicide</option>
                <option>fungicide</option>
                <option>insecticide</option>
                <option>fertilizer</option>
                <option>biological</option>
                <option>adjuvant</option>
                <option>seed treatment</option>
                <option>other</option>
              </select>
              <div class="actions">
                <button class="btn-accept" onclick="sendDecision(${{idx}},'accept')">✅ Accept</button>
                <button class="btn-junk"   onclick="sendDecision(${{idx}},'junk')">🗑 Mark Junk</button>
                <button class="btn-skip"   onclick="sendDecision(${{idx}},'skip')">⏭ Skip</button>
              </div>
            </div>
          </div>`;
      }});
      updateProgress();
    }}
    render();
  </script>
</body>
</html>
"""


def generate_review_html(run_id: str, output_dir: Path | None = None) -> Path | None:
    """
    Build a self-contained review HTML file for all open review_queue entries.

    Returns the Path of the generated file, or None if the queue is empty.
    """
    rows = query(
        f"""
        SELECT feature_id, raw_product_name, flow_published_at, queued_at
        FROM   {settings.review_queue_table}
        WHERE  resolved = FALSE
        ORDER  BY queued_at DESC
        """
    )

    if not rows:
        logger.info("Review queue is empty — no HTML generated.")
        return None

    if output_dir is None:
        from .config import PROJECT_ROOT

        output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    items_json = json.dumps(rows, default=str)
    html = _HTML_TEMPLATE.format(
        run_id=run_id,
        generated_at=generated_at,
        items_json=items_json,
    )

    out_path = output_dir / f"review_{run_id}.html"
    out_path.write_text(html, encoding="utf-8")
    logger.info("Review UI written to %s (%d items)", out_path, len(rows))
    return out_path
