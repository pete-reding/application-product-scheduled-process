# Agricultural Product Name Normalizer

A daily automated pipeline that normalizes free-text agricultural product names captured by sprayer applicator machines into a validated, classified product catalog.

Farmers operating sprayer equipment enter product names as free text, resulting in misspellings, abbreviations, rate-embedded strings, multi-product tank mix entries, NPK ratio variants, and junk entries. This pipeline extracts, normalizes, and classifies those entries against a known product catalog — identifying product name, brand, category (herbicide, fertilizer, fungicide, insecticide, biological, adjuvant, etc.), and NPK analysis where applicable.

---

## Architecture

```
agmri.agmri.base_feature  (read-only MotherDuck share)
         │
         ▼  CDC watermark (flow_published_at)
  ┌──────────────┐
  │   Extract    │  JSON parsing — product + tankMix paths
  └──────┬───────┘
         ▼
  ┌──────────────────────────────────┐
  │   9-Step Matching Cascade        │
  │                                  │
  │  1. Junk filter                  │
  │  2. Exact mapping table          │
  │  3. Catalog exact match          │
  │  4. Abbreviation dictionary      │
  │  5. NPK regex  (e.g. 28-0-0)    │
  │  6. 2,4-D variant detection      │
  │  7. Custom regex rules           │
  │  8. Fuzzy token overlap          │
  │  9. No match → review queue      │
  └──────┬───────────────────────────┘
         ▼
  my_db.product_normalization.*  (writable)
  ├── normalization_decisions    (append-only audit log)
  ├── review_queue               (NO_MATCH entries)
  ├── pipeline_watermark         (CDC state)
  └── run_log                    (run history)
         │
         ▼
  review_<run_id>.html           (self-contained browser UI)
```

### Read / Write Split

| Database | Access | Purpose |
|---|---|---|
| `agmri` | read-only share | Source machine application records |
| `product_normalization_table` | read-only share | Product catalog |
| `my_db` | **writable** | All pipeline outputs |

This split is encoded centrally in `src/product_normalizer/config.py` as `settings.AGMRI`, `settings.CATALOG`, and `settings.W`.

---

## Project Structure

```
.
├── src/
│   └── product_normalizer/
│       ├── __init__.py
│       ├── cli.py            # Typer CLI entry point
│       ├── config.py         # Settings + DB constants
│       ├── db.py             # MotherDuck connection factory
│       ├── extract.py        # CDC extraction + JSON parsing
│       ├── matchers.py       # 9-step matching cascade
│       ├── notify.py         # macOS notifications
│       ├── pipeline.py       # Daily orchestrator
│       ├── review_ui.py      # Self-contained HTML review UI
│       └── writer.py         # Append-only decision ingestion
├── tests/
│   ├── conftest.py
│   ├── test_config.py
│   ├── test_extract.py
│   ├── test_matchers.py
│   └── test_writer.py
├── sql/
│   ├── 001_create_schema.sql
│   ├── 002_create_tables.sql
│   ├── 003_create_sequences.sql
│   └── 004_seed_data.sql
├── scripts/
│   ├── setup.py                          # Infrastructure setup
│   └── com.agmri.product-normalizer.plist  # macOS launchd agent
├── .env.example
├── .gitignore
├── pyproject.toml
└── README.md
```

---

## Prerequisites

- macOS (Apple Silicon M1+)
- Python 3.11+
- MotherDuck account with access to:
  - `agmri` share (read)
  - `product_normalization_table` share (read)
  - `my_db` (write)

---

## Installation

```bash
# 1. Clone the repo
git clone https://github.com/pete-reding/application-product-scheduled-process.git
cd application-product-scheduled-process

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install the package with dev dependencies
pip install -e ".[dev]"

# 4. Configure environment
cp .env.example .env
# Edit .env and set MOTHERDUCK_TOKEN (and other values)
```

---

## Initial Setup

Run the setup script once to create all infrastructure tables in MotherDuck:

```bash
# Verify connectivity first
python scripts/setup.py --verify

# Create schema, tables, and seed reference data
python scripts/setup.py

# Force recreate (drops all pipeline tables first — destructive)
python scripts/setup.py --force
```

---

## Usage

### Run the pipeline manually

```bash
normalize run
```

### Dry run (no writes)

```bash
normalize run --dry-run
```

### Check pipeline status

```bash
normalize status
normalize status --last 20
```

### Open the latest review UI

```bash
normalize review
```

### Re-seed reference tables

```bash
normalize seed
```

---

## Scheduling (macOS launchd)

The pipeline is designed to run daily at 06:00 via macOS launchd.

```bash
# 1. Edit the plist — replace <USER> and <PROJECT_DIR> with real values
nano scripts/com.agmri.product-normalizer.plist

# 2. Install
cp scripts/com.agmri.product-normalizer.plist \
   ~/Library/LaunchAgents/com.agmri.product-normalizer.plist

# 3. Load
launchctl load ~/Library/LaunchAgents/com.agmri.product-normalizer.plist

# 4. Verify
launchctl list | grep agmri

# 5. Trigger manually
launchctl start com.agmri.product-normalizer
```

Logs are written to:
- `logs/launchd_stdout.log`
- `logs/launchd_stderr.log`
- `logs/pipeline.log`

---

## 9-Step Matching Cascade

| Step | Method | Description |
|---|---|---|
| 1 | Junk filter | Drops blanks, pure numerics, single chars, known garbage strings |
| 2 | Exact mapping | Pre-built lookup table of known raw→normalized pairs |
| 3 | Catalog exact | Case-insensitive match against the product catalog |
| 4 | Abbreviation | Expands industry abbreviations (RU → Roundup) then re-matches catalog |
| 5 | NPK regex | Detects fertilizer ratio strings (28-0-0, 18-46-0, etc.) |
| 6 | 2,4-D variants | Normalises 2,4D / 24D / 2-4-D / 2 4 D → canonical form |
| 7 | Custom rules | Regex-based rules stored in DB (rate-embedded strings, etc.) |
| 8 | Fuzzy | RapidFuzz `token_set_ratio` ≥ threshold (default: 72) |
| 9 | No match | Entry queued for human review via browser UI |

---

## Review Workflow

When the pipeline finds entries it cannot resolve, it:

1. Writes them to `my_db.product_normalization.review_queue`
2. Generates a self-contained `output/review_<run_id>.html` file
3. Sends a macOS notification with a link to the file

Open the HTML file in any browser to review and classify each entry. Decisions are saved to a `decisions.json` sidecar file which is ingested on the next pipeline run.

---

## Running Tests

```bash
pytest

# With coverage report
pytest --cov=product_normalizer --cov-report=html
open htmlcov/index.html
```

All tests are fully offline — no MotherDuck connection required.

---

## Database Tables

All tables live in `my_db.product_normalization.*`.

| Table | Purpose |
|---|---|
| `normalization_decisions` | Append-only audit log of every match decision |
| `review_queue` | Unresolved entries awaiting human review |
| `pipeline_watermark` | CDC state — last successfully processed timestamp |
| `run_log` | Summary stats for every pipeline run |
| `abbreviation_dictionary` | Industry abbreviation → expansion mappings |
| `exact_mapping` | Known raw text → canonical product mappings |
| `custom_rules` | Active regex rules with priority ordering |

---

## Environment Variables

See `.env.example` for all available configuration options.

| Variable | Required | Default | Description |
|---|---|---|---|
| `MOTHERDUCK_TOKEN` | ✅ | — | MotherDuck authentication token |
| `FUZZY_THRESHOLD` | | `72` | Minimum fuzzy match score (0–100) |
| `WRITE_DB` | | `my_db` | Writable MotherDuck database |
| `GDRIVE_FOLDER_ID` | | — | Google Drive export folder |
| `LOG_LEVEL` | | `INFO` | `DEBUG` \| `INFO` \| `WARNING` \| `ERROR` |

---

## License

MIT
