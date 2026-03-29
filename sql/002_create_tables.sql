-- ─────────────────────────────────────────────────────────────────────────────
-- 002_create_tables.sql
-- All 7 pipeline infrastructure tables in IntelinairAnalyzeDB.product_normalization.
-- Safe to run multiple times (CREATE TABLE IF NOT EXISTS).
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. CDC watermark
CREATE TABLE IF NOT EXISTS IntelinairAnalyzeDB.product_normalization.pipeline_watermark (
    pipeline_name   VARCHAR     NOT NULL,
    watermark_ts    TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Append-only decision log
CREATE TABLE IF NOT EXISTS IntelinairAnalyzeDB.product_normalization.normalization_decisions (
    id              BIGINT      DEFAULT nextval('IntelinairAnalyzeDB.product_normalization.decisions_seq'),
    feature_id      VARCHAR     NOT NULL,
    flow_published_at TIMESTAMPTZ,
    raw_product_name VARCHAR    NOT NULL,
    match_method    VARCHAR     NOT NULL,   -- junk | exact_map | catalog_exact | …
    normalized_name VARCHAR,
    product_id      VARCHAR,
    category        VARCHAR,
    npk_analysis    VARCHAR,
    confidence      DOUBLE,
    notes           VARCHAR,
    run_id          VARCHAR     NOT NULL,
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. Human review queue
CREATE TABLE IF NOT EXISTS IntelinairAnalyzeDB.product_normalization.review_queue (
    id              BIGINT      DEFAULT nextval('IntelinairAnalyzeDB.product_normalization.review_seq'),
    feature_id      VARCHAR     NOT NULL,
    flow_published_at TIMESTAMPTZ,
    raw_product_name VARCHAR    NOT NULL,
    run_id          VARCHAR     NOT NULL,
    queued_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved        BOOLEAN     NOT NULL DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ,
    resolved_by     VARCHAR,
    resolution_note VARCHAR
);

-- 4. Abbreviation dictionary (seed data in 003_seed.sql)
CREATE TABLE IF NOT EXISTS IntelinairAnalyzeDB.product_normalization.abbreviation_dictionary (
    id          INTEGER DEFAULT nextval('IntelinairAnalyzeDB.product_normalization.abbrev_seq'),
    abbreviation VARCHAR NOT NULL,
    expansion    VARCHAR NOT NULL,
    notes        VARCHAR,
    created_at   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 5. Exact mapping table
CREATE TABLE IF NOT EXISTS IntelinairAnalyzeDB.product_normalization.exact_mapping (
    id              INTEGER DEFAULT nextval('IntelinairAnalyzeDB.product_normalization.exact_seq'),
    raw_text        VARCHAR NOT NULL UNIQUE,
    product_id      VARCHAR,
    normalized_name VARCHAR NOT NULL,
    category        VARCHAR,
    notes           VARCHAR,
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 6. Custom regex rules
CREATE TABLE IF NOT EXISTS IntelinairAnalyzeDB.product_normalization.custom_rules (
    id              INTEGER DEFAULT nextval('IntelinairAnalyzeDB.product_normalization.rules_seq'),
    pattern         VARCHAR NOT NULL,           -- Python regex
    normalized_name VARCHAR NOT NULL,
    product_id      VARCHAR,
    category        VARCHAR,
    priority        INTEGER NOT NULL DEFAULT 100,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    notes           VARCHAR,
    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 7. Pipeline run log
CREATE TABLE IF NOT EXISTS IntelinairAnalyzeDB.product_normalization.run_log (
    id                  BIGINT  DEFAULT nextval('IntelinairAnalyzeDB.product_normalization.runlog_seq'),
    run_id              VARCHAR NOT NULL,
    watermark_start     TIMESTAMPTZ,
    watermark_end       TIMESTAMPTZ,
    total_candidates    INTEGER,
    resolved            INTEGER,
    queued_for_review   INTEGER,
    duration_seconds    DOUBLE,
    status              VARCHAR NOT NULL DEFAULT 'success',
    error_message       VARCHAR,
    logged_at           TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
