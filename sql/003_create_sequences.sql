-- ─────────────────────────────────────────────────────────────────────────────
-- 003_create_sequences.sql
-- Auto-increment sequences for all tables.
-- Must be run BEFORE 002_create_tables.sql.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SEQUENCE IF NOT EXISTS my_db.product_normalization.decisions_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS my_db.product_normalization.review_seq    START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS my_db.product_normalization.abbrev_seq    START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS my_db.product_normalization.exact_seq     START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS my_db.product_normalization.rules_seq     START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS my_db.product_normalization.runlog_seq    START 1 INCREMENT 1;
