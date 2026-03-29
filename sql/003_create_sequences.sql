-- 003_create_sequences.sql
-- Sequences must be created before tables (002_create_tables.sql).
USE IntelinairAnalyzeDB;
CREATE SEQUENCE IF NOT EXISTS product_normalization.decisions_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS product_normalization.review_seq    START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS product_normalization.abbrev_seq    START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS product_normalization.exact_seq     START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS product_normalization.rules_seq     START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS product_normalization.runlog_seq    START 1 INCREMENT 1;
