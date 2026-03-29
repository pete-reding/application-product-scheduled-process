"""
config.py
---------
Central configuration — all database names, schema paths, and tunable
thresholds are defined here.  Consumed by every other module.

Read / write split
~~~~~~~~~~~~~~~~~~
  AGMRI   — read-only share (source machine data)
  CATALOG — read-only share (product catalog)
  W       — writable database (all pipeline outputs)
"""

from __future__ import annotations

from functools import cached_property
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root (two levels up from this file: src/product_normalizer/ → project root)
# parents[0]=product_normalizer  parents[1]=src  parents[2]=project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── MotherDuck ────────────────────────────────────────────────────
    motherduck_token: str = Field(..., description="MotherDuck auth token")

    # ── Database names ────────────────────────────────────────────────
    agmri_db: str = Field("agmri", description="Read-only agmri source share")
    catalog_db: str = Field(
        "product_normalization_table", description="Read-only product catalog share"
    )
    write_db: str = Field("my_db", description="Writable output database")
    pipeline_schema: str = Field(
        "product_normalization", description="Schema inside write_db for pipeline tables"
    )

    # ── Matching thresholds ───────────────────────────────────────────
    fuzzy_threshold: int = Field(72, ge=0, le=100)
    min_token_length: int = Field(2, ge=1)

    # ── Google Drive ──────────────────────────────────────────────────
    gdrive_folder_id: str = Field("", description="Drive folder ID for daily exports")

    # ── macOS notifications ───────────────────────────────────────────
    macos_sound: str = Field("Glass")

    # ── Logging ───────────────────────────────────────────────────────
    log_level: str = Field("INFO")
    log_dir: Path = Field(PROJECT_ROOT / "logs")

    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return upper

    # ── Convenience aliases (read/write split) ────────────────────────
    @cached_property
    def AGMRI(self) -> str:  # noqa: N802
        """Fully-qualified prefix for agmri source tables."""
        return self.agmri_db

    @cached_property
    def CATALOG(self) -> str:  # noqa: N802
        """Fully-qualified prefix for catalog tables."""
        return self.catalog_db

    @cached_property
    def W(self) -> str:  # noqa: N802
        """Writable schema prefix: <write_db>.<pipeline_schema>"""
        return f"{self.write_db}.{self.pipeline_schema}"

    # ── Table FQNs ────────────────────────────────────────────────────
    @cached_property
    def source_table(self) -> str:
        return f"{self.AGMRI}.agmri.base_feature"

    @cached_property
    def catalog_table(self) -> str:
        return f"{self.CATALOG}.main.product_catalog"

    @cached_property
    def watermark_table(self) -> str:
        return f"{self.W}.pipeline_watermark"

    @cached_property
    def decisions_table(self) -> str:
        return f"{self.W}.normalization_decisions"

    @cached_property
    def review_queue_table(self) -> str:
        return f"{self.W}.review_queue"

    @cached_property
    def abbreviations_table(self) -> str:
        return f"{self.W}.abbreviation_dictionary"

    @cached_property
    def custom_rules_table(self) -> str:
        return f"{self.W}.custom_rules"

    @cached_property
    def exact_map_table(self) -> str:
        return f"{self.W}.exact_mapping"

    @cached_property
    def run_log_table(self) -> str:
        return f"{self.W}.run_log"


# Singleton — import this everywhere
settings = Settings()  # type: ignore[call-arg]
