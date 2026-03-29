"""
test_config.py
--------------
Unit tests for Settings and the read/write split constants in config.py.
"""

from __future__ import annotations

import pytest


class TestSettings:
    def test_settings_loads_from_env(self, mock_env):
        from product_normalizer.config import Settings

        s = Settings(motherduck_token="tk_test")  # type: ignore[call-arg]
        assert s.motherduck_token == "tk_test"

    def test_default_database_names(self):
        from product_normalizer.config import Settings

        s = Settings(motherduck_token="tk_test")  # type: ignore[call-arg]
        assert s.agmri_db == "agmri"
        assert s.catalog_db == "product_normalization_table"
        assert s.write_db == "my_db"
        assert s.pipeline_schema == "product_normalization"

    def test_rw_split_constants(self):
        from product_normalizer.config import Settings

        s = Settings(motherduck_token="tk_test")  # type: ignore[call-arg]
        assert s.AGMRI == "agmri"
        assert s.CATALOG == "product_normalization_table"
        assert s.W == "my_db.product_normalization"

    def test_fully_qualified_table_names(self):
        from product_normalizer.config import Settings

        s = Settings(motherduck_token="tk_test")  # type: ignore[call-arg]
        assert s.source_table == "agmri.agmri.base_feature"
        assert s.decisions_table == "my_db.product_normalization.normalization_decisions"
        assert s.watermark_table == "my_db.product_normalization.pipeline_watermark"
        assert s.review_queue_table == "my_db.product_normalization.review_queue"

    def test_invalid_log_level_raises(self):
        from product_normalizer.config import Settings
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Settings(motherduck_token="tk_test", log_level="NOTALEVEL")  # type: ignore[call-arg]

    def test_fuzzy_threshold_bounds(self):
        from product_normalizer.config import Settings
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Settings(motherduck_token="tk_test", fuzzy_threshold=150)  # type: ignore[call-arg]

        with pytest.raises(ValidationError):
            Settings(motherduck_token="tk_test", fuzzy_threshold=-1)  # type: ignore[call-arg]

    def test_valid_fuzzy_threshold(self):
        from product_normalizer.config import Settings

        s = Settings(motherduck_token="tk_test", fuzzy_threshold=80)  # type: ignore[call-arg]
        assert s.fuzzy_threshold == 80
