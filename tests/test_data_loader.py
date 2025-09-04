import os
import tempfile
import pandas as pd
import pytest

from account.data_loader import read_comments_csv, validate_dataframe

# Sample correct data for tests
SAMPLE_OK = pd.DataFrame({
    "timestamp": ["2025-03-01T12:00:00Z", "2025-03-02T13:30:00Z"],
    "media_id": ["m1", "m2"],
    "media_caption": ["Caption 1", "Caption 2"],
    "comment_text": ["Love this scent!", "Where can I buy?"]
})

# Sample data with missing required columns
SAMPLE_MISSING = pd.DataFrame({
    "time": ["2025-03-01T12:00:00Z"],
    "media_id": ["m1"],
    "comment_text": ["ok"]
})

# Helper to write temporary CSV files
def write_temp_csv(df, path):
    df.to_csv(path, index=False)

# --- Tests ---

def test_read_correct_sample_dataframe(tmp_path):
    p = tmp_path / "ok.csv"
    write_temp_csv(SAMPLE_OK, p)
    df = read_comments_csv(str(p))
    ok, errors = validate_dataframe(df)
    assert ok
    assert errors == []
    assert "timestamp" in df.columns
    assert "comment_text" in df.columns

def test_missing_expected_columns_faails(tmp_path):
    p = tmp_path / "missing.csv"
    write_temp_csv(SAMPLE_MISSING, p)
    with pytest.raises(ValueError):
        read_comments_csv(str(p))

def test_bad_path_provided_fails():
    with pytest.raises(FileNotFoundError):
        read_comments_csv("/non/existent/path.csv")

