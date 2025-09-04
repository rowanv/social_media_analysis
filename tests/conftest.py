# tests/conftests.py
import pytest
import pandas as pd

@pytest.fixture
def SAMPLE_OK_DF():
    return pd.DataFrame({
        "timestamp": ["2025-03-01T12:00:00Z", "2025-03-02T13:30:00Z"],
        "media_id": ["m1", "m2"],
        "media_caption": ["Caption 1", "Caption 2"],
        "comment_text": ["Love this scent!", "Where can I buy?"]
    })

@pytest.fixture
def SAMPLE_MISSING_DF():
    return pd.DataFrame({
        "time": ["2025-03-01T12:00:00Z"],
        "media_id": ["m1"],
        "comment_text": ["ok"]
    })
