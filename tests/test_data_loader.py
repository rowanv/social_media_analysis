import pytest
from account.data_loader import read_comments_csv, validate_dataframe

def test_read_ok(tmp_path, SAMPLE_OK_DF):
    p = tmp_path / "ok.csv"
    SAMPLE_OK_DF.to_csv(p, index=False)
    df = read_comments_csv(str(p))
    ok, errors = validate_dataframe(df)
    assert ok
    assert errors == []
    assert "timestamp" in df.columns
    assert "comment_text" in df.columns

def test_missing_expected_columns_fails(tmp_path, SAMPLE_MISSING_DF):
    p = tmp_path / "missing.csv"
    SAMPLE_MISSING_DF.to_csv(p, index=False)
    with pytest.raises(ValueError):
        read_comments_csv(str(p))

def test_bad_path():
    with pytest.raises(FileNotFoundError):
        read_comments_csv("/non/existent/path.csv")
