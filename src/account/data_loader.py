"""
Data loader utilities for Instagram comments analysis.

Provides:
 - read_comments_csv(path) -> pd.DataFrame
 - validate_dataframe(df) -> tuple(bool, list[str])

Design goals:
 - clear errors (ValueError) when input is malformed
 - tolerant datetime parsing
 - small functions to allow easy unit testing
"""
from __future__ import annotations

import os
from typing import Tuple, List
import pandas as pd
import numpy as np

REQUIRED_COLUMNS = {"timestamp", "media_id", "media_caption", "comment_text"}


def read_comments_csv(path: str, tz: str = "UTC") -> pd.DataFrame:
    """Read CSV into pandas DataFrame with parsing & basic normalization.

    Args:
        path: path to CSV file
        tz: timezone of incoming timestamps; if timestamps are timezone-aware, this is ignored

    Returns:
        DataFrame with columns: timestamp (tz-aware UTC), media_id, media_caption, comment_text

    Raises:
        FileNotFoundError: if path doesn't exist
        ValueError: if required columns are missing after reading
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")

    try:
        df = pd.read_csv(path)
    except Exception as e:
        raise ValueError(f"Unable to read CSV: {e}")

    # Validate required cols
    cols = set(df.columns.astype(str))
    missing = REQUIRED_COLUMNS - cols
    if missing:
        raise ValueError(f"Missing required columns: {sorted(list(missing))}")

    # Keep only required columns (preserve order)
    df = df[[c for c in ["timestamp","media_id","media_caption","comment_text"]]]

    # Parse timestamps robustly
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    # Report rows with bad timestamps
    bad_ts = df["timestamp"].isna()
    if bad_ts.any():
        # keep rows but warn in a column
        df.loc[bad_ts, "_bad_timestamp"] = True
    else:
        df["_bad_timestamp"] = False

    return df


def validate_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Simple validator returning (ok, errors).

    Checks for required columns and basic sanity checks (non-empty comment_text, timestamp parsed).
    """
    errors: List[str] = []
    if not isinstance(df, pd.DataFrame):
        return False, ["Input is not a pandas DataFrame"]

    cols = set(df.columns.astype(str))
    missing = REQUIRED_COLUMNS - cols
    if missing:
        errors.append(f"Missing columns: {sorted(list(missing))}")
        return False, errors

    # Check comment_text presence
    if df["comment_text"].isna().all():
        errors.append("All comment_text values are NA")

    # Check timestamps
    if "timestamp" in df.columns:
        if df["timestamp"].isna().all():
            errors.append("All timestamp values failed to parse or are NA")

    ok = len(errors) == 0
    return ok, errors
