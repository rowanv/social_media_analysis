"""
Analysis engine skeleton.

This module defines an AnalysisEngine class that will be filled with the metrics and
reporting code (KPIs, caption-level analysis, time-series). For now it provides a
clean API to accept a validated DataFrame and run placeholder pipeline steps.
"""
from __future__ import annotations

import pandas as pd
from dataclasses import dataclass
from typing import Optional


@dataclass
class AnalysisConfig:
    timezone: str = "Europe/London"


class AnalysisEngine:
    def __init__(self, df: pd.DataFrame, config: Optional[AnalysisConfig] = None):
        """Initialize engine with a validated DataFrame (see data_loader.validate_dataframe)."""
        self.df = df.copy()
        self.config = config or AnalysisConfig()
        self._validate_input()

    def _validate_input(self):
        if not isinstance(self.df, pd.DataFrame):
            raise ValueError("df must be a pandas DataFrame")
        for col in ["timestamp","media_id","media_caption","comment_text"]:
            if col not in self.df.columns:
                raise ValueError(f"Missing required column: {col}")

    def prepare(self):
        """Prepare DataFrame: normalize timestamps, add local date/hour, and clean text columns."""
        # Placeholder: implement normalization & enrichment
        self.df["timestamp"] = pd.to_datetime(self.df["timestamp"], utc=True)
        try:
            self.df["ts_local"] = self.df["timestamp"].dt.tz_convert(self.config.timezone)
        except Exception:
            self.df["ts_local"] = self.df["timestamp"]
        self.df["date"] = self.df["ts_local"].dt.date
        self.df["hour"] = self.df["ts_local"].dt.hour
        return self.df

    def compute_kpis(self):
        """Compute core KPIs: daily/hourly counts, sentiment placeholders, etc.

        Returns: dict of DataFrames
        """
        # Placeholder implementation: return counts only
        daily = self.df.groupby("date").size().reset_index(name="comments")
        hourly = self.df.groupby("hour").size().reset_index(name="comments")
        return {"daily": daily, "hourly": hourly}

    # @TODO: add more methods here for caption analysis, purchase intent detection, etc.
    