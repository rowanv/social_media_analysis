"""
Analysis engine skeleton.

This module defines an AnalysisEngine class that will be filled with the metrics and
reporting code (KPIs, caption-level analysis, time-series). For now it provides a
clean API to accept a validated DataFrame and run placeholder pipeline steps.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional

@dataclass
class AnalysisConfig:
    timezone: str = "Europe/London"
    early_viral_z_threshold: float = 3.5  # robust z-score threshold for viral detection

class AnalysisEngine:
    def __init__(self, df: pd.DataFrame, config: Optional[AnalysisConfig] = None):
        self.df = df.copy()
        self.config = config or AnalysisConfig()
        self._validate_input()
        self.viral_table = None  # placeholder for early viral table

    def _validate_input(self):
        required_cols = ["timestamp","media_id","media_caption","comment_text"]
        for col in required_cols:
            if col not in self.df.columns:
                raise ValueError(f"Missing required column: {col}")
        self.df["timestamp"] = pd.to_datetime(self.df["timestamp"], errors="coerce", utc=True)
        self.df = self.df.dropna(subset=["timestamp"]).reset_index(drop=True)

    def prepare(self):
        """Prepare DataFrame: normalize timestamps, add local date/hour, clean text columns."""
        try:
            self.df["ts_local"] = self.df["timestamp"].dt.tz_convert(self.config.timezone)
        except Exception:
            self.df["ts_local"] = self.df["timestamp"]
        self.df["date"] = self.df["ts_local"].dt.date
        self.df["hour"] = self.df["ts_local"].dt.hour
        return self.df

    def compute_kpis(self):
        """Compute core KPIs: daily/hourly counts."""
        daily = self.df.groupby("date").size().reset_index(name="comments")
        hourly = self.df.groupby("hour").size().reset_index(name="comments")
        return {"daily": daily, "hourly": hourly}

    # --- New method for early viral detection ---
    def detect_early_viral_posts(self):
        """Flag posts as potential viral based on comments received in first 24 hours."""
        self.df["first_comment_ts"] = self.df.groupby("media_caption")["timestamp"].transform("min")
        self.df["hours_since_first"] = (self.df["timestamp"] - self.df["first_comment_ts"]).dt.total_seconds() / 3600

        first_24h = self.df[self.df["hours_since_first"] <= 24]
        early_comments = first_24h.groupby("media_caption").size().reset_index(name="early_comments")

        median_early = early_comments["early_comments"].median()
        mad_early = np.median(np.abs(early_comments["early_comments"] - median_early))
        mad_early = mad_early if mad_early > 0 else 1

        early_comments["robust_z"] = 0.6745 * (early_comments["early_comments"] - median_early) / mad_early
        early_comments["potential_viral"] = (early_comments["robust_z"] > self.config.early_viral_z_threshold).astype(int)

        # Merge with all posts to include posts with no early comments
        all_posts = pd.DataFrame(self.df["media_caption"].unique(), columns=["media_caption"])
        self.viral_table = all_posts.merge(
            early_comments[["media_caption", "potential_viral"]],
            on="media_caption",
            how="left"
        ).fillna(0)
        self.viral_table["potential_viral"] = self.viral_table["potential_viral"].astype(int)
        return self.viral_table

    def save_viral_table(self, path="viral_post_flags.csv"):
        if self.viral_table is None:
            raise ValueError("Run detect_early_viral_posts() first.")
        self.viral_table.to_csv(path, index=False)
