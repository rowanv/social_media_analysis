import pytest
import pandas as pd
from account.analysis import AnalysisEngine, AnalysisConfig

# Use fixtures

def test_engine_init(SAMPLE_OK_DF):
    engine = AnalysisEngine(SAMPLE_OK_DF)
    assert engine.df.shape[0] == SAMPLE_OK_DF.shape[0]
    assert isinstance(engine.config, AnalysisConfig)


def test_engine_prepare(SAMPLE_OK_DF):
    engine = AnalysisEngine(SAMPLE_OK_DF)
    df_prepared = engine.prepare()
    # Check new columns exist
    assert "ts_local" in df_prepared.columns
    assert "date" in df_prepared.columns
    assert "hour" in df_prepared.columns
    # Check correct number of rows preserved
    assert df_prepared.shape[0] == SAMPLE_OK_DF.shape[0]


def test_compute_kpis_structure(SAMPLE_OK_DF):
    engine = AnalysisEngine(SAMPLE_OK_DF)
    engine.prepare()
    kpis = engine.compute_kpis()
    assert isinstance(kpis, dict)
    assert "daily" in kpis
    assert "hourly" in kpis
    assert isinstance(kpis["daily"], pd.DataFrame)
    assert isinstance(kpis["hourly"], pd.DataFrame)


def test_invalid_dataframe_init():
    with pytest.raises(ValueError):
        AnalysisEngine(pd.DataFrame({"bad": [1,2,3]}))


def test_engine_timezone_override(SAMPLE_OK_DF):
    config = AnalysisConfig(timezone="UTC")
    engine = AnalysisEngine(SAMPLE_OK_DF, config=config)
    df_prepared = engine.prepare()
    # Check that ts_local is still datetime64[ns, UTC]
    assert str(df_prepared["ts_local"].dtype) == 'datetime64[ns, UTC]'
