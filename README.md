# social_media_analysis
Toy analysis of a corpus of posts left on a social media account


### Setup

#### Virtual Env generation
```
python3 -m venv proj_social_media_analysis
source social_media_analysis/bin/activate
pip install -r requirements.txt
```

#### Run tests
```bash
export PYTHONPATH=$PWD/src
pytest -q
```


#### Example usage:
```bash
python scripts/run_analysis.py --input data/account_comments_mar2025.csv
```

## Notes
- `src/account/data_loader.py` contains robust readers and validation. If your data is large, consider switching to chunked reading or Dask.
- `src/account/analysis.py` contains an `AnalysisEngine` skeleton to be filled with feature extraction and KPIs.
```
```

Modularization/scalability
- new data loaders (parquet, etc. can be added to `src/account/data_loader.py`)
- specify account name
