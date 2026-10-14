# B: within-year patterns and forecasting examples

This is the small, classroom-style B module. It uses Spark DataFrames and
window functions; Matplotlib only plots the small grouped results. A's formal
input is `data/clean/observations/`, not the old Stage 1 sample.

## What B produces

1. `scripts/b_patterns.py` groups valid observations by year/month and by
   hour. It writes `artifacts/b/year_month.csv`, `hour.csv`, and two PNGs.
   Rates are **wet observed five-minute readings / observed readings**.
   Changing station coverage and missing times remain limitations; these are
   descriptive network summaries, not national rainfall totals.
2. `scripts/b_prepare.py` writes `data/b_model/examples/`, one row per usable
   station and prediction time. It also writes `label_counts/`, `baseline/`,
   and `baseline_evaluation/`. The baseline is the **training** positive rate
   at each threshold; evaluation gives its Brier score on each split.
3. `scripts/b_windows.py` contains the window and threshold functions C can
   import. `tests/test_b_windows.py` compares the same Spark window function
   with A's synthetic and real known-answer queries.
4. `scripts/b_export_checks.py` exports both small query sets in A's requested
   JSON format. A's independent `window_audit.py` reports PASS for all 21
   synthetic and 15 real queries, including a query without a reading at `t`.
   This small diagnostic adapter uses Spark exact-time joins; the full table
   still uses `b_windows.py`.

The chosen experimental thresholds are 0, 1, and 5 mm. They are B's choices,
not assignment rules. `add_thresholds(examples)` gives C `q_mm` and a binary
`label`, with a strict `future_30_mm > q_mm` comparison. Keeping the base table
at one row per station/time avoids storing the same features three times.
Empty, negative, nonfinite, and finer-than-nine-decimal threshold grids are
rejected by `add_thresholds`.

## Window definition

At station `s`, prediction time `t`, the future total uses observations at
`t+5, t+10, t+15, t+20, t+25, t+30` minutes. The 5-minute reading at `t` is
assumed available as a feature, following A's research convention; the source
does not prove its exact real-time arrival. `past_15_mm`, `past_30_mm`, and
`past_60_mm` include `t` and require respectively 3, 6, and 12 uninterrupted
five-minute observations. We keep only rows with complete past 60 minutes,
complete future 30 minutes, and all future target times in the same split.
Missing observations are never filled with zero. Sums and threshold comparisons
remain decimal; model vector conversion is C's responsibility. A's cleaned
`station_id + observation_time` uniqueness and five-minute grid are required.

Training: 2017-01-01 through 2022-12-31; validation: 2023; test: 2024,
all in `Asia/Singapore`. Historical feature windows may reach into an earlier
split because they precede `t`. Future label windows cannot cross a split.

## Run

Use the environment in `docs/ENVIRONMENT.md` and run from the project root:

```bash
bash scripts/run.sh tests/test_b_windows.py
bash scripts/run.sh scripts/b_patterns.py --sample
bash scripts/run.sh scripts/b_prepare.py --sample
bash scripts/run.sh scripts/b_patterns.py
bash scripts/run.sh scripts/b_prepare.py
bash scripts/run.sh scripts/b_export_checks.py synthetic
bash scripts/run.sh scripts/b_export_checks.py real
python3 scripts/window_audit.py --observations data/handoff/stage6_window_checks/synthetic_observations.csv --queries data/handoff/stage6_window_checks/synthetic_queries.csv --actual artifacts/b/audit/synthetic_actual.json --output artifacts/b/audit/synthetic_audit.json
python3 scripts/window_audit.py --observations data/handoff/stage6_window_checks/real_observations.csv --queries data/handoff/stage6_window_checks/real_queries.csv --actual artifacts/b/audit/real_actual.json --output artifacts/b/audit/real_audit.json
```

The `--sample` runs use A's deliberately selected station-days to check code;
their rates must not be reported as population results. C reads the full
`data/b_model/examples/`, expands thresholds with `add_thresholds`, and fits
encoders/scalers/models on `split = train` only. `baseline/` is a Spark CSV
directory holding the training probability for each `q_mm`.

For C, the minimal handoff is:

```python
from b_windows import add_thresholds

base = spark.read.parquet("data/b_model/examples")
train = add_thresholds(base.filter("split = 'train'"))
validation = add_thresholds(base.filter("split = 'validation'"))
test = add_thresholds(base.filter("split = 'test'"))
```

Run C's script from the project root and include `scripts/` on `PYTHONPATH`
or keep C's script inside `scripts/`. The model feature columns are
`station_id`, `month`, `hour`, `current_5_mm`, `past_15_mm`, `past_30_mm`,
`past_60_mm`, and `q_mm`. `future_30_mm` is answer data and must not enter
the model feature vector. Station encoding and any model preprocessing are
fitted on `train` only.

The transferred `.venv` originally contained an absolute Python symlink to A's
computer and macOS blocked copied native packages. Its backup is outside this
project at `../.venv_from_A_20261013/`. This machine now has a working local
`.venv` based on its Python 3.11.15 and installed numerical libraries, with
PySpark 3.5.8 and Py4J copied from the backup. The `scripts/run.sh` B commands
above were checked in that environment. It differs from A's exact lockfile;
record that difference in the final report. For another computer, create a
fresh environment with `scripts/setup.sh` rather than copying a `.venv` or
clicking through macOS warnings one file at a time.

A's existing `handoff_manifest.json` is a 2026-10-11 snapshot and does not list
these newer B files or `data/b_model/`. Include B's code, report material,
figures, and generated model input when the group makes its final submission.
