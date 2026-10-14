# B report material (English, for the group report)

## Within-year patterns

Using A's cleaned 2017–2024 observations, I counted a five-minute reading as
wet when `rainfall_mm > 0`. For each Singapore-time year/month and hour, Spark
computed observed reading count, wet reading count, recorded rainfall sum, and
station count. Wet rate is wet readings divided by observed readings. Missing
readings were not counted as zero. The monthly results and hourly results are
in `artifacts/b/year_month.csv` and `hour.csv`; the figures are
`month_wet_rate.png` and `hour_wet_rate.png` in the same directory.

Pooling the observed records across the eight years, January had a 4.381% wet
rate and November 4.075%; July had 1.940%. The year-by-month heatmap shows
substantial year-to-year differences rather than the same monthly pattern in
every year. In the hourly pooled data, the wet rate rose from 1.084% at 23:00
to a peak of 5.815% at 15:00; 16:00 was 5.746%. These are descriptive rates of
the available station readings. Station membership and missing observation
times vary, so these rates are not area-weighted Singapore rainfall or a
causal explanation of rainfall timing.

## Forecasting examples and baseline

For each station and observed prediction time `t`, the target is the sum of
the six exact five-minute readings at `t+5` through `t+30` minutes. All six
must exist. We used the current five-minute reading and trailing 15-, 30-,
and 60-minute totals as historical features; all 12 readings in the trailing
60-minute window must exist. Month, hour, and station ID are also included.
The timestamp-as-interval-end convention and availability of the reading at
`t` are research assumptions from A's data contract. Future rainfall never
enters a feature. The threshold is an additional input: for each chosen
`q` in {0, 1, 5} mm, the label is `future_30_mm > q` (strict inequality).

We split by Singapore-time prediction date: 2017–2022 training, 2023
validation, 2024 test. A future target window crossing a split boundary is
discarded. The resulting compact table has 34,023,004 training station-times,
6,686,625 validation station-times, and 6,338,426 test station-times. These
counts are before expanding the three threshold choices. The table is stored
at `data/b_model/examples/`, partitioned by split. Exact rainfall sums and
thresholds are decimal, avoiding rounding at label boundaries.

The constant baseline predicts the observed training-set positive rate for
each threshold: 6.006% for 0 mm, 2.333% for 1 mm, and 0.811% for 5 mm. Its
test Brier scores are respectively 0.05958, 0.02505, and 0.00828. These
figures are a comparison point for C's model; the small Brier score at 5 mm
must be read alongside the rare positive rate (0.835% in the test set).

## Verification and limits

B's Spark window function matched 35 observed-anchor cases from A's
synthetic/real known-answer checks. Cases include missing future readings,
exact threshold equality, slightly above/below thresholds, and split
boundaries. B additionally exported the full diagnostic schema from Spark:
A's independent checker passed all 21 synthetic and 15 real queries with no
field mismatches, including a query with no observation at `t`. The full-data
preparation and within-year summaries also ran successfully on the local
machine. Parquet footer counts independently matched
the three split counts above. B's actual run used macOS ARM64, Java 17.0.20.1,
Python 3.11.15, PySpark 3.5.8, and Spark `local[4]` with a 4 GiB driver.
Python 3.11.15 differs from A's pinned 3.11.14 because the copied Python
environment could not be launched on this machine; this difference should be
listed in the final environment description. This is a retrospective experiment:
historical data may have been revised, and the first real-time availability
of each reading is not established. We did not use update timestamps as
predictors.

AI usage declaration for B: OpenAI Codex assisted in implementing and checking
the B scripts, preparing these descriptions, and interpreting the aggregate
tables. The group should review this declaration and the results before
submitting the final report.
