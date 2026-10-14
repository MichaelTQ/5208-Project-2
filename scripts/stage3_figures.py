"""Publication-style static figures built only from small Spark summaries."""
import json
import os
from pathlib import Path
os.environ.setdefault("MPLCONFIGDIR", str(Path(__file__).resolve().parents[1] / ".cache/matplotlib"))
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image
from common import ROOT, sha256_file
from stage2_common import write_json


def main():
    p = ROOT / "artifacts/stage3"
    tables, figures = p / "tables", p / "figures"
    figures.mkdir(parents=True, exist_ok=True)
    complete = json.loads((p / "analysis_complete.json").read_text())
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False, "axes.spines.right": False, "savefig.facecolor": "white", "pdf.fonttype": 42})
    manifest = []

    def save(fig, name, sources, note):
        fig.savefig(figures / f"{name}.png", dpi=180, bbox_inches="tight")
        fig.savefig(figures / f"{name}.pdf", bbox_inches="tight")
        with Image.open(figures / f"{name}.png") as im:
            dimensions = list(im.size)
        manifest.append({"name": name, "png": str((figures / f"{name}.png").relative_to(ROOT)), "pdf": str((figures / f"{name}.pdf").relative_to(ROOT)), "png_size": dimensions, "data_sources": [{"path": str((tables / src).relative_to(ROOT)), "sha256": sha256_file(tables / src)} for src in sources], "interpretation": note})
        plt.close(fig)

    all_year = pd.read_csv(tables / "all_station_year.csv")
    network = pd.read_csv(tables / "network_year.csv").sort_values("observation_year")
    cohort = pd.read_csv(tables / "cohort_year.csv").sort_values("observation_year")
    matched = pd.read_csv(tables / "station_matched_annual.csv").sort_values("observation_year")
    global_year = pd.read_csv(tables / "global_matched_annual.csv").sort_values("observation_year")
    meta = pd.read_csv(tables / "cohort_metadata.csv").set_index("station_id")
    pooled = pd.read_csv(tables / "global_matched_pooled.csv").sort_values("rainfall_rate_mm_per_5min")
    years = network.observation_year.to_numpy()
    order = all_year.groupby("station_id").calendar_coverage.mean().sort_values(ascending=False).index
    matrix = all_year.pivot(index="station_id", columns="observation_year", values="calendar_coverage").reindex(order)
    fig, ax = plt.subplots(figsize=(10.5, 17), layout="constrained")
    image = ax.imshow(matrix.to_numpy(), aspect="auto", cmap="viridis", vmin=0, vmax=1)
    ax.set_xticks(np.arange(8), years); ax.set_yticks(np.arange(len(order)), order, fontsize=7.5)
    ax.set_xlabel("Observation year (SGT)"); ax.set_ylabel("Station ID (ordered by mean calendar coverage)")
    ax.set_title("Calendar coverage across all 91 station IDs", loc="left", fontweight="bold")
    fig.colorbar(image, ax=ax, fraction=.035, label="Observed slots / full-calendar slots")
    fig.suptitle("Zero coverage means no valid observations; it does not mean zero rainfall.", y=1.01, fontsize=10)
    save(fig, "01_calendar_coverage", ["all_station_year.csv"], "All 91 IDs and all eight years; annual calendar denominators, zero observations are not zero rainfall.")

    fig, axes = plt.subplots(2, 2, figsize=(13, 8.4), layout="constrained")
    series = [(network, "rainfall_rate_mm_per_5min", "wet_rate", "All network: record weighted", "#718096"), (cohort, "equal_station_rainfall_rate", "equal_station_wet_rate", "Fixed 20 stations: observed times", "#D28A25"), (matched, "equal_station_rainfall_rate", "equal_station_wet_rate", "Per-station calendar matched: equal weights", "#2563B4")]
    for frame, rain, wet, label, color in series:
        axes[0, 0].plot(years, frame[rain], marker="o", label=label, color=color, linewidth=2)
        axes[0, 1].plot(years, frame[wet] * 100, marker="o", color=color, linewidth=2)
    for col, metric in enumerate(["equal_station_rainfall_rate", "equal_station_wet_rate"]):
        values = global_year[metric] * (100 if col == 1 else 1)
        axes[1, col].plot(years, values, marker="o", color="#36845F", linewidth=2)
        axes[1, col].set_title("Joint support: only 3,976 slots in 5 months", loc="left", fontsize=11)
    axes[0, 0].set_title("Observed rainfall rate on different supports", loc="left", fontsize=11)
    axes[0, 1].set_title("Wet fraction (strictly positive readings)", loc="left", fontsize=11)
    for row in range(2):
        axes[row, 0].set_ylabel("Mean mm per observed 5-minute slot")
        axes[row, 1].set_ylabel("Wet slots / observed slots (%)")
    for ax in axes.flat:
        ax.set_xticks(years, years, rotation=45); ax.set_ylim(bottom=0); ax.grid(axis="y", alpha=.2)
    axes[0, 0].legend(fontsize=8, loc="lower left")
    fig.suptitle("Annual comparisons depend on calendar support\nThese are observed-sample rates, not complete annual or national rainfall.", fontsize=13, fontweight="bold")
    save(fig, "02_annual_support_comparison", ["network_year.csv", "cohort_year.csv", "station_matched_annual.csv", "global_matched_annual.csv"], "Primary station-specific support controls within-station calendar positions; joint support only covers five months and gives a different ordering.")

    support = pd.read_csv(tables / "station_support_months.csv", dtype={"month": str})
    month_matrix = support.pivot(index="station_id", columns="month", values="count").reindex(index=complete["cohort"], columns=[f"{m:02}" for m in range(1, 13)]).fillna(0)
    proportions = month_matrix.div(month_matrix.sum(axis=1), axis=0) * 100
    global_months = pd.read_csv(tables / "global_support_months.csv").set_index("month")["count"].reindex(range(1, 13), fill_value=0)
    fig, axes = plt.subplots(1, 2, figsize=(13, 7), layout="constrained", gridspec_kw={"width_ratios": [1.6, 1]})
    im = axes[0].imshow(proportions, aspect="auto", cmap="Blues", vmin=0)
    axes[0].set_yticks(range(20), complete["cohort"]); axes[0].set_xticks(range(12), range(1, 13))
    axes[0].set_title("Each station's eight-year matched support", loc="left", fontsize=11)
    axes[0].set_xlabel("Calendar month"); fig.colorbar(im, ax=axes[0], shrink=.7, label="Share of a station's matched slots (%)")
    axes[1].bar(range(1, 13), global_months, color="#36845F"); axes[1].set_xticks(range(1, 13))
    axes[1].set_xlabel("Calendar month"); axes[1].set_ylabel("Shared 5-minute calendar keys")
    axes[1].set_title("All 20 stations jointly matched", loc="left", fontsize=11)
    axes[1].text(.03, .95, "Only 5 months\n70.7% in September", transform=axes[1].transAxes, ha="left", va="top", fontsize=10)
    fig.suptitle("Missingness restricts the calendar positions available for comparison", fontweight="bold")
    save(fig, "03_calendar_support_distribution", ["station_support_months.csv", "global_support_months.csv"], "Support distributions are observation availability, not monthly rainfall patterns; the joint panel is seasonally restricted.")

    fig, axes = plt.subplots(1, 2, figsize=(12, 8), layout="constrained", sharey=True)
    labels = [f"{s}  {meta.loc[s, 'station_name']}" for s in pooled.station_id]
    y = np.arange(len(pooled))
    axes[0].barh(y, pooled.rainfall_rate_mm_per_5min, color="#36845F")
    axes[1].barh(y, pooled.wet_rate * 100, color="#537EAF")
    axes[0].set_yticks(y, labels, fontsize=8); axes[0].set_xlabel("Mean mm per observed 5-minute slot")
    axes[1].set_xlabel("Wet slots / observed slots (%)")
    for ax in axes:
        ax.grid(axis="x", alpha=.2)
    fig.suptitle("Station differences on identical limited calendar support\n31,808 readings per station over 8 years; only 5 calendar months represented", fontsize=12, fontweight="bold")
    save(fig, "04_station_differences", ["global_matched_pooled.csv", "cohort_metadata.csv"], "Spatial comparison uses exactly the same calendar positions for all 20 stations; rankings only describe this five-month subset.")

    positions = pooled.merge(meta.reset_index(), on="station_id")
    fig, ax = plt.subplots(figsize=(10, 7), layout="constrained")
    scatter = ax.scatter(positions.longitude, positions.latitude, c=positions.rainfall_rate_mm_per_5min, cmap="viridis", s=95, edgecolor="white", linewidth=.6)
    for i, r in positions.iterrows():
        offset = (-38, -12) if r.station_id == "S114" else (8, 9) if r.station_id == "S121" else (5, 5 if i % 2 else -10)
        ax.annotate(r.station_id, (r.longitude, r.latitude), xytext=offset, textcoords="offset points", fontsize=8)
    ax.set_xlabel("Longitude (degrees)"); ax.set_ylabel("Latitude (degrees)"); ax.grid(alpha=.15)
    ax.set_aspect(1 / np.cos(np.deg2rad(positions.latitude.mean())), adjustable="datalim")
    fig.colorbar(scatter, ax=ax, label="Observed mean mm / 5-minute slot")
    ax.set_title("Sampled station locations and rainfall rates\nJoint support: 3,976 calendar slots × 8 years; five months only", loc="left", fontsize=12, fontweight="bold")
    fig.text(.12, -.025, "Station points only. No spatial interpolation or area-weighted national estimate.", fontsize=9)
    save(fig, "05_station_locations", ["global_matched_pooled.csv", "cohort_metadata.csv"], "Coordinates of fixed-cohort station points; no interpolated surface and no claim of area-weighted national rainfall.")

    fig, ax = plt.subplots(figsize=(10, 5.5), layout="constrained")
    for column, label, color in [("wet_p50_mm", "Wet p50", "#64748B"), ("wet_p90_mm", "Wet p90", "#D28A25"), ("wet_p99_mm", "Wet p99", "#2563B4")]:
        ax.plot(years, network[column], marker="o", color=color, label=label, linewidth=2)
    ax.set_xticks(years); ax.set_ylim(bottom=0); ax.set_ylabel("mm per wet 5-minute observation")
    ax.set_title("Wet-reading intensity across the observed network", loc="left", fontweight="bold")
    ax.legend(); ax.grid(axis="y", alpha=.2)
    fig.text(.12, -.035, "Wet = rainfall > 0; approximate rank quantiles (accuracy 10,000). Station mix and missingness vary.", fontsize=9)
    save(fig, "06_wet_reading_quantiles", ["network_year.csv"], "Conditional wet reading distribution, approximate quantiles; unbalanced network observation support varies by year.")
    write_json(p / "figure_manifest.json", manifest)
    print(f"Generated {len(manifest)} PNG figures and {len(manifest)} vector PDF figures")


if __name__ == "__main__":
    main()
