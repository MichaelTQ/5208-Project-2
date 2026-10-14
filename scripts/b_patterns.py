"""B: two simple within-year views of observed five-minute rainfall."""

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[1]


def summarize(observations, keys):
    return (observations.groupBy(*keys)
            .agg(F.count("*").alias("observed_slots"),
                 F.sum((F.col("rainfall_mm") > 0).cast("long")).alias("wet_slots"),
                 F.sum("rainfall_mm").alias("observed_rainfall_mm"),
                 F.countDistinct("station_id").alias("stations"))
            .withColumn("wet_rate", F.col("wet_slots") / F.col("observed_slots"))
            .withColumn("mean_mm_per_observed_slot",
                        F.col("observed_rainfall_mm") / F.col("observed_slots")))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("rainfall-B-patterns")
             .master("local[4]").config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "64")
             .config("spark.sql.session.timeZone", "Asia/Singapore").getOrCreate())
    try:
        source = ROOT / ("data/handoff/stage2_sample" if args.sample else "data/clean/observations")
        output = ROOT / ("artifacts/b_sample" if args.sample else "artifacts/b")
        output.mkdir(exist_ok=True)
        obs = spark.read.parquet(str(source)).select("station_id", "observation_time", "rainfall_mm")
        obs = obs.withColumn("year", F.year("observation_time"))
        obs = obs.withColumn("month", F.month("observation_time"))
        obs = obs.withColumn("hour", F.hour("observation_time"))
        monthly = summarize(obs, ["year", "month"]).orderBy("year", "month").toPandas()
        hourly = summarize(obs, ["hour"]).orderBy("hour").toPandas()
        monthly.to_csv(output / "year_month.csv", index=False)
        hourly.to_csv(output / "hour.csv", index=False)

        grid = monthly.pivot(index="year", columns="month", values="wet_rate")
        fig, ax = plt.subplots(figsize=(9, 4))
        im = ax.imshow(grid * 100, aspect="auto", cmap="Blues", vmin=0)
        ax.set_xticks(range(len(grid.columns)), grid.columns)
        ax.set_yticks(range(len(grid.index)), grid.index)
        ax.set_xlabel("Month (Singapore time)")
        ax.set_ylabel("Year")
        ax.set_title("Wet five-minute readings by year and month")
        fig.colorbar(im, ax=ax, label="Wet readings / observed readings (%)")
        fig.tight_layout()
        fig.savefig(output / "month_wet_rate.png", dpi=180)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(hourly["hour"], hourly["wet_rate"] * 100, marker="o")
        ax.set_xticks(range(0, 24, 2))
        ax.set_xlabel("Hour (Singapore time)")
        ax.set_ylabel("Wet readings / observed readings (%)")
        ax.set_title("Observed rainfall occurrence by hour, 2017–2024")
        ax.grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig(output / "hour_wet_rate.png", dpi=180)
        plt.close(fig)
        print(f"Saved two figures and their summary tables in {output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
