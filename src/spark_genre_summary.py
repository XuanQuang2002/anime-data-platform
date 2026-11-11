import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


def main():
    """Read one silver snapshot, display analytics, and optionally save them."""
    parser = argparse.ArgumentParser(
        description="Calculate anime analytics from one silver snapshot."
    )
    parser.add_argument(
        "--silver-dir",
        required=True,
        help="Directory containing anime.parquet and anime_genres.parquet.",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save analytics as Parquet in data/reports/spark_analytics.",
    )
    args = parser.parse_args()

    silver_dir = Path(args.silver_dir)

    spark = (
        SparkSession.builder
        .appName("AnimeAnalytics")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        anime = spark.read.parquet(
            str(silver_dir / "anime.parquet")
        ).select(
            "anilist_id",
            "title_romaji",
            "title_english",
            "season_year",
            "season",
            "average_score",
            "popularity",
            "snapshot_date",
        )

        genres = spark.read.parquet(
            str(silver_dir / "anime_genres.parquet")
        ).select(
            "anilist_id",
            "genre_name",
        )

        # Keep unrated titles so the title counts include them.
        anime_genres = anime.join(
            genres,
            on="anilist_id",
            how="inner",
        )

        genre_summary = (
            anime_genres
            .groupBy("snapshot_date", "genre_name")
            .agg(
                F.countDistinct("anilist_id").alias("anime_count"),
                F.count("average_score").alias("rated_anime_count"),
                F.round(
                    F.avg("average_score"), 2
                ).alias("avg_anime_score"),
            )
            .orderBy(
                "snapshot_date",
                F.desc("anime_count"),
                "genre_name",
            )
        )

        # Titles without a known release season cannot enter these reports.
        seasonal_anime = anime.filter(
            F.col("season_year").isNotNull()
            & F.col("season").isNotNull()
        )

        season_summary = (
            seasonal_anime
            .groupBy("snapshot_date", "season_year", "season")
            .agg(
                F.countDistinct("anilist_id").alias("anime_count"),
                F.count("average_score").alias("rated_anime_count"),
                F.round(
                    F.avg("average_score"), 2
                ).alias("avg_anime_score"),
            )
            .orderBy("snapshot_date", "season_year", "season")
        )

        rated_anime = seasonal_anime.filter(
            F.col("average_score").isNotNull()
        )

        # Tie breakers make the selected five titles repeatable.
        ranking_window = (
            Window
            .partitionBy("snapshot_date", "season_year", "season")
            .orderBy(
                F.desc("average_score"),
                F.col("popularity").desc_nulls_last(),
                F.asc("anilist_id"),
            )
        )

        top_anime = (
            rated_anime
            .withColumn(
                "score_position",
                F.row_number().over(ranking_window),
            )
            .filter(F.col("score_position") <= 5)
            .select(
                "snapshot_date",
                "season_year",
                "season",
                "score_position",
                "anilist_id",
                F.coalesce(
                    "title_english",
                    "title_romaji",
                    F.col("anilist_id").cast("string"),
                ).alias("title"),
                "average_score",
                "popularity",
            )
            .orderBy(
                "snapshot_date",
                "season_year",
                "season",
                "score_position",
            )
        )

        reports = {
            "genre_summary": genre_summary,
            "season_summary": season_summary,
            "top_anime": top_anime,
        }

        output_dir = (
            Path("/opt/airflow/data/reports/spark_analytics")
            / silver_dir.parent.name
            / silver_dir.name
        )

        for name, report in reports.items():
            report.show(50, truncate=False)

            if args.save:
                report.write.mode("errorifexists").parquet(
                    str(output_dir / name)
                )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()