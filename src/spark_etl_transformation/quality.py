import json
from datetime import datetime, timezone

import great_expectations as gx
from pyspark.shell import spark
from pyspark.sql import functions as F

from pathlib import Path
from src.logger import get_logger
from src.quality_report import write_json
from src.spark_etl_transformation.spark_etl import (
    run_paths,
    silver_schemas,
)
from src.transform import TABLE_KEYS

logger = get_logger("yearly_quality")


def table_expectations(name: str) -> list:
    """Define GX key, range, and required-metadata checks for one silver table."""
    checks = []
    keys = list(TABLE_KEYS[name])

    for column in keys:
        checks.append(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column,
            )
        )

    if len(keys) == 1:
        checks.append(
            gx.expectations.ExpectColumnValuesToBeUnique(
                column=keys[0],
            )
        )

    else:
        checks.append(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=keys,
            )
        )

    checks.append(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="anilist_id",
            min_value=1,
            max_value=2_147_483_647,
        )
    )

    if name == "anime":
        for column in (
            "fetched_at",
            "snapshot_date",
            "start_year",
        ):
            checks.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=column,
                )
            )

        for column in ("average_score", "mean_score"):
            checks.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=column,
                    min_value=0,
                    max_value=100,
                )
            )

        for column in (
            "episodes",
            "duration_min",
            "popularity",
            "favourites",
        ):
            checks.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=column,
                    min_value=0,
                    max_value=2_147_483_647,
                )
            )

    elif name == "anime_tags":
        checks.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="rank",
                min_value=0,
                max_value=100,
            )
        )

    return checks


def check_quality(spark, source: dict) -> None:
    """Validate saved Parquet, persist results, and raise when loading must stop."""
    paths = run_paths(source)

    manifest = json.loads(
        (paths["reports"] / "silver_manifest.json")
        .read_text(encoding="utf-8")
    )

    report = {
        "run_id": manifest["run_id"],
        "validation_method": manifest["validation_method"],
        "passed": False,
        "source_counts": {},
        "tables": {},
    }

    try:
        frames = {}
        counts = {}

        for name, schema in silver_schemas().items():
            table_dir = Path(manifest["silver_paths"][name]).parent
            frame = spark.read.parquet(str(table_dir))

            if (
                frame.schema.simpleString()
                != schema.simpleString()
            ):
                raise RuntimeError(
                    f"Unexpected schema: {name}"
                )

            frames[name] = frame
            counts[name] = frame.count()

        rejected_dir = Path(manifest["rejected_path"]).parent
        rejected = spark.read.parquet(str(rejected_dir))

        expected_rejected = (
            "struct<source_file:string,row_number:bigint,"
            "record_type:string,raw_record_json:string,"
            "reasons:array<string>>"
        )

        if (
            rejected.schema.simpleString()
            != expected_rejected
        ):
            raise RuntimeError(
                "Unexpected rejected schema"
            )

        rejected_count = rejected.count()

        bronze_count = spark.read.parquet(
            manifest["bronze_path"]
        ).count()

        source_counts = {
            "input_count": bronze_count,
            "valid_count": counts["anime"],
            "rejected_count": rejected_count,
        }

        report["source_counts"] = source_counts

        for field, count in source_counts.items():
            if count != manifest[field]:
                raise RuntimeError(
                    f"Manifest count mismatch: {field}"
                )

        if (
            bronze_count
            != counts["anime"] + rejected_count
        ):
            raise RuntimeError(
                "Reconciliation failed: input != valid + rejected"
            )

        if counts["airings"] != 0:
            raise RuntimeError(
                "Yearly title extraction must not invent airings"
            )

        anime = frames["anime"]

        wrong_year = anime.filter(
            F.col("start_year").isNull()
            | (F.col("start_year") != source["year"])
        )

        if wrong_year.limit(1).count():
            raise RuntimeError(
                "Anime belongs to another release year"
            )

        expected_fetched = F.to_timestamp(
            F.lit(manifest["fetched_at"])
        )

        if anime.filter(
            ~F.col("fetched_at").eqNullSafe(
                expected_fetched
            )
        ).limit(1).count():
            raise RuntimeError(
                "Stored collection time differs from the manifest"
            )

        expected_date = F.to_date(
            F.from_utc_timestamp(
                "fetched_at",
                "Asia/Ho_Chi_Minh",
            )
        )

        if anime.filter(
            ~F.col("snapshot_date").eqNullSafe(
                expected_date
            )
        ).limit(1).count():
            raise RuntimeError(
                "Snapshot date differs from Vietnam collection date"
            )

        for name, frame in frames.items():
            if name != "anime":
                unknown = frame.join(
                    anime.select("anilist_id"),
                    on="anilist_id",
                    how="left_anti",
                )

                if unknown.limit(1).count():
                    raise RuntimeError(
                        f"Unknown anime reference in {name}"
                    )

        context = gx.get_context(mode="ephemeral")

        datasource = context.data_sources.add_spark(
            name="yearly_silver"
        )

        failed_tables = []

        for name, frame in frames.items():
            if counts[name] == 0:
                report["tables"][name] = {
                    "row_count": 0,
                    "passed": True,
                    "note":
                        "Empty table; schema and reconciliation checked.",
                }
                continue

            asset = datasource.add_dataframe_asset(
                name=name
            )

            definition = (
                asset.add_batch_definition_whole_dataframe(
                    name="all_rows"
                )
            )

            suite = gx.ExpectationSuite(
                name=f"{name}_suite"
            )

            for expectation in table_expectations(name):
                suite.add_expectation(expectation)

            suite = context.suites.add(suite)

            validation = gx.ValidationDefinition(
                name=f"{name}_validation",
                data=definition,
                suite=suite,
            )

            validation = (
                context.validation_definitions.add(
                    validation
                )
            )

            result = validation.run(
                batch_parameters={"dataframe": frame}
            )

            report["tables"][name] = (
                result.to_json_dict()
            )

            if not result.success:
                failed_tables.append(name)

        if failed_tables:
            raise RuntimeError(
                f"GX failed for tables: {failed_tables}"
            )

        if rejected_count:
            raise RuntimeError(
                f"{rejected_count} rejected records require review"
            )

        report["passed"] = True

        logger.info(
            "Yearly quality passed: %s",
            source_counts,
        )

    except Exception as error:
        report["error"] = str(error)
        raise

    finally:
        report["checked_at"] = (
            datetime.now(timezone.utc).isoformat()
        )

        write_json(
            paths["reports"] / "quality.json",
            report,
        )