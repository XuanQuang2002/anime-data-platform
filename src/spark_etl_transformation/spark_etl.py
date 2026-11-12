import argparse
import json
from datetime import datetime
from pathlib import Path

import polars as pl

from src.config import PROJECT_ROOT
from src.flatten import build_silver_tables, flatten_records
from src.logger import get_logger
from src.quality_report import write_json
from src.transform import SILVER_SCHEMAS, normalize_anime
from src.validate import validate_anime

logger = get_logger("yearly_spark")

def run_paths(source: dict) -> dict:
    """Return isolated output directories for this yearly execution."""
    for field in ("run_id", "attempt"):
        value = source[field]
        if (
            not value
            or value in (".", "..")
            or Path(value).name != value
        ):
            raise ValueError(f"Invalid {field}")

    paths = {}

    for layer in ("bronze", "silver", "rejected", "reports"):
        paths[layer] = (
            PROJECT_ROOT
            / "data"
            / layer
            / "yearly"
            / str(source["year"])
            / source["run_id"]
            / source["attempt"]
        )

    return paths

def silver_schemas() -> dict:
    """Convert the existing column definitions to Spark schemas."""
    from pyspark.sql import types as T
    types = {
        pl.Int64: T.LongType(),
        pl.Float64: T.DoubleType(),
        pl.String: T.StringType(),
        pl.Boolean: T.BooleanType(),
        pl.Date: T.DateType(),
        pl.Datetime("us", "UTC"): T.TimestampType(),
    }

    schemas = {}

    for name, columns in SILVER_SCHEMAS.items():
        fields = []

        for column, dtype in columns.items():
            fields.append(
                T.StructField(column, types[dtype], True)
            )

        schemas[name] = T.StructType(fields)

    return schemas

def unpack_raw(text: str, year: int) -> tuple:
    """Read yearly raw records from current responses or legacy media lists."""
    payload = json.loads(text)

    if not isinstance(payload, dict):
        raise ValueError("Raw payload must be a JSON object")

    if payload.get("year") != year:
        raise ValueError("Raw file belongs to another year")

    if payload.get("selection") != "start_date_year":
        raise ValueError("Expected a yearly start-date extraction")

    fetched_at = datetime.fromisoformat(payload["fetched_at"])

    if fetched_at.utcoffset() is None:
        raise ValueError("Raw fetched_at must include a timezone")

    pages = payload.get("pages")

    if not isinstance(pages, list) or not pages:
        raise ValueError(
            "Raw payload must contain a nonempty pages list"
        )

    if all(isinstance(page, list) for page in pages):
        records = []

        for page in pages:
            records.extend(page)

    else:
        records = flatten_records(payload, "seasonal")

    return (
        fetched_at.isoformat(),
        [
            json.dumps(record, ensure_ascii=False)
            for record in records
        ],
    )

def json_date(value):
    """Serialize native date and datetime values as ISO strings."""
    return value.isoformat()

def normalize_record(
    raw_json: str,
    fetched_at: str,
    year: int,
) -> tuple:
    """Reuse row rules and return table JSON or rejection reasons."""
    try:
        row = normalize_anime(json.loads(raw_json))
        errors = validate_anime(row, "seasonal")

        if row.get("start_year") != year:
            errors.append(
                "start_year does not match the requested year"
            )

        if errors:
            return None, errors

        tables = build_silver_tables(
            [row],
            datetime.fromisoformat(fetched_at),
        )

        return json.dumps(
            tables,
            default=json_date,
            ensure_ascii=False,
        ), []

    except (ValueError, TypeError) as error:
        return None, [str(error)]

def prepare_bronze(spark, source: dict) -> None:
    """Preserve every raw record in bronze and save a completed manifest."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    paths = run_paths(source)

    schema = T.StructType([
        T.StructField("fetched_at", T.StringType()),
        T.StructField(
            "records",
            T.ArrayType(T.StringType()),
        ),
    ])

    unpack = F.udf(unpack_raw, schema)

    parsed = (
        spark.read.text(
            source["raw_path"],
            wholetext=True,
        )
        .select(
            unpack(
                "value",
                F.lit(source["year"]),
            ).alias("payload")
        )
        .cache()
    )

    try:
        metadata = parsed.select(
            "payload.fetched_at"
        ).first()

        if metadata is None:
            raise ValueError("Raw file is empty")

        bronze = (
            parsed.select(
                F.col("payload.fetched_at").alias("fetched_at"),
                F.posexplode("payload.records").alias(
                    "position",
                    "raw_record_json",
                ),
            )
            .select(
                F.lit(source["raw_path"]).alias("source_file"),
                (F.col("position") + 1)
                .cast("long")
                .alias("row_number"),
                "fetched_at",
                "raw_record_json",
            )
        )

        input_count = bronze.count()

        bronze.repartition(2).write.mode("overwrite").parquet(
            str(paths["bronze"] / "anime")
        )

        manifest = dict(source)
        manifest.update({
            "input_count": input_count,
            "fetched_at": metadata["fetched_at"],
            "bronze_path": str(paths["bronze"] / "anime"),
        })

        write_json(
            paths["reports"] / "bronze_manifest.json",
            manifest,
        )

        logger.info(
            "Bronze complete: input=%s",
            input_count,
        )

    finally:
        parsed.unpersist()

def transform_to_silver(spark, source: dict) -> None:
    """Normalize bronze rows, preserve rejects, and write typed silver tables."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    paths = run_paths(source)

    manifest_path = paths["reports"] / "bronze_manifest.json"
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    schemas = silver_schemas()

    tables_schema = T.StructType([
        T.StructField(name, T.ArrayType(schema))
        for name, schema in schemas.items()
    ])

    normalized_schema = T.StructType([
        T.StructField("tables_json", T.StringType()),
        T.StructField(
            "reasons",
            T.ArrayType(T.StringType()),
        ),
    ])

    normalize = F.udf(
        normalize_record,
        normalized_schema,
    )

    bronze = spark.read.parquet(
        manifest["bronze_path"]
    )

    prepared = (
        bronze.withColumn(
            "normalized",
            normalize(
                "raw_record_json",
                "fetched_at",
                F.lit(source["year"]),
            ),
        )
        .withColumn(
            "tables",
            F.from_json(
                "normalized.tables_json",
                tables_schema,
                {
                    "timestampFormat":
                    "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX"
                },
            ),
        )
        .withColumn(
            "anime_id",
            F.col("tables.anime")[0]["anilist_id"],
        )
    )

    window = Window.partitionBy(
        "anime_id"
    ).orderBy("row_number")

    prepared = (
        prepared.withColumn(
            "position",
            F.row_number().over(window),
        )
        .withColumn(
            "reasons",
            F.when(
                F.size("normalized.reasons") > 0,
                F.col("normalized.reasons"),
            )
            .when(
                F.col("position") > 1,
                F.array(
                    F.lit("Duplicate anilist_id")
                ),
            )
            .otherwise(
                F.array().cast("array<string>")
            ),
        )
        .cache()
    )

    try:
        valid = prepared.filter(
            F.size("reasons") == 0
        )

        rejected = prepared.filter(
            F.size("reasons") > 0
        ).select(
            "source_file",
            "row_number",
            F.lit("seasonal").alias("record_type"),
            "raw_record_json",
            "reasons",
        )

        valid_count = valid.count()
        rejected_count = rejected.count()

        if (
            manifest["input_count"]
            != valid_count + rejected_count
        ):
            raise RuntimeError("Reconciliation failed")

        silver_paths = {}

        for name in schemas:
            destination = paths["silver"] / name

            table = (
                valid.select(
                    F.explode(
                        f"tables.{name}"
                    ).alias("item")
                )
                .select("item.*")
            )

            table.write.mode("overwrite").parquet(
                str(destination)
            )

            silver_paths[name] = str(
                destination / "part-*.parquet"
            )

        rejected_dir = paths["rejected"] / "records"

        rejected.write.mode("overwrite").parquet(
            str(rejected_dir)
        )

        manifest.update({
            "valid_count": valid_count,
            "rejected_count": rejected_count,
            "silver_paths": silver_paths,
            "rejected_path": str(
                rejected_dir / "part-*.parquet"
            ),
            "validation_method":
                "Python row validators + Great Expectations",
        })

        write_json(
            paths["reports"] / "silver_manifest.json",
            manifest,
        )

        logger.info(
            "Silver complete: input=%s valid=%s rejected=%s",
            manifest["input_count"],
            valid_count,
            rejected_count,
        )

    finally:
        prepared.unpersist()


def main():
    """Run one Spark pipeline stage using a small JSON task argument."""
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "quality"],
        required=True,
    )
    parser.add_argument("--source", required=True)

    args = parser.parse_args()
    source = json.loads(args.source)

    if source["record_type"] != "seasonal":
        raise ValueError(
            "Yearly anime records must use record_type='seasonal'"
        )

    spark = (
        SparkSession.builder
        .appName(f"AnimeYearly_{args.layer}")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.parquet.outputTimestampType",
            "TIMESTAMP_MICROS",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.layer == "bronze":
            prepare_bronze(spark, source)

        elif args.layer == "silver":
            transform_to_silver(spark, source)

        else:
            from src.spark_etl_transformation.quality import (
                check_quality,
            )

            check_quality(spark, source)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()