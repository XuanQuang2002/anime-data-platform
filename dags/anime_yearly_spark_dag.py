import json
from datetime import datetime, timedelta, timezone

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

TIMEZONE = "Asia/Ho_Chi_Minh"
APPLICATION = (
    "/opt/airflow/src/spark_etl_transformation/spark_etl.py"
)
SOURCE = (
    "{{ ti.xcom_pull(task_ids='extract_to_raw') | tojson }}"
)


with DAG(
    dag_id="anime_yearly_backfill_dag",
    start_date=pendulum.datetime(
        2023, 1, 1, tz=TIMEZONE
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "admin",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "year": Param(
            2023,
            type="integer",
            minimum=1900,
        ),
    },
    tags=["anime", "yearly", "spark"],
    description=(
        "Yearly raw JSON to bronze, silver, "
        "GX validation, and PostgreSQL."
    ),
) as dag:

    @task(pool="anilist_api", multiple_outputs=False)
    def extract_to_raw() -> dict:
        """Extract one yearly file and identify this processing execution."""
        from src.extract import extract_year

        context = get_current_context()

        source = extract_year(
            context["params"]["year"]
        )

        source["record_type"] = "seasonal"
        source["run_id"] = (
            f"anime_yearly_backfill_dag__{context['run_id']}"
        )
        source["attempt"] = (
            datetime.now(timezone.utc)
            .strftime("%Y%m%d_%H%M%S_%f")
        )

        return source

    bronze = SparkSubmitOperator(
        task_id="prepare_bronze",
        application=APPLICATION,
        conn_id="spark_local",
        application_args=[
            "--layer", "bronze",
            "--source", SOURCE,
        ],
        driver_memory="1g",
        conf={
            "spark.ui.showConsoleProgress": "false",
        },
        name="anime_yearly_bronze",
    )

    silver = SparkSubmitOperator(
        task_id="transform_to_silver",
        application=APPLICATION,
        conn_id="spark_local",
        application_args=[
            "--layer", "silver",
            "--source", SOURCE,
        ],
        driver_memory="1g",
        conf={
            "spark.ui.showConsoleProgress": "false",
        },
        name="anime_yearly_silver",
    )

    quality = SparkSubmitOperator(
        task_id="check_quality",
        application=APPLICATION,
        conn_id="spark_local",
        application_args=[
            "--layer", "quality",
            "--source", SOURCE,
        ],
        driver_memory="1g",
        conf={
            "spark.ui.showConsoleProgress": "false",
        },
        name="anime_yearly_quality",
    )

    @task(pool="anime_gold", multiple_outputs=False)
    def load_gold(source: dict) -> dict:
        """Load the exact silver attempt after its saved quality gate passes."""
        from src.load import load_to_gold
        from src.spark_etl_transformation.spark_etl import (
            run_paths,
        )

        paths = run_paths(source)

        result = json.loads(
            (paths["reports"] / "silver_manifest.json")
            .read_text(encoding="utf-8")
        )

        quality_report = json.loads(
            (paths["reports"] / "quality.json")
            .read_text(encoding="utf-8")
        )

        if (
            quality_report["run_id"] != result["run_id"]
            or quality_report["passed"] is not True
        ):
            raise RuntimeError(
                "A passing quality report is required"
            )

        for field, count in (
            quality_report["source_counts"].items()
        ):
            if result[field] != count:
                raise RuntimeError(
                    f"Quality count mismatch: {field}"
                )

        return load_to_gold(result)

    @task
    def execution_report(
        source: dict,
        load: dict,
    ) -> str:
        """Save a truthful report for the validated and committed yearly batch."""
        from src.quality_report import write_json
        from src.spark_etl_transformation.spark_etl import (
            run_paths,
        )

        paths = run_paths(source)

        result = json.loads(
            (paths["reports"] / "silver_manifest.json")
            .read_text(encoding="utf-8")
        )

        quality_report = json.loads(
            (paths["reports"] / "quality.json")
            .read_text(encoding="utf-8")
        )

        has_data = result["valid_count"] > 0
        expected_status = (
            "loaded" if has_data else "no_data"
        )

        if (
            load["run_id"] != result["run_id"]
            or load["status"] != expected_status
            or load["database_committed"] != has_data
            or quality_report["passed"] is not True
        ):
            raise RuntimeError(
                "Cannot report an unsuccessful load as successful"
            )

        report = dict(result)
        report.update({
            "status": "success",
            "load": load,
            "quality_report_path": str(
                paths["reports"] / "quality.json"
            ),
            "completed_at":
                datetime.now(timezone.utc).isoformat(),
        })

        path = paths["reports"] / "execution.json"
        write_json(path, report)

        return str(path)

    @task(pool="anime_gold")
    def update_checkpoint(
        source: dict,
        report_path: str,
    ) -> str:
        """Advance yearly state only after its successful execution report."""
        from src.quality_report import save_checkpoint
        from src.spark_etl_transformation.spark_etl import (
            run_paths,
        )

        path = (
            run_paths(source)["reports"]
            / "silver_manifest.json"
        )

        result = json.loads(
            path.read_text(encoding="utf-8")
        )

        return save_checkpoint(
            result,
            report_path,
        )

    source = extract_to_raw()

    source >> bronze >> silver >> quality

    loaded = load_gold(source)
    quality >> loaded

    report_path = execution_report(
        source,
        loaded,
    )

    update_checkpoint(
        source,
        report_path,
    )