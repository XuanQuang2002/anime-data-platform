from datetime import date, timedelta
from zoneinfo import ZoneInfo

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

TIMEZONE = "Asia/Ho_Chi_Minh"

with DAG(
    dag_id="anime_daily_dag",
    schedule=CronTriggerTimetable(
        "5 0 * * *",
        timezone=TIMEZONE,
    ),
    start_date=pendulum.datetime(2023, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "admin",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "target_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="For manual runs, enter YYYY-MM-DD.",
        ),
        "force_refresh": Param(
            False,
            type="boolean",
            description="Create a new raw snapshot while preserving old files.",
        ),
    },
    tags=["anime", "daily"],
    description="Fetch a day's airing schedules, build silver, and check quality.",
) as dag:

    @task(pool="anilist_api", multiple_outputs=False)
    def extract_to_raw() -> dict:
        """Fetch one fixed day and return its raw path and partition."""
        from src.extract import extract_daily

        context = get_current_context()
        target_date_text = context["params"]["target_date"]

        if target_date_text is not None:
            target_date = date.fromisoformat(target_date_text)
        else:
            logical_date = context.get("logical_date")

            if logical_date is None:
                raise ValueError(
                    "Set target_date to YYYY-MM-DD for this manual run."
                )

            target_date = logical_date.astimezone(
                ZoneInfo(TIMEZONE)
            ).date()

        return {
            "raw_path": extract_daily(
                target_date,
                force_refresh=context["params"]["force_refresh"],
            ),
            "partition": target_date.isoformat(),
        }

    @task(multiple_outputs=False)
    def transform_to_silver(source: dict) -> dict:
        """Build daily silver and preserve its source identity."""
        from src.transform import clean_to_silver

        context = get_current_context()

        result = clean_to_silver(
            raw_path=source["raw_path"],
            run_id=f"anime_daily_dag__{context['run_id']}",
            record_type="airings",
        )

        result["raw_path"] = source["raw_path"]
        result["partition"] = source["partition"]
        return result

    @task(multiple_outputs=False)
    def quality_check(result: dict) -> dict:
        """Validate silver and persist a passed or failed quality report."""
        from src.quality_report import write_quality_report
        from src.transform import check_quality

        try:
            quality = check_quality(result)

        except Exception as error:
            write_quality_report(
                result,
                passed=False,
                error=str(error),
            )
            raise

        write_quality_report(result, passed=True)
        return quality

    @task(pool="anime_gold", multiple_outputs=False)
    def load_gold(result: dict) -> dict:
        """Load quality-checked Parquet tables into PostgreSQL."""
        from src.load import load_to_gold

        return load_to_gold(result)

    @task
    def execution_report(
        result: dict,
        quality: dict,
        load: dict,
    ) -> str:
        """Save source counts and the completed database load outcome."""
        from src.quality_report import write_execution_report

        return write_execution_report(result, quality, load)

    @task(pool="anime_gold")
    def update_checkpoint(result: dict, report_path: str) -> str:
        """Record a successful partition after its report is saved."""
        from src.quality_report import save_checkpoint

        return save_checkpoint(result, report_path)

    source = extract_to_raw()
    silver_result = transform_to_silver(source)
    quality_result = quality_check(silver_result)
    load_result = load_gold(silver_result)

    quality_result >> load_result

    report_path = execution_report(
        silver_result,
        quality_result,
        load_result,
    )
    update_checkpoint(silver_result, report_path)

if __name__=="__main__":
    dag.test()