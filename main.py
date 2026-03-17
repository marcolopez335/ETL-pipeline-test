import argparse
from pathlib import Path
import polars as pl
from common.logging import get_logger, setup_logging, INFO
from conversion.shared import test_connection, load_config, CACHE_DIR
from conversion.console import print_header, print_success, print_error, print_info, interactive_sql, console
from conversion import stories_table as stories
from conversion import epics_table as epics

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "conversion" / "output"

setup_logging(
    workflow_name=Path(__file__).stem,
    log_dir=SCRIPT_DIR / "logs",
    console_level=INFO,
)
logger = get_logger(__name__)


def _load_tables_from_cache(config: dict, load_stories: bool, load_epics: bool) -> dict:
    """Load DataFrames from cache/output files without running the pipeline."""
    tables = {}

    if load_stories:
        stories_cfg = config["stories"]
        cache_path = CACHE_DIR / stories_cfg["cache_filename"]
        if cache_path.exists():
            tables["stories"] = pl.read_parquet(cache_path)
            print_info(f"Loaded [bold]stories[/] from cache ({tables['stories'].height:,} rows)")
        else:
            print_error("Stories cache not found. Run the pipeline first.")

    if load_epics:
        epics_cfg = config["epics"]
        cache_path = CACHE_DIR / epics_cfg["cache_filename"]
        if cache_path.exists():
            tables["epics"] = pl.read_parquet(cache_path)
            print_info(f"Loaded [bold]epics[/] from cache ({tables['epics'].height:,} rows)")
        else:
            print_error("Epics cache not found. Run the pipeline first.")

    return tables


def main():
    parser = argparse.ArgumentParser(description="ODBC data pipeline")
    parser.add_argument("--stories", action="store_true", help="Run stories pipeline only")
    parser.add_argument("--epics", action="store_true", help="Run epics pipeline only")
    parser.add_argument("--update-cache", action="store_true", help="Update history caches only (no hyper export)")
    parser.add_argument("--test", action="store_true", help="Test the database connection")
    parser.add_argument("--publish", action="store_true", help="Publish hyper files to Tableau after export")
    parser.add_argument("--query", action="store_true", help="Open interactive SQL shell after pipeline runs")
    parser.add_argument("--query-only", action="store_true", help="Open SQL shell loading from cache (no pipeline run)")
    args = parser.parse_args()

    config = load_config()

    console.print()
    console.rule("[bold cyan]ODBC Data Pipeline[/]", style="cyan")
    console.print()

    # Query-only mode — load from cache and go straight to SQL shell
    if args.query_only:
        load_stories = args.stories or (not args.stories and not args.epics)
        load_epics = args.epics or (not args.stories and not args.epics)
        sql_tables = _load_tables_from_cache(config, load_stories, load_epics)
        if sql_tables:
            interactive_sql(sql_tables)
        return

    # If no flags given, run everything
    run_all = not (args.stories or args.epics or args.update_cache or args.test)

    if args.test:
        print_header("Connection Test")
        db = config["database"]["name"]
        success = test_connection(database=db)
        if success:
            logger.info("Connection test passed")
            print_success("Connection test passed")
        else:
            logger.error("Connection test failed")
            print_error("Connection test failed")
        return

    if args.update_cache:
        if args.stories or (not args.stories and not args.epics):
            stories.run_update_cache(config)
        if args.epics or (not args.stories and not args.epics):
            epics.run_update_cache(config)
        return

    sql_tables = {}

    if run_all or args.stories:
        df_stories = stories.run(config, publish=args.publish)
        sql_tables["stories"] = df_stories

    if run_all or args.epics:
        df_epics, df_acrp = epics.run(config, publish=args.publish)
        sql_tables["epics"] = df_epics
        sql_tables["acrp"] = df_acrp

    console.rule("[bold green]Done[/]", style="green")
    console.print()

    if args.query and sql_tables:
        interactive_sql(sql_tables)


if __name__ == "__main__":
    main()
