import argparse
from pathlib import Path
from common.logging import get_logger, setup_logging, INFO
from conversion.shared import test_connection, load_config
from conversion.console import print_header, print_success, print_error, interactive_sql, console
from conversion import stories_table as stories
from conversion import epics_table as epics

SCRIPT_DIR = Path(__file__).parent

setup_logging(
    workflow_name=Path(__file__).stem,
    log_dir=SCRIPT_DIR / "logs",
    console_level=INFO,
)
logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="ODBC data pipeline")
    parser.add_argument("--stories", action="store_true", help="Run stories pipeline only")
    parser.add_argument("--epics", action="store_true", help="Run epics pipeline only")
    parser.add_argument("--update-cache", action="store_true", help="Update history caches only (no hyper export)")
    parser.add_argument("--test", action="store_true", help="Test the database connection")
    parser.add_argument("--publish", action="store_true", help="Publish hyper files to Tableau after export")
    parser.add_argument("--query", action="store_true", help="Open interactive SQL shell after pipeline runs")
    args = parser.parse_args()

    config = load_config()

    console.print()
    console.rule("[bold cyan]ODBC Data Pipeline[/]", style="cyan")
    console.print()

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
