import argparse
from pathlib import Path

import polars as pl
from common.logging import get_logger, setup_logging, INFO

from conversion import shared
from conversion.shared import test_connection, load_config
from conversion.console import (
    print_header, print_success, print_error, print_info, console,
)
from conversion import stories_table as stories
from conversion import epics_table as epics
from sql_shell import interactive_sql

SCRIPT_DIR = Path(__file__).parent

# Logging is configured exactly once, here at the entry point.
setup_logging(
    workflow_name=Path(__file__).stem,
    log_dir=SCRIPT_DIR / "logs",
    console_level=INFO,
)
logger = get_logger(__name__)


def _load_tables_from_cache(config: dict, load_stories: bool, load_epics: bool) -> dict:
    """Load DataFrames from cache files without running the pipeline."""
    tables = {}

    if load_stories:
        cache_path = shared.CACHE_DIR / config["stories"]["cache_filename"]
        if cache_path.exists():
            tables["stories"] = pl.read_parquet(cache_path)
            print_info(f"Loaded [bold]stories[/] from cache ({tables['stories'].height:,} rows)")
        else:
            print_error("Stories cache not found. Run the pipeline first.")

    if load_epics:
        cache_path = shared.CACHE_DIR / config["epics"]["cache_filename"]
        if cache_path.exists():
            tables["epics"] = pl.read_parquet(cache_path)
            print_info(f"Loaded [bold]epics[/] from cache ({tables['epics'].height:,} rows)")
        else:
            print_error("Epics cache not found. Run the pipeline first.")

    return tables


def _resolve_publish_targets(args) -> tuple[bool, list[str] | None]:
    """Decide whether to publish and to which servers.

    Returns ``(do_publish, targets_or_none)``. ``None`` means "publish to all
    configured servers"; otherwise it's an explicit subset like ``["tst"]``.
    """
    do_publish = args.publish or args.publish_tst or args.publish_prd
    if not do_publish:
        return False, None
    if args.publish:
        return True, None
    targets: list[str] = []
    if args.publish_tst:
        targets.append("tst")
    if args.publish_prd:
        targets.append("prd")
    return True, targets


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AMMM Jira ETL pipeline")
    parser.add_argument("--stories", action="store_true", help="Run stories pipeline only")
    parser.add_argument("--epics", action="store_true", help="Run epics pipeline only")
    parser.add_argument("--update-cache", action="store_true",
                        help="Update history caches only (no hyper export)")
    parser.add_argument("--test", action="store_true", help="Test the database connection")
    parser.add_argument("--publish", action="store_true",
                        help="Publish hyper files to all Tableau servers (tst + prd)")
    parser.add_argument("--publish-tst", action="store_true",
                        help="Publish hyper files to Tableau TST only")
    parser.add_argument("--publish-prd", action="store_true",
                        help="Publish hyper files to Tableau PRD only")
    parser.add_argument("--force", action="store_true",
                        help="Bypass cache shrinkage safety check")
    parser.add_argument("--query", action="store_true",
                        help="Open interactive SQL shell after pipeline runs")
    parser.add_argument("--query-only", action="store_true",
                        help="Open SQL shell loading from cache (no pipeline run)")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    do_publish, publish_targets = _resolve_publish_targets(args)

    config = load_config()
    db = config["database"]["name"]

    console.print()
    console.rule("[bold cyan]ODBC Data Pipeline[/]", style="cyan")
    console.print()

    # Query-only mode — load from cache and go straight to SQL shell
    if args.query_only:
        load_stories = args.stories or not (args.stories or args.epics)
        load_epics = args.epics or not (args.stories or args.epics)
        sql_tables = _load_tables_from_cache(config, load_stories, load_epics)
        if sql_tables:
            interactive_sql(sql_tables)
        return 0

    if args.test:
        print_header("Connection Test")
        ok = test_connection(database=db, config=config)
        if ok:
            logger.info("Connection test passed")
            print_success("Connection test passed")
            return 0
        logger.error("Connection test failed")
        print_error("Connection test failed")
        return 1

    # If no pipeline flags given, run everything
    run_all = not (args.stories or args.epics or args.update_cache)

    if args.update_cache:
        if args.stories or run_all:
            stories.run_update_cache(config, force=args.force)
        if args.epics or run_all:
            epics.run_update_cache(config, force=args.force)
        return 0

    # Pre-flight credential check — runs outside spinners so stdin is
    # available if the user needs to re-enter credentials
    if not test_connection(database=db, config=config):
        print_error("Cannot connect to database. Aborting.")
        return 1

    sql_tables: dict = {}

    if run_all or args.stories:
        df_stories = stories.run(
            config, publish=do_publish, publish_targets=publish_targets, force=args.force,
        )
        sql_tables["stories"] = df_stories

    if run_all or args.epics:
        df_epics, df_acrp = epics.run(
            config, publish=do_publish, publish_targets=publish_targets, force=args.force,
        )
        sql_tables["epics"] = df_epics
        sql_tables["acrp"] = df_acrp

    console.rule("[bold green]Done[/]", style="green")
    console.print()

    if args.query and sql_tables:
        interactive_sql(sql_tables)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
