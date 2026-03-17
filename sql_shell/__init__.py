"""
sql_shell — Interactive SQL query shell for Polars DataFrames.

Usage:
    from sql_shell import interactive_sql

    # Pass DataFrames as named tables
    interactive_sql({"users": df_users, "orders": df_orders})

    # Or run from CLI with parquet files
    # python -m sql_shell data.parquet --name mytable
"""

from sql_shell.shell import interactive_sql

__all__ = ["interactive_sql"]
