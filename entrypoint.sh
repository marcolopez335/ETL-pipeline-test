#!/usr/bin/env sh
# Container entrypoint: verify the database connection, then run the ETL.
# Any arguments are passed straight to main.py (e.g. --publish, --publish-tst).
set -eu
cd /app
python main.py --test
exec python main.py "$@"
