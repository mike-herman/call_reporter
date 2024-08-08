#!/bin/bash
set -e

# SETTING UP LOG FILE
# Create the .logs directory if not exists.
if [ ! "test -d superset_home/.logs" ]; then mkdir superset_home/.logs; fi


DB_FILE="superset_home/superset.db"

if [ ! -f "$DB_FILE" ]; then
  echo "Superset is initialized because the database file does not exist..." >> superset_home/.logs/superset_init.log

  superset fab create-admin \
              --username "${SUPERSET_ADMIN}" \
              --password "${SUPERSET_PASSWORD}" \
              --firstname Superset \
              --lastname Admin \
              --email admin@example.com
  superset db upgrade
  superset init
#  superset set_database_uri -d DW -u duckdb:///superset_home/call_reporter.duckdb
  superset import-dashboards --path superset_home/assets/dashboard_export.zip
else
  echo "Initialization is skipped because the database file exists." >> superset_home/.logs/superset_init.log
fi

exec gunicorn --bind  "0.0.0.0:8088" --access-logfile '-' --error-logfile '-' --workers 1 --worker-class gthread --threads 20 --timeout 60 --limit-request-line 0 --limit-request-field_size 0 "superset.app:create_app()"
