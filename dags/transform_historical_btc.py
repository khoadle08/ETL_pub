"""DAG that runs a transformation on data in DuckDB using the Astro SDK"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime
import pandas as pd

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c

# ----------------- #
# Astro SDK Queries #
# ----------------- #


# Create a reporting table that counts heat days per year for each city location
@aql.transform(pool="duckdb")
def create_historical_weather_reporting_table(in_table: Table,):
    return """
        SELECT time, open , high, low, close, volume
        FROM {{ in_table }}
    """

# --- #
# DAG #
# --- # 

@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the btc data is ready in DuckDB
    schedule=None,
    catchup=False,
    default_args=gv.default_args,
    description="Runs transformations on btc data in DuckDB.",
    tags=["part_2"],
)
def transform_historical_weather():

    create_historical_weather_reporting_table(
        in_table=Table(
            name=c.IN_HISTORICAL_BTC_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
        output_table=Table(
            name=c.REPORT_HISTORICAL_BTC_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
    )