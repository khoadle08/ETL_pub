"""DAG that runs a transformation on data in DuckDB using the Astro SDK"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c

# -------- #
# Datasets #
# -------- #

in_btc_dataset = Table(c.IN_CURRENT_BTC_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB)


# ----------------- #
# Astro SDK Queries #
# ----------------- #


# run a SQL transformation on the 'in_btc' table in order to create averages
# over different time periods
@aql.transform(pool="duckdb")
def create_btc_reporting_table(
    in_btc: Table,
):
    return """
        SELECT CAST(Date AS DATE) AS date, 
        Open,
        High,
        Low,
        Close,
        Volume,
        FROM {{ in_btc }}
    """


# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in DuckDB
    schedule=[in_btc_dataset],
    catchup=False,
    default_args=gv.default_args,
    description="Runs a transformation on btc data in DuckDB.",
    tags=["part_1"],
)
def transform_curent_btc_data():

    # input the raw climate data and save the outcome of the transformation to a
    # permanent reporting table
    create_btc_reporting_table(
        in_btc=Table(name=c.IN_CURRENT_BTC_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB),
        output_table=Table(name=c.HISTORY_CURRENT_BTC_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB),
    )


transform_curent_btc_data()