"""DAG that retrieves current weather information and loads it into DuckDB."""

# --------------- #
# Package imports #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag, task
import pandas as pd
import ccxt
from datetime import datetime

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c
from include.money_utils import check_current_btc

# -------- #
# Datasets #
# -------- #
start_dataset = Dataset("start")

# --- #
# DAG #
# --- #

# a dataframe decorated function turns a returned Pandas dataframe into
# a Astro SDK Table object
@aql.dataframe(pool="duckdb")
def turn_json_into_table(in_json):
    return pd.DataFrame(in_json)


@dag(
    start_date=datetime(2024, 1, 1),
    # this DAG runs as soon as the "DS_START" Dataset has been produced to
    schedule_interval='*/1 * * * *',
    catchup=False,
    default_args=gv.default_args,
    description="DAG that retrieves BTC value information and saves it to a local JSON.",
    tags=["part_1"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def extract_current_data():
    @task
    def get_current_btc(**context):
        """Use from the local
        'monet_utils' module to retrieve the current btc
        from the ccxt."""


        # btc_value = check_current_btc()

        end = []
        exchange = ccxt.binance() # Thay thế bằng sàn giao dịch mong muốn

        symbol = 'BTC/USDT' # Cặp giao dịch BTC/USDT

        timeframe = '1m' # Khung thời gian 1 phút

        ohlcv = exchange.fetch_ohlcv(symbol, timeframe)
        for candle in ohlcv:
            timestamp = datetime.fromtimestamp(candle[0]/1000.0).strftime('%Y-%m-%d %H:%M:%S')
            open_price = candle[1]
            high_price = candle[2]
            low_price = candle[3]
            close_price = candle[4]
            volume = candle[5]
        end.append({
            "timestamp" : timestamp,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
        })

        return end

    # set dependencies to get current weather
    current_btc = get_current_btc()

    # use the @aql.dataframe decorated function to write the JSON returned from
    # the get_current_weather task as a permanent table to DuckDB
    turn_json_into_table(
        current_btc,
        output_table=Table(
            name=c.IN_CURRENT_BTC_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
    )


extract_current_data()