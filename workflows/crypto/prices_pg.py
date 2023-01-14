from typing import Dict, Optional

from phidata.table.sql.postgres import PostgresTable
from phidata.task import TaskArgs, task
from phidata.utils.log import logger
from phidata.utils.print_table import print_table
from phidata.workflow import Workflow

from sqlmodel import SQLModel, Field

from workflows.sql_dbs import POSTGRES_APP, POSTGRES_CONN_ID

##############################################################################
# A workflow to write cryptocurrency price data to a
# postgres table: `crypto_prices_daily`
##############################################################################


# Step 1: Define a postgres table for storing crypto price data
# 1.1 Create a SQLModel for the table
class CryptoPrices(SQLModel, table=True):
    ds: str = Field(primary_key=True)
    hr: str = Field(primary_key=True)
    ticker: str = Field(primary_key=True)
    usd: Optional[float] = None
    usd_market_cap: Optional[float] = None
    usd_24h_vol: Optional[float] = None
    usd_24h_change: Optional[float] = None
    last_updated_at: Optional[int] = None

    __tablename__ = "crypto_prices"


# 1.2 Define a PostgresTable object for the table
crypto_prices_table = PostgresTable(
    name=CryptoPrices.__tablename__,
    data_model=CryptoPrices,
    db_app=POSTGRES_APP,
    airflow_conn_id=POSTGRES_CONN_ID,
)


# Step 2: Create tasks to load PostgresTable
# 2.1 Download price data
@task
def load_crypto_prices(**kwargs) -> bool:
    """
    Download prices and load postgres table.
    """
    import httpx
    import pyarrow as pa

    coins = ["bitcoin", "ethereum", "litecoin", "ripple", "tether"]
    run_date = TaskArgs.from_kwargs(kwargs).run_date
    run_day = run_date.strftime("%Y-%m-%d")
    run_hour = run_date.strftime("%H")

    logger.info(f"Downloading prices for ds={run_day}/hr={run_hour}")
    response: Dict[str, Dict] = httpx.get(
        url="https://api.coingecko.com/api/v3/simple/price",
        params={
            "ids": ",".join(coins),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
            "include_last_updated_at": "true",
        },
    ).json()

    # Create pyarrow.Table
    # https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.from_pylist
    table = pa.Table.from_pylist(
        [
            {
                "ds": run_day,
                "hr": run_hour,
                "ticker": coin_name,
                "usd": coin_data["usd"],
                "usd_market_cap": coin_data["usd_market_cap"],
                "usd_24h_vol": coin_data["usd_24h_vol"],
                "usd_24h_change": coin_data["usd_24h_change"],
                "last_updated_at": coin_data["last_updated_at"],
            }
            for coin_name, coin_data in response.items()
        ]
    )

    # Write the dataframe to table
    return crypto_prices_table.write_table(table, create_table=True)


# 2.2 Drop existing price data to prevent duplicates
@task
def drop_existing_prices(**kwargs) -> bool:
    """
    Drop rows for current window (ds + hour) to prevent duplicates
    """
    args: TaskArgs = TaskArgs.from_kwargs(kwargs)
    run_date = args.run_date
    run_day = run_date.strftime("%Y-%m-%d")
    run_hour = run_date.strftime("%H")

    logger.info(f"Dropping rows for: ds={run_day}/hr={run_hour}")
    try:
        crypto_prices_table.delete(where=f"ds = '{run_day}' AND hr = '{run_hour}'")
    except Exception as e:
        logger.error(f"Error dropping rows: {e}")
    return True


# 2.3: Create task to analyze data in PostgresTable
@task
def analyze_crypto_prices(**kwargs) -> bool:
    """
    Read PostgresTable
    """
    import pyarrow as pa

    run_date = TaskArgs.from_kwargs(kwargs).run_date
    run_day = run_date.strftime("%Y-%m-%d")
    run_hour = run_date.strftime("%H")

    logger.info(f"Reading prices for ds={run_day}/hr={run_hour}")
    # https://arrow.apache.org/docs/python/dataset.html#filtering-data
    table: pa.Table = crypto_prices_table.read_table()
    print_table(
        title="Crypto Prices", header=table.column_names, rows=table.to_pylist()
    )

    # Use polars to analyze data
    # import polars as pl
    # df: pl.DataFrame = pl.DataFrame(table)
    # logger.info(df)

    # Use pandas to analyze data
    # import pandas as pd
    # df: pd.DataFrame = table.to_pandas()
    # logger.info(df)

    return True


# Step 3: Instantiate the tasks
load_prices = load_crypto_prices()
drop_prices = drop_existing_prices(enabled=True)
analyze_prices = analyze_crypto_prices(enabled=False)

# Step 4: Create a Workflow to run these tasks
crypto_prices = Workflow(
    name="crypto_prices_pg",
    tasks=[drop_prices, load_prices, analyze_prices],
    outputs=[crypto_prices_table],
    # Airflow tasks created by this workflow are ordered using the graph param
    # graph = { downstream: [upstream_list] }
    # The downstream task will run after tasks in the upstream_list
    # To run download_prices after drop_prices:
    graph={
        load_prices: [drop_prices],
        analyze_prices: [load_prices],
    },
)

# Step 5: Create a DAG to run the workflow on a schedule
dag = crypto_prices.create_airflow_dag()
