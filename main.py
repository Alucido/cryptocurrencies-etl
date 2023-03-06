"""Define Dagster Op, Job and Schedule."""

import os
import pandas as pd
import pyspark.sql
import sys
from dagster import (
    DynamicOut, DynamicOutput, job, op, schedule, OpExecutionContext, 
    RunRequest, ScheduleEvaluationContext, RunRequest)

os.environ["HADOOP_HOME"] = os.path.join(
    os.sep.join(sys.executable.split(os.sep)[:-1]), "hadoop")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from datetime import datetime, timedelta
import functions


assets = ["bitcoin", "ethereum", "tether", "xrp", "bitcoin-cash", "litecoin",
    "bitcoin-sv", "eos", "binance-coin", "tezos", "unus-sed-leo", "chainlink",
    "cardano", "tron", "huobi-token", "monero", "stellar", "ethereum-classic",
    "usd-coin", "dash"]


@op
def extract_conv_rate() -> pd.DataFrame:
    print("Getting conversion rate...")
    return functions.get_conv_rate()


@op(config_schema={"date_beg": str, "date_end": str})
def transform_conv_rate(
    context: OpExecutionContext,
    raw: pd.DataFrame
    ) -> pyspark.sql.DataFrame:
    date_beg = context.op_config["date_beg"]
    date_end = context.op_config["date_end"]
    spark = pyspark.sql.SparkSession.builder.master("local[1]").appName(
        "assetPrice").getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    raw_spark = functions.convert_conv_rate_spark_df(raw, spark)
    return functions.transform_conv_rate(raw_spark, date_beg, date_end)


@op(
    out={"res": DynamicOut()},
    config_schema={"date_beg": str, "date_end": str}
    )
def extract_asset_price(context: OpExecutionContext) -> pyspark.sql.DataFrame:
    date_beg = context.op_config["date_beg"]
    date_end = context.op_config["date_end"]
    spark = pyspark.sql.SparkSession.builder.master("local[1]").appName(
        "assetPrice").getOrCreate()
    for asset in assets:
        print("Getting {} asset data...".format(asset))
        yield DynamicOutput(
            functions.extract_asset_price(date_beg, date_end, asset),
            output_name="data_asset_raw",
            mapping_key=asset
            )


@op
def transform_asset_price(
    data_asset_raw: pd.DataFrame,
    data_conv_ftd: pd.DataFrame
    ) -> pyspark.sql.DataFrame:
    spark = pyspark.sql.SparkSession.builder.master("local[1]").appName(
        "assetPrice").getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    data_asset_raw_spark = functions.convert_asset_price_spark_df(
        data_asset_raw, spark)
    data_asset_ftd = functions.transform_asset_price(data_asset_raw_spark)

    # Merge, get EUR asset price and format output
    print("Formatting and loading {} asset data into database...".format(
        data_asset_raw_spark["asset"]))
    res = functions.merge_asset_conv(data_asset_ftd, data_conv_ftd)
    res = functions.get_eur_price(res)
    return functions.transform_output_df(res)


@op
def load_asset_price(res: pyspark.sql.DataFrame):
    functions.load_to_ddb(res, "crypto_valuation")


@job
def etl_crypto_asset():
    raw_conv_rate = extract_conv_rate()
    ftd_conv_rate = transform_conv_rate(raw_conv_rate)
    raw_assets_prices = extract_asset_price()
    ftd_assets_prices = raw_assets_prices.map(
        lambda raw: transform_asset_price(
            data_asset_raw=raw,
            data_conv_ftd=ftd_conv_rate
        ))
    ftd_assets_prices.map(load_asset_price)


@schedule(
    cron_schedule="45 6 * * *",
    job=etl_crypto_asset,
    execution_timezone="Europe/Paris",
)
def etl_schedule(context: ScheduleEvaluationContext):
    date_beg = datetime.combine(
        context.scheduled_execution_time.date() - timedelta(days=1),
        datetime.min.time()
    ).strftime("%Y-%m-%d")
    date_end = datetime.combine(
        context.scheduled_execution_time.date(),
        datetime.min.time()
    ).strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        run_config={
            "ops": {
                "configurable_op": {
                    "config": {"date_beg": date_beg, "date_end": date_end}
                    }
                }
            },
        tags={"date_end": date_end},
    )