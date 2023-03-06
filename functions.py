"""Functions module."""

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from decimal import Decimal
from time import sleep

import pyspark
from pyspark.sql.functions import col, expr, lit, to_date, udf
from pyspark.sql.types import StringType, StructField, StructType


def extract_asset_price(date_beg, date_end, asset_key):
    """
    Retrieve raw asset price data from Messari API.

    Args:
        date_beg (str): Data retrieval beginning date, included. Format is
        "%Y-%m-%dT%H:%M:%SZ".
        date_end (datetime): Data retrieval end date, included. Format is
        "%Y-%m-%dT%H:%M:%SZ".
    
    Returns:
        dict: dict with the following "key": value pairs:
            - "data": Values as a list of list.
            - "asset": Asset name.

    """
    metrics_id = "price"
    endpoint = ("https://data.messari.io/api/v1/assets/{0}/metrics/{1}/"
                "time-series".format(asset_key, metrics_id))
    response = requests.get(
        endpoint,
        headers={"content-type": "application/json"},
        params={
            "start": date_beg,
            "end": date_end,
            "interval": "1d",
            "format": "json",
            "timestamp-format": "rfc3339"
            }
        )
    return {"data": response.json()["data"]["values"], "asset": asset_key}


def convert_asset_price_spark_df(data_asset, spark):
    """
    Convert raw API asset price into a Spark DataFrame.

    Args:
        data_asset (dict): dict with the following "key": value pairs:
            - "data": Values as a list of list.
            - "asset": Asset name.
        spark (SparkSession): Spark session.
    
    Returns:
        dict: "data" key: Values as a list of list; "asset" key: asset name.

    """
    data, asset = data_asset["data"], data_asset["asset"]
    col_names_asset = ["timestamp", "close"]
    fields = [StructField(col, StringType(), True) for col in col_names_asset]
    schema = StructType(fields)
    return {"data": spark.createDataFrame(data, schema), "asset": asset}


def transform_asset_price(raw):
    """
    Format raw crypto-asset price.
    
    Args:
        data_asset (dict): dict with the following "key": value pairs:
            - "data": Values as a list of list.
            - "asset": Asset name.
    
    Returns:
        pyspark.sql.dataframe.DataFrame: Formatted asset daily, closing price.
    
    """
    data, asset = raw["data"], raw["asset"]
    ftd = raw.select(col("timestamp").alias("date"), col("close").alias("usd"))
    ftd = ftd.withColumn("date", expr("substring(date, 1, length(date)-10)"))
    ftd = ftd.withColumn("date", to_date("date", "yyyy-MM-dd"))
    return ftd.withColumn("asset", lit(asset))

    
def extract_conv_rate():
    """
    Get EUR/USD conversion rate.
    
    Note: Data is not sliced yet because missing points on the weekends should
    be forward-filled beforehand.
    
    Returns:
        pd.DataFrame: Raw conversion rate.
    
    """
    response = requests.get(
        "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/"
        "euro_reference_exchange_rates/html/usd.xml")
    obs = BeautifulSoup(response.text, "lxml-xml").find_all("Obs")
    rows_list = []
    for node in obs:
        dict_row = {"TIME_PERIOD": node.get(
            "TIME_PERIOD"), "OBS_VALUE": node.get("OBS_VALUE")}
        rows_list.append(dict_row)
    raw = pd.DataFrame(rows_list)
    raw["TIME_PERIOD"] = pd.to_datetime(raw["TIME_PERIOD"])
    return raw


def get_conv_rate_spark_df(data_conv_rate, spark):
    """
    Convert raw API conversion rate into a Spark DataFrame.

    Args:
        data_conv_rate (pd.DataFrame): Conversion rate data.
        spark (SparkSession): Spark session.
    
    Returns:
        pyspark.sql.dataframe.DataFrame: Conversion rate as a Spark DataFrame.

    """
    return spark.createDataFrame(data_conv_rate)


def transform_conv_rate(raw, date_beg, date_end):
    """
    Format raw conversion rate.
    
    Args:
        raw (pyspark.sql.dataframe.DataFrame): Raw conversion rate.
        date_beg (str): Data retrieval beginning date, included. Format is
        "%Y-%m-%dT%H:%M:%SZ".
        date_end (str): Data retrieval end date, included. Format is
        "%Y-%m-%dT%H:%M:%SZ".
    
    Returns:
        ftd (pyspark.sql.dataframe.DataFrame): Formatted conversion rate.
    
    """
    date_beg = datetime.strptime(date_beg, "%Y-%m-%dT%H:%M:%SZ")
    date_end = datetime.strptime(date_end, "%Y-%m-%dT%H:%M:%SZ")
    ftd = raw.withColumnRenamed("TIME_PERIOD", "date").withColumnRenamed(
        "OBS_VALUE", "rate")
    ftd.withColumn("rate", col("rate").cast("double"))
    ftd = ftd.withColumn("date", to_date("date"))
    # All conversion rates have been downloaded. Slice them
    return ftd.filter((col("date") >= date_beg) & (col("date") <= date_end))


def merge_asset_conv(asset_price, conv_rate):
    """
    Merge asset_price and conv_rate pd.DataFrames.

    Args:
        asset (pyspark.sql.dataframe.DataFrame): Formatted asset prices.
        conv_rate (pyspark.sql.dataframe.DataFrame): Formatted conversion rates.
    
    """
    data = asset_price.join(conv_rate, ["date"], "inner")
    # There are no crypto-currencies quotations on the weekend, but there are
    # euro / dollar exchange rates, which creates NaNs. Delete them.
    return data.dropna(subset=["usd"])


def get_eur_price(data):
    """ Convert asset price in US dollars to EUR. """
    data = data.withColumn("eur", expr("usd * rate"))
    return data

    
def float_to_decimal(num):
    return Decimal(str(num))


def transform_output_df(data):
    """
    Format output DataFrame.

    Data is formatted as a pandas.DataFrame because there are issues uploading
    directly Spark DataFrames into DynamoDB.
    
    Args:
        data (pyspark.sql.dataframe.DataFrame): Output data.
    
    Returns:
        data (pd.DataFrame): Formatted output data as a pandas DataFrame.
    
    """
    data = data.drop("rate").toPandas()
    # Convert any floats to decimals before DynamoDB insertion
    for i in data.columns:
        datatype = data[i].dtype
        if datatype == "float64":
            data[i] = data[i].apply(float_to_decimal)
    data["date"] = pd.to_datetime(data["date"]).dt.strftime("%Y-%m-%d")
    return data


def load_to_ddb(data, table_name):
    """
    Load asset data into DynamoDB.
    
    Args:
        data (pyspark.sql.dataframe.DataFrame): Data to load.
        table_name (str) : Name of the table in the DynamoDB where data should
        be imported.
    
    Returns:
        None
    
    """
    s3 = boto3.resource("s3")
    ddb = boto3.resource("dynamodb", region_name="eu-west-3")
    table = ddb.Table(table_name)
    # Prepare the data for batch write
    data_dict = data.to_dict("records")
    exponential_backoff_load(data_dict, table)


def exponential_backoff_load(data_dict, table, max_retries=5, backoff_time=1):
    """
    Implementation of exponential backoff for loading data into DynamoDB.
    
    Args:
        data_dict (dict): Data to load as dict format.
        table (ddb.Table): DynamoDB table in which data should be loaded.
        max_retries (int, optional): Maximum number of retries. The default is
        5.
        backoff_time (float, optional): Initial backoff time in seconds. The
        default is 1.
    
    Returns:
        None
    
    """
    retries = 0
    while True:
        try:
            with table.batch_writer() as batch:
                for data in data_dict:
                    batch.put_item(Item=data)
            break
        except Exception as e:
            if retries >= max_retries:
                raise e
            retries += 1
            sleep(backoff_time)
            backoff_time *= 2