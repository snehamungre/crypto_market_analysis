from pyspark.sql import DataFrame, SparkSession
from pathlib import Path
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("AnalyseProcessed").getOrCreate()


def read_parquet() -> DataFrame:
    notebook_dir = Path().resolve()
    data_path = notebook_dir.parent / "data" / "processed"

    df = spark.read.option("header", True).parquet(str(data_path))

    return df


def current_statistics(df) -> DataFrame:
    max_date = df.select(F.max("updated_date")).collect()[0][0]

    recent_data = df.filter(F.col("updated_date") == max_date).select(
        "name",
        "current_price",
        "market_cap",
        "market_cap_rank",
        "updated_date",
        "total_volume",
    )

    rank_price_spec = Window.partitionBy("updated_date").orderBy(desc("current_price"))

    curr_top_price = recent_data.withColumn(
        "current_price_rank", rank().over(rank_price_spec)
    )

    curr_top_market = recent_data.orderBy(desc("market_cap"))

    return curr_top_price, curr_top_market, max_date


"""
Returns average price and average market cap with ranked column
"""


def aggregate_statistics(df: DataFrame) -> DataFrame:
    # create a temp view
    df.select(
        "name", "current_price", "market_cap", "total_volume", "updated_date"
    ).createOrReplaceTempView("crypto_prices")

    # ranked avg market cap
    # todo save this to files?
    avg_market_cap = spark.sql(
        """
    with avg_market as (
        SELECT name, avg(market_cap) as avg_market_cap
        FROM crypto_prices 
        GROUP BY name
    )
    
    SELECT * , RANK() 
    OVER (ORDER BY avg_market_cap DESC) as avg_market_cap_rank
    FROM avg_market
    """
    )

    # ranked avg market cap
    avg_price = spark.sql(
        """
    WITH average_prices AS (
        SELECT name, ROUND(avg(current_price), 3) as average_price
        FROM crypto_prices 
        GROUP BY name
        ORDER BY avg(current_price) DESC
    )
    SELECT *, RANK() OVER (ORDER BY average_price DESC) as avg_price_rank
    FROM average_prices
    """
    )

    return avg_market_cap, avg_price


def vol_to_market_ratio(df) -> DataFrame:
    vol_market_ratio = (
        # recheck as the values as they are being divided
        df.filter((F.col("total_volume") != 0) & (F.col("market_cap") != 0))
        .withColumn(
            "vol_market_ratio",
            F.round(F.col("total_volume") / F.col("market_cap"), 5),
        )
        .orderBy(F.col("vol_market_ratio").desc())
        .select(
            "name",
            "vol_market_ratio",
        )
    )

    rank_vol_market_spec = Window.orderBy(desc("vol_market_ratio"))
    window_spec = Window.partitionBy("name").orderBy(desc("vol_market_ratio"))

    # gets the highest vol-to-market ratio for each coin and ranks them
    vol_market_ratio_rank = (
        vol_market_ratio.withColumn("rn", row_number().over(window_spec))
        .filter("rn = 1")
        .drop("rn")
        .withColumn("vol_market_rank", rank().over(rank_vol_market_spec))
    )

    return vol_market_ratio_rank


def top_performing_asset(avg_market, avg_price, vol_market_ratio) -> DataFrame:
    top_performing_assets = avg_market.join(avg_price, on=["name"]).join(
        vol_market_ratio.select(
            "name", "total_volume", "vol_market_ratio", "vol_market_rank"
        ),
        on=["name"],
    )

    window_spec = Window.orderBy(asc("top_performing_score"))

    top_performing_assets = (
        top_performing_assets.withColumn(
            "top_performing_score",
            round(
                top_performing_assets.avg_market_cap_rank * 0.4
                + top_performing_assets.avg_price_rank * 0.4
                + top_performing_assets.vol_market_rank * 0.2
            ),
        )
        .withColumn("top_performing_rank", rank().over(window_spec))
        .orderBy("top_performing_rank")
    )

    return top_performing_assets

def analysis():
    df = read_parquet()

    curr_top_price, curr_top_market, date = current_statistics(df)
    avg_market_cap, avg_price = aggregate_statistics(df)
    vol_market_ratio = vol_to_market_ratio(df)

    top_performing_assets = top_performing_asset(
        avg_market_cap, avg_price, vol_market_ratio
    )

    # save files to disk
    path = "data/analytics/"
    try:
        # Write & Save Files in .parquet format (overwrites)
        curr_top_price.write.mode("append").partitionBy("updated_date").parquet(
            path + "curr_top_price"
        )
        curr_top_market.write.mode("append").partitionBy("updated_date").parquet(
            path + "curr_top_market"
        )
        avg_market_cap.write.mode("overwrite").parquet(path + "avg_market_cap")
        avg_price.write.mode("overwrite").parquet(path + "avg_price")
        vol_market_ratio.write.mode("overwrite").parquet(path + "vol_market_ratio")
        top_performing_assets.write.mode("overwrite").parquet(
            path + "top_performing_assets"
        )
    except Exception as e:
        print(f"Unable to write data to local storage due to {e}")
        raise
