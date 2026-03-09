from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pathlib import Path
from pyspark.sql.functions import to_timestamp

schema = StructType(
    [
        StructField("ath", DoubleType(), True),
        StructField("ath_change_percentage", DoubleType(), True),
        StructField("ath_date", StringType(), True),
        StructField("atl", DoubleType(), True),
        StructField("atl_change_percentage", DoubleType(), True),
        StructField("atl_date", StringType(), True),
        StructField("circulating_supply", DoubleType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("fully_diluted_valuation", LongType(), True),
        StructField("high_24h", DoubleType(), True),
        StructField("id", StringType(), True),
        StructField("image", StringType(), True),
        StructField("last_updated", StringType(), True),
        StructField("low_24h", DoubleType(), True),
        StructField("market_cap", LongType(), True),
        StructField("market_cap_change_24h", DoubleType(), True),
        StructField("market_cap_change_percentage_24h", DoubleType(), True),
        StructField("market_cap_rank", LongType(), True),
        StructField("max_supply", DoubleType(), True),
        StructField("name", StringType(), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField(
            "roi",
            StructType(
                [
                    StructField("currency", StringType(), True),
                    StructField("percentage", DoubleType(), True),
                    StructField("times", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "sparkline_in_7d",
            StructType(
                [
                    StructField("price", ArrayType(DoubleType()), True),
                ]
            ),
            True,
        ),
        StructField("symbol", StringType(), True),
        StructField("total_supply", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
    ]
)

"""
Used to process of most recent file
"""
def process_new():
    # starting the Spark Session
    spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

    # get the most recent file (unprocessed)

    data_path = Path().resolve().parent / "data" / "raw"
    dates = list((data_path.glob("*.json")))

    filename = sorted(dates)[-1].name

    # Read all raw JSON files in folder raw
    df = (
        spark.read.option("multiLine", True)
        .option("header", True)
        .schema(schema)
        .json(f"data/raw/{filename}")
    )

    # Define what you want to ignore
    exclude_cols = ["last_updated"]

    subset_cols = [c for c in df.columns if c not in exclude_cols]

    df = df.drop_duplicates(subset_cols)

    # type cast data
    df = (
        df.withColumn("last_updated", to_timestamp("last_updated"))
        .withColumn("ath_date", to_timestamp("ath_date"))
        .withColumn("atl_date", to_timestamp("atl_date"))
        .drop("image", "symbol", "roi")
    )

    # Access the column and field using F.col
    price_array = F.col("sparkline_in_7d.price")

    # finds mean of the 7 day prices obtained
    df = df.withColumn(
        "7d_avg",
        F.aggregate(price_array, F.lit(0.0), lambda acc, x: acc + x)
        / F.size(price_array),
    )

    # finds high of the 7 day prices obtained
    df = df.withColumn("7d_max", F.array_max(price_array))

    # finds low of the 7 day prices obtained
    df = df.withColumn("7d_low", F.array_min(price_array))

    df = df.drop("sparkline_in_7d")

    df = df.withColumn("updated_date", F.to_date("last_updated"))

    try:
        # Write & Save File in .parquet format
        df.write.mode("append").partitionBy("updated_date").parquet("data/processed")
    except:
        print("Unable to write data to local storage")

"""
Used for initial processing of all the dates
"""
def processing_all():
    # starting the Spark Session
    spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

    # Read all raw JSON files in folder raw
    df = (
        spark.read.option("multiLine", True)
        .option("header", True)
        .schema(schema)
        .json("data/raw")
    )

    # Define what you want to ignore
    exclude_cols = ["last_updated"]

    subset_cols = [c for c in df.columns if c not in exclude_cols]

    df = df.drop_duplicates(subset_cols)

    # type cast data
    df = (
        df.withColumn("last_updated", to_timestamp("last_updated"))
        .withColumn("ath_date", to_timestamp("ath_date"))
        .withColumn("atl_date", to_timestamp("atl_date"))
        .drop("image", "symbol", "roi")
    )

    # Access the column and field using F.col
    price_array = F.col("sparkline_in_7d.price")

    # finds mean of the 7 day prices obtained
    df = df.withColumn(
        "7d_avg",
        F.aggregate(price_array, F.lit(0.0), lambda acc, x: acc + x)
        / F.size(price_array),
    )

    # finds high of the 7 day prices obtained
    df = df.withColumn("7d_max", F.array_max(price_array))

    # finds low of the 7 day prices obtained
    df = df.withColumn("7d_low", F.array_min(price_array))

    df = df.drop("sparkline_in_7d")

    df = df.withColumn("updated_date", F.to_date("last_updated"))

    try:
        # Write & Save File in .parquet format
        df.write.mode("overwrite").partitionBy("updated_date").parquet("data/processed")
    except:
        print("Unable to write data to local storage")
