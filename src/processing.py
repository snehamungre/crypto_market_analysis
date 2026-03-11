from pyspark.sql import DataFrame, SparkSession
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


def read_file() -> DataFrame:
    # starting the Spark Session
    spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

    # get the most recent file (unprocessed)
    data_path = Path().resolve().parent / "data" / "raw"
    dates = list((data_path.glob("*.json")))

    filename = sorted(dates)[-1].name
    

    # Read most recent raw JSON files in folder raw
    df = (
        spark.read.option("multiLine", True)
        .option("header", True)
        .schema(schema)
        .json(f"data/raw/{filename}")
    )

    return df


def read_files() -> DataFrame:
    # starting the Spark Session
    spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

    # Read all raw JSON files in folder raw
    df = (
        spark.read.option("multiLine", True)
        .option("header", True)
        .schema(schema)
        .json("data/raw")
    )

    return df


def calculate_sparkline_stats(df) -> DataFrame:
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

    return df


def data_verification(df) -> DataFrame:
    datetime = ["last_updated"]

    # new col with just date
    df = df.withColumn("updated_date", F.to_date("last_updated"))

    check_dup_cols = [c for c in df.columns if c not in datetime]

    df = df.drop_duplicates(check_dup_cols)

    # type cast data and drop col
    df = (
        df.withColumn("last_updated", to_timestamp("last_updated"))
        .withColumn("ath_date", to_timestamp("ath_date"))
        .withColumn("atl_date", to_timestamp("atl_date"))
        .drop("image", "symbol", "roi")
    )

    filter_cols = [
        "current_price",
        "market_cap",
        "total_volume",
        "circulating_supply",
        "total_supply",
    ]

    count_before_filter = df.count()
    coins_before_filtered = df.select("id")

    df = df.filter(F.expr(" AND ".join([f"{c} >= 0" for c in filter_cols])))
    df = df.dropna(subset=filter_cols)

    df = df.filter(F.col("total_volume") <= F.col("market_cap"))

    count_after_filter = count_before_filter - df.count()

    coins_filtered = [
        c["id"] for c in coins_before_filtered.subtract(df.select("id")).collect()
    ]

    # logging the number of rows removed and their ids
    print(f"The number of coins filtered:{count_after_filter}")
    print(f"The coins filtered:{coins_filtered}")

    if count_after_filter <= 0.3 * count_before_filter:
        raise Exception(
            f"An error occurred because as there isn't sufficient valid data \n Before:  {count_before_filter} \n After: {count_after_filter} "
        )

    return df


def save_to_parquet(df):
    try:
        df.write.mode("append").partitionBy("updated_date").parquet("data/processed")
    except Exception as e:
        print(f"Unable to write data to local storage due to {e}")
        raise


"""
Used to process of most recent file
"""


def process_new():
    df = read_file()
    df = calculate_sparkline_stats(df)
    df = data_verification(df)
    save_to_parquet(df)


"""
Used for initial processing of all the dates
"""


def process_all():
    df = read_files()
    df = calculate_sparkline_stats(df)
    df = data_verification(df)

    try:
        # Write & Save Files in .parquet format (overwrites)
        df.write.mode("overwrite").partitionBy("updated_date").parquet("data/processed")
    except Exception as e:
        print(f"Unable to write data to local storage due to {e}")
        raise
