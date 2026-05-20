import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Processing {
  // defining the schema
  val schema = StructType(Array(
    StructField("ath", DoubleType, true),
    StructField("ath_change_percentage", DoubleType, true),
    StructField("ath_date", StringType, true),
    StructField("atl", DoubleType, true),
    StructField("atl_change_percentage", DoubleType, true),
    StructField("atl_date", StringType, true),
    StructField("circulating_supply", DoubleType, true),
    StructField("current_price", DoubleType, true),
    StructField("fully_diluted_valuation", LongType, true),
    StructField("high_24h", DoubleType, true),
    StructField("id", StringType, true),
    StructField("image", StringType, true),
    StructField("last_updated", StringType, true),
    StructField("low_24h", DoubleType, true),
    StructField("market_cap", LongType, true),
    StructField("market_cap_change_24h", DoubleType, true),
    StructField("market_cap_change_percentage_24h", DoubleType, true),
    StructField("market_cap_rank", LongType, true),
    StructField("max_supply", DoubleType, true),
    StructField("name", StringType, true),
    StructField("price_change_24h", DoubleType, true),
    StructField("price_change_percentage_24h", DoubleType, true),
    StructField("roi", StructType(Array(
      StructField("currency", StringType, true),
      StructField("percentage", DoubleType, true),
      StructField("times", DoubleType, true)
    )), true),
    StructField("sparkline_in_7d", StructType(Array(
      StructField("price", ArrayType(DoubleType), true)
    )), true),
    StructField("symbol", StringType, true),
    StructField("total_supply", DoubleType, true),
    StructField("total_volume", DoubleType, true)
  ))

  def main(args: Array[String]): Unit = {
    // Airflow will pass the date as the first argument 
    if (args.length < 1) {
      System.err.println("Usage: Processing <date_YYYY-MM-DD>")
      System.exit(1)
    }
    val processDate = args(0)

    // Initialize Spark with Hive Support enabled
    val spark = SparkSession.builder()
      .appName("CryptoMarketProcessing")
      .enableHiveSupport()
      .getOrCreate()

    // Enable dynamic partition overwrite to safely update data day-by-day
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    // Execution Flow
    var df = readFiles(spark, processDate)
    df = calculateSparklineStats(df)
    df = dataVerification(df)
    saveToParquetAndHive(spark, df, processDate)

    spark.stop()
  }

  // Incremental Read Method
  def readFiles(spark: SparkSession, processDate: String): DataFrame = {
    val dataPath = s"data/raw/*$processDate*.json"
    spark.read
      .option("multiLine", true)
      .option("header", true)
      .schema(schema)
      .json(dataPath)
  }

  // Transformation Method
  def calculateSparklineStats(df: DataFrame): DataFrame = {
    val priceArray = col("sparkline_in_7d.price")

    df.withColumn("7d_max", array_max(priceArray))
      .withColumn("7d_low", array_min(priceArray))
      .withColumn("7d_avg", expr("aggregate(sparkline_in_7d.price, 0D, (acc, x) -> acc + x) / size(sparkline_in_7d.price)"))
      .drop("sparkline_in_7d")
  }

  // Verification and Filtering Method
  def dataVerification(df: DataFrame): DataFrame = {
    // Add date column
    var processedDf = df.withColumn("updated_date", to_date(col("last_updated")))

    // Drop duplicates ignoring 'last_updated'
    val checkDupCols = processedDf.columns.filterNot(_ == "last_updated")
    processedDf = processedDf.dropDuplicates(checkDupCols)

    // Type casting and dropping columns
    processedDf = processedDf
      .withColumn("last_updated", to_timestamp(col("last_updated")))
      .withColumn("ath_date", to_timestamp(col("ath_date")))
      .withColumn("atl_date", to_timestamp(col("atl_date")))
      .drop("image", "symbol", "roi")

    val filterCols = Seq("current_price", "market_cap", "total_volume", "circulating_supply", "total_supply")
    val countBefore = processedDf.count()
    val coinsBefore = processedDf.select("id")

    // Filter >= 0
    val condition = filterCols.map(c => col(c) >= 0).reduce(_ && _)
    processedDf = processedDf.filter(condition).na.drop(filterCols)

    // Filter volume < market_cap
    processedDf = processedDf.filter(col("total_volume") < col("market_cap"))

    val countAfter = processedDf.count()
    
    // Find dropped coins
    val coinsFiltered = coinsBefore.except(processedDf.select("id"))
      .collect()
      .map(row => row.getString(0)) // Extract string from Row

    println(s"The number of coins filtered: ${countBefore - countAfter}")
    println(s"The coins filtered: ${coinsFiltered.mkString(", ")}")

    if (countAfter <= 0.1 * countBefore) {
      throw new Exception(s"An error occurred because there isn't sufficient valid data \n Before: $countBefore \n After: $countAfter")
    }

    processedDf
  }

  // Save Method
  def saveToParquetAndHive(spark: SparkSession, df: DataFrame, processDate: String): Unit = {
    try {
      df.write
        .mode("overwrite")
        .partitionBy("updated_date")
        .parquet("data/processed")

      // Register/Update the Hive External Table
      spark.sql(s"""
        CREATE EXTERNAL TABLE IF NOT EXISTS processed_crypto_data (
          id STRING,
          name STRING,
          current_price DOUBLE,
          market_cap BIGINT,
          total_volume DOUBLE,
          circulating_supply DOUBLE,
          total_supply DOUBLE,
          max_supply DOUBLE,
          fully_diluted_valuation BIGINT,
          high_24h DOUBLE,
          low_24h DOUBLE,
          price_change_24h DOUBLE,
          price_change_percentage_24h DOUBLE,
          market_cap_change_24h DOUBLE,
          market_cap_change_percentage_24h DOUBLE,
          market_cap_rank BIGINT,
          ath DOUBLE,
          ath_change_percentage DOUBLE,
          ath_date TIMESTAMP,
          atl DOUBLE,
          atl_change_percentage DOUBLE,
          atl_date TIMESTAMP,
          last_updated TIMESTAMP,
          7d_max DOUBLE,
          7d_low DOUBLE,
          7d_avg DOUBLE
        )
        PARTITIONED BY (updated_date DATE)
        STORED AS PARQUET
        LOCATION 'data/processed'
      """)

      // Let Hive know a new partition folder was added
      spark.sql(s"""
        ALTER TABLE processed_crypto_data 
        ADD IF NOT EXISTS PARTITION (updated_date='$processDate')
      """)

    } catch {
      case e: Exception =>
        println(s"Unable to write data to local storage/Hive due to ${e.getMessage}")
        throw e
    }
  }
}