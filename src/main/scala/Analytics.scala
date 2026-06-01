import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Analytics {
  def main(args: Array[String]): Unit = {
    // Airflow will pass the date as the first argument (e.g., "2026-03-12")
    if (args.length < 2) {
      System.err.println("Usage: Analytics <date_YYYY-MM-DD>")
      System.exit(1)
    }
    val processDate = args(0)
    val basePath = args(1)


    val spark = SparkSession.builder()
      .appName("CryptoMarketAnalysis")
      .enableHiveSupport()
      .getOrCreate()

    // Enable dynamic partition overwrite for our daily metrics
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sparkContext.setLogLevel("ERROR")

    // Read ALL processed data (for historical averages)
    val df = spark.read
      .option("header", "true")
      .parquet(s"$basePath/data/processed")
      .cache() 

    // Perform Transformations
    val (currTopPrice, currTopMarket) = currentStatistics(df, processDate)
    val (avgMarketCap, avgPrice) = aggregateStatistics(spark, df)
    val volMarketRatio = volToMarketRatio(df)
    val topPerforming = topPerformingAsset(avgMarketCap, avgPrice, volMarketRatio)

    // Save to Parquet and update Hive
    saveAnalytics(spark, currTopPrice, currTopMarket, avgMarketCap, avgPrice, volMarketRatio, topPerforming, processDate,basePath)

    spark.stop()
  }

  def currentStatistics(df: DataFrame, processDate: String): (DataFrame, DataFrame) = {
    val recentData = df.filter(col("updated_date") === processDate)
      .select("name", "current_price", "market_cap", "market_cap_rank", "updated_date", "total_volume")

    val rankPriceSpec = Window.partitionBy("updated_date").orderBy(col("current_price").desc)
    val currTopPrice = recentData.withColumn("current_price_rank", rank().over(rankPriceSpec))
    val currTopMarket = recentData.orderBy(col("market_cap").desc)

    (currTopPrice, currTopMarket)
  }

  def aggregateStatistics(spark: SparkSession, df: DataFrame): (DataFrame, DataFrame) = {
    // Create temp view to use pure SQL just like in your Python script
    df.select("name", "current_price", "market_cap", "total_volume", "updated_date")
      .createOrReplaceTempView("crypto_prices")

    val avgMarketCap = spark.sql("""
      WITH avg_market AS (
          SELECT name, avg(market_cap) as avg_market_cap
          FROM crypto_prices 
          GROUP BY name
      )
      SELECT *, RANK() OVER (ORDER BY avg_market_cap DESC) as avg_market_cap_rank
      FROM avg_market
    """)

    val avgPrice = spark.sql("""
      WITH average_prices AS (
          SELECT name, ROUND(avg(current_price), 3) as average_price
          FROM crypto_prices 
          GROUP BY name
      )
      SELECT *, RANK() OVER (ORDER BY average_price DESC) as avg_price_rank
      FROM average_prices
    """)

    (avgMarketCap, avgPrice)
  }

  def volToMarketRatio(df: DataFrame): DataFrame = {
    val volMarketRatio = df.filter(col("total_volume") =!= 0 && col("market_cap") =!= 0)
      .withColumn("vol_market_ratio", round(col("total_volume") / col("market_cap"), 5))
      .select("name", "vol_market_ratio", "total_volume")

    val rankVolMarketSpec = Window.orderBy(col("vol_market_ratio").desc)
    val windowSpec = Window.partitionBy("name").orderBy(col("vol_market_ratio").desc)

    volMarketRatio
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")
      .withColumn("vol_market_rank", rank().over(rankVolMarketSpec))
  }

  def topPerformingAsset(avgMarket: DataFrame, avgPrice: DataFrame, volMarketRatio: DataFrame): DataFrame = {
    var topPerforming = avgMarket
      .join(avgPrice, Seq("name"))
      .join(volMarketRatio.select("name", "total_volume", "vol_market_ratio", "vol_market_rank"), Seq("name"))

    val windowSpec = Window.orderBy(col("top_performing_score").asc)

    topPerforming = topPerforming
      .withColumn("top_performing_score", round(
        col("avg_market_cap_rank") * 0.4 + 
        col("avg_price_rank") * 0.4 + 
        col("vol_market_rank") * 0.2
      ))
      .withColumn("top_performing_rank", rank().over(windowSpec))
      .orderBy("top_performing_rank")

    topPerforming
  }

  def saveAnalytics(spark: SparkSession, 
                  currTopPrice: DataFrame, currTopMarket: DataFrame, 
                  avgMarketCap: DataFrame, avgPrice: DataFrame, 
                  volMarketRatio: DataFrame, topPerforming: DataFrame, 
                  processDate: String, basePath: String): Unit = {
    val analyticsPath = s"$basePath/data/analytics"

    try {
      // --- currTopPrice (partitioned) ---
      currTopPrice.write.mode(SaveMode.Overwrite)
        .partitionBy("updated_date")
        .parquet(s"$analyticsPath/curr_top_price")

      spark.sql(s"""
        CREATE EXTERNAL TABLE IF NOT EXISTS analytics_curr_top_price (
          name STRING,
          current_price DOUBLE,
          market_cap BIGINT,
          market_cap_rank BIGINT,
          total_volume DOUBLE,
          current_price_rank INT
        )
        PARTITIONED BY (updated_date DATE)
        STORED AS PARQUET
        LOCATION '$analyticsPath/curr_top_price'
      """)
      spark.sql(s"ALTER TABLE analytics_curr_top_price ADD IF NOT EXISTS PARTITION (updated_date='$processDate')")

      // --- currTopMarket (partitioned) ---
      currTopMarket.write.mode(SaveMode.Overwrite)
        .partitionBy("updated_date")
        .parquet(s"$analyticsPath/curr_top_market")

      spark.sql(s"""
        CREATE EXTERNAL TABLE IF NOT EXISTS analytics_curr_top_market (
          name STRING,
          current_price DOUBLE,
          market_cap BIGINT,
          market_cap_rank BIGINT,
          total_volume DOUBLE
        )
        PARTITIONED BY (updated_date DATE)
        STORED AS PARQUET
        LOCATION '$analyticsPath/curr_top_market'
      """)
      spark.sql(s"ALTER TABLE analytics_curr_top_market ADD IF NOT EXISTS PARTITION (updated_date='$processDate')")

      // --- Unpartitioned historical tables (no ALTER TABLE needed) ---
      avgMarketCap.write.mode(SaveMode.Overwrite)
        .parquet(s"$analyticsPath/avg_market_cap")

      avgPrice.write.mode(SaveMode.Overwrite)
        .parquet(s"$analyticsPath/avg_price")

      volMarketRatio.write.mode(SaveMode.Overwrite)
        .parquet(s"$analyticsPath/vol_market_ratio")

      topPerforming.write.mode(SaveMode.Overwrite)
        .parquet(s"$analyticsPath/top_performing_assets")

    } catch {
      case e: Exception =>
        println(s"Unable to write analytics data due to: ${e.getMessage}")
        throw e
    }
  }
}