import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class AnalysisTest extends AnyFunSuite with DataFrameSuiteBase {

  val schema = StructType(Array(
  StructField("name",            StringType,  true),
  StructField("current_price",   DoubleType,  true),
  StructField("market_cap",      LongType,    true),
  StructField("market_cap_rank", LongType,    true),
  StructField("total_volume",    DoubleType,  true),
  StructField("updated_date",    StringType,  true)
  ))

  // --- dataAnalytics tests ---
  test("currentStatistics ranks coins correctly by price and market cap") {
    val processDate = "2026-05-01"
    val data = Seq(
      Row("Bitcoin",  70187.0, 1381651251183L, 1L, 20154184933.0, processDate),
      Row("Ethereum", 1986.61, 239754873918L,  2L, 16933980770.0, processDate),
      Row("BNB",      634.23,  85497466223L,   4L, 746909186.0,   processDate)
    )
   val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val (currTopPrice, currTopMarket) = Analytics.currentStatistics(df, processDate)

    // verify price ranking
    val priceRow = currTopPrice.filter("name = 'Bitcoin'").collect()(0)
    assert(priceRow.getAs[Int]("current_price_rank") == 1)

    val bnbRow = currTopPrice.filter("name = 'BNB'").collect()(0)
    assert(bnbRow.getAs[Int]("current_price_rank") == 3)

    // verify market cap ordering — Bitcoin should be first
    val topMarketRow = currTopMarket.collect()(0)
    assert(topMarketRow.getAs[String]("name") == "Bitcoin")
  }

  test("aggregateStatistics ranks coins correctly by price and market cap") {
    val data = Seq(
      // Date 1
      Row("Bitcoin",  60000.0, 800000000000L,  1L, 18000000000.0, "2026-05-01"),
      Row("Ethereum", 1800.0,  1200000000000L, 2L, 15000000000.0, "2026-05-01"),
      Row("BNB",      600.0,   80000000000L,   4L, 700000000.0,   "2026-05-01"),

      // Date 2
      Row("Bitcoin",  80000.0, 900000000000L,  1L, 22000000000.0, "2026-05-02"),
      Row("Ethereum", 2200.0,  1600000000000L, 2L, 18000000000.0, "2026-05-02"),
      Row("BNB",      700.0,   90000000000L,   4L, 800000000.0,   "2026-05-02")
    )

   val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val (avgMarketCap, avgPrice) = Analytics.aggregateStatistics(spark,df)

    // verify price ranking
    val priceRow = avgPrice.filter("name = 'Bitcoin'").collect()(0)
    assert(priceRow.getAs[Int]("avg_price_rank") == 1)

    val bnbRow = avgPrice.filter("name = 'BNB'").collect()(0)
    assert(bnbRow.getAs[Int]("avg_price_rank") == 3)

    // verify market cap ordering — Bitcoin should be first
    val topMarketRow = avgMarketCap.collect()(0)
    assert(topMarketRow.getAs[String]("name") == "Ethereum")
  }

  test("volToMarketRatio excludes rows where total_volume is null") {
    val data = Seq(
      Row("Bitcoin",  1381651251183L, 20154184933.0, "2026-05-01"),
      Row("Ethereum", 239754873918L,  null,           "2026-05-01")
    )
    val schema = StructType(Array(
      StructField("name",         StringType, true),
      StructField("market_cap",   LongType,   true),
      StructField("total_volume", DoubleType, true),
      StructField("updated_date", StringType, true)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val result = Analytics.volToMarketRatio(df)
    assert(result.filter("name = 'Ethereum'").count() == 0)
  }

  test("volToMarketRatio filters duplicate coins with lower volToMarket ratio") {
    val data = Seq(
      // Bitcoin - two dates, different ratios (0.05 should win)
      Row("Bitcoin", 1381651251183L, 69075625559.0, "2026-05-01"),
      Row("Bitcoin", 1381651251183L, 20154184933.0, "2026-05-02"),

      // Ethereum - one date
      Row("Ethereum", 239754873918L, 16933980770.0, "2026-05-01"),

      // BNB - one date
      Row("BNB", 85497466223L, 746909186.0, "2026-05-01")
    )

    val schema = StructType(Array(
      StructField("name",         StringType, true),
      StructField("market_cap",   LongType,   true),
      StructField("total_volume", DoubleType, true),
      StructField("updated_date", StringType, true)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val volToMarket = Analytics.volToMarketRatio(df)

    // verify no of rows 
    assert(volToMarket.count() == 3)

    // Ensure that Bitcoin 0.05 should win
    val bitcoinRatio = volToMarket.filter("name = 'Bitcoin'").collect()(0).getAs[Double]("vol_market_ratio")
    assert(math.abs(bitcoinRatio - 0.05) < 0.001)
  }

  test("top performing score is calculated correctly using weighted formula") {
    val avgMarket = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("Bitcoin",  1381651251183.0, 1),
      Row("Ethereum", 239754873918.0,  2),
      Row("BNB",      85497466223.0,   3)
    )), StructType(Array(
      StructField("name",                StringType, true),
      StructField("avg_market_cap",      DoubleType, true),
      StructField("avg_market_cap_rank", IntegerType, true)
    )))

    val avgPrice = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("Bitcoin",  70187.0, 1),
      Row("Ethereum", 1986.61, 2),
      Row("BNB",      634.23,  3)
    )), StructType(Array(
      StructField("name",           StringType, true),
      StructField("average_price",  DoubleType, true),
      StructField("avg_price_rank", IntegerType, true)
    )))

    val volMarket = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("Bitcoin",  20154184933.0, 0.05, 1),
      Row("Ethereum", 16933980770.0, 0.07, 2),
      Row("BNB",      746909186.0,   0.008, 3)
    )), StructType(Array(
      StructField("name",            StringType, true),
      StructField("total_volume",    DoubleType, true),
      StructField("vol_market_ratio",DoubleType, true),
      StructField("vol_market_rank", IntegerType, true)
    )))

    val result = Analytics.topPerformingAsset(avgMarket, avgPrice, volMarket)
    val bitcoinRow = result.filter("name = 'Bitcoin'").collect()(0)

    // 1*0.4 + 1*0.4 + 1*0.2 = 1.0
    assert(math.abs(bitcoinRow.getAs[Double]("top_performing_score") - 1.0) < 0.001)
    assert(bitcoinRow.getAs[Int]("top_performing_rank") == 1)
  }
}