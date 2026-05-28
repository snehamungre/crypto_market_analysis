import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class ProcessingTest extends AnyFunSuite with DataFrameSuiteBase {

  val schema = StructType(Array(
    StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("current_price", DoubleType, true),
    StructField("market_cap", LongType, true),
    StructField("total_volume", DoubleType, true),
    StructField("circulating_supply", DoubleType, true),
    StructField("total_supply", DoubleType, true),
    StructField("last_updated", StringType, true),
    StructField("ath_date", StringType, true),
    StructField("atl_date", StringType, true),
    StructField("sparkline_in_7d", StructType(Array(
      StructField("price", ArrayType(DoubleType), true)
    )), true)
  ))

  // --- dataVerification tests ---

  test("duplicate rows differing only in last_updated are deduplicated") {
    val data = Seq(
      Row("btc", "Bitcoin", 68000.0, 1000000L, 500000.0, 900000.0, 1000000.0, "2026-05-01T10:00:00Z", "2021-01-01T00:00:00Z", "2019-01-01T00:00:00Z", Row(Array(100.0, 200.0))),
      Row("btc", "Bitcoin", 68000.0, 1000000L, 500000.0, 900000.0, 1000000.0, "2026-05-01T11:00:00Z", "2021-01-01T00:00:00Z", "2019-01-01T00:00:00Z", Row(Array(100.0, 200.0)))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val result = Processing.dataVerification(df)
    assert(result.count() == 1)
  }

  test("rows with negative critical fields are filtered out") {
    val data = Seq(
      Row("btc", "Bitcoin", 68000.0, 1000000L, 500000.0, 900000.0, 1000000.0, "2026-05-01T10:00:00Z", "2021-01-01T00:00:00Z", "2019-01-01T00:00:00Z", Row(Array(100.0, 200.0))),
      Row("eth", "Ethereum", -1.0,    1000000L, 500000.0, 900000.0, 1000000.0, "2026-05-01T10:00:00Z", "2021-01-01T00:00:00Z", "2019-01-01T00:00:00Z", Row(Array(100.0, 200.0)))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val result = Processing.dataVerification(df)
    assert(result.count() == 1)
    assert(result.filter("id = 'btc'").count() == 1)
  }

  test("rows with null critical fields are dropped") {
    val data = Seq(
      Row("btc", "Bitcoin", 68000.0, 1000000L, 500000.0, 900000.0, 1000000.0, "2026-05-01T10:00:00Z", "2021-01-01T00:00:00Z", "2019-01-01T00:00:00Z", Row(Array(100.0, 200.0))),
      Row("eth", "Ethereum", null,    1000000L, 500000.0, 900000.0, 1000000.0, "2026-05-01T10:00:00Z", "2021-01-01T00:00:00Z", "2019-01-01T00:00:00Z", Row(Array(100.0, 200.0)))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val result = Processing.dataVerification(df)
    assert(result.count() == 1)
  }

  test("rows where total_volume >= market_cap are filtered out") {
    val data = Seq(
      Row("btc", "Bitcoin", 68000.0, 1000000L, 500000.0,  900000.0, 1000000.0, "2026-05-01T10:00:00Z", "2021-01-01T00:00:00Z", "2019-01-01T00:00:00Z", Row(Array(100.0, 200.0))),
      Row("eth", "Ethereum", 2000.0, 1000000L, 2000000.0, 900000.0, 1000000.0, "2026-05-01T10:00:00Z", "2021-01-01T00:00:00Z", "2019-01-01T00:00:00Z", Row(Array(100.0, 200.0)))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val result = Processing.dataVerification(df)
    assert(result.count() == 1)
    assert(result.filter("id = 'btc'").count() == 1)
  }

  test("pipeline halts when more than 90% of records are filtered") {
    // 1 valid row, 9 invalid (negative price) -> 90% dropped -> should throw
    val data = (1 to 9).map(i =>
      Row(s"coin$i", s"Coin$i", -1.0, 1000000L, 500000.0, 900000.0, 1000000.0, "2026-05-01T10:00:00Z", "2021-01-01T00:00:00Z", "2019-01-01T00:00:00Z", Row(Array(100.0, 200.0)))
    ) :+ Row("btc", "Bitcoin", 68000.0, 1000000L, 500000.0, 900000.0, 1000000.0, "2026-05-01T10:00:00Z", "2021-01-01T00:00:00Z", "2019-01-01T00:00:00Z", Row(Array(100.0, 200.0)))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    assertThrows[Exception] {
      Processing.dataVerification(df)
    }
  }

  // --- calculateSparklineStats tests ---

  test("sparkline stats correctly compute max, min, and avg") {
    val sparklineSchema = StructType(Array(
      StructField("id", StringType, true),
      StructField("sparkline_in_7d", StructType(Array(
        StructField("price", ArrayType(DoubleType), true)
      )), true)
    ))
    val data = Seq(
      Row("btc", Row(Array(100.0, 200.0, 300.0)))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), sparklineSchema)
    val result = Processing.calculateSparklineStats(df)

    val row = result.collect()(0)
    assert(row.getAs[Double]("7d_max") == 300.0)
    assert(row.getAs[Double]("7d_low") == 100.0)
    assert(math.abs(row.getAs[Double]("7d_avg") - 200.0) < 0.001)
  }
}