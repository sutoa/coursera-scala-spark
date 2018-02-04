package timeusage

import org.apache.spark.sql._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  val spark = SparkSession.builder()
      .master("local")
  .appName("testing")
//  .enableHiveSupport()
  .getOrCreate()

  test("create with toDf"){
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val trades = Seq(
    Trade("ibm", 100, 10, "buy", 1),
    Trade("appl", 1000, 5, "sell", 199),
    Trade("msft", 24.5, 100, "buy", 1.4)
    ).toDF()

    trades.show()

    val dirCol = when('direction === "buy", "B")
      .when('direction === "sell", "S")
      .otherwise("-").alias("dir")

    trades.select(dirCol).show()

  }

  test("createDataframe"){
    val trades = Seq(
    Trade("ibm", 100, 10, "buy", 1),
    Trade("appl", 1000, 5, "sell", 199),
    Trade("msft", 24.5, 100, "buy", 1.4)
    )

    val df = spark.createDataFrame(trades)

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // using UDF
    val names = array($"qty", $"price", $"commission")
    val addthem: (Seq[Double] => Double) = ds => ds.sum
    val addthemUdf = udf(addthem)
    df.select(addthemUdf(names)).show()

    // using reduce
    val columnList = List(col("qty"), col("price"), col("commission"))
    val column1 = columnList.reduce(_+_).as("aaa")
    df.select(column1).show
    df.select(columnList.reduce(_ + _) as "bbb").show()

    // have to declare list as List[Column] so that the explicit conversion can take place. $"qty" returns ColumnName instead of Column
    val columnNameList: List[Column] = List($"qty", $"price", $"commission")
    val column2 = columnNameList.reduce(_+_).as("ccc")
    df.select(column2).show

    // have to declare list as List[Column] so that the explicit conversion can take place. 'qty returns Symbol instead of Column
    val symbolList: List[Column] = List('qty, 'price, 'commission)
    val column3 = symbolList.reduce(_+_).as("ddd")
    df.select(column3).show

  }


  test("test average") {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val trades = Seq(
      Trade("ibm", 100, 10, "buy", 1),
      Trade("appl", 1000, 5, "sell", 199),
      Trade("msft", 24.5, 100.50, "buy", 1.4)
    )

    val df = spark.createDataFrame(trades)

    df.groupBy($"direction")
      .agg(
        round(avg("qty"), 1).as("otherone"),
        round(avg("commission"), 0).as("commission"))
      .show

  }

  test("df to ds conversion") {
    import spark.implicits._

    val trades = Seq(
      Trade("ibm", 100, 10, "buy", 1),
      Trade("appl", 1000, 5, "sell", 199),
      Trade("msft", 24.5, 100.50, "buy", 1.4)
    )

    val df = spark.createDataFrame(trades)

    val ds = df.as[Trade]
    ds.show

  }

  test("grouping with dataset") {
    import spark.implicits._
    import org.apache.spark.sql.expressions.scalalang.typed._

    val trades = Seq(
      Trade("ibm", 100, 10, "buy", 1),
      Trade("appl", 1000, 5, "sell", 199),
      Trade("msft", 24.5, 100.50, "buy", 1.4)
    )

    val ds = trades.toDS()

    ds.groupByKey(_.direction)
      .agg(
        sum(_.qty)
      )
      // the grouped column is named 'value', how do i give it an alias?
      .orderBy($"value".desc).show()

  }
}

case class Trade(stock: String, price: Double, qty: Double, direction: String, commission: Double)
