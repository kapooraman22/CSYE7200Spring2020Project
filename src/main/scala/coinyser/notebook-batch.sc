// The code in this worksheet is meant to be run in Zeppelin.
import org.apache.spark.sql.{Dataset, SparkSession}
implicit val spark: SparkSession = SparkSession.builder.master("local[*]").appName("coinyser").getOrCreate()
import org.apache.spark.sql.functions._
import spark.implicits._
val z = new {
  def show[A](ds: Dataset[A]): Unit = ds.show(false)
}

val transactions = spark.read.parquet(  "/home/varsh/CSYE7200Spring2020Project-master/CSYE7200Spring2020Project-master/data/transactions")
z.show(transactions.sort($"timestamp"))

val group = transactions.groupBy(window($"timestamp", "20 minutes"))

val tmpAgg = group.agg(
  count("tid").as("count"),
  avg("price").as("avgPrice"),
  stddev("price").as("stddevPrice"),
  last("price").as("lastPrice"),
  sum("amount").as("sumAmount"))

val aggregate = tmpAgg.select("window.start", "count", "avgPrice", "lastPrice", "stddevPrice", "sumAmount").sort("start").cache()

z.show(aggregate)