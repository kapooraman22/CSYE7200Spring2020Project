package coinyser

import java.net.URI
import java.time.Instant
import java.util.Scanner
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions.{explode, from_json, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object BatchProducer {
  def jsonToHttpTransactions(json: String)(implicit spark:
  SparkSession): Dataset[HttpTransaction] = {
    import spark.implicits._
    val ds: Dataset[String] = Seq(json).toDS()
    val txSchema: StructType = Seq.empty[HttpTransaction].toDS().schema
    val schema = ArrayType(txSchema)
    val arrayColumn = from_json($"value".cast(StringType), schema)
    ds.select(explode(arrayColumn).alias("v"))
      .select("v.*")
      .as[HttpTransaction]
  }

  def httpToDomainTransactions(ds: Dataset[HttpTransaction]): Dataset[Transaction] = {
    import ds.sparkSession.implicits._
    ds.select(
      $"date".cast(LongType).cast(TimestampType).as("timestamp"),
      $"date".cast(LongType).cast(TimestampType).cast(DateType).as("date"),
      $"tid".cast(IntegerType),
      $"price".cast(DoubleType),
      $"type".cast(BooleanType).as("sell"),
      $"amount".cast(DoubleType))
      .as[Transaction]
  }
}