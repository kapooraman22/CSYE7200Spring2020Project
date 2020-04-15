package Prediction

import java.sql.{Connection, DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.cloudera.sparkts.models.ARIMA
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io._
import scala.math.BigDecimal
/*
import sys
import os

system.env['HADOOP_HOME'] = "C:/Mine/Spark/hadoop-2.6.0"
system.path.append("C:/Mine/Spark/hadoop-2.6.0/bin")

import scala.collection.JavaConverters._
val environmentVars = System.getenv().asScala
*/


object Predict_Price extends App
{
    System.setProperty("hadoop.home.dir", "C:\\Opt\\hadoop-2.6.0\\")
    // Configuration parameters
    val url = "jdbc:mysql://localhost:3306/mydatabase"
    //val driver = "com.mysql.jdbc.Driver"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "root"
    val password = "aman1aman"
    var connection:Connection = _
    var responseFileName = "response.json"
    var realdate =  "2019-10-29T00:00:00Z"

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val outSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    var date = dateFormat.parse(realdate)
    var mytime = Calendar.getInstance()
    val stamp = new Timestamp(System.currentTimeMillis());
    val today = new Date(stamp.getTime());
    val jsonmapper = new ObjectMapper()
    jsonmapper.registerModule(DefaultScalaModule)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    insert_into_db()

    // Spark accessing DB
    val APP_NAME = "Bitcoin Prediction"
    var period = 15
    var lastdate =  "2020-01-01"
    val conf = new SparkConf().setAppName(APP_NAME).setMaster("local[2]")
    val sparkcontext = new SparkContext(conf)
    val spark = SparkSession
        .builder()
        .appName(APP_NAME)
        .getOrCreate()
    var dataread = spark.read
        .format("jdbc")
        .option("url", url)
        .option("user", username)
        .option("password", password)
    val df = dataread.option("dbtable","(SELECT time, price FROM bitcoin_price order by time) as btc").load()
    df.createOrReplaceTempView("opp")
    val days = df.collect().flatMap((row: Row) => Array(row.get(0)))
    val btcprice = df.collect().flatMap((row: Row) => Array(row.get(1).toString.toDouble))
    lastdate = days(days.length-1).toString

    // Creating HTTP server for REST API
    implicit val system = ActorSystem("Prediction.Predict_Price")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    // Defining logic for route
    val route =
        get
        {
            path("lastWeekPrice")  // API to get last week's prices
            {
                parameter()
                {
                    var res = List[Map[String, Any]]()
                    try
                    {
                        Class.forName(driver)
                        connection = DriverManager.getConnection(url, username, password)
                        val statement = connection.createStatement
                        val oneWeekBefore = getEarlierDate(today, -7)
                        val seq = statement.executeQuery(s"SELECT time, price from bitcoin_price where time between '" + convertDatetoString(oneWeekBefore, outSdf) + "' AND '" + convertDatetoString(today, outSdf) + "'  order by time desc")
                        while (seq.next)
                        {
                            res ::= Map("time"-> seq.getString("time"), "price"-> seq.getString("price"))
                        }
                    }
                    catch
                    {
                        case ex: Exception => ex.printStackTrace
                    }

                    connection.close
                    complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, jsonmapper.writeValueAsString(res)))
                }
            } ~
            path("lastMonthPrice")   // API to get last month's prices
            {
                parameter()
                {
                    var res = List[Map[String, Any]]()
                    try
                    {
                        Class.forName(driver)
                        connection = DriverManager.getConnection(url, username, password)
                        val statement = connection.createStatement
                        val oneMonthBefore = getMonthBefore(today)
                        val seq = statement.executeQuery(s"SELECT time, price from bitcoin_price where time between '" + convertDatetoString(oneMonthBefore, outSdf) + "' AND '" + convertDatetoString(today, outSdf) + "'  order by time desc")
                        while (seq.next)
                        {
                            res ::= Map("time"-> seq.getString("time"), "price"-> seq.getString("price"))
                        }
                    }
                    catch
                    {
                        case ex: Exception => ex.printStackTrace
                    }

                    connection.close
                    complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, jsonmapper.writeValueAsString(res)))
                }
            } ~
            path("customPrice")   // API to get prices in between custom dates
            {
                parameter('start.as[String], 'end.as[String])
                {
                    (start,end) =>
                    var res = List[Map[String, Any]]()
                    try
                    {
                        Class.forName(driver)
                        connection = DriverManager.getConnection(url, username, password)
                        val statement = connection.createStatement
                        val seq = statement.executeQuery(s"SELECT time, price FROM bitcoin_price where time between '$start' and '$end' order by time desc")
                        while (seq.next)
                        {
                            res ::= Map("time"-> seq.getString("time"), "price"-> seq.getString("price"))
                        }
                    }
                    catch
                    {
                        case e: Exception => e.printStackTrace
                    }
                    connection.close
                    complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, jsonmapper.writeValueAsString(res)))
                }
            } ~
            path("movingAverage")    // API to get moving average of n day's price
            {
                parameter('start.as[String], 'end.as[String], 'n.as[Int])
                {
                    (start,end,n) =>
                    var btcp  = List[Double]()
                    var res = List[Map[String, Any]]()
                    try
                    {
                        Class.forName(driver)
                        connection = DriverManager.getConnection(url, username, password)
                        val statement = connection.createStatement
                        var runprice : Double=0;
                        val rs = statement.executeQuery(s"SELECT time, price FROM bitcoin_price where time between '$start' and '$end' order by time asc")
                        while (rs.next)
                        {
                            val price = rs.getString("price").toDouble
                            val time = rs.getString("time")
                            if (res.size>=n)
                            {
                                val temp = btcp(n-1)
                                runprice=((runprice*n) - temp + price)/n
                                //runprice = (price+(runprice*res.size))/(res.size+1)
                                runprice = BigDecimal(runprice).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble
                                res :+= Map("time"-> time, "price"->runprice.toString)
                            }
                            else
                            {
                                runprice = (price+(runprice*res.size))/(res.size+1)
                                res :+= Map("time"-> time, "price"->price.toString)
                            }
                            btcp ::=price //////// storing the actual btc price in separate list because mapped list will contain moving average price
                        }
                    }
                    catch
                    {
                        case e: Exception => e.printStackTrace
                    }
                    connection.close
                    complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, jsonmapper.writeValueAsString(res)))
                }
            } ~
            path("predictPrice")    // API to predict price for next 15 days
            {
                var futuredate =  "2020-04-20"
                val pdateFormat = new SimpleDateFormat("yyyy-MM-dd")
                var pdate = pdateFormat.parse(lastdate)
                var ptime = Calendar.getInstance()
                ptime.setTime(pdate)
                //var pptime = ptime.clone().asInstanceOf[Calendar]
                var res = List[Map[String, Any]]()

                // Prediction
                var tmp : Double = 0.0
                val actual = new DenseVector(btcprice)
                val model = ARIMA.fitModel(1,0,1,actual)
                println("ARIMA model with parameter = (" + model.p + "," + model.d + "," + model.q + ") and Akaike Information Criteria =" + model.approxAIC(actual)  )
                val predicted = model.forecast(actual, period)
                for (i <- 365 until predicted.size)
                {
                    ptime.add(Calendar.DATE,1)
                    futuredate = pdateFormat.format(ptime.getTime)
                    tmp = BigDecimal(predicted(i)).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble
                    res :+= Map("time"->futuredate,"price"-> tmp.toString)
                }
                /*val metrics = new MultilabelMetrics(model)
                println(s"Recall = ${metrics.recall}")
                println(s"Precision = ${metrics.precision}")
                println(s"F1 measure = ${metrics.f1Measure}")
                println(s"Accuracy = ${metrics.accuracy}")*/

                complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, jsonmapper.writeValueAsString(res)))
            }
        }

        val bindingFuture = Http().bindAndHandle(route, "localhost", 8081)
        println(s"Service Started. Here is the list of REST APIs supported")
        println(s"1. To get the bitcoin price of last week: http://localhost:8081/lastWeekPrice")
        println(s"2. To get the bitcoin price of last month: http://localhost:8081/lastMonthPrice")
        println(s"3. To get the bitcoin price between two custom dates http://localhost:8081/customPrice?start=2018-12-20&end=2018-12-28")
        println(s"4. To get the n days moving average between two custom dates http://localhost:8081/movavg?start=2018-12-15&end=2018-12-28&n=2")
        println(s"5. To get the next 15 days prediction of the bitcoin prices http://localhost:8081/predictPrice")

        def convertDatetoString(date: java.util.Date, sdf: SimpleDateFormat): String ={
            val dateString = sdf.format(date)
            return dateString
        }

        def getEarlierDate(date: java.util.Date, num: Int): java.util.Date ={
            val calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(Calendar.DATE, num);
            return calendar.getTime
        }

        def getMonthBefore(date: java.util.Date): java.util.Date ={
            val calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(Calendar.MONTH, -1);
            return calendar.getTime
        }

        
}
