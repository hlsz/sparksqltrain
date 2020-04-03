package com.data.common

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Collections, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.joda.time.DateTime

object DataProcess {

  var assetDF: DataFrame = null

  var entrustDF: DataFrame = null

  var capitalFlowDF: DataFrame = null

  var industryDF: DataFrame = null

  var tradeDF: DataFrame = null
  var stockTradeDF: DataFrame = null
  var aggedTradeDF: DataFrame = null
  var mergedDF: DataFrame = null

  var riskDF: DataFrame = null
  var factorDF: DataFrame = null
  var mktIndIdxdDF: DataFrame = null
  var mktEqudDF: DataFrame = null
  var mktEqudDFAdj: DataFrame = null
  var holdDf: DataFrame = null
  var rawHoldDF: DataFrame = null
  var accountDF: DataFrame = null
  var limitPriceDF: DataFrame = null
  var stockIPODF: DataFrame = null
  var announcementDF: DataFrame = null
  var mktBasicIdxDF: DataFrame = null
  var indexDF: DataFrame = null
  var analystForecastDF: DataFrame = null
  var billboardDF: DataFrame = null

  var ashareHoldDF: DataFrame = null
  var ashareTradeDF: DataFrame = null
  var exchageRateDF: DataFrame = null
  var lastDay: String = null
  var firstDay: String = null
  var dayNum: Integer = null
  var accountState: DataFrame = null

  var cashDF: DataFrame = null
  var stockHoldDF: DataFrame = null
  var statTradeDF: DataFrame = null
  var rawTradeDF: DataFrame = null
  var keepNum = 3
  var octFundTrade: DataFrame = null
  var otcFundHold: DataFrame = null
  var rawOtcFundHold: DataFrame = null
  var octFundCategory: DataFrame = null
  var octFundNav: DataFrame = null

  var octFundBank: DataFrame = null
  var otcFundRating: DataFrame = null

  var conf: SparkConf = null
  var session: SQLContext = null
  var loadCustomerPath = ""
  var loadDatayesPath = ""
  var dumpPath = ""

  var themeSecRel: DataFrame = null
  var startDate: String = null
  var endDate: String = null
  var preDate: String = null
  var month1AgoDate: String = null
  var tradeDateList: util.ArrayList[String] = null

  var userAsset: DataFrame = null
  var bankmShare: DataFrame = null
  var rawBankmShare: DataFrame = null
  var bankmDeliver: DataFrame = null
  var rawBankmDeliver: DataFrame = null
  var rawSecumShare: DataFrame = null
  var secumShare: DataFrame = null
  var secumDeliver: DataFrame = null
  var fundHoldDf: DataFrame = null
  var managementInnerHoldDetail: DataFrame = null
  var managementHoldDetail: DataFrame = null
  var financeHoldAsset: DataFrame = null
  var outHoldFundDetail: DataFrame = null

  import org.apache.spark.sql.functions._

  val trimSpaceUDF = udf { s: String => s.trim }
  val joinStrings = udf { (arrayCol: Seq[String]) => arrayCol.mkString(" ") }
  val joinDoubles = udf { (arrayCol: Seq[Double]) => arrayCol.mkString(" ") }
  val joinArray = udf { (arrayCol: Seq[Any]) => arrayCol.mkString(" ") }
  val calcRate = udf { (x: Double, y: Double) => (y - x) / x }
  val stringAddWithBlank = udf { (x: String, y: String) => x + " " + y }
  val getMonth = udf { s: String => s.substring(0, 6) }
  val customerFiles = Seq("holdInfo", "cash", "capticalFlow", "traderecord", "assetPrice", "otcFund",
    "otcFundHold", "userAsset", "bankmShare", "bankmDeliver", "secumShare", "secumDeliver", "entrust")
  val noDataFile = Seq("accountInfo", "accountState", "fundAccount", "cbsStockHolder", "stockHoler",
    "optStockHolder", "secumHolder", "bankmHolder", "tgTblCustomerConfig")

  val convert2Year = udf((rate: Double, daynum: Int) => (rate - 1) * (255.0 / daynum))
  val convert2YearCompund = udf((rate: Double, daynum: Int) => Math.pow(rate, 250.0 / daynum) - 1)

  val parseOtcRankData = udf((s: String) => {
    if (s == null) {
      -1
    } else if (!s.contains("/")) {
      0
    } else {
      val a: Array[String] = s.split("/")
      1 - Integer.parseInt(a(0)) / (Integer.parseInt(a(1)) * 1.0)
    }
  })


  val prop = new Properties()
  var host = "localhost"
  var port = 3306
  var database = ""

  val databaseIntelligence = "intelligence"


  def setSession(sessionInit: SQLContext): Unit = {
    session = sessionInit
  }

  def setConf(selfConf: SparkConf): Unit = {
    conf = selfConf
    dumpPath = conf.get("spark.schedule.dumpPath")
    if (conf.contains("spark.schedule.keepNum")) {
      keepNum = Integer.parseInt(conf.get("spark.schedule.keepNum"))
    }
    if (conf.contains("spark.mysql.user")) {
      host = conf.get("spark.mysql.host")
      port = Integer.parseInt(conf.get("spark.mysql.port"))
      database = conf.get("spark.mysql.database")
      prop.setProperty("user", conf.get("spark.mysql.user"))
      prop.setProperty("password", conf.get("spark.mysql.password"))
      prop.setProperty("driver", conf.get("spark.mysql.driver"))

    }
  }

  def setPath(loadCustomer: String, loadDatayes: String, dump: String): Unit = {
    loadCustomerPath = loadCustomer
    loadDatayesPath = loadDatayes
    dumpPath = dump
  }

  def readData(fileName: String): DataFrame = {
    var loadPath = conf.get("spark.data." + fileName)
    var df: DataFrame = null
    if (customerFiles.contains(fileName) || noDataFile.contains(fileName)) {
      val schemaString = conf.get("spark.schema." + fileName)

      var fileSchema = new StructType()
      for (column <- schemaString.split(" ")) {
        val items = column.split(":")
        val df = items(2) match {
          case "string" => StringType
          case "String" => StringType
          case "number" => FloatType
          case "double" => DoubleType
          case "Double" => DoubleType
          case "int" => IntegerType
          case "Date" => DateType
          case _ => NullType
        }
        val nullable = if (items(3).equalsIgnoreCase("true")) true else false
        fileSchema = fileSchema.add(items(1), df, nullable)
      }

      if (!noDataFile.contains(fileName)) {
        loadPath = buildLoadPath(loadPath)
      }

      df = session.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("delimiter", "\u0001")
        .option("quote", "\"")
        .option("nullValue", "null")
        .option("mode", "DROPMALFORMED")
        .schema(fileSchema)
        .load(loadPath)
    } else {
      df = session.read.parquet(loadPath)
    }
    df
  }

  def buildLoadPath(loadPath: String): String = {
    var begin = Integer.parseInt(startDate.substring(0, 4))
    val end = DateTime.now.getYear

    val dayOfYear = LocalDate.now.getDayOfYear
    if (dayOfYear <= 8 || dayOfYear == 365 || dayOfYear == 366 || startDate.endsWith("0101")) {
      begin = begin - 1
    }

    val resultPath = loadPath + "/{" + (begin to end).map(_.toString).mkString("*,") + "*}/"
    return resultPath
  }

  def dumpResult(fileName: String, df: DataFrame): Unit = {
    if (conf.get("spark.schedule.resultType") == "csv") {
      val partion = Integer.parseInt(conf.get("spark.schedule.resultPartion"))
      df.coalesce(partion).write.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("nullValue", "")
        .mode("overwrite")
        .save(dumpPath + fileName)
    }
    if (conf.get("spark.schedule.resultType") == " parquet") {
      df.write.mode("overwrite").parquet(dumpPath + fileName)
    }
  }


  val categoryMap = Map("T" -> "fund",
    "M" -> "fund",
    "h" -> "stock",
    "L" -> "fund",
    "U" -> "bond")

  val changeCategory = (i: String) => categoryMap.get(i) match {
    case Some(x) => x
    case None => "other"
  }
  val changeCategoryFunc = udf(changeCategory)

  def hasColumn(df: DataFrame, colName: String): Boolean = df.columns.contains(colName)

  def f[T](v: T): String = v match {
    case _: Int => "Int"
    case _: String => "String"
    case _ => "Unknown"
  }


  def loadData(session: HiveContext, mode: Int = 0, startDate: String = "20100101", endDate: String, header: String = "true",
               writeDB: Boolean): Unit = {
    this.startDate = startDate
    this.endDate = endDate


    preDate = readData("mktBasicIdx")
      .withColumnRenamed("tradeDate", "date")
      .select("ticker", "date")
      .filter(col("ticker") === "0003000")
      .select("date")
      .filter(col("date") < startDate)
      .orderBy(col("date").desc)
      .first()
      .getString(0)

    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    this.month1AgoDate = formatter.format(LocalDate.parse(endDate, formatter).minusMonths(1))

    exchageRateDF = readData("exchangeRate").filter(col("date") >= preDate && col("date") < endDate)
      .withColumnRenamed("money_type", "moneyType")
    exchageRateDF = broadcast(exchageRateDF)
    assetDF = readData("assetPrice")
      .filter(col("date") >= preDate && col("date") <= endDate)
      .repartition(col("stockID"))
      .select("stockID", "date", "asset_price", "closing_price", "exchangeCD")

    userAsset = readData("userAsset")
      .filter(col("date") >= preDate && col("date") <= endDate)
      .withColumn("debt", col("crdtAsset") + col("crdtBalance") - col("crdtNetAsset"))
      .withColumn("debt", when(col("debt") < 0, 0d).otherwise(col("debt")))
      .withColumn("totalAsset", col("debt") + col("totalNetAsset"))
      .repartition(col("userID"))
      .select("userID", "date", "totalNetAsset", "totalAsset", "debt", "optAsset", "crdtNetAsset")

    setTradeDateList()


    dayNum = tradeDateList.size()

    industryDF = readData("swIndustryInfo").select("stockID","industryName1ST")
        .repartition(col("stockID"))
    industryDF = broadcast(industryDF)

    accountState = readData("accountState").filter(col("openDate")<=endDate)
      //去除消户用户
        .filter(col("CLIENT_STATUS") =!= 3)
        .select("userID","openDate").repartition(col("userID"))
    accountDF = readData("accountInfo").join(accountState, Seq("userID")).select("userID","openDate")


  }
  def setTradeDateList():Unit = {
    val b:Array[Row] = assetDF.select("date").distinct().collect()
    val dateList = new util.ArrayList[String]()
    for (row: Row <- b) {
      val date = row.getString(0)
      dateList.add(date)
    }
    Collections.sort(dateList)
    tradeDateList = dateList
  }


  def writeToDB(df: DataFrame, tableName: String, writeDB: Boolean): Unit ={
    val partion = Integer.parseInt(conf.get("spark.schedule.resultPartion"))
    df.coalesce(partion).write.format("com.databricks.spark.csv")
      .option("header","false")
      .mode("overwrite")
      .save(dumpPath + "df" + tableName)
  }

  def appendToDB(df: DataFrame, tableName: String, writeDB: Boolean) :Unit = {
    if(!writeDB) {
      return
    }else{
      df.write.mode(SaveMode.Append).jdbc(s"jdbc:mysql://$host:$port/$databaseIntelligence?userUnicode=true&characterEncoding=UTF-8&MultiQueries=true", tableName, prop)
    }
  }

  def releaseCache(df:DataFrame):Unit ={
    if(df != null) {
      df.unpersist()
    }
  }

  def readFromMySqlTable(sql:String):DataFrame ={
    val df = session.read.format("jdbc")
      .option("url",s"jdbc:mysql://$host:$port/$databaseIntelligence?userUnicode=true&characterEncoding=UTF-8&MultiQueries=true")
      .option("dbtable",sql)
      .option("driver",prop.getProperty("driver"))
      .option("user",prop.getProperty("user"))
      .option("passwor",prop.getProperty("password"))
      .load()
    df
  }

  def readResultDataFromHDFS(path: String):DataFrame ={
    val schemaMap = new util.HashMap[String, String]()
    schemaMap.put("/df/assetdf", "C0:date:string:false C1:UserID:string:false C2:totalAsset:number:true")
    schemaMap.put("/df/stockprofitdf","C0:userID:string:false C1:date:string:false C2:dailyProfit:number:true")

    if(schemaMap.containsKey(path)){


      val schemaString = schemaMap.get(path)

      var fileSchema = new StructType()
      for(column <- schemaString.split(" ")){
        val items = column.split(":")
        val df = items(2) match {
          case "string" => StringType
          case "String" => StringType
          case "number" => FloatType
          case "double" => DoubleType
          case "Double" => DoubleType
          case "int" => IntegerType
          case "Date" => DateType
          case _ => NullType
        }
        val nullable = if(items(3).equalsIgnoreCase("true")) true else false
        fileSchema = fileSchema.add(items(1), df, nullable)
      }

      val df:DataFrame = session.read.format("com.databricks.spark.csv")
        .option("header","true")
        .option("delimiter",",")
        .option("quote","\"")
        .option("nullValue","null")
        .option("mode","DROPMALFORMED")
        .schema(fileSchema)
        .load(dumpPath + path)

      df
    } else {
      null
    }
  }



}



