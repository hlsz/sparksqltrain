/**
  * 通联数据机密
  * --------------------------------------------------------------------
  * 通联数据股份公司版权所有 © 2013-2020
  *
  * 注意：本文所载所有信息均属于通联数据股份公司资产。本文所包含的知识和技术概念均属于
  * 通联数据产权，并可能由中国、美国和其他国家专利或申请中的专利所覆盖，并受商业秘密或
  * 版权法保护。
  * 除非事先获得通联数据股份公司书面许可，严禁传播文中信息或复制本材料。
  *
  * DataYes CONFIDENTIAL
  * --------------------------------------------------------------------
  * Copyright © 2013-2020 DataYes, All Rights Reserved.
  *
  * NOTICE: All information contained herein is the property of DataYes
  * Incorporated. The intellectual and technical concepts contained herein are
  * proprietary to DataYes Incorporated, and may be covered by China, U.S. and
  * Other Countries Patents, patents in process, and are protected by trade
  * secret or copyright law.
  * Dissemination of this information or reproduction of this material is
  * strictly forbidden unless prior written permission is obtained from DataYes.
  */

package com.data.spark.sparksql.bdb.util

/**
 * Created by liang.zhao on 2016/12/28.
 */

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Collections, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext, SaveMode}
import org.joda.time.DateTime


import scala.math.pow

object DataProcessor {
  //
  var assetDF: DataFrame = null
  //委托流水
  var entrustDF: DataFrame = null
  //资金流水
  var capitalFlowDF: DataFrame = null
  //行业分类
  var industryDF: DataFrame = null

  //交易流水
  var tradeDF: DataFrame = null

  var stockTradeDF : DataFrame = null
  var aggedTradeDF: DataFrame = null
  var mergedDF: DataFrame = null

  //barra因子
  var riskDF: DataFrame = null
  //通联因子
  var factorDF: DataFrame = null
  //指数日行情
  var mktIndIdxdDF: DataFrame = null
  //股票日行情
  var mktEqudDF: DataFrame = null
  //股票日行情复权
  var mktEqudDFAdj: DataFrame = null
  //持仓数据
  var holdDF: DataFrame = null
  var rawHoldDF: DataFrame = null
  //账户信息
  var accountDF: DataFrame = null
  //涨跌停价
  var limitPriceDF: DataFrame = null
  //股票上市日期
  var stockIPODF: DataFrame = null
  //公告分类
  var announcementDF: DataFrame = null
  //基准沪深300
  var mktBasicIdxDF: DataFrame = null
  //申万行业指数
  var indexDF: DataFrame = null
  //分析师
  var analystForecastDF: DataFrame = null
  //龙虎榜
  var billboardDF: DataFrame = null

  //A股股票持仓数据
  var ashareHoldDF: DataFrame = null
  //A股股票交易数据
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
  var otcFundTrade: DataFrame = null
  var otcFundHold: DataFrame = null
  var rawOtcFundHold: DataFrame = null
  var otcFundCategory: DataFrame = null

  var otcFundNav: DataFrame = null


  var otcFundRank: DataFrame = null
  var otcFundRating: DataFrame = null
  var conf: SparkConf = null
  var session: SQLContext = null
  var loadCustomerPath = ""
  var loadDatayesPath = ""
  var dumpPath = ""

  var themeSecRel:DataFrame = null

//  var stockLabels:DataFrame = null

  var startDate : String = null
  var endDate : String = null
  var preDate : String = null

  var month1AgoDate : String = null
  var tradeDateList : util.ArrayList[String] = null

  var userAsset : DataFrame = null
  var bankmShare : DataFrame = null
  var rawBankmShare : DataFrame = null
  var bankmDeliver : DataFrame = null
  var rawBankmDeliver : DataFrame = null
  var rawSecumShare : DataFrame = null
  var secumShare : DataFrame = null
  var secumDeliver : DataFrame = null
  var fundHoldDf : DataFrame = null

  var managementInnerHoldDetail : DataFrame = null
  var managementHoldDetail : DataFrame = null
  var financeHoldAsset : DataFrame = null

  var outHoldFundDetail : DataFrame = null

  val trimSpaceUDF = udf { s: String => s.trim }
  val joinStrings = udf((arrayCol: Seq[String]) => arrayCol.mkString(" "))
  val joinDoubles = udf((arrayCol: Seq[Double]) => arrayCol.mkString(" "))
  val joinArray = udf((arrayCol: Seq[Any]) => arrayCol.mkString(" "))
  val calcRate = udf((x: Double, y: Double) => (y - x) / x)
  val stringAddWithBlank = udf((x: String, y: String) => x + " " + y)
  val getMonth = udf{s: String => s.substring(0,6)}
  val customerFiles = Seq("holdInfo", "cash", "capitalFlow", "tradeRecord",
    "assetPrice", "otcFund", "otcFundHold", "userAsset", "bankmShare", "bankmDeliver",
    "secumShare", "secumDeliver","entrust")
  val noDataFile = Seq("accountInfo", "accountState","fundAccount","cbsStockHolder",
    "stockHolder","optStockHolder","secumHolder","bankmHolder","tgTblCustomerConfig")

  val convert2Year = udf((rate: Double, daynum: Int) => (rate - 1) * (250.0 / daynum))
  val convert2YearCompund = udf((rate: Double, daynum: Int) => Math.pow(rate, 250.0 / daynum) - 1)


  val parseOtcRankData = udf((s: String) => {
    if (s == null) {
      -1
    } else if (!s.contains("/")) {
      0
    } else {
      val a : Array[String] = s.split("/")
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
    if(conf.contains("spark.schedule.keepNum")) {
      keepNum = Integer.parseInt(conf.get("spark.schedule.keepNum"))
    }
    if(conf.contains("spark.mysql.user")) {
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
    var df:DataFrame = null
    if(customerFiles.contains(fileName) || noDataFile.contains(fileName)) {
      val schemaString = conf.get("spark.schema."+fileName)

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
        val nullable = if(items(3).equalsIgnoreCase("true")) true else false
        fileSchema = fileSchema.add(items(1), df, nullable)
      }

      if (!noDataFile.contains(fileName)) {
        // 认为cliennt和clientinfo两张表不需要处理成日期格式的,直接根据path读取就可以
        loadPath = buildLoadPath(loadPath)
      }

      df = session.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("delimiter","\u0001")

        .option("quote","\"")
        .option("nullValue","null")
        .option("mode", "DROPMALFORMED")
        .schema(fileSchema)
        .load(loadPath)
    }else {
      df = session.read.parquet(loadPath)
    }
    df
  }


  def buildLoadPath (loadPath : String):String = {
    var begin = Integer.parseInt(startDate.substring(0, 4))
    val end = DateTime.now.getYear

    val dayOfYear = LocalDate.now().getDayOfYear
    if (dayOfYear <= 8 || dayOfYear == 365 || dayOfYear == 366 || startDate.endsWith("0101")) {
      // 因为收益率计算要向前推一天取数据，这些情况下担心前面那一天的数据没有读入，所以把begin向前推
      begin = begin - 1
    }

    val resultPath = loadPath + "/{" + (begin to end).map(_.toString).mkString("*,") + "*}/"
    return resultPath
  }

  def dumpResult(fileName: String, df: DataFrame): Unit = {
    if(conf.get("spark.schedule.resultType")=="csv") {
      val partion = Integer.parseInt(conf.get("spark.schedule.resultPartion"))
      df.coalesce(partion).write.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("nullValue", "")
        .mode("overwrite")
        .save(dumpPath + fileName)
    }
    if(conf.get("spark.schedule.resultType")=="parquet") {
      df.write.mode("overwrite").parquet(dumpPath + fileName)
    }
  }


  // 9,g,Z都算成国债逆回购
  val categoryMap = Map("T" -> "fund", "M" -> "fund", "h" -> "stock", "L" -> "fund", "U" -> "bond", "u" -> "bond",
  "c" -> "stock", "l" -> "fund", "Z" -> "repurchase", "A" -> "fund", "K" -> "fund", "6" -> "fund",
  "z" -> "stock", "4" -> "stockApplication", "0" -> "stock", "9" -> "repurchase", "j" -> "fund", "3" -> "warrant",
  "5" -> "stockApplication", "7" -> "stockApplication", "D" -> "warrant", "Y"->"bond", "S"->"fund","I"->"bond",
  "H" -> "bondApplication", "G" -> "bondApplication", "X" -> "bond", "a" -> "bond", "g"->"repurchase", "1"->"fund",
    "b" -> "fund", "m"->"manageProd")

  val changeCategory = (i: String) => categoryMap.get(i) match {
    case Some(x) => x
    case None => "other"
  }
  val changeCategoryFunc = udf(changeCategory)

  def hasColumn(df: DataFrame, colName: String) = df.columns.contains(colName)

  def f[T](v: T) = v match {
    case _: Int => "Int"
    case _: String => "String"
    case _ => "Unknown"
  }

  def loadData(session: HiveContext, mode: Int = 0, startDate: String = "20100101", endDate: String, header: String = "true", writeDB: Boolean): Unit = {
    import session.implicits._
    this.startDate = startDate
    this.endDate = endDate


    preDate = readData("mktBasicIdx")
      .withColumnRenamed("tradeDate","date")
      .select("ticker","date")
      .filter(col("ticker") === "000300")
      .select("date")
      .filter(col("date") < startDate)
      .orderBy(col("date").desc)
      .first()
      .getString(0)


    val formatter : DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    this.month1AgoDate = formatter.format(LocalDate.parse(endDate, formatter).minusMonths(1))


    exchageRateDF = readData("exchangeRate").filter(col("date")>=preDate && col("date")<=endDate)
      .withColumnRenamed("money_type","moneyType")
    exchageRateDF = broadcast(exchageRateDF)
    assetDF = readData("assetPrice")
      .filter(col("date")>=preDate && col("date")<=endDate)
      .repartition(col("stockID"))
      .select("stockID","date","asset_price","closing_price","exchangeCD")


    userAsset = readData("userAsset")
      .filter(col("date")>=preDate && col("date")<=endDate)
      .withColumn("debt", col("crdtAsset") + col("crdtBalance") - col("crdtNetAsset"))
      .withColumn("debt", when(col("debt") < 0, 0d).otherwise(col("debt")))
      .withColumn("totalAsset", col("debt") + col("totalNetAsset"))
      .repartition(col("userID"))
      .select("userID", "date", "totalNetAsset", "totalAsset", "debt", "optAsset", "crdtNetAsset")

    // 根据价格信息设定交易日信息
    setTradeDateList()

    dayNum = tradeDateList.size()

    //行业分类
    industryDF = readData("swIndustryInfo").select("stockID","industryName1ST")
      .repartition(col("stockID"))
    industryDF = broadcast(industryDF)

    accountState = readData("accountState").filter(col("openDate")<=endDate)
      // 干掉已经销户的
      .filter(col("CLIENT_STATUS") !== "3")
      .select("userID","openDate").repartition(col("userID"))
    accountDF = readData("accountInfo").join(accountState, Seq("userID")).select("userID","openDate")


    // 交易流水原始数据
    rawTradeDF = readData("tradeRecord")
      .filter(col("date")>=preDate && col("date")<=endDate)
      .repartition(col("userID"), col("date"))
      .join(accountState,Seq("userID"))
      .withColumn("stockID", when(col("stockId") === " ","00000").otherwise(col("stockId")))



    // 这里是为了处理深市的红股入账问题，问题是红股先入账，后除权
    val bonusStock = rawTradeDF
      .filter(col("business_flag") === 4015 && col("exchangeCD") === "2" && (col("stockType") === "0" || col("stockType") === "c"))
      .select("userID","date","fund_account","stockID","num")
      .groupBy("userID","date","fund_account","stockID")
      .agg(sum("num").alias("bonusStockNum"))



    //持仓数据
    //TODO
    rawHoldDF = readData("holdInfo")
      .filter(col("date")>=preDate && col("date")<=endDate)
      .repartition(col("userID"))
      .join(accountState,Seq("userID"))
      .filter(col("stockType") !== "4")
      .join(assetDF, Seq("stockID","exchangeCD","date"), "left")
      .withColumn("category",changeCategoryFunc(col("stockType")))
      .withColumn("last_price",when((col("category")==="stock") && (col("closing_price")!==0),col("closing_price")).otherwise(col("asset_price")))
      .withColumn("last_price",when(col("last_price").isNull || (col("last_price")===0),col("cost_price")).otherwise(col("last_price")))
      .filter(col("ASSET_PROP") !== "7")

    holdDF = rawHoldDF
      .join(bonusStock, Seq("userID","stockId", "fund_account","date"),"left")
      .withColumn("bonusStockNum",when(col("bonusStockNum").isNull,0).otherwise(col("bonusStockNum")))
      .withColumn("holdNum",when(col("closing_price") !== 0,col("holdNum") - col("bonusStockNum")).otherwise(col("holdNum")))
      .withColumn("mktVal",when(col("last_price").isNull || col("last_price")===0.0 , col("holdNum")*col("cost_price")).otherwise(col("holdNum")*col("last_price")))
      .withColumn("costVal",col("holdNum")*col("cost_price"))
      .select("userID","fund_account","date","stockID","holdNum","mktVal","last_price","cost_price","costVal","moneyType","category","exchangeCD","correct_amount","stockType","ASSET_PROP")
      .join(exchageRateDF,Seq("date","moneyType"),"left").na.fill(1)
      .withColumn("mktVal",col("mktVal")*col("bill_rate"))
      .withColumn("stockMktVal",when(col("category")==="stock",col("mktVal")).otherwise(0))
      .withColumn("innerFundMktVal", when(col("category")==="fund",col("mktVal")).otherwise(0))
      .withColumn("bondMktVal", when(col("category")==="bond",col("mktVal")).otherwise(0))

      // 逆回购统计,第二句是为了通过后面对holdnum>0的判定
      .withColumn("repurchaseMktVal", when(col("category")==="repurchase",col("correct_amount") * 100).otherwise(0))
      // 逆回购的市值
      .withColumn("mktVal", when(col("category")==="repurchase",col("repurchaseMktVal")).otherwise(col("mktVal")))
      .withColumn("holdNum", when(col("category")==="repurchase",col("correct_amount")).otherwise(col("holdNum")))
      .repartition(col("userID"))
      .groupBy("userID","date","fund_account","stockID","category","exchangeCD","stockType","ASSET_PROP")
      .agg(sum("holdNum").alias("holdNum"),round(sum("mktVal"),2).alias("mktVal"),sum("stockMktVal").alias("stockMktVal"),
        sum("innerFundMktVal").alias("innerFundMktVal"),sum("bondMktVal").alias("bondMktVal"),
        sum("repurchaseMktVal").alias("repoAsset"),
        round(mean("last_price"),2).alias("last_price"), round(sum("costVal"),2).alias("costVal"))
      .withColumn("cost_price", round(col("costVal")/col("holdNum"),2))
      .cache()


    lastDay = holdDF.select(max("date")).first().getString(0)
    firstDay = holdDF.select(min("date")).first().getString(0)

    System.out.println("finish two first")


    val existsHoldDate :DataFrame = broadcast(readFromMysqlTable("(select distinct date from holddf) as t"))
      .select("date").distinct().withColumn("flag",lit(1))
      .repartition(col("date"))




    appendToDB(holdDF.filter($"holdNum" >0)
        .join(existsHoldDate, Seq("date"), "left")
      .withColumn("flag", when(col("flag") === 1, 1).otherwise(0))
      .filter(col("flag") === 0)
      .drop("flag")
      .select("userID","date","stockID","category","holdNum","mktVal","last_price","cost_price"), "holddf",writeDB)

    //股票持仓
    stockHoldDF = holdDF.filter(col("category")==="stock")
    //资金流水
    capitalFlowDF = readData("capitalFlow")
      .join(accountState,Seq("userID"))
      .filter(col("date")>=preDate && col("date")<=endDate)
      .join(exchageRateDF,Seq("date","moneyType"),"left").na.fill(1)
      .withColumn("occur_balance",round(col("occur_balance")*col("bill_rate"),2))
      .withColumn("post_balance",round(col("post_balance")*col("bill_rate"),2))
      .repartition(col("userID"))
      .filter(col("ASSET_PROP") !== "7")

    val existsCapitalDate :DataFrame = readFromMysqlTable("(select distinct date from capitalflowdf) as t")
      .select("date").distinct().withColumn("flag",lit(1))
      .repartition(col("date"))



    appendToDB(capitalFlowDF
      .join(existsCapitalDate, Seq("date"), "left")
      .withColumn("flag", when(col("flag") === 1, 1).otherwise(0))
      .filter(col("flag") === 0)
      .drop("flag")
      .select("userID","date","moneyType","EXCHANGE_TYPE","occur_balance","post_balance","business_flag"), "capitalflowdf",writeDB)

    //现金 这里没有用correct_balance,因为兴业的数据不全好像是，是通过两个日期来替换的这个correctBalance
    cashDF = readData("cash")
      .filter(col("date")>=preDate && col("date")<=endDate)
      // 过滤掉两融
      .filter(col("ASSET_PROP") !== "7")

      .select("userID","date","FUND_ACCOUNT","moneyType","current_balance","CORRECT_BALANCE").distinct()
      .join(exchageRateDF,Seq("date","moneyType"),"left").na.fill(1)
      .withColumn("current_balance",col("current_balance")*col("bill_rate"))
      .repartition(col("userID"))
      .join(accountState,Seq("userID"))
      .groupBy("userID","date").agg(sum("current_balance").alias("current_balance"))


    tradeDF = rawTradeDF
      .filter(col("asset_prop") !== "7")

      .repartition(col("userID"))
      .withColumn("commission",round(col("fare0")+col("fare1")+col("fare2")+col("fare3")+col("farex"),2))
      .withColumn("tradeVal", round(when(col("tradeWay")==="2",lit(-1)*col("tradeVal")).otherwise(col("tradeVal")),2))
      .select("userID","fund_account","date","business_flag","stockID","tradeWay","stockType","num","price","tradeVal","fare0","fare1", "fare2", "post_amount","tradeTime","business_times","commission","exchangeCD","clear_balance")
      .where((col("business_flag")===3001) || (col("business_flag")===3002) || (col("business_flag")===4001) ||
        (col("business_flag")===4002) || (col("business_flag")===4004) || (col("business_flag")===4005) ||
        (col("business_flag")===4105) || (col("business_flag")===4104) ||  (col("business_flag")===4151) ||
        (col("business_flag")===4152) || (col("business_flag")===4702) || (col("business_flag")===4703) ||
        (col("business_flag")===4701) || (col("business_flag")===4704) || (col("business_flag")===4705) ||
        (col("business_flag")===4706) ||  (col("business_flag")===4721) ||  (col("business_flag")===4722) ||
        (col("business_flag")===4014) || (col("business_flag")===4015) ||
        (col("business_flag")===4071) ||
        (col("business_flag")===4016) || (col("business_flag")===4018) ||  (col("business_flag")===4034)||  (col("business_flag")===4020)
        // 44150是证券理财的清盘，直接返回银行卡
        ||  (col("business_flag")===44150)
        // 货币基金的申购和赎回,在场内
        ||  (col("business_flag")===4175) || (col("business_flag")===4176)
      )
      .withColumn("category",changeCategoryFunc(col("stockType")))
      .cache()




    val existsTradeDate :DataFrame = readFromMysqlTable("(select distinct date from tradedf) as t")
      .select("date").distinct().withColumn("flag",lit(1))
      .repartition(col("date"))

    appendToDB(tradeDF
      .join(existsTradeDate, Seq("date"), "left")
      .withColumn("flag", when(col("flag") === 1, 1).otherwise(0))
      .filter(col("flag") === 0)
      .drop("flag")
      .select("userID","date","business_flag","stockID","tradeWay","stockType",
      "num","price","tradeVal","fare0","fare1","fare2","post_amount","tradeTime","commission",
       "category"), "tradedf",writeDB)



    stockTradeDF = tradeDF
      .filter(col("category") === "stock")
      .filter((col("business_flag")===4001) ||
      (col("business_flag")===4002) || (col("business_flag")===4004) || (col("business_flag")===4005) ||
      (col("business_flag")===4105) || (col("business_flag")===4104) ||  (col("business_flag")===4151) ||
      (col("business_flag")===4152) || (col("business_flag")===4702) || (col("business_flag")===4703) ||
      (col("business_flag")===4701) || (col("business_flag")===4704) || (col("business_flag")===4705) ||
      (col("business_flag")===4706) || (col("business_flag")===4071))


    aggedTradeDF = tradeDF
      .filter(col("category")==="stock")
      // 这里是对普通帐户的担保平划入和划出做特殊处理，赋予tradeVal值，这样在ability里面计算个股收益的时候，不会因为担保品的划入和划出出问题
      .withColumn("tradeVal",when(col("tradeVal") === 0 && ((col("business_flag")===4721) ||  (col("business_flag")===4722)), col("price") * col("num")).otherwise(col("tradeVal")))
      .groupBy("userID", "fund_account", "stockID", "date")
      .agg(sum("tradeVal").alias("tradeVal"), sum("num").alias("num"))
      .withColumn("price",round(abs(col("tradeVal")/col("num")),2))


    ashareTradeDF = rawTradeDF
      // 过滤掉两融
      .filter(col("asset_prop") !== "7")
      .filter((col("stockType") === "0")
      && ((col("exchangeCD") === "1") || (col("exchangeCD") === "2"))).cache()

    mergedDF = aggedTradeDF.join(stockHoldDF,Seq("userID","fund_account", "date","stockID"),"outer").na.fill(0).cache()


    //风险因子
    riskDF = readData("bFactor").filter(col("date")>=startDate && col("date")<=endDate)
    //通联因子
    factorDF = readData("dFactor")
      .filter(col("date")>=startDate && col("date")<=endDate)
    //申万行业指数日行情
    mktIndIdxdDF = readData("mktIdxd")
      .filter(col("date")>=startDate && col("date")<=endDate)
      .repartition(col("date"))

    //股票日行情
    mktEqudDF = readData("mktEqud")
      .drop("ID")
      .withColumnRenamed("tickerSymbol","stockID")
      .filter(col("tradeDate")>=startDate && col("tradeDate")<=endDate)
      .withColumnRenamed("tradeDate","date")
      .withColumn("rate", calcRate(col("preClosePrice"), col("closePrice"))).drop("preClosePrice")
      .withColumn("vwap", round(col("turnoverValue")/col("turnoverVol"),2))
      .repartition(col("date"),col("stockID"))

    mktEqudDFAdj = readData("mktEqudAdj")
      .drop("ID")
      .filter(col("tradeDate")>=startDate && col("tradeDate")<=endDate)
      .withColumnRenamed("tradeDate","date")
      .withColumnRenamed("tickerSymbol","stockID")
      .repartition(col("date"),col("stockID"))
      .select("date","stockID","closePrice")

    //基准沪深300
    mktBasicIdxDF = readData("mktBasicIdx")
      .filter(col("tradeDate")>=startDate && col("tradeDate")<=endDate)
      .withColumnRenamed("tradeDate","date")
      .withColumn("chgPCT", calcRate(col("preCloseIndex"), col("closeIndex"))).drop("preCloseIndex")
    mktBasicIdxDF = broadcast(mktBasicIdxDF)

    //涨跌停价
    limitPriceDF = readData("limitPrice").drop("ID")
      .filter(col("date")>=startDate && col("date")<=endDate)
    //股票上市日期
    stockIPODF = readData("stockIPO")
      .filter(col("listDate")>=startDate && col("listDate")<=endDate)
    //公告分类
    announcementDF = readData("announcement")
      .filter(col("publishDate")>=startDate && col("publishDate")<=endDate)
    //龙虎榜
    billboardDF = readData("mktRankListStocks")
      .filter(col("tradeDate")>=startDate && col("tradeDate")<=endDate)
      .withColumn("stockID", trimSpaceUDF(col("stockID")))
    //分析师
    analystForecastDF = readData("analystForecast")
      .filter(col("intoDate")>=startDate && col("intoDate")<=endDate)
      .withColumn("stockID", trimSpaceUDF(col("stockID")))



    otcFundTrade = readData("otcFund").filter(col("init_date")>=preDate && col("init_date")<=endDate)
      .repartition(col("userID"))

    val octFundShare = otcFundTrade.filter(col("business_flag")===143)
      .select("userID","init_date","fund_account","stockID","shares")
      .withColumnRenamed("init_date","date")
      .withColumnRenamed("shares","dividendShare")
      .groupBy("userID","date","fund_account","stockID").agg(sum(col("dividendShare")).alias("dividendShare"))
      .select("userID","date","fund_account","stockID","dividendShare")

    otcFundCategory = readData("otcFundCategory").withColumnRenamed("secId","securityId")


    otcFundNav = readData("otcFundNav")
      .filter(col("endDate")>=preDate && col("endDate")<=endDate)
      .join(otcFundCategory.select("securityId","stockID"),Seq("securityId"),"left")
      .withColumnRenamed("endDate","date").withColumn("nav", col("nav").cast(FloatType))
      .repartition(col("stockID"))
      .select("stockID","date","nav")

    rawOtcFundHold = readData("otcFundHold")
      .filter(col("date")>=preDate && col("date")<=endDate)
      .repartition(col("userID"))

    otcFundHold = rawOtcFundHold
      .join(octFundShare,Seq("userID","date","fund_account","stockID"),"left")
      .withColumn("dividendShare", when(col("dividendShare").isNull,0).otherwise(col("dividendShare")))
      .withColumn("dividendLead", lead("dividendShare",1).over(Window.partitionBy("userID","fund_account","stockID").orderBy("date")))

     //
      .withColumn("current_share",when(col("dividendLead") !== 0, col("current_share") + col("dividendLead")).otherwise(col("current_share")))
      .join(otcFundNav, Seq("stockID", "date"), "left").na.fill(1)
      .withColumn("category",changeCategoryFunc(col("stock_type")))
      // 这里单独处理fund吧，求影响范围最小
      .withColumn("current_share", round(when(col("category") === "fund",col("current_share") - col("back_share")).otherwise(col("current_share")),3))
      .withColumn("nav", round(col("nav"),3))
      // market_value直接用原始数据
      .select("stockID","date","userID","category","stock_type","buy_date","moneyType","begin_share","current_share","market_value","nav")
      .cache()

    if (!FileOperator.checkExist("df/otcfundholddf/_SUCCESS")) {
      writeToDB(otcFundHold.filter(col("date") >= startDate && col("date") <= endDate)
          .filter(col("current_share") > 0)
        , "otcfundholddf", writeDB)
    }


    otcFundRank = readData("otcFundRank")
      .withColumnRenamed("SECURITY_ID","securityId")
      .withColumnRenamed("END_DATE","endDate")
      .filter(col("endDate")>=startDate && col("endDate")<=endDate)
      .withColumnRenamed("RANK_TYPE","rankType")
      // 几个重要参数是string类型的，"47/210"这种的，需要手动转化下
      .withColumn("rankReturnRate1M",parseOtcRankData(col("RANK_RETURN_RATE_1M")))
      .withColumn("rankReturnRate3M",parseOtcRankData(col("RANK_RETURN_RATE_3M")))
      .withColumn("rankReturnRate6M",parseOtcRankData(col("RANK_RETURN_RATE_6M")))
      .select("securityId","endDate","rankType","rankReturnRate1M","rankReturnRate3M","rankReturnRate6M")

      .join(otcFundCategory.select("securityId","stockID"),Seq("securityId"),"left")
      .withColumnRenamed("endDate","date")


    otcFundRating = readData("otcFundRating").join(otcFundCategory.select("securityId","stockID"),Seq("securityId"),"left").filter(col("period")===3)
        .withColumn("month",getMonth(col("endDate")))
        .repartition(col("stockID"))

    themeSecRel = readData("themes").select("theme_id","theme_name","base_date")
      .join(readData("themeSecRel").select("theme_id","ticker_symbol","score"),Seq("theme_id"),"right")
      .withColumnRenamed("ticker_symbol", "stockID")
      .select("theme_id","theme_name","base_date","stockID","score")





    rawBankmShare = readData("bankmShare")
      .filter(col("date")>=preDate && col("date")<=endDate)
      .repartition(col("userId"))
      .withColumnRenamed("money_type", "moneyType")
      .join(exchageRateDF,Seq("date","moneyType"),"left").na.fill(1)
      .withColumn("begin_amount",col("begin_amount")*col("bill_rate"))
      .select("date","userId","begin_amount","prod_code")
    bankmShare = rawBankmShare
      .groupBy("userId", "date").agg(sum("begin_amount").alias("begin_amount"))

    rawBankmDeliver = readData("bankmDeliver")
      .filter(col("date")>=preDate && col("date")<=endDate)
      .repartition(col("userId"))
      .select("date","business_flag","userId","prod_code","money_type","entrust_balance", "prod_name")

    bankmDeliver = rawBankmDeliver
    .withColumnRenamed("money_type", "moneyType")
      .join(exchageRateDF,Seq("date","moneyType"),"left").na.fill(1)
      .withColumn("entrust_balance",col("entrust_balance")*col("bill_rate"))
      .select("date","business_flag","userId","prod_code","entrust_balance")


    rawSecumShare = readData("secumShare")
      .filter(col("date")>=preDate && col("date")<=endDate)
      .repartition(col("userID"))
      .withColumnRenamed("MONEY_TYPE", "moneyType")
      .join(exchageRateDF,Seq("date","moneyType"),"left").na.fill(1)
      .withColumnRenamed("SECUM_MARKET_VALUE", "marketValue")
      .select("date","FUND_ACCOUNT","userID","CURRENT_AMOUNT","marketValue","PROD_COST_PRICE","PROD_CODE","PRODTA_NO")

    secumShare = rawSecumShare.groupBy("userID", "date").agg(sum("marketValue").alias("marketValue"))
      .select("userID","date","marketValue")

    secumDeliver = readData("secumDeliver")
      .filter(col("date")>=preDate && col("date")<=endDate)
      .repartition(col("userID"))
      .withColumnRenamed("MONEY_TYPE", "moneyType")
      .join(exchageRateDF,Seq("date","moneyType"),"left").na.fill(1)
      .withColumn("entrust_balance",col("ENTRUST_BALANCE")*col("bill_rate"))
      .select("userID","date","BUSINESS_FLAG","PROD_CODE","moneyType","entrust_balance","PRODTA_NO","PROD_NAME")




    val inHoldFund = holdDF.filter(col("category")==="fund")
      .withColumnRenamed("innerFundMktVal", "market_value")
      .withColumnRenamed("holdNum", "share")
      .select("userId","date","stockID","market_value","stockType","share")
    // 暂时把场外基金当作公募基金
     outHoldFundDetail = otcFundHold
      .filter(col("category") === "fund")
      .filter(!col("stockID").startsWith("BB"))
      .filter(col("market_value") > 0)
      .withColumnRenamed("stock_type", "stockType")
      .withColumnRenamed("current_share","share")
      .select("userID","date","stockID","market_value","stockType","share")
    fundHoldDf = inHoldFund.unionAll(outHoldFundDetail)


    managementInnerHoldDetail = DataProcessor.otcFundHold
      .filter(col("stockID").startsWith("BB"))
      .withColumn("market_value", col("current_share") * col("nav"))
      .filter(col("market_value") > 0)
      .withColumnRenamed("stock_type", "stockType")
      .select("userID","date","stockID","market_value","stockType")


    managementHoldDetail = rawSecumShare.filter(col("PROD_CODE").startsWith("BB"))
      .withColumnRenamed("PROD_CODE","stockID")
      .withColumnRenamed("marketValue","market_value")
      .select("userID","date","stockID","market_value")
      .unionAll(managementInnerHoldDetail.select("userID","date","stockID","market_value"))


    financeHoldAsset = DataProcessor.secumShare
      .select("userID","date","marketValue")
      .withColumnRenamed("marketValue","totalNetAsset")
      .unionAll(DataProcessor.bankmShare
        .select("userID","date","begin_amount")
        .withColumnRenamed("begin_amount", "totalNetAsset"))
      .unionAll(managementInnerHoldDetail
        .groupBy("userID", "date")
        .agg(sum(col("market_value")).alias("totalNetAsset"))
        .select("userID","date","totalNetAsset"))
      .groupBy("userID","date")
      .agg(sum("totalNetAsset").alias("totalNetAsset"))

    {
      // 写产品持有情况df
      var productdf = rawHoldDF.select("userID", "stockId", "category").distinct()

      productdf = productdf
        .unionAll(rawOtcFundHold.select("userID", "stockID", "stock_type").distinct()
          .withColumn("category", changeCategoryFunc(col("stock_type")))
          .withColumn("category", when(col("stockID").startsWith("BB"), "manageProd").otherwise(col("category")))
          .select("userID", "stockID", "category"))


      val manageInSecumShare = rawSecumShare.filter(col("PROD_CODE").startsWith("BB"))
        .select("userID","PROD_CODE")
        .withColumnRenamed("PROD_CODE","productId")
        .withColumnRenamed("userID","customerNo")
        .withColumn("productType" ,lit(3))
        .select("customerNo", "productId", "productType")

      val receipt = rawSecumShare.filter(col("PRODTA_NO") === "CZZ")
        .select("userID","PROD_CODE")
        .withColumnRenamed("PROD_CODE","productId")
        .withColumnRenamed("userID","customerNo")
        .distinct()
        .select("customerNo", "productId")
        .withColumn("productType" ,lit(4))
        .select("customerNo", "productId", "productType")

      productdf = productdf.filter(col("category") === "stock" || col("category") === "fund" || col("category") === "manageProd")
        .withColumn("productType", when(col("category") === "stock", 1).otherwise(0))
        .withColumn("productType", when(col("category") === "fund", 2).otherwise(col("productType")))
        .withColumn("productType", when(col("category") === "manageProd", 3).otherwise(col("productType")))
        .withColumnRenamed("stockId", "productId")
        .withColumnRenamed("userId", "customerNo")
        .select("customerNo", "productId", "productType")
        .unionAll(receipt)
        .unionAll(manageInSecumShare)

      if(!FileOperator.checkExist("df/productdf/_SUCCESS")) {
        writeToDB(productdf, "productdf", writeDB)
      }

    }

  }


  /**
    * 得到所有交易日日期
    */
  def setTradeDateList() = {
    val b : Array[Row] = assetDF.select("date").distinct().collect()
    val dateList = new util.ArrayList[String]()
    for (row : Row <- b) {
      val date = row.getString(0)
      dateList.add(date)
    }
    Collections.sort(dateList)
    tradeDateList = dateList
  }


  def writeToDB(df: DataFrame, tableName: String, writeDB: Boolean): Unit ={
    val partion = Integer.parseInt(conf.get("spark.schedule.resultPartion"))
    df.coalesce(partion).write.format("com.databricks.spark.csv")
      .option("header", "false")
      .mode("overwrite")
      .save(dumpPath + "df/" + tableName)
  }

  def appendToDB(df: DataFrame, tableName: String, writeDB: Boolean): Unit ={
////  刷客户历史数据需要用到的代码
//    val partion = 10
//    df.coalesce(partion).write.format("com.databricks.spark.csv")
//      .option("header", "false")
//      .mode("overwrite")
//      .save(dumpPath + "df/" + tableName)
    if (!writeDB) {
      return
    }
    df.write.mode(SaveMode.Append).jdbc(s"jdbc:mysql://$host:$port/$databaseIntelligence?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true", tableName, prop)
  }


  def releaseCache(df: DataFrame): Unit ={
   if (df != null) {
     df.unpersist()
   }
  }

  def readFromMysqlTable(sql: String): DataFrame ={
    val df = session.read.format("jdbc")
      .option ("url", s"jdbc:mysql://$host:$port/$databaseIntelligence?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true")
      .option ("dbtable", sql)
      .option ("driver", prop.getProperty("driver"))
      .option ("user", prop.getProperty("user"))
      .option ("password", prop.getProperty("password"))
      .load()
    df
  }





  def readResultDataFromHDFS(path: String): DataFrame ={

    val schemaMap = new util.HashMap[String,String]()
    schemaMap.put("/df/assetdf", "C0:date:string:false C1:userID:string:false C2:totalAsset:number:true C3:totalNetAsset:number:true C4:rate:number:true C5:dailyProfit:number:true C6:currentBalance:number:true C7:position:number:true")
    schemaMap.put("/df/stockprofitdf", "C0:userID:string:false C1:date:string:false C2:dailyProfit:number:true C3:mktVal:number:true")

    if (schemaMap.containsKey(path)) {

      val schemaString = schemaMap.get(path)

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
        val nullable = if(items(3).equalsIgnoreCase("true")) true else false
        fileSchema = fileSchema.add(items(1), df, nullable)
      }
      val df:DataFrame = session.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter",",")
        .option("quote","\"")
        .option("nullValue","null")
        .option("mode", "DROPMALFORMED")
        .schema(fileSchema)
        .load(dumpPath + path)

      df
    } else {
      null
    }
  }

}