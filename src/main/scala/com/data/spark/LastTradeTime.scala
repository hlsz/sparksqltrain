package com.data.spark

import com.data.utils.DateUtils

class LastTradeTime
{

  val spark = new  SparkInitConf().getSparkSession()

  def  lastTradeTime(calcuDate:Int, tableName:String, dataSourTab:String, targetCustTab:String, dateContains:Int): Unit =
  {


    val calcDateVal = DateUtils.intToDate(calcuDate)
    // addOrMinusDayToLong
    val dateContainsVal = DateUtils.dateToInt(DateUtils.addOrMinusDay(calcDateVal, -dateContains))

    spark.sql("use bigdata")
    spark.sql("create table IF NOT EXISTS bigdata." +tableName +" ( c_custno string, branch_no string, " +
      " l_date int, c_businessflag string, c_remark string, lastdate_dvalue int ) " +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      " LINES TERMINATED BY ‘\n’ collection items terminated by '-' " +
      " map keys terminated by ':' " +
      " stored as textfile ")

    val lastTradeDateDF = spark.sql("select c_custno, l_date, f_businessamount, c_businessflag, c_remark " +
      " from ( " +
      " select c_custno, l_date, c_businessflag, c_remark, f_businessamount, " +
      " row_number() over (partition by c_custno order by l_date desc ) rn " +
      " from bigdata." +dataSourTab + " t " +
      " where l_date < " + calcuDate +
      " and l_date >=  "+dateContainsVal+" ) " +
      " where rn = 1 ")

    lastTradeDateDF.createOrReplaceTempView("lastTradeDate")

    val tradeTimeLastDF =spark.sql("select c_custno, branch_no, l_date, c_businessflag, c_remark ," +
      " (case when l_date = 0 and "+dateContains+" =3 the 100" +
      "       when l_date = 0 and "+dateContains+" =6 the 200 " +
      "       when l_date = 0 and "+dateContains+" =9 the 400 " +
      "  else datediff ("+DateUtils.dateFormat(calcuDate,"yyyyMMdd")+", l_date )  and ) as lastdate_dvalue " +
      " from (select a.c_custno, a.branch_no, nvl(l_date,0) as l_date, nvl(c_businessflag, 0) as c_businessflag, " +
      "             c_remark " +
      "        from (select c_custno, branch_no from global_temp."+targetCustTab+")  a " +
      "        left  outer join lastTradeDate b on a.c_custno=b.c_custno ) ")

    tradeTimeLastDF.createOrReplaceTempView("tradeTimeLast")

    spark.sql("insert into bigdata."+tableName+" select * from tradeTimeLast")

    spark.stop()

  }

}

object LastTradeTime {

  //  new LastTradeTime().lastTradeTime()

}
