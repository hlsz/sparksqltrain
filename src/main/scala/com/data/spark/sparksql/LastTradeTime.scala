package com.data.spark.sparksql

import com.data.utils.DateUtils

class LastTradeTime
{

  val spark = new  SparkInitConf().getSparkSession("LastTradeTime","yarn-client")

  def  lastTradeTime(calcuDate:Int, tableName:String, dataSourTab:String, targetCustTab:String, dateContains:Int): Unit =
  {


    val calcDateVal = DateUtils.intToDate(calcuDate)
    // addOrMinusDayToLong
    val dateContainsVal = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -dateContains))
    println("dateContainsVal:"+dateContainsVal)

    spark.sql("use bigdata")

    spark.sql("drop table if exists bigdata."+tableName)
    spark.sql("create table IF NOT EXISTS bigdata." +tableName +
      " ( c_custno string, branch_no string, " +
      " l_date int, c_businessflag string,  lastdate_dvalue int ) " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"} " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")

    val lastTradeDateDF = spark.sql("select c_custno, l_date, f_businessamount, c_businessflag " +
      " from ( " +
      " select c_custno, l_date, c_businessflag, f_businessamount, " +
      " row_number() over (partition by c_custno order by l_date desc ) rn " +
      " from bigdata." +dataSourTab + " t " +
      " where l_date < " + calcuDate +
      " and l_date >=  "+dateContainsVal+" ) d " +
      " where rn = 1 ")

    lastTradeDateDF.createOrReplaceTempView("lastTradeDateTmp")

    val tradeTimeLastDF =spark.sql("select c_custno, branch_no, l_date, c_businessflag,  " +
      " (case when l_date = 0 and "+dateContains+" =3 then 100" +
      "       when l_date = 0 and "+dateContains+" =6 then 200 " +
      "       when l_date = 0 and "+dateContains+" =9 then 400 " +
      "  else datediff ('"+DateUtils.intToDateStr(calcuDate)+"' "+
      // hivesql => sparksql    concat('\'','a' ,'b','\'') => 'concat('\\'','a' ,'b','\\'')
      s" , concat(substr(l_date ,0,4),'-'," +  //此处concat中无需添加单引号，${raw"'\''"},
      s"          substr(l_date ,5,2),'-',substr(l_date,7,2) ))" + // 此处concat中无需添加单引号 ${raw"'\''"}
      s" end ) as lastdate_dvalue " +
      " from (select a.c_custno, a.branch_no, nvl(l_date,0) as l_date, nvl(c_businessflag, '0') as c_businessflag  " +
      "        from (select c_custno, branch_no from "+targetCustTab+")  a " +
      "        left  outer join lastTradeDateTmp b on a.c_custno=b.c_custno ) ")

    tradeTimeLastDF.createOrReplaceTempView("tradeTimeLastTmp")

    spark.sql("insert overwrite table bigdata."+tableName+" select * from tradeTimeLastTmp")

    spark.stop()

  }

}

object LastTradeTime {

  def main(args: Array[String]): Unit = {
    new LastTradeTime().lastTradeTime(20190401,"trade_time_last","trade_get_data","c_cust_branch_tb",3)

  }

}
