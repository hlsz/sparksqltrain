package com.data.spark

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class  TransactionAmount
{
  val conf = new SparkConf()
    .setAppName("TransactionAmount")
    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    //    .config("spark.sql.warehouse.dir","/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()

  val sqlContext = spark.sqlContext

  case class CCustBranchTb(cCustNo:String, branchNo:String, organFlag:String)

  def transactionAmount(calcuDate:Int, approchMonths:Int, remoteMonths:Int, tableName:String, dataSourTable:String, targetCustTab:String): Unit =
  {
    spark.sql("use bigdata")
    //    val caseClassDS = Seq(CCustBranchTb("c0001","001","01")).toDS()
    //    caseClassDS.show()

    val calcDateVal = DateUtils.intToDate(calcuDate)
    // addOrMinusDayToLong
    val approchMonthsVal = DateUtils.dateToInt(DateUtils.addOrMinusDay(calcDateVal, -approchMonths))
    val remoteMonthsVal = DateUtils.dateToInt(DateUtils.addOrMinusDay(calcDateVal, -remoteMonths))

    val schema = StructType(List(
      StructField("c_custno", StringType, true),
      StructField("appro_months_amount",DoubleType, true),
      StructField("remo_months_amount", DoubleType, true),
      StructField("amount_tendency",DoubleType, true),
      StructField("branch_no", StringType, true)
    ))

    //create Hive table
    sqlContext.sql("create table IF NOT EXISTS  "+ tableName +
      "( c_custno  string, appro_months_amount double, remo_months_amount double, " +
      " amount_tendency double, branch_no string ) " +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      " LINES TERMINATED BY ‘\n’ collection items terminated by '-' " +
      " map keys terminated by ':' " +
      " stored as textfile ")


    val appro_months_amount_sql = spark.sql("select c_custno, " +
      "(case when sum(f_businessbalance) is null then 0 else sum(f_businessbalance) end ) as appro_months_amount " +
      "from  bigdata."+dataSourTable+
      "where c_custno in (select c_custno from global_temp."+targetCustTab+" )" +
      " and l_date <  " + calcuDate +
      " and l_date >= " + approchMonthsVal +
      " group by c_custno  ")

    appro_months_amount_sql.createOrReplaceTempView("appro_months_amount_tab")

    val remot_months_amount_sql = spark.sql("select c_custno, " +
      "(case when sum(f_businessbalance) is null  then 0 else sum(f_businessbalance) end ) as appro_months_amount " +
      "from  bigdata."+dataSourTable+
      "where c_custno in (select c_custno from global_temp."+targetCustTab+" )" +
      " and l_date <  " + calcuDate +
      " and l_date >= " + remoteMonthsVal +
      " group by c_custno  ")

    //remot_months_amount_sql.limit(5).write.mode("overwrite").json("remot_months.json")
    //    remot_months_amount_sql.select("c_custno", "f_businessbalance")
    //      .filter("l_date < calcuDate and l_date >= remoteMonthsVal")

    // global_temp  global view
    remot_months_amount_sql.createOrReplaceTempView("remot_months_amount_tab")

    val amountTransactionDF =  spark.sql("select c_custno, appro_months_amount, remo_months_amount,  " +
      " (case when remo_months_amount = 0 then 0 else appro_months_amount * ( " + remoteMonths +
      "  / " + approchMonths +") / remo_months_amounts end ) as amount_tendency, branch_no  " +
      " from ( select a.c_custno, (case when c.appro_months_amount is null then 0 else c.appro_months_amount  end) as appro_months_amount, " +
      " (case when c.remo_months_amount is null then 0 else c.remo_months_amount end) as  remo_months_amount ," +
      " branch_no " +
      " from (select c_custno,branch_no from global_temp."+ targetCustTab +" ) a " +
      " left join remot_months_amount_tab b on a.c_custno =  b.c_custno " +
      " left join appro_months_amount_tab  c on a.c_custno = c.c_custno ) "
    )
    amountTransactionDF.createOrReplaceTempView("amountTransactionTmp")

    spark.sql("insert into overwrite bigdata."+tableName + "select * from amountTransactionTmp ")

    spark.stop()
}

}

object TransactionAmount {

//  new TransactionAmount().transactionAmount()
}
