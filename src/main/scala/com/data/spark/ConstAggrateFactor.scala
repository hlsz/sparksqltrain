package scala.com.data.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class ConstAggrateFactor {

  val conf = new SparkConf()
    .setAppName("ConstAggrateFactor")
    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata.db")
    //数据倾斜
    .config("spark.sql.shuffle.partitions", 500)
    .enableHiveSupport()
    .getOrCreate()

  def constAggrateFactor(inputDate:Int): Unit =
  {
    spark.sql("use bigdata")
    spark.sql(
      s"""
         | create  table  IF NOT EXISTS  bigdata.const_aggratefacort
         |   (client_id int,
         |	tg_tag string,
         |	stock_num double,
         |	avg_businessprice double,
         |	tag_asset string,
         |	tag_time string,
         |	tag_lastdeciaml string,
         |	deciaml_intervalday double,
         |	asset_rate number,
         |	buy_rate double,
         |	sale_rate double,
         |	buysale_rate double,
         |	cust_flag string,
         |	age int,
         |	approch_idle_rate double,
         |	remote_idle_rate double,
         |	idle_rate_tendency double,
         |	appro_in_frequency double,
         |	appro_out_frequency double,
         |	appro_frequency_dvalue double,
         |	appro_in_sum double,
         |	appro_out_sum double,
         |	appro_sum_dvalue double,
         |	remote_in_frequency double,
         |	remot_out_frequency double,
         |	remot_frequency_dvalue double,
         |	remot_in_sum double,
         |	remot_out_sum double,
         |	remot_sum_dvalue double,
         |	in_frequency_tendency double,
         |	out_frequency_tendency double,
         |	in_sum_tendency double,
         |	out_sum_tendency double,
         |	frequency_dvalue_tendency double,
         |	sum_dvalue_tendency double,
         |	open_date int,
         |	l_date int,
         |	lastdate_dvalue double,
         |	c_businessflag string,
         |	input_date int,
         |	appro_months_amount double,
         |	remo_months_amount double,
         |	amount_tendency double,
         |	appro_months_count double,
         |	remo_months_count double,
         |	frequency_tendency double,
         |	f_fare0_approch double,
         |	f_fare0_remote double,
         |	f_fare0_tendency double,
         |	open_date_dvalue double,
         |	peak_vasset double
         |   )
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n’"}
         |
         | stored as textfile
       """.stripMargin)

    spark.sql("delete from   bigdata.const_aggratefacort where input_date =  " +inputDate )

    spark.sql(
      s"""
         |insert  into const_aggratefacort
         |	select  a.client_id
         |	,-1
         |	,b.STOCK_NUM
         |	,c.AVG_PRICE
         |	,d.TAG_ASSET
         |	,e.TAG_TIME
         |	,f.TAG_LASTDECIAML
         |	,k.deciaml_intervalday
         |	,nvl(g.asset_rate,0)
         |	,h.BUY_RATE
         |	,h.SALE_RATE
         |	,h.BUYSALE_RATE
         |	,-1
         |	,nvl(l.AGE,-1) age
         |	,m.APPROCH_IDLE_RATE
         |	,m.REMOTE_IDLE_RATE
         |	,m.IDLE_RATE_TENDENCY
         |	,n.APPRO_IN_FREQUENCY
         |	,n.APPRO_OUT_FREQUENCY
         |	,n.APPRO_FREQUENCY_DVALUE
         |	,n.APPRO_IN_SUM
         |	,n.APPRO_OUT_SUM
         |	,n.APPRO_SUM_DVALUE
         |	,n.REMOTE_IN_FREQUENCY
         |	,n.REMOT_OUT_FREQUENCY
         |	,n.REMOT_FREQUENCY_DVALUE
         |	,n.REMOT_IN_SUM
         |	,n.REMOT_OUT_SUM
         |	,n.REMOT_SUM_DVALUE
         |	,n.IN_FREQUENCY_TENDENCY
         |	,n.OUT_FREQUENCY_TENDENCY
         |	,n.IN_SUM_TENDENCY
         |	,n.OUT_SUM_TENDENCY
         |	,n.FREQUENCY_DVALUE_TENDENCY
         |	,n.SUM_DVALUE_TENDENCY
         |	,o.OPEN_DATE
         |	,o.L_DATE
         |	,o.LASTDATE_DVALUE
         |	,o.C_BUSINESSFLAG
         |	,${inputDate} input_date
         |	,o.APPRO_MONTHS_AMOUNT
         |	,o.REMO_MONTHS_AMOUNT
         |	,o.AMOUNT_TENDENCY
         |	,o.APPRO_MONTHS_COUNT
         |	,o.REMO_MONTHS_COUNT
         |	,o.FREQUENCY_TENDENCY
         |	,o.F_FARE0_APPROCH
         |	,o.F_FARE0_REMOTE
         |	,o.F_FARE0_TENDENCY
         |	,o.OPEN_DATE_DVALUE
         |	,nvl(o.PEAK_VASSET,-1) PEAK_VASSET
         |   from c_cust_branch_tb a
         |      left join result_clientstocknum  b on a.client_id = b.client_id
         |     left join result_bondavgprice  c on a.client_id = c.client_id
         |     left join result_constassetdivide  d on concat('c',a.client_id) = d.cust_no
         |     left join result_const_timetag e on a.client_id = e.client_id
         |     left join  result_constisdeciaml f on a.client_id = f.client_id
         |     left join  result_consdeciamldate k on a.client_id = k.client_id
         |     left join  result_assetturnover g on a.client_id = g.client_id
         |     left join  result_clientoprrate h on a.client_id = h.client_id
         |     left join  cust_age_tb l on a.client_id = l.client_id
         |     left join  capital_idal_rate_tb m on concat('c',a.client_id) = m.C_CUSTNO
         |     left join banktransfer_result_tb n on a.client_id = n.client_id
         |     left join custresult_aggreatecol o  on concat('c',a.client_id) = o.C_CUSTNO
         |     left join remotpeak_assete w on concat('c',a.client_id) = w.C_CUSTNO
         |     where b.input_date = ${inputDate}
         |     and c.input_date = ${inputDate}  and d.input_date = ${inputDate}
         |     and e.input_date = ${inputDate}  and f.input_date = ${inputDate}
         |     and k.input_date = ${inputDate}  and h.input_date = ${inputDate}
         |     and l.input_date = ${inputDate}  and m.input_date = ${inputDate}
         |     and n.input_date = ${inputDate}  and o.input_date = ${inputDate}
         |     and  g.input_date = ${inputDate}
         |     and  w.input_date = ${inputDate}
       """.stripMargin)

    spark.stop()

  }

}

object ConstAggrateFactor
{
  def main(args: Array[String]): Unit = {
    new ConstAggrateFactor().constAggrateFactor((20190410))
  }
}
