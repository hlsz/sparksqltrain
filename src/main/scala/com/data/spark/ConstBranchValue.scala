package scala.com.data.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class ConstBranchValue {

  val conf = new SparkConf()
    .setAppName("ConstBranchValue")
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

  def constBranchValue(inputDate:Int): Unit =
  {
    spark.sql("use bigdata")
    spark.sql(
      s"""
         | create  table  IF NOT EXISTS   bigdata.result_branchavgmed
         |   (	branch_no string,
         |	app_avgstocknum double,
         |	app_medstocknum double,
         |	app_avgprice double,
         |	app_medprice double,
         |	app_avgbuyrate double,
         |	app_avgsalerate double,
         |	app_avgbuysalrate double,
         |	app_medbuyrate double,
         |	app_medsalerate double,
         |	app_medbuysalrate double,
         |	app_avgdeciamlday double,
         |	app_meddeciamlday double,
         |	remo_avgstocknum double,
         |	remo_medstocknum double,
         |	remo_avgprice double,
         |	remo_medprice double,
         |	remo_avgbuyrate double,
         |	remo_avgsalerate double,
         |	remo_avgbuysalrate double,
         |	remo_medbuyrate double,
         |	remo_medsalerate double,
         |	remo_medbuysalrate double,
         |	remo_avgdeciamlday double,
         |	remo_meddeciamlday double,
         |	appro_amount_avg double,
         |	remo_amount_avg double,
         |	amount_tend_avg double,
         |	appro_count_avg double,
         |	remo_count_avg double,
         |	frequency_tend_avg double,
         |	last_dv_avg double,
         |	appro_fare0_avg double,
         |	remo_fare0_avg double,
         |	fare0_tend_avg double,
         |	open_d_dvalue_avg double,
         |	appro_amount_me double,
         |	remo_amount_med double,
         |	amount_tend_med double,
         |	appro_count_med int,
         |	remo_count_med int,
         |	frequency_tend_med double,
         |	last_dv_med double,
         |	appro_fare0_med double,
         |	remo_fare0_med double,
         |	fare0_tend_med double,
         |	open_d_dvalue_med double,
         |	approavg_idlerate double,
         |	appromed_idlerate double,
         |	remoteavg_idlerate double,
         |	remotemed_idlerate double,
         |	appro_avginsum double,
         |	appro_avgoutsum double,
         |	appro_avginfrequ double,
         |	appro_avgoutfrequ double,
         |	appro_avgsumdvalue double,
         |	appro_avgfrequdvalue double,
         |	avginsumtrendency double,
         |	avgoutsumtrendency double,
         |	avginfrequtendency double,
         |	avgoutfrequtendency double,
         |	avgsumdvaluetendency double,
         |	avgfrequdvaluetendency double,
         |	appro_medinsum double,
         |	appro_medoutsum double,
         |	appro_medinfrequ double,
         |	appro_medoutfrequ double,
         |	appro_medsumdvalue double,
         |	appro_medfrequdvalue double,
         |	medinsumtrendency double,
         |	medoutsumtrendency double,
         |	medinfrequtendency double,
         |	medoutfrequtendency double,
         |	medsumdvaluetendency double,
         |	medfrequdvaluetendency double,
         |	remo_avginsum double,
         |	remo_avgoutsum double,
         |	remo_avginfrequ double,
         |	remo_avgoutfrequ double,
         |	remo_avgsumdvalue double,
         |	remo_avgfrequdvalue double,
         |	remo_medinsum double,
         |	remo_medoutsum double,
         |	remo_medinfrequ double,
         |	remo_medoutfrequ double,
         |	remo_medsumdvalue double,
         |	remo_medfrequdvalue double,
         |	insert_date int,
         |	input_date int,
         |	avg_age int,
         |	med_age int,
         |	peak_vasset_avg double,
         |	peak_vasset_med double,
         |	avg_assetrate double,
         |	med_assetrate double  )
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n’"}
         |
         | stored as textfile
       """.stripMargin  )

    spark.sql("delete from  bigdata.result_branchavgmed  where input_date = "+ inputDate)

    spark.sql(
      s"""
         | insert  into bigdata.result_branchavgmed (
         |    branch_no,
         |	app_avgstocknum,
         |	app_medstocknum,
         |	app_avgprice,
         |	app_medprice,
         |	app_avgbuyrate,
         |	app_avgsalerate,
         |	 app_avgbuysalrate,
         |	app_medbuyrate,
         |	app_medsalerate,
         |	app_medbuysalrate,
         |	app_avgdeciamlday,
         |	app_meddeciamlday,
         |	remo_avgstocknum,
         |	remo_medstocknum,
         |	remo_avgprice,
         |	remo_medprice,
         |	remo_avgbuyrate,
         |	remo_avgsalerate,
         |	remo_avgbuysalrate,
         |	remo_medbuyrate,
         |	remo_medsalerate,
         |	remo_medbuysalrate,
         |	remo_avgdeciamlday,
         |	remo_meddeciamlday,
         |	APPRO_AMOUNT_AVG,
         |	REMO_AMOUNT_AVG  ,
         |	AMOUNT_TEND_AVG  ,
         |	APPRO_COUNT_AVG  ,
         |	REMO_COUNT_AVG   ,
         |	FREQUENCY_TEND_AVG  ,
         |	LAST_DV_AVG      ,
         |	peak_vasset_avg,
         |	APPRO_FARE0_AVG  ,
         |	REMO_FARE0_AVG   ,
         |	FARE0_TEND_AVG   ,
         |	OPEN_D_DVALUE_AVG  ,
         |	APPRO_AMOUNT_ME  ,
         |	REMO_AMOUNT_MED  ,
         |	AMOUNT_TEND_MED  ,
         |	APPRO_COUNT_MED  ,
         |	REMO_COUNT_MED   ,
         |	FREQUENCY_TEND_MED  ,
         |	LAST_DV_MED      ,
         |	APPRO_FARE0_MED  ,
         |	REMO_FARE0_MED   ,
         |	FARE0_TEND_MED   ,
         |	OPEN_D_DVALUE_MED ,
         |	peak_vasset_med,
         |	approavg_idlerate ,
         |	appromed_idlerate ,
         |	remoteavg_idlerate ,
         |	remotemed_idlerate ,
         |	appro_avginsum
         |	 ,appro_avgoutsum
         |	 ,appro_avginfrequ
         |	 ,appro_avgoutfrequ
         |	 ,appro_avgsumdvalue
         |	 ,appro_avgfrequdvalue
         |	 ,avginsumtrendency
         |	 ,avgoutsumtrendency
         |	 ,avginfrequtendency
         |	 ,avgoutfrequtendency
         |	 ,avgsumdvaluetendency
         |	 ,avgfrequdvaluetendency
         |	 ,appro_medinsum
         |	 ,appro_medoutsum
         |	 ,appro_medinfrequ
         |	 ,appro_medoutfrequ
         |	 ,appro_medsumdvalue
         |	 ,appro_medfrequdvalue
         |	 ,medinsumtrendency
         |	 ,medoutsumtrendency
         |	 ,medinfrequtendency
         |	 ,medoutfrequtendency
         |	 ,medsumdvaluetendency
         |	 ,medfrequdvaluetendency
         |	 ,remo_avginsum
         |	 ,remo_avgoutsum
         |	 ,remo_avginfrequ
         |	 ,remo_avgoutfrequ
         |	 ,remo_avgsumdvalue
         |	 ,remo_avgfrequdvalue
         |	 ,remo_medinsum
         |	 ,remo_medoutsum
         |	 ,remo_medinfrequ
         |	 ,remo_medoutfrequ
         |	 ,remo_medsumdvalue
         |	 ,remo_medfrequdvalue
         |	,avg_age
         |	,med_age
         |	,avg_assetrate
         |	,med_assetrate
         |	,INSERT_DATE
         |	,INPUT_DATE)
         |		 select
         |		  a.branch_no ,
         |		  a.avgappro_stocknum,
         |		  a.medappro_stocknum,
         |		  b.branchappro_no,
         |		  b.branchappro_med,
         |		  c.avg_buyrate,
         |		  c.avg_salerate,
         |		  c.avg_buysalerate,
         |		  c.med_buyrate,
         |		  c.med_salerate,
         |		  c.med_buysalerate,
         |		  d.avg_deciamlintervalday,
         |		  d.med_deciamlintervalday,
         |		  a.avg_stocknum,
         |		  a.med_stocknum,
         |		  b.branch_avg,
         |		  b.branch_med,
         |		  e.avg_buyrate,
         |		  e.avg_salerate,
         |		  e.avg_buysalerate,
         |		  e.med_buyrate,
         |		  e.med_salerate,
         |		  e.med_buysalerate,
         |		 f.avg_deciamlintervalday,
         |		 f.med_deciamlintervalday,
         |		 h.APPRO_AMOUNT_AVG  ,
         |		h.REMO_AMOUNT_AVG   ,
         |		h.AMOUNT_TEND_AVG   ,
         |		h.APPRO_COUNT_AVG   ,
         |		h.REMO_COUNT_AVG    ,
         |		h.FREQUENCY_TEND_AVG,
         |		h.LAST_DV_AVG       ,
         |		h.peak_vasset_avg    ,
         |		h.APPRO_FARE0_AVG   ,
         |		h.REMO_FARE0_AVG    ,
         |		h.FARE0_TEND_AVG    ,
         |		round(h.OPEN_D_DVALUE_AVG/30,2) OPEN_D_DVALUE_AVG ,
         |		h.APPRO_AMOUNT_MED  ,
         |		h.REMO_AMOUNT_MED   ,
         |		h.AMOUNT_TEND_MED   ,
         |		h.APPRO_COUNT_MED   ,
         |		h.REMO_COUNT_MED    ,
         |		h.FREQUENCY_TEND_MED,
         |		h.LAST_DV_MED       ,
         |		h.APPRO_FARE0_MED   ,
         |		h.REMO_FARE0_MED    ,
         |		h.FARE0_TEND_MED    ,
         |		round(h.OPEN_D_DVALUE_MED/30,2) OPEN_D_DVALUE_MED ,
         |		h.peak_vasset_med  ,
         |		i.approavg_idlerate ,
         |		i.appromed_idlerate ,
         |		i.remoteavg_idlerate ,
         |		i.remotemed_idlerate ,
         |		j.APPRO_AVGINSUM
         |		,j.APPRO_AVGOUTSUM
         |		,j.APPRO_AVGINFREQU
         |		,j.APPRO_AVGOUTFREQU
         |		,j.APPRO_AVGSUMDVALUE
         |		,j.APPRO_AVGFREQUDVALUE
         |		,j.AVGINSUMTRENDENCY
         |		,j.AVGOUTSUMTRENDENCY
         |		,j.AVGINFREQUTENDENCY
         |		,j.AVGOUTFREQUTENDENCY
         |		,round(j.AVGSUMDVALUETENDENCY,2) AVGSUMDVALUETENDENCY
         |		,round(j.AVGFREQUDVALUETENDENCY,2) AVGFREQUDVALUETENDENCY
         |		,j.APPRO_MEDINSUM
         |		,j.APPRO_MEDOUTSUM
         |		,j.APPRO_MEDINFREQU
         |		,j.APPRO_MEDOUTFREQU
         |		,j.APPRO_MEDSUMDVALUE
         |		,j.APPRO_MEDFREQUDVALUE
         |		,j.MEDINSUMTRENDENCY
         |		,j.MEDOUTSUMTRENDENCY
         |		,j.MEDINFREQUTENDENCY
         |		,j.MEDOUTFREQUTENDENCY
         |		,j.MEDSUMDVALUETENDENCY
         |		,j.MEDFREQUDVALUETENDENCY
         |		,j.REMO_AVGINSUM
         |		,j.REMO_AVGOUTSUM
         |		,j.REMO_AVGINFREQU
         |		,j.REMO_AVGOUTFREQU
         |		,j.REMO_AVGSUMDVALUE
         |		,j.REMO_AVGFREQUDVALUE
         |		,j.REMO_MEDINSUM
         |		,j.REMO_MEDOUTSUM
         |		,j.REMO_MEDINFREQU
         |		,j.REMO_MEDOUTFREQU
         |		,j.REMO_MEDSUMDVALUE
         |		,j.REMO_MEDFREQUDVALUE
         |		,k.avg_age
         |		,k.med_age
         |		,l.avgappro_assetrate
         |		,l.medappro_assetrate
         |		,h.INSERT_DATE
         |		,${inputDate}
         |			 from result_branchstock a
         |        left join result_branchdoprice b   on a.branch_no = b.branch_no
         |			  left  join branchappro_traderate c  on c.branch_no = a.branch_no
         |			  left  join resultappro_branchdeciaml d  on d.branch_no = a.branch_no
         |			  left  join branch_traderate e  on e.branch_no = a.branch_no
         |			  left  join result_branchdeciaml f  on f.branch_no = a.branch_no
         |			  left  join ( select  case when a.branch_no = 'ALL' then -1 else a.branch_no end  branchno,a.*
         |			  from  b_all_avg_med_tb  a) h  on h.branchno = a.branch_no
         |			  left join result_branchidle i on i.branch_no= a.branch_no
         |			  left join result_branchbanktransfer j on j.branch_no= a.branch_no
         |			  left join result_branchage k on k.branch_no =  a.branch_no
         |			  left join result_branchtrunover l on l.branch_no = a.branch_no
         |				where a.input_date =${inputDate}
         |			   and b.input_date =${inputDate}
         |			   and  c.input_date =${inputDate}
         |			   and  d.input_date =${inputDate}
         |			   and  e.input_date =${inputDate}
         |			   and  f.input_date =${inputDate}
         |			   and h.input_date =${inputDate}
         |			   and i.input_date =${inputDate}
         |				 and j.input_date =${inputDate}
         |				 and k.input_date =${inputDate}
         |				 and l.input_date =${inputDate}
       """.stripMargin)
    spark.stop()

  }

}
object ConstBranchValue
{
  def main(args: Array[String]): Unit = {

    new ConstBranchValue().constBranchValue(20190401)

  }
}