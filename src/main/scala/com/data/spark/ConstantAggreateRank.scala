package scala.com.data.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class ConstantAggreateRank {

  val conf = new SparkConf()
    .setAppName("ConstantAggreateRank")
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

  def constantAggreateRank(inputDate:Int): Unit =
  {

    spark.sql("use bigdata")

    spark.sql(
      s"""
         | create  table  IF NOT EXISTS  bigdata.custresult_aggreatecol
         |   (c_custno string,
         |	branch_no string,
         |	appro_months_amount double ,
         |	remo_months_amount double ,
         |	amount_tendency double ,
         |	appro_months_count double ,
         |	remo_months_count double ,
         |	frequency_tendency double ,
         |	l_date int,
         |	c_businessflag string,
         |	lastdate_dvalue double,
         |	f_fare0_approch double ,
         |	f_fare0_remote double ,
         |	f_fare0_tendency double,
         |	open_date int,
         |	open_date_dvalue double,
         |	trade_b_amount_rank double,
         |	trade_b_frequency_rank double,
         |	last_b_trade_time_rank double,
         |	fare0_b_tend_rank double,
         |	open_date_b_rank double,
         |	trade_all_amount_rank double,
         |	trade_all_frequency_rank double,
         |	last_all_trade_time_rank double,
         |	fare0_all_tend_rank double,
         |	open_date_all_rank double,
         |	peak_vasset double,
         |	stock_num double,
         |	super_compclinum double,
         |	super_brachclinum double,
         |	avg_price double,
         |	super_comcliprice double,
         |	super_branchcliprice double,
         |	buy_rate double,
         |	sale_rate double,
         |	buysale_rate double,
         |	super_buycompcli double,
         |	super_buybranchcli double,
         |	super_salecompcli double,
         |	super_salebranchcli double,
         |	super_buysalecompcli double,
         |	super_buysalebranchcli double,
         |	tag_time double,
         |	tag_asset double,
         |	deciaml_intervalday double,
         |	tag_lastdeciaml double,
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
         |	stockappro_num double,
         |	superappro_snumcompcli double,
         |	superappro_snumbranchcli double,
         |	avgappro_price double,
         |	superappro_pricecomcli double,
         |	superappro_pricebranchcli double,
         |	buyappro_rate double,
         |	saleappro_rate double,
         |	buysaleappro_rate double,
         |	superappro_buycompcli double,
         |	superappro_buybranchcli double,
         |	superappro_salecompcli double,
         |	superappro_salebranchcl double,
         |	superappro_buysalecompcli double,
         |	superappro_buysalebranchcli double,
         |	tagappro_asset double,
         |	deciamlappro_intervalday double,
         |	superappro_decomcli double,
         |	superappro_debranchcli double,
         |	tagappro_time double,
         |	insert_date int,
         |	input_date int,
         |	appro_taglastdeciaml double,
         |	fare0_b_rank double,
         |	fare0_all_rank double,
         |	approidle_b_rank double,
         |	approidle_all_rank double,
         |	stocknum_ten double,
         |	price_ten double,
         |	buyrate_ten double,
         |	salerate_ten double,
         |	buysalerate_ten double,
         |	deciaml_intervalday_ten double,
         |	super_comage double,
         |	super_branchage double,
         |	peakasset_b_rank double,
         |	peakasset_all_rank double,
         |	asset_rate double,
         |	superappro_comassetrate double,
         |	superappro_branchassetrate double,
         |	assetrate_trentency double
         |   )
         |    ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n’"}
         |
         | stored as textfile
       """.stripMargin)
    spark.sql("delete from  bigdata.custresult_aggreatecol where input_date = "+ inputDate)

    spark.sql(
      s"""
         | insert  into bigdata.custresult_aggreatecol
         |   (c_custno,branch_no,
         |    appro_months_amount,remo_months_amount,
         |   amount_tendency,appro_months_count,remo_months_count,frequency_tendency,
         |   l_date,c_businessflag,lastdate_dvalue,f_fare0_approch,
         |   f_fare0_remote,f_fare0_tendency,open_date,open_date_dvalue,peak_vasset,
         |  trade_b_amount_rank,
         |   trade_b_frequency_rank,
         |   last_b_trade_time_rank,
         |  fare0_b_tend_rank,
         |  open_date_b_rank,
         |  trade_all_amount_rank,
         |  trade_all_frequency_rank,
         |  last_all_trade_time_rank
         |  ,fare0_all_tend_rank
         |  ,open_date_all_rank
         |  ,STOCK_NUM,
         |   SUPER_COMPCLINUM,
         |   SUPER_BRACHCLINUM,
         |   AVG_PRICE,
         |   SUPER_COMCLIPRICE,
         |   SUPER_BRANCHCLIPRICE,
         |   BUY_RATE,
         |   SALE_RATE,
         |   BUYSALE_RATE,
         |   SUPER_BUYCOMPCLI,
         |   SUPER_BUYBRANCHCLI,
         |   SUPER_SALECOMPCLI,
         |   SUPER_SALEBRANCHCLI,
         |   SUPER_BUYSALECOMPCLI,
         |   SUPER_BUYSALEBRANCHCLI,
         |   TAG_TIME,
         |   tag_asset,
         |	DECIAML_INTERVALDAY,
         |	TAG_LASTDECIAML ,
         |	APPRO_TAGLASTDECIAML      ,
         |	AGE   ,
         |	APPROCH_IDLE_RATE         ,
         |	REMOTE_IDLE_RATE,
         |	IDLE_RATE_TENDENCY        ,
         |	APPRO_IN_FREQUENCY        ,
         |	APPRO_OUT_FREQUENCY       ,
         |	APPRO_FREQUENCY_DVALUE    ,
         |	APPRO_IN_SUM    ,
         |	APPRO_OUT_SUM   ,
         |	APPRO_SUM_DVALUE,
         |	REMOTE_IN_FREQUENCY       ,
         |	REMOT_OUT_FREQUENCY       ,
         |	REMOT_FREQUENCY_DVALUE    ,
         |	REMOT_IN_SUM    ,
         |	REMOT_OUT_SUM   ,
         |	REMOT_SUM_DVALUE,
         |	IN_FREQUENCY_TENDENCY     ,
         |	OUT_FREQUENCY_TENDENCY    ,
         |	IN_SUM_TENDENCY ,
         |	OUT_SUM_TENDENCY,
         |	FREQUENCY_DVALUE_TENDENCY ,
         |	SUM_DVALUE_TENDENCY       ,
         |	STOCKAPPRO_NUM ,
         |	SUPERAPPRO_SNUMCOMPCLI       ,
         |	SUPERAPPRO_SNUMBRANCHCLI      ,
         |	AVGAPPRO_PRICE ,
         |	SUPERAPPRO_PRICECOMCLI        ,
         |	SUPERAPPRO_PRICEBRANCHCLI     ,
         |	BUYAPPRO_RATE  ,
         |	SALEAPPRO_RATE ,
         |	BUYSALEAPPRO_RATE        ,
         |	SUPERAPPRO_BUYCOMPCLI    ,
         |	SUPERAPPRO_BUYBRANCHCLI  ,
         |	SUPERAPPRO_SALECOMPCLI   ,
         |	SUPERAPPRO_SALEBRANCHCL  ,
         |	SUPERAPPRO_BUYSALECOMPCLI,
         |	SUPERAPPRO_BUYSALEBRANCHCLI        ,
         |	TAGAPPRO_ASSET ,
         |	DECIAMLAPPRO_INTERVALDAY ,
         |	SUPERAPPRO_DECOMCLI      ,
         |	SUPERAPPRO_DEBRANCHCLI   ,
         |	TAGAPPRO_TIME  ,
         |	fare0_b_rank, fare0_all_rank, approidle_b_rank, approidle_all_rank,
         |	stocknum_ten,
         |	price_ten,
         |	buyrate_ten,
         |	salerate_ten,
         |	buysalerate_ten,
         |	deciaml_intervalday_ten,
         |	super_comage,
         |	super_branchage,
         |	peakasset_b_rank,
         |	peakasset_all_rank,
         |	asset_rate,
         |	superappro_comassetrate,
         |	superappro_branchassetrate,
         |	assetrate_trentency,
         |	insert_date,
         |	input_date )
         |	 select a.c_custno,
         |			a.branch_no,
         |			round(a.appro_months_amount,2),
         |			a.remo_months_amount,
         |		   case when a.amount_tendency <=0 then 0
         |　　　　　　　　when  a.amount_tendency <0.5 and a.amount_tendency　>0  then 1
         |				when a.amount_tendency >=0.5 and a.amount_tendency<0.9 then 2
         |				when a.amount_tendency >=0.9 and a.amount_tendency<1.1 then 3
         |				when a.amount_tendency >=1.1 and a.amount_tendency<1.5 then 4
         |				when a.amount_tendency >=1.5 then 5 end amount_tendency,
         |			a.appro_months_count,
         |			a.remo_months_count,
         |			case when  a.frequency_tendency <=0 then 0
         |				when  a.frequency_tendency <0.5 and a.frequency_tendency　>0  then 1
         |				when a.frequency_tendency >=0.5 and a.frequency_tendency<0.9 then 2
         |				when a.frequency_tendency >=0.9 and a.frequency_tendency<1.1 then 3
         |				when a.frequency_tendency >=1.1 and a.frequency_tendency<1.5 then 4
         |				when a.frequency_tendency >=1.5 then 5 end frequency_tendency,
         |				a.l_date,
         |				a.c_businessflag,
         |				a.lastdate_dvalue,
         |				a.f_fare0_approch,
         |				a.f_fare0_remote,
         |			case when a.f_fare0_tendency <=0 then 0
         |　　　　 when  a.f_fare0_tendency <0.5 and a.f_fare0_tendency　>0  then 1
         |				when a.f_fare0_tendency >=0.5 and a.f_fare0_tendency<0.9 then 2
         |				when a.f_fare0_tendency >=0.9 and a.f_fare0_tendency<1.1 then 3
         |				when a.f_fare0_tendency >=1.1 and a.f_fare0_tendency<1.5 then 4
         |				when a.f_fare0_tendency >=1.5 then 5 end f_fare0_tendency,
         |			a.open_date,
         |			round(a.open_date_dvalue/30,2),
         |			a.peak_vasset,
         |			 round(b.trade_b_amount_rank,2),
         |			round( b.trade_b_frequency_rank,2),
         |			round(b.last_b_trade_time_rank,2),
         |			round(b.fare0_b_tend_rank,2),
         |			round(b.open_date_b_rank,2),
         |			round(b.trade_all_amount_rank,2),
         |			round(b.trade_all_frequency_rank,2),
         |			round(b.last_all_trade_time_rank,2),
         |			round(b.fare0_all_tend_rank,2),
         |			round(b.open_date_all_rank,2),
         |			c.STOCK_NUM,
         |			round(c.SUPER_COMPCLI,2),
         |			round(c.SUPER_BRACHCLI,2),
         |			d.AVG_PRICE,
         |			d.SUPER_COMCLI,
         |			d.SUPER_BRANCHCLI,
         |			e.BUY_RATE,
         |			e.SALE_RATE,
         |			e.BUYSALE_RATE,
         |			round(e.SUPER_BUYCOMPCLI,2),
         |			round(e.SUPER_BUYBRANCHCLI,2),
         |			round(e.SUPER_SALECOMPCLI,2),
         |			round(e.SUPER_SALEBRANCHCLI,2),
         |			round(e.SUPER_BUYSALECOMPCLI,2),
         |			round(e.SUPER_BUYSALEBRANCHCLI,2),
         |			f.tag_time,
         |			o.tag_asset,
         |			j.deciaml_intervalday,
         |			k.tag_lastdeciaml,
         |			z.tag_lastdeciaml,
         |			l.age,
         |			m.approch_idle_rate*100,
         |			m.remote_idle_rate,
         |			case when　m.idle_rate_tendency <=0 then 0
         |				when m.idle_rate_tendency <0.5 and  m.idle_rate_tendency>0  then 1
         |				when m.idle_rate_tendency >=0.5 and m.idle_rate_tendency<0.9 then 2
         |				when m.idle_rate_tendency >=0.9 and m.idle_rate_tendency<1.1 then 3
         |				when m.idle_rate_tendency >=1.1 and m.idle_rate_tendency<1.5 then 4
         |				when m.idle_rate_tendency >=1.5 then 5 end idle_rate_tendency,
         |				round(n.APPRO_IN_FREQUENCY,2),
         |				n.APPRO_OUT_FREQUENCY         ,
         |				n.APPRO_FREQUENCY_DVALUE      ,
         |				n.APPRO_IN_SUM      ,
         |				n.APPRO_OUT_SUM     ,
         |				n.APPRO_SUM_DVALUE  ,
         |				n.REMOTE_IN_FREQUENCY         ,
         |				n.REMOT_OUT_FREQUENCY         ,
         |				n.REMOT_FREQUENCY_DVALUE      ,
         |				n.REMOT_IN_SUM      ,
         |				n.REMOT_OUT_SUM     ,
         |				n.REMOT_SUM_DVALUE  ,
         |			case
         |				  when n.IN_FREQUENCY_TENDENCY <=0 then 0
         |				  when n.IN_FREQUENCY_TENDENCY <0.5 and  n.IN_FREQUENCY_TENDENCY>0  then 1
         |				when n.IN_FREQUENCY_TENDENCY >=0.5 and n.IN_FREQUENCY_TENDENCY<0.9 then 2
         |				when n.IN_FREQUENCY_TENDENCY >=0.9 and n.IN_FREQUENCY_TENDENCY<1.1 then 3
         |				when n.IN_FREQUENCY_TENDENCY >=1.1 and n.IN_FREQUENCY_TENDENCY<1.5 then 4
         |				when n.IN_FREQUENCY_TENDENCY > =1.5 then 5 end IN_FREQUENCY_TENDENCY ,
         |			case
         |				when n.OUT_FREQUENCY_TENDENCY <=0 then 0
         |				when n.OUT_FREQUENCY_TENDENCY <0.5 and n.OUT_FREQUENCY_TENDENCY>0  then 1
         |				when n.OUT_FREQUENCY_TENDENCY >=0.5 and n.OUT_FREQUENCY_TENDENCY<0.9 then 2
         |				when n.OUT_FREQUENCY_TENDENCY >=0.9 and n.OUT_FREQUENCY_TENDENCY<1.1 then 3
         |				when n.OUT_FREQUENCY_TENDENCY >=1.1 and n.OUT_FREQUENCY_TENDENCY<1.5 then 4
         |				when n.OUT_FREQUENCY_TENDENCY >=1.5 then 5 end OUT_FREQUENCY_TENDENCY,
         |			case
         |				when n.IN_SUM_TENDENCY <=0  then 0
         |				when n.IN_SUM_TENDENCY <0.5  and n.IN_SUM_TENDENCY>0 then 1
         |				when n.IN_SUM_TENDENCY >=0.5 and n.IN_SUM_TENDENCY<0.9 then 2
         |				when n.IN_SUM_TENDENCY >=0.9 and n.IN_SUM_TENDENCY<1.1 then 3
         |				when n.IN_SUM_TENDENCY >=1.1 and n.IN_SUM_TENDENCY<1.5 then 4
         |				when n.IN_SUM_TENDENCY >=1.5 then 5 end IN_SUM_TENDENCY ,
         |			case
         |				when n.OUT_SUM_TENDENCY <=0 then 0
         |				when n.OUT_SUM_TENDENCY <0.5 and n.OUT_SUM_TENDENCY>0 then 1
         |				when n.OUT_SUM_TENDENCY >=0.5 and n.OUT_SUM_TENDENCY<0.9 then 2
         |				when n.OUT_SUM_TENDENCY >=0.9 and n.OUT_SUM_TENDENCY<1.1 then 3
         |				when n.OUT_SUM_TENDENCY >=1.1 and n.OUT_SUM_TENDENCY<1.5 then 4
         |				when n.OUT_SUM_TENDENCY >=1.5 then 5 end OUT_SUM_TENDENCY,
         |			case
         |				when n.FREQUENCY_DVALUE_TENDENCY <=0 then 0
         |				when n.FREQUENCY_DVALUE_TENDENCY <0.5  and n.FREQUENCY_DVALUE_TENDENCY>0 then 1
         |				when n.FREQUENCY_DVALUE_TENDENCY >=0.5 and n.FREQUENCY_DVALUE_TENDENCY<0.9 then 2
         |				when n.FREQUENCY_DVALUE_TENDENCY >=0.9 and n.FREQUENCY_DVALUE_TENDENCY<1.1 then 3
         |				when n.FREQUENCY_DVALUE_TENDENCY >=1.1 and n.FREQUENCY_DVALUE_TENDENCY<1.5 then 4
         |				when n.FREQUENCY_DVALUE_TENDENCY >=1.5 then 5 end FREQUENCY_DVALUE_TENDENCY,
         |			case
         |				when n.SUM_DVALUE_TENDENCY <=0 then 0
         |				when n.SUM_DVALUE_TENDENCY <0.5 and n.SUM_DVALUE_TENDENCY>0 then 1
         |				when n.SUM_DVALUE_TENDENCY >=0.5 and n.SUM_DVALUE_TENDENCY<0.9 then 2
         |				when n.SUM_DVALUE_TENDENCY >=0.9 and n.SUM_DVALUE_TENDENCY<1.1 then 3
         |				when n.SUM_DVALUE_TENDENCY >=1.1 and n.SUM_DVALUE_TENDENCY<1.5 then 4
         |				when n.SUM_DVALUE_TENDENCY >=1.5 then 5 end SUM_DVALUE_TENDENCY,
         |			c.stockappro_num      ,
         |			round( c.superappro_compcli,2)  ,
         |			round(c.superappro_brachcli,2) ,
         |			round(d.avgappro_price,2)      ,
         |			round(d.superappro_comcli,2)   ,
         |			round(d.superappro_branchcli,2) ,
         |			round(s.BUY_RATE,2)*100,
         |			round(s.SALE_RATE) *100   ,
         |			round(s.BUYSALE_RATE) *100,
         |			round(s.SUPER_BUYCOMPCLI,2)       ,
         |			round(s.SUPER_BUYBRANCHCLI,2)    ,
         |			round(s.SUPER_SALECOMPCLI,2),
         |			round(s.SUPER_SALEBRANCHCLI,2),
         |			round(s.SUPER_BUYSALECOMPCLI,2)   ,
         |			round(s.SUPER_BUYSALEBRANCHCLI,2) ,
         |			p.tag_asset    ,
         |			q.deciaml_intervalday    ,
         |			round(q.super_comcli,2),
         |			round(q.super_branchcli,2)        ,
         |			r.tag_time     ,
         |			b.fare0_b_rank,
         |			b.fare0_all_rank ,
         |			round(m.approidle_b_rank,2),
         |			round(m.approidle_all_rank,2),
         |			case
         |				when c.stocknum_ten <=0 then 0
         |				when c.stocknum_ten <0.5 and c.stocknum_ten>0 then 1
         |				when c.stocknum_ten >=0.5 and c.stocknum_ten<0.9 then 2
         |				when c.stocknum_ten >=0.9 and c.stocknum_ten<1.1 then 3
         |				when c.stocknum_ten >=1.1 and c.stocknum_ten<1.5 then 4
         |				when c.stocknum_ten >=1.5 then 5 end stocknum_ten,
         |			case
         |				when d.price_ten <=0 then 0
         |				when d.price_ten <0.5 and d.price_ten>0  then 1
         |				when d.price_ten >=0.5 and d.price_ten<0.9 then 2
         |				when d.price_ten >=0.9 and d.price_ten<1.1 then 3
         |				when d.price_ten >=1.1 and d.price_ten<1.5 then 4
         |				when d.price_ten >=1.5 then 5 end price_ten,
         |			case
         |				when s.buy_rate <=0 then 0
         |				when s.buy_rate <0.5  and s.buy_rate>0 then 1
         |				when s.buy_rate >=0.5 and s.buy_rate<0.9 then 2
         |				when s.buy_rate >=0.9 and s.buy_rate<1.1 then 3
         |				when s.buy_rate >=1.1 and s.buy_rate<1.5 then 4
         |				when s.buy_rate >=1.5 then 5 end buyrate_ten ,
         |			case
         |				when s.sale_rate <=0  then 0
         |				when s.sale_rate <0.5 and s.sale_rate>0 then 1
         |				when s.sale_rate >=0.5 and s.sale_rate<0.9 then 2
         |				when s.sale_rate >=0.9 and s.sale_rate<1.1 then 3
         |				when s.sale_rate >=1.1 and s.sale_rate<1.5 then 4
         |				when s.sale_rate >=1.5 then 5 end salerate_ten ,
         |			case
         |				when s.buysale_rate<=0 then 0
         |				when s.buysale_rate <0.5  and s.buysale_rate >0 then 1
         |				when s.buysale_rate >=0.5 and s.buysale_rate<0.9 then 2
         |				when s.buysale_rate >=0.9 and s.buysale_rate<1.1 then 3
         |				when s.buysale_rate >=1.1 and s.buysale_rate<1.5 then 4
         |				when s.buysale_rate >=1.5 then 5 end buysalerate_ten,
         |			case when nvl(j.deciaml_intervalday,0) = 0 then 0 else
         |				round(nvl(q.deciaml_intervalday,0)/j.deciaml_intervalday,2)  end deciaml_intervalday_ten,
         |			round(l.super_comage,2),
         |			round(l.super_branchage,2),
         |			round(b.peakasset_b_rank,2),
         |			round(b.peakasset_all_rank,2),
         |			nvl(round(y.asset_rate,2),0)*100,
         |			nvl(round(y.superappro_comcli,2),0),
         |			nvl(round(y.superappro_branchcli,2),0),
         |			case
         |				when y.assetrate_tren <=0 then 0
         |				when y.assetrate_tren <0.5 and y.assetrate_tren>0  then 1
         |				when y.assetrate_tren >=0.5 and y.assetrate_tren<0.9 then 2
         |				when y.assetrate_tren >=0.9 and y.assetrate_tren<1.1 then 3
         |				when y.assetrate_tren >=1.1 and y.assetrate_tren<1.5 then 4
         |				when y.assetrate_tren >=1.5 then 5 end assetrate_trentency,
         |			a.insert_date,
         |			${inputDate} input_date
         |			from   bigdata.cal_data_tb a
         |			 left join bigdata.client_rank_tb b  on a.c_custno = b.c_custno
         |			 left  join result_clientstocknum c on c.client_id = substr(a.c_custno,2)
         |			left join result_bondavgprice d  on d.client_id = substr(a.c_custno,2)
         |			left join result_clientoprrate e  on e.client_id = substr(a.c_custno,2)
         |			left join result_const_timetag f on concat('c',f.client_id) = a.c_custno
         |			left join  result_consdeciamldate j on concat('c',j.client_id) = a.c_custno
         |			left join  result_constisdeciaml k on concat('c',k.client_id) = a.c_custno
         |			left join  result_custage l on  l.c_custno  = a.c_custno
         |			left join  result_idle_rate m on a.c_custno  = m.C_CUSTNO
         |			left join  banktransfer_result_tb n on  concat('c',n.client_id) = a.c_custno
         |			left join  resultappro_clientoprrate s on  concat('c',s.client_id) = a.c_custno
         |			left join  resultappro_assetdiv p  on  p.cust_no = a.c_custno
         |			left join  resultappro_consdeciamldate q  on  concat('c',q.client_id) = a.c_custno
         |			left join  resultappro_timetag r  on  concat('c',r.client_id) = a.c_custno
         |			left join  result_constassetdivide o  on  o.cust_no = a.c_custno
         |			left join resultappro_constisdeciaml z on concat('c',z.CLIENT_ID) = a.c_custno
         |			left join  result_approturnover y on concat('c',y.client_id) = a.c_custno
         |			where
         |			a.input_date =${inputDate}
         |			and b.input_date =${inputDate}
         |			and c.input_date =${inputDate}
         |			and d.input_date =${inputDate}
         |			and  e.input_date =${inputDate}
         |			and  f.input_date =${inputDate}
         |			and  j.input_date =${inputDate}
         |			and  k.input_date =${inputDate}
         |			and l.input_date =${inputDate}
         |			and m.input_date =${inputDate}
         |			and n.input_date =${inputDate}
         |			and  s.input_date =${inputDate}
         |			and p.input_date =${inputDate}
         |			and q.input_date =${inputDate}
         |			and r.input_date =${inputDate}
         |			and o.input_date =${inputDate}
         |			and z.input_date = ${inputDate}
         |			and y.input_date = ${inputDate}
       """.stripMargin)
    spark.stop()

  }

}

object ConstantAggreateRank
{
  def main(args: Array[String]): Unit = {
    new ConstantAggreateRank().constantAggreateRank(20190410)
  }
}
