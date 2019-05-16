# LOAD PYSPARK LIBRARIES
import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *
import atexit
import numpy as np
import datetime

import pandas as pd
import timeit
import datetime
from itertools import combinations
import scipy.optimize as sco
import itertools
from functools import reduce
import types
import logging


def statistics( weights):
    weights = np.array(weights)  # 合并后的权重
    port_returns = np.sum(data_three_df.mean() * weights) * 252  # 预期组合年化收益
    port_variance = np.sqrt(np.dot(weights.T, np.dot(data_three_df.cov() * 252, weights)))  # 组合标准差
    return np.array([port_returns, port_variance, port_returns / port_variance])

# 静态方法
# @staticmethod  # 不需要绑定,调用注意
def min_sharpe( weights):  # 最小化夏普指数的负值   计算资产组合收益率/波动率/夏普率。
    return -1 * statistics(weights)[2]

# 但是我们定义一个函数对 方差进行最小化
def min_variance( weights):  # 计算资产组合收益率/波动率/夏普率。
    return statistics(weights)[1]

# 实例方法
def outputCombination( prodInfo):  # 默认第一个参数为实例对象
    combins = list(combinations(prodInfo, 3))
    return combins


def mapOptmal(combList):
    data_three_df['prod_1'] = data_df1[str(combList[0])]
    data_three_df['prod_2'] = data_df1[str(combList[1])]
    data_three_df['prod_3'] = data_df1[str(combList[2])]

    cons = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
    opts = sco.minimize(min_sharpe, noaLst, method='SLSQP', bounds=bnds, constraints=cons)
    optsRound = opts['x'].round(3)
    startRound = statistics(optsRound).round(3)
    # logging.info(str(combList) + ":"  +' 计算完了')
    print '{}\t:process sucessufully'.format(combList)
    return [combList, optsRound, startRound]



if __name__ == "__main__":

    spark = SparkSession.builder\
        .appName("prodSpark")\
        .master("yarn-client")\
        .enableHiveSupport()\
        .getOrCreate()
    sc = spark.sparkContext

    # line = sc.textFile("file:///D:/code/python/bigdata/src/com/dataservice/combination/ofprice.txt")
    # parts = line.map(lambda l: l.split(","))
    # ofprice = parts.map(lambda p: Row(init_date=int(p[0]), fund_code=int(p[1]), nav=float(p[2])))
    #
    # schema = StructType([StructField("init_date", IntegerType(), True),
    #                      StructField("fund_code", IntegerType(), True),
    #                      StructField("nav", DoubleType(), True)])
    #
    # ofPriceTb = spark.createDataFrame(ofprice, schema)
    # # ofPriceTb.show()
    # ofPriceTb.createOrReplaceTempView("ods_ofprice")

    noa = 3
    # 约束是所有参数(权重)的总和为1。这可以用minimize函数的约定表达如下
    # 我们还将参数值(权重)限制在0和1之间。这些值以多个元组组成的一个元组形式提供给最小化函数
    bnds = tuple((0, 1) for x in range(noa))
    noaLst = noa * [1. / noa, ]
    #     self.engin = create_engine('oracle://dcetl:bigdata#6868@172.16.2.232:1521/czjmdb1')

    STARTTIME = 20180101
    ENDTIME = 20190101

    spark.sql("use bigdata")
    #ofpriceDF = spark.sql("""SELECT C.init_date, C.nav, C.fund_code, 
    #                                LAG(C.nav, 1) OVER(PARTITION BY C.fund_code  ORDER  BY  C.init_date) as next_nav  
    #                          FROM bigdata.ods_ofprice C  
    #                         WHERE C.init_date >= %d
    #                         AND C.init_date < %d """ % (STARTTIME, ENDTIME))
							 
	ofpriceDF = spark.sql("""SELECT DISTINCT *
                               FROM (SELECT C1.init_date, C1.nav as day_nav1, C1.fund_code as FP_CODE, A.FUND_NAME as FP_NAME, 
                                            LAG(C1.nav, 1) OVER(PARTITION BY C1.fund_code  ORDER  BY  C1.init_date) as next_nav
                                       FROM dcraw.hs08_his_ofprice@CZDCDB2 C1 
                              INNER JOIN dcraw.hs08_ofstkcode@czdcdb2 A 
                                 ON A.FUND_CODE = C1.FUND_CODE 
                                AND A.FUND_STATUS = 0 
                              WHERE C1.init_date >= %d AND C1.init_date < %d
                              union all (SELECT C2.INIT_DATE, C2.NET_VALUE as day_nav1, C2.prod_code as FP_CODE, A1.PROD_NAME as FP_NAME, LAG(C2.NET_VALUE, 1) OVER(PARTITION BY C2.prod_code  ORDER  BY  C2.init_date) as next_nav
                                           FROM dcraw.hs08_his_prodprice@CZDCDB2 C2 
                                          INNER JOIN dcraw.hs08_prodcode@czdcdb2 A1
                                             ON A1.prod_code = C2.prod_code 
                                            AND A1.PROD_STATUS = 1 
                                     WHERE C2.init_date >= %d AND C2.init_date < %d)) """ % (STARTTIME, ENDTIME,STARTTIME, ENDTIME))
							 
							 
    # ofpriceDF.show()
    ofpriceDF.createOrReplaceTempView("ofpriceTmp")

    #groupValueDF = spark.sql("""select  C2.fund_code, ROUND(AVG(day_nav)  * 252, 6) year_nav 
    #                             from (  SELECT C1.init_date, C1.fund_code, C1.nav, next_nav, ROUND(LN(C1.nav/next_nav),6) day_nav
    #                                       FROM  ofpriceTmp  C1  ) C2  GROUP BY C2.fund_code ORDER BY year_nav DESC """)
										   
    groupValueDF = spark.sql("""SELECT C3.FP_CODE, ROUND((RETURN_FUND)/STDDEV_FUND,4) PROD_SHARP
                                  FROM (SELECT C22.FP_CODE, AVG(BBB)*252 RETURN_FUND, STDDEV(C22.day_nav1) STDDEV_FUND
                                          FROM (SELECT C11.FP_CODE, C11.day_nav1, LN(C11.day_nav1/next_nav) BBB
                                                  FROM ofpriceTmp C11
                                         WHERE next_nav IS NOT NULL)C22
                                         GROUP BY C22.FP_CODE)C3
                                 WHERE C3.STDDEV_FUND >0
                                 ORDER BY ROUND((RETURN_FUND)/STDDEV_FUND,4) DESC """)

    # groupValueDF.show()
    # bd = sc.broadcast(groupValueDF.collect())
    groupValueDF.createOrReplaceTempView("groupValueTmp")
    prodInfo = groupValueDF.filter("PROD_SHARP > 0").limit(350)  # 收益为正的产品，这里需要计算的产品数，后面全部使用这个产品进行计算
    getProdInfo = prodInfo.toPandas()


    prodValue = spark.sql("""select   c1.FP_CODE, c1.init_date , ROUND(LN(c1.day_nav1/next_nav),6) day_nav
                                     FROM  ofpriceTmp c1 """)
    # prodValue.show()
    prodValueDF = prodValue.toPandas()


	
	#列的值转换为列名
    # pivots = beijingGeoHourPopAfterDrop.groupBy("geoHash").pivot("hour").sum("countGeoPerHour").na.fill(0)
    pivots = prodValue.groupBy("init_date").pivot("FP_CODE").max("day_nav").na.fill(0).collect()
    pivots = sorted(pivots)
    pivotsTb = spark.createDataFrame(pivots)
    data_df1 = pivotsTb.toPandas()
    data_df1 = data_df1.set_index("init_date")
	

    # 获取前68个产品
    colList = getProdInfo.iloc[:,0].tolist()  # prodInfo.select("fund_code").foreach(lambda s: s)
    print "colList:", colList  #[100,102,103]

    cnt = prodInfo.count()
    # 获取产品组合
    print "outputCombination start time:", datetime.datetime.now()
    combinationLst = outputCombination(colList)
    print "outputCombination end time:", datetime.datetime.now()

    # 计算
    print "relst start time: ", datetime.datetime.now()
    data_three_df = pd.DataFrame()
    # combinationdf = spark.createDataFrame(combinationLst,['prod_1', 'prod_2', 'prod_3'] )
    # rdd = sc.parallelize(combinationLst).foreach(lambda x: mapOptmal(x))
    lines = sc.parallelize(combinationLst)
    row = lines.map(mapOptmal).collect()

    #rdd = sc.makeRdd(combinationLst).map(lambda x: mapOptmal(x))
    # rdd.foreach(print)
    print "relst end time: ", datetime.datetime.now()
