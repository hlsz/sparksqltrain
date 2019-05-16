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


if __name__ == "__main__":

    spark = SparkSession.builder\
        .appName("prodSpark")\
        .master("yarn-client")\
        .enableHiveSupport()\
        .getOrCreate()
    sc = spark.sparkContext


    STARTTIME = 20190510

    spark.sql("use bigdata")

    sourceSelect = spark.sql("""select   distinct *   from （
                                    select INIT_DATE,FP_CODE_1,FP_CODE_2,FP_CODE_3,WEIGHT_1,WEIGHT_2,WEIGHT_3,COUNT_RETURNS,COUNT_RISK,COUNT_SHARP,
                                    max(COUNT_RETURNS) over(partition by INIT_DATE ) max_COUNT_RETURNS,
                                    min(COUNT_RISK)  over(partition by INIT_DATE ) min_COUNT_RISK,
                                    max(COUNT_SHARP)  over(partition by INIT_DATE ) max_COUNT_SHARP
                                    from dm_comb_result
                                    where INIT_DATE = %d
                                    ） 
                               where COUNT_RETURNS = max_COUNT_RETURNS or COUNT_RISK= min_COUNT_RISK or COUNT_SHARP = max_COUNT_SHARP
                                    """ % (STARTTIME))
    sourceSelect.createOrReplaceTempView("source_select")

    spark.sql("""create table if not exists  bigdata.dm_comb_result_select (
                INIT_DATE int,
                FP_CODE_1 string, 
                FP_CODE_2 string, 
                FP_CODE_3 string, 
                WEIGHT_1 double, 
                WEIGHT_2 double, 
                WEIGHT_3 double, 
                COUNT_RETURNS double, 
                COUNT_RISK double, 
                COUNT_SHARP double
            )  PARTITIONED BY (dt int)
             ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
             LINES TERMINATED BY '\\n'
             stored as textfile """)

    spark.sql(""" insert overwrite table bigdata.dm_comb_result_select  
                   select INIT_DATE,
                    FP_CODE_1, 
                    FP_CODE_2, 
                    FP_CODE_3, 
                    WEIGHT_1, 
                    WEIGHT_2, 
                    WEIGHT_3, 
                    COUNT_RETURNS, 
                    COUNT_RISK, 
                    COUNT_SHARP  from source_select """)

