#!/bin/bash

export JAVA_HOME=/usr/java/jdk1.8.0_151
export HIVE_HOME=//opt/cloudera/parcels/CDH/lib/hive
export SQOOP_HOME=/opt/cloudera/parcels/CDH/lib/sqoop
export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2
export SPARK_KERBEROS_FILE=spark.keytab
export SPARK_KERBEROS_USER=spark@HAHADOOP.COM
export HIVE_KERBEROS_FILE=hive.keytab
export HIVE_KERBEROS_USER=hive@HAHADOOP.COM

pps=prodSpark_prod_source.py
ss=sourceSelect.py

if [[ ! -e $HIVE_KERBEROS_FILE ]]; then
     echo "$HIVE_KERBEROS_FILE not exists"
     exit 0
fi

if [[ ! -e $SPARK_KERBEROS_FILE ]]; then
    echo "$SPARK_KERBEROS_FILE not exists"
    exit 0

fi

#date1 = date +%Y%m%d
#date2 = date -d 'last year' +%Y%m%d

#if test -z "$*"
#if [ ! -n "$*" ]
#if [ "$*" = "" ]
#if [[ ! "$*" ]];then


if [[ ! $# == 2 ]]; then
    echo "Usage: $0 date1 date2"
    exit
fi

date1=$1
date2=$2

import_ofprice $date1
if [[ $? -ne 0 ]] ; then
    echo "import_ofprice failed"
    exit
else
    echo "import_ofprice succeed"
fi

import_ofstkcode
if [[ $? -ne 0 ]]; then
    echo "import_ofstkcode failed"
    exit
else
    echo "import_ofstkcode succeed"
fi


import_hs08_his_prodprice $date1
if [[ $? -ne 0 ]]; then
    echo "import_hs08_his_prodprice failed"
    exit
else
    echo "import_hs08_his_prodprice succeed"
fi


import_prodprice
if [[ $? -ne 0 ]]; then
    echo "import_prodprice failed"
    exit
else
    echo "import_prodprice succeed"
fi


prodSpark_prod_source $date1 $date2
if [[ $? -ne 0 ]]; then
    echo "prodSpark_prod_source failed"
    exit
else
    echo "prodSpark_prod_source succeed"
fi

sourceSelect $date1
if [[ $? -ne 0 ]]; then
    echo "sourceSelect failed"
else
    echo "sourceSelect succeed"
fi

export_dm_comb_result_select
if [[ $? -ne 0 ]]; then
    echo "export_dm_comb_result_select failed"
else
    echo "export_dm_comb_result_select succeed"
fi





# 增量导入hs08_his_ofprice表，附带一个系统参数，筛选的时间（当天时间）
function import_ofprice()
{
    su hdfs -l -c "/usr/bin/kinit -kt $HIVE_KERBEROS_FILE  $HIVE_KERBEROS_USER"
    su hdfs -l -c "
    $SQOOP_HOME/bin/sqoop import \
    --connect jdbc:oracle:thin:@172.16.2.232:1521:czjmdb1 \
    --username dcetl \
    --password bigdata#6868 \
    --query \\\"select INIT_DATE, FUND_CODE, NAV from  dcraw.hs08_his_ofprice@czdcdb2 t  where 1=1 and \$CONDITIONS\\\" \
    --target-dir /user/hive/warehouse/bigdata.db/ods_ofprice \
    --fields-terminated-by '\t' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    -m 1  \
    --incremental append \
    --check-column INIT_DATE \
    --last-value $date1  \
    --hive-overwrite \
    --hive-database bigdata \
    --hive-table ods_ofprice "
}

# 导入hs08_ofstkcode表，该表需要进行全部导入覆盖
function import_ofstkcode()
{   su hdfs -l -c "/usr/bin/kinit -kt $HIVE_KERBEROS_FILE  $HIVE_KERBEROS_USER"
    su hdfs -l -c "
    $SQOOP_HOME/bin/sqoop import \
	--connect jdbc:oracle:thin:@172.16.2.232:1521:czjmdb1 \
    --username dcetl --password bigdata#6868 \
    --query \\\"select  FUND_CODE , FUND_STATUS  from  dcraw.hs08_ofstkcode@czdcdb2 t  where  1 = 1 and \$CONDITIONS\\\" \
    --target-dir /user/hive/warehouse/bigdata.db/hs08_ofstkcode \
    --delete-target-dir \
    --fields-terminated-by '\t' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    -m 1 \
    --hive-import \
    --hive-database bigdata \
    --hive-table hs08_ofstkcode "
}

# 增量导入hs08_his_prodprice表，附带一个系统参数，筛选的时间（当天时间）
function import_hs08_his_prodprice()
{
    su hdfs -l -c "/usr/bin/kinit -kt $HIVE_KERBEROS_FILE  $HIVE_KERBEROS_USER"
    su hdfs -l -c "
    $SQOOP_HOME/bin/sqoop import \
    --connect jdbc:oracle:thin:@172.16.2.232:1521:czjmdb1 \
    --username dcetl \
    --password bigdata#6868 \
    --query \\\"select INIT_DATE, NET_VALUE, prod_code from dcraw.hs08_his_prodprice@CZDCDB2  t  where  1=1 and \$CONDITIONS\\\" \
    --target-dir /user/hive/warehouse/bigdata.db/hs08_his_prodprice \
    --fields-terminated-by '\t' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    -m 1  \
    --incremental append \
    --check-column INIT_DATE \
    --last-value $date1 \
    --hive-overwrite \
    --hive-database bigdata \
    --hive-table hs08_his_prodprice "
}

# 导入hs08_prodcode表，该表需要进行全部导入覆盖
function import_prodprice()
{
    su hdfs -l -c "/usr/bin/kinit -kt $HIVE_KERBEROS_FILE  $HIVE_KERBEROS_USER"
    su hdfs -l -c "
    $SQOOP_HOME/bin/sqoop import --connect jdbc:oracle:thin:@172.16.2.232:1521:czjmdb1 \
    --username dcetl --password bigdata#6868 \
    --query \\\"select prod_code ,PROD_STATUS  from  dcraw.hs08_prodcode@czdcdb2 t  where  1 = 1 and \$CONDITIONS\\\" \
    --target-dir /user/hive/warehouse/bigdata.db/hs08_prodcode \
    --delete-target-dir \
    --input-fields-terminated-by '\t' \
    --input-lines-terminated-by '\n'   \
    --null-string '\\N' \
    --null-non-string '\\N' \
    -m 1 \
    --hive-import \
    --hive-database bigdata \
    --hive-table hs08_prodcode "
}

# 执行产品组合优化的计算，运行prodSpark_prod_source.py脚本，附带2个系统参数，当前时间，去年时间
function prodSpark_prod_source()
{   su spark -l -c "/usr/bin/kinit -kt $SPARK_KERBEROS_FILE $SPARK_KERBEROS_USER"
    su spark -l -c "
    $SPARK_HOME/bin/spark2-submit  \
    --master yarn \
    --deploy-mode cluster  \
    --principal 'spark@HAHADOOP.COM' \
    --keytab 'spark.keytab' \
    --conf spark.pyspark.python=/usr/local/anaconda2/bin/python \
    --conf spark.pyspark.driver.python=/usr/local/anaconda2/bin/python \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.default.parallelism=180 \
    --conf spark.shuffle.file.buffer=128K\
    --conf spark.reducer.maxSizeInFlight=96M \
    --conf spark.shuffle.consolidateFiles=true \
    --driver-memory 2g \
    --executor-memory 4g \
    --executor-cores 3 \
    --num-executors 30 \
     $pps $date1 $date2 "
}

# 执行数据筛选脚本sourceSelect.py  附带一个系统参数，筛选的时间（当天时间）
function sourceSelect()
{
    su spark -l -c "/usr/bin/kinit -kt $SPARK_KERBEROS_FILE $SPARK_KERBEROS_USER"
    su spark -l -c "
    $SPARK_HOME/bin/spark2-submit  \
    --master yarn \
    --deploy-mode cluster  \
    --principal 'spark@HAHADOOP.COM' \
    --keytab 'spark.keytab' \
    --conf spark.pyspark.python=/usr/local/anaconda2/bin/python \
    --conf spark.pyspark.driver.python=/usr/local/anaconda2/bin/python \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.default.parallelism=180 \
    --conf spark.shuffle.file.buffer=128K \
    --conf spark.reducer.maxSizeInFlight=96M \
    --conf spark.shuffle.consolidateFiles=true \
    --driver-memory 2g \
    --executor-memory 4g \
    --executor-cores 3 \
    --num-executors 30 \
     $ss $date1 "
}

# 把筛选后的数据集导入建模库中
function export_dm_comb_result_select()
{
    su hdfs -l -c "/usr/bin/kinit -kt $HIVE_KERBEROS_FILE  $HIVE_KERBEROS_USER"
    su hive -l -c "
    $SOOP_HOME/bin/sqoop export \
    --connect jdbc:oracle:thin:@172.16.2.232:1521:czjmdb1 \
    --username dcetl \
    --password bigdata#6868 \
    --table DM_COMB_RESULT \
    --columns \\\"init_date,fp_code_1,fp_code_2,fp_code_3,weight_1,weight_2,weight_3,count_returns,count_risk,count_sharp\\\" \
    --export-dir /user/hive/warehouse/bigdata.db/dm_comb_result_select \
    --input-fields-terminated-by '\t' \
    --input-lines-terminated-by '\n'   \
    -m 1 "
}
