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
dir="$(cd $(dirname $0);pwd)"
echo $dir


if [[ ! $# == 2 ]]; then
    echo "Usage: $0 date1 date2"
    exit 0
fi

date1=$1
date2=$2

# 执行产品组合优化的计算，运行prodSpark_prod_source.py脚本，附带2个系统参数，当前时间，去年时间
function prodSpark_prod_source()
{   runuser -l  spark -s /bin/bash  -c  "/usr/bin/kinit -kt $dir/$SPARK_KERBEROS_FILE $SPARK_KERBEROS_USER"
    local SPARK_EXEC="
    $SPARK_HOME/bin/spark-submit  \
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
     $1 $2 $3 "
     echo $SPARK_EXEC
     runuser -l  spark -s /bin/bash  -c "$SPARK_EXEC"
}

# 执行数据筛选脚本sourceSelect.py  附带一个系统参数，筛选的时间（当天时间）
function sourceSelect()
{
    runuser -l  spark -s /bin/bash  -c  "/usr/bin/kinit -kt $dir/$SPARK_KERBEROS_FILE $SPARK_KERBEROS_USER"
    local SPARK_EXEC="
    $SPARK_HOME/bin/spark-submit  \
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
     $1 $2"
     echo $SPARK_EXEC
     return 0
     runuser -l  spark -s /bin/bash  -c  "$SPARK_EXEC"
}

# 把筛选后的数据集导入建模库中
function export_dm_comb_result_select()
{
    runuser -l hive -s /bin/bash -c "/usr/bin/kinit -kt $dir/$HIVE_KERBEROS_FILE  $HIVE_KERBEROS_USER"
    local SQOOP_EXEC="
    $SOOP_HOME/bin/sqoop export \
    --connect jdbc:oracle:thin:@172.16.2.232:1521:czjmdb1 \
    --username dcetl \
    --password bigdata#6868 \
    --table DM_COMB_RESULT \
    --columns 'init_date,fp_code_1,fp_code_2,fp_code_3,weight_1,weight_2,weight_3,count_returns,count_risk,count_sharp' \
    --export-dir /user/hive/warehouse/bigdata.db/dm_comb_result_select \
    --input-fields-terminated-by '\t' \
    --input-lines-terminated-by '\n'   \
    -m 1 "
    echo $SQOOP_EXEC
    return 0
    runuser -l hive -s /bin/bash -c "$SQOOP_EXEC"
}

prodSpark_prod_source $pps $date1 $date2
if [[ $? -ne 0 ]]; then
    echo "prodSpark_prod_source failed"
    exit 0
else
    echo "prodSpark_prod_source succeed"
fi

sourceSelect $ss $date1
if [[ $? -ne 0 ]]; then
    echo "sourceSelect failed"
    exit 0
else
    echo "sourceSelect succeed"
fi

export_dm_comb_result_select
if [[ $? -ne 0 ]]; then
    echo "export_dm_comb_result_select failed"
    exit 0
else
    echo "export_dm_comb_result_select succeed"
fi
