#!/bin/bash

export JAVA_HOME=/usr/java/jdk1.8.0_151
export HIVE_HOME=//opt/cloudera/parcels/CDH/lib/hive
export SQOOP_HOME=/opt/cloudera/parcels/CDH/lib/sqoop
export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2
export SPARK_KERBEROS_FILE=spark.keytab
export SPARK_KERBEROS_USER=spark@HAHADOOP.COM
export HIVE_KERBEROS_FILE=hive.keytab
export HIVE_KERBEROS_USER=hive@HAHADOOP.COM

dir="$(cd $(dirname $0);pwd)"
echo $dir

#date1 = date +%Y%m%d
#date2 = date -d 'last year' +%Y%m%d

#if test -z "$*"
#if [ ! -n "$*" ]
#if [ "$*" = "" ]
#if [[ ! "$*" ]];then


if [[ ! $# == 1 ]]; then
    echo "Usage: $0 date1"
    exit 0
fi

date1=$1



# 增量导入hs08_his_ofprice表，附带一个系统参数，筛选的时间（当天时间）
function import_ofprice()
{
    runuser -l hive -s /bin/bash -c "/usr/bin/kinit -kt $dir/$HIVE_KERBEROS_FILE  $HIVE_KERBEROS_USER"
	local SQOOP_EXEC="$SQOOP_HOME/bin/sqoop import \
    --connect jdbc:oracle:thin:@172.16.2.232:1521:czjmdb1 \
    --username dcetl \
    --password bigdata#6868 \
    --query 'select INIT_DATE, FUND_CODE, NAV from  dcraw.hs08_his_ofprice@czdcdb2 t  where 1=1 and \$CONDITIONS' \
    --target-dir /user/hive/warehouse/bigdata.db/ods_ofprice \
    --fields-terminated-by '\t' \
    --lines-terminated-by '\n' \
    --null-string '\\\N' \
    --null-non-string '\\\N' \
    -m 1  \
    --incremental append \
    --check-column INIT_DATE \
    --last-value $1  \
    --hive-overwrite \
    --hive-database bigdata \
    --hive-table ods_ofprice "
	echo $SQOOP_EXEC
	return 0
    runuser -l  hive -s /bin/bash  -c "$SQOOP_EXEC"
}

# 导入hs08_ofstkcode表，该表需要进行全部导入覆盖
function import_ofstkcode()
{
   runuser -l hive -s /bin/bash -c "/usr/bin/kinit -kt $dir/$HIVE_KERBEROS_FILE  $HIVE_KERBEROS_USER"
	local SQOOP_EXEC="$SQOOP_HOME/bin/sqoop import \
	--connect jdbc:oracle:thin:@172.16.2.232:1521:czjmdb1 \
    --username dcetl --password bigdata#6868 \
    --query 'select  FUND_CODE , FUND_STATUS  from  dcraw.hs08_ofstkcode@czdcdb2 t  where  1 = 1 and \$CONDITIONS' \
    --target-dir /user/hive/warehouse/bigdata.db/hs08_ofstkcode \
    --delete-target-dir \
    --fields-terminated-by '\t' \
    --lines-terminated-by '\n' \
    --null-string '\\\N' \
    --null-non-string '\\\N' \
    -m 1 \
    --hive-import \
    --hive-database bigdata \
	--hive-overwrite \
    --hive-table hs08_ofstkcode "
	echo $SQOOP_EXEC
    return 0
    runuser -l  hive -s /bin/bash  -c "$SQOOP_EXEC"
}

# 增量导入hs08_his_prodprice表，附带一个系统参数，筛选的时间（当天时间）
function import_hs08_his_prodprice()
{
    runuser -l hive -s /bin/bash -c "/usr/bin/kinit -kt $dir/$HIVE_KERBEROS_FILE  $HIVE_KERBEROS_USER"
	SQOOP_EXEC="$SQOOP_HOME/bin/sqoop import \
    --connect jdbc:oracle:thin:@172.16.2.232:1521:czjmdb1 \
    --username dcetl \
    --password bigdata#6868 \
    --query 'select INIT_DATE, NET_VALUE, prod_code from dcraw.hs08_his_prodprice@CZDCDB2  t  where  1=1 and \$CONDITIONS' \
    --target-dir /user/hive/warehouse/bigdata.db/hs08_his_prodprice \
    --fields-terminated-by '\t' \
    --lines-terminated-by '\n' \
    --null-string '\\\N' \
    --null-non-string '\\\N' \
    -m 1  \
    --incremental append \
    --check-column INIT_DATE \
    --last-value $1 \
    --hive-overwrite \
    --hive-database bigdata \
    --hive-table hs08_his_prodprice"
	echo $SQOOP_EXEC
	return 0
    runuser -l  hive -s /bin/bash  -c "$SQOOP_EXEC"
}

# 导入hs08_prodcode表，该表需要进行全部导入覆盖
function import_prodprice()
{
    runuser -l hive -s /bin/bash -c "/usr/bin/kinit -kt $dir/$HIVE_KERBEROS_FILE  $HIVE_KERBEROS_USER"
    local SQOOP_EXEC="
    $SQOOP_HOME/bin/sqoop import --connect jdbc:oracle:thin:@172.16.2.232:1521:czjmdb1 \
    --username dcetl --password bigdata#6868 \
    --query 'select prod_code ,PROD_STATUS  from  dcraw.hs08_prodcode@czdcdb2 t  where  1 = 1 and \$CONDITIONS' \
    --target-dir /user/hive/warehouse/bigdata.db/hs08_prodcode \
    --delete-target-dir \
    --input-fields-terminated-by '\t' \
    --input-lines-terminated-by '\n'   \
    --null-string '\\\N' \
    --null-non-string '\\\N' \
    -m 1 \
    --hive-import \
    --hive-database bigdata \
	--hive-overwrite \
    --hive-table hs08_prodcode "
	echo $SQOOP_EXEC
    return 0
    runuser -l  hive -s /bin/bash  -c "$SQOOP_EXEC"
}


import_ofprice $date1
if [[ $? -ne 0 ]] ; then
    echo "import_ofprice failed"
    exit 0
else
    echo "import_ofprice succeed"
fi

import_ofstkcode
if [[ $? -ne 0 ]]; then
    echo "import_ofstkcode failed"
    exit 0
else
    echo "import_ofstkcode succeed"
fi


import_hs08_his_prodprice $date1
if [[ $? -ne 0 ]]; then
    echo "import_hs08_his_prodprice failed"
    exit 0
else
    echo "import_hs08_his_prodprice succeed"
fi


import_prodprice
if [[ $? -ne 0 ]]; then
    echo "import_prodprice failed"
    exit 0
else
    echo "import_prodprice succeed"
fi