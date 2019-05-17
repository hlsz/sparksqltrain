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

if [[ ! -e $dir/$HIVE_KERBEROS_FILE ]]; then
     echo "$dir/$HIVE_KERBEROS_FILE not exists"
     exit 0
fi

if [[ ! -e $SPARK_KERBEROS_FILE ]]; then
    echo "$dir/$SPARK_KERBEROS_FILE not exists"
    exit 0
fi

if [[ ! -e $pps ]]; then
    echo "$dir/$pps not exists"
    exit 0
fi

if [[ ! -e $ss ]]; then
    echo "$dir/$ss not exists"
    exit 0
fi


if [[ ! $# == 2 ]]; then
    echo "Usage: $0 date1 date2"
    exit 0
fi

date1=$1
date2=$2

echo "Process start!"

source $dir/sqoop_imp_data.sh $date1
source $dir/spark_comb.sh $date1 $date2

echo "Process end!"

