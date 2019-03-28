#!/usr/bin/env bash

export SQOOP_HOME=/opt/cloudera/parcipal/CDH/lib/sqoop
hostname="192.168.0.1"
port="3306"
user="root"
password="admin"
database="spark"
table="PREDO_COUNT"
oc_date="20190301"
opts=$@

getparam(){
arg=$1
echo $opts |xargs -n1 |cut -b 2- |awk -F'=' '{if($1=="'"$arg"'") print $2}'
}

IncStart=`getparam inc_start`
InEnd=`getparam inc_end`


function db_incr_to_hive(){
    ${SQOOP_HOME}/bin/sqoop import  -connect jdbc:mysql://${hostname}:${port}/${database} \
    --username ${user} --password ${password} --table ${table}   \
    --fields-terminated-by '\t'  --lines-terminated-by '\n'
    --null-string '\\N'   --null-non-string '\\N' \
    -m 1 \
    --incremental  append  \
    --check-column oc_date --last-value ${oc_date} \
    --hive-import \
    --hive-table ${table}
}

function db_all_to_hive(){
    ${SQOOP_HOME}/bin/sqoop import  -connect jdbc:mysql://${hostname}:${port}/${database} \
    --username ${user} --password ${password} --table ${table}   \
    --fields-terminated-by '\t'  --lines-terminated-by '\n'  \
    --null-string '\\N'   --null-non-string '\\N' \
     -m 1 \
    --delete-target-dir \
    --hive-database defult \
    --hive-import --hive-table ${table}
}

if [$# -eq 0]
then
   while true
   do
     do_all_to_hive
     sleep 120
   done
   exit
fi