#!/usr/bin/env bash
#!/bin/bash
. ~/.bashrc
#这里要用户自己填写的,有两个地方，第一个是设置tableName的设置,第二个是sqoop语句要自己看表看需求来写
#使用说明
#       抽取指定的表名的数据
#       如果用户在执行该脚本的时候不传入日期参数则默认为当前日期的前一天进行
#       否则为用户指定的日期,格式为yyyyMMdd,如要抽取2015-05-10的数据,那么执行的方式应该是如下
#               sh  脚本名 20150510

#声明获取的数据表名
table=ods_crm_rolegroup

logfile=/home/9003547/sqooplog/$table.txt
#日期
#得到业务日期，如果没有传入日期参数，则由系统直接赋值
if [ -z $1 ]
then
  d_date=`date +%Y%m%d --date="-1 day"`
else
  d_date=$1
fi
echo $d_date  >> $logfile

d_date_format=`echo ${d_date} | awk '{print substr($d_date,1,4)"-"substr($0,5,2)"-"substr($0,7,2)}'`
echo ${d_date_format} >> $logfile

start_date=`date '+%Y-%m-%d %H:%M:%S'`
echo ${start_date} >> $logfile
tableName=$table"_"$d_date
echo $tableName
#判断表在hive中是否存在，存在就删除,这里hive中的库名为dino
hive -e  "drop table if exists dino.$tableName"

#sqoop  抽取数据,这里全表抽取
sqoop --options-file /home/9003547/sqooppath/fdw.txt \
--table $tableName \
--delete-target-dir --target-dir  /user/hive/tmp/$tableName \
--hive-import --hive-table dino.$tableName \
--hive-drop-import-delims --create-hive-table \
--hive-overwrite \
--null-non-string '\\N'  \
--null-string '\\N'  \
--fields-terminated-by '\t' \
--lines-terminated-by '\n'      \
--m 1

#判断sqoop执行结果,etl_status为0表示失败,为1表示成功,由于这里无法获取到错误信息,只能以"errors"代替
if [ $? -ne 0 ];then
        etl_status=0
        etl_error="errors"
        echo "执行失败" >> $logfile
else
        etl_status=1
        #etl_error="\\N"
        echo "执行成功" >> $logfile
fi
#日志写入到hive中,参数顺序对应说明

#table_name=$1
#etl_date=$2
#etl_status=$3
#start_time=$4
#etl_errors=$5
#write audit of etl's recored  into  hive's table