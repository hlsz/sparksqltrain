#!/usr/bin/env bash
# 变量设置，之后应该是传入参数

mdb='kaipao'
hdb='zhengyuan'
table='water_friend_rel'
check_col='create_time'
ds='2019-04-22'


# 1.判断hive中是否有分区表

hive -e "show columns from ${hdb}.${table}_di" |grep -v 'WARN:' > tmp1.txt
a=`cat tmp1.txt`

# 2.判断时间戳的位数
tf=`cat ${mdb}.${table}.timestamp`
if [ -n "${tf}" ]; then
    echo "时间戳长度文件存在"
    l=${#tf}
else
    echo "时间戳长度文件不存在，需创建"
    mysql -u datateam -pRJ4f84S4RjfRxRMm -h172.16.8.4 -A ${mdb}  -e "select max(${check_col}) from ${mdb}.${table} where ${check_col} >unix_timestamp(date_sub('${ds}', interval 30 day))" |awk 'NR>1' >${mdb}.${table}.timestamp
    tf=`cat ${mdb}.${table}.timestamp`
    l=${#m1}
fi


# 编写语句


if [[ ! -n "$a" && l -eq 13 ]]; then
  echo "全量导入"
  #录入全量导入的代码
  hive -e "drop table if exists ${hdb}.${table}"
  sqoop import --connect jdbc:mysql://172.16.8.4:3306/${mdb}?tinyInt1isBit=false --username datateam --password RJ4f84S4RjfRxRMm --table ${table}   --compression-codec=snappy     --as-parquetfile  -m 2 --hive-import --hive-overwrite     --hive-database ${hdb}

  hive -e "show columns from ${hdb}.${table}" |grep -v 'WARN:' > tmp1.txt
  b=`cat tmp1.txt| wc -l`

  hive -e "show create table ${hdb}.${table};" >1
  c=`head -$[${b}+1] 1`
  # 把MySQL查询的固定字段拿出来
  hive -e "show columns from ${hdb}.${table}" |grep -v 'WARN:' > ${hdb}.${table}.columns

  sed -i '2,500s/^/,/' ${hdb}.${table}.columns

  hive -e "alter table ${hdb}.${table} rename to ${hdb}.${table}_tmp;"

  hive -e "${c} partitioned by (ds string) stored as parquet;"
  hive -e "alter table ${hdb}.${table} rename to ${hdb}.${table}_di;"

  hive -e "set hive.exec.dynamic.partition.mode=nonstrict;
           set hive.exec.dynamic.partition=true;
           set hive.exec.max.dynamic.partitions.pernode=10000;
           set hive.exec.max.dynamic.partitions=20000;
           set hive.support.quoted.identifiers=none;
           INSERT OVERWRITE TABLE ${hdb}.${table}_di partition(ds)
           select *,to_date(cast(${check_col}/1000 as timestamp)) from ${hdb}.${table}_tmp;"

  hive -e "drop table ${hdb}.${table}_tmp;"


elif [[ -n "$a" && l -eq 13 ]]; then
  echo "增量导入，有表结构，历史有数据，本分区有数据"

  hive -e "alter table ${hdb}.${table}_di drop partition (ds='${ds}');"
  hive -e "alter table ${hdb}.${table}_di add partition (ds='${ds}');"

  hive -e "select unix_timestamp(concat('${ds}',' 00:00:00'));" >tmp2.txt
  j1=`head -1 tmp2.txt`
  j=$[$j1*1000]
  f=`cat ${hdb}.${table}.columns`
  # 大于等于这个分区的最小值 小于等于这个分区的最大值
  CONDITIONS=''

  g="select ${f} from ${table}"
  sqoop import --connect jdbc:mysql://172.16.8.4:3306/${mdb}?tinyInt1isBit=false --username datateam --password RJ4f84S4RjfRxRMm --query " ${g} where ${check_col} > unix_timestamp('${ds}')*1000 and ${check_col} <= unix_timestamp(date_add('${ds}', interval 1 day))*1000 and \$CONDITIONS" --compression-codec=snappy --as-parquetfile --target-dir /user/hive/warehouse/${hdb}.db/${table}_di/ds=${ds} --incremental append --check-column ${check_col} --last-value ${j} -m 1


elif [[ ! -n "$a" && l -eq 10 ]]; then
  echo "全量导入"
  #录入全量导入的代码
  hive -e "drop table if exists ${hdb}.${table}"
  sqoop import --connect jdbc:mysql://172.16.8.4:3306/${mdb}?tinyInt1isBit=false --username datateam --password RJ4f84S4RjfRxRMm --table ${table}   --compression-codec=snappy     --as-parquetfile  -m 2 --hive-import --hive-overwrite     --hive-database ${hdb}

  hive -e "show columns from ${hdb}.${table}" |grep -v 'WARN:' > tmp1.txt
  b=`cat tmp1.txt| wc -l`

  hive -e "show create table ${hdb}.${table};" >1
  c=`head -$[${b}+1] 1`
  # 把MySQL查询的固定字段拿出来
  hive -e "show columns from ${hdb}.${table}" |grep -v 'WARN:' > ${hdb}.${table}.columns

  sed -i '2,500s/^/,/' ${hdb}.${table}.columns

  hive -e "alter table ${hdb}.${table} rename to ${hdb}.${table}_tmp;"

  hive -e "${c} partitioned by (ds string) stored as parquet;"
  hive -e "alter table ${hdb}.${table} rename to ${hdb}.${table}_di;"

  hive -e "set hive.exec.dynamic.partition.mode=nonstrict;
           set hive.exec.dynamic.partition=true;
           set hive.exec.max.dynamic.partitions.pernode=10000;
           set hive.exec.max.dynamic.partitions=20000;
           set hive.support.quoted.identifiers=none;
           INSERT OVERWRITE TABLE ${hdb}.${table}_di partition(ds)
           select *,to_date(cast(${check_col}*1.0 as timestamp)) from ${hdb}.${table}_tmp;"

  hive -e "drop table ${hdb}.${table}_tmp;"


elif [[ -n "$a" && l -eq 10 ]]; then
  echo "增量导入，有表结构，历史有数据，本分区有数据"

  hive -e "alter table ${hdb}.${table}_di drop partition (ds='${ds}');"
  hive -e "alter table ${hdb}.${table}_di add partition (ds='${ds}');"

  hive -e "select unix_timestamp(concat('${ds}',' 00:00:00'));" >tmp2.txt
  j=`head -1 tmp2.txt`
  f=`cat ${hdb}.${table}.columns`
  # 大于等于这个分区的最小值 小于等于这个分区的最大值
  CONDITIONS=''

  g="select ${f} from ${table}"
  sqoop import --connect jdbc:mysql://172.16.8.4:3306/${mdb}?tinyInt1isBit=false --username datateam --password RJ4f84S4RjfRxRMm --query " ${g} where ${check_col} > unix_timestamp('${ds}') and ${check_col} <= unix_timestamp(date_add('${ds}', interval 1 day)) and \$CONDITIONS" --compression-codec=snappy --as-parquetfile --target-dir /user/hive/warehouse/${hdb}.db/${table}_di/ds=${ds} --incremental append --check-column ${check_col} --last-value ${j} -m 1


else
  echo "其他异常"
fi