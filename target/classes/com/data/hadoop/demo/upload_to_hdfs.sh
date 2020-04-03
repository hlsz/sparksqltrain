#!/bin/sh

#get yesterday format string
#yesterday=`date --date='1 days ago' +%Y_%m_%d`

#testing cleaning data

yesterday=$1

#upload logs to hdfs
hadoop fs -put /apache_logs/access_${yesterday}.log /hmbbs_logs

#cleanning data
hadoop jar cleaned.jar /hmbbs_logs/access_${yesterday}.log

# 使用hive对清洗后的数据进行统计
# 建立一个外部分区表
"CREATE EXTERNAL TABLE hmbbs(ip string, atime string, url string) PARTITIONED BY (logdate string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/hmbbs_cleaned';"
"ALTER TABLE hmbbs ADD PARTITION(logdate='${yesterday}') LOCATION '/hmbbs_cleaned/${yesterday}';"

hive -e "ALTER TABLE hmbbs ADD PARTITION(logdate='${yesterday}') LOCATION '/hmbbs_cleaned/${yesterday}';"
hive -e "SELECT COUNT(1) FROM hmbbs WHERE logdate='2013_05_30';"

# 统计每日的pv
hive -e "CREATE TABLE hmbbs_pv_2013_05_30 AS SELECT COUNT(1) AS PV FROM hmbbs WHERE logdate='2013_05_30';"
hive -e "select * from hmbbs_pv_2013_05_30;"

# 统计每日的注册用户数
"CREATE TABLE hmbbs_reguser_2013_05_30 AS SELECT COUNT(1) AS REGUSER FROM hmbbs WHERE logdate='2013_05_30' AND INSTR(url,'member.php?mod=register')>0;"
hive -e "SELECT COUNT(1) AS REGUSER FROM hmbbs WHERE logdate='2013_05_30' AND INSTR(url,'member.php?mod=register')>0;"

# 统计每日的独立ip（去重）
"CREATE TABLE hmbbs_ip_2013_05_30 AS SELECT COUNT(DISTINCT ip) AS IP FROM hmbbs WHERE logdate='2013_05_30';"
hive -e "CREATE TABLE hmbbs_ip_2013_05_30 AS SELECT COUNT(DISTINCT ip) AS IP FROM hmbbs WHERE logdate='2013_05_30';"

# 统计每日的跳出用户
"CREATE TABLE hmbbs_jumper_2013_05_30 AS SELECT COUNT(1) AS jumper FROM (SELECT COUNT(ip) AS times FROM hmbbs WHERE logdate='2013_05_30' GROUP BY ip HAVING times=1) e;"
hive -e "CREATE TABLE hmbbs_jumper_2013_05_30 AS SELECT COUNT(1) AS jumper FROM (SELECT COUNT(ip) AS times FROM hmbbs WHERE logdate='2013_05_30' GROUP BY ip HAVING times=1) e;"

# 把每天统计的数据放入一张表 （表连接）
"CREATE TABLE hmbbs_2013_05_30 AS SELECT '2013_05_30', a.pv, b.reguser, c.ip, d.jumper FROM hmbbs_pv_2013_05_30 a
  JOIN hmbbs_reguser_2013_05_30 b ON 1=1 JOIN hmbbs_ip_2013_05_30 c ON 1=1 JOIN hmbbs_jumper_2013_05_30 d ON 1=1 ;"

# 创建完了,查看一下：
#show tables;
#select * from hmbbs_2013_05_30 ;

# 使用sqoop把hmbbs_2013_05_30表中数据导出到mysql中
# 在里面创建一个数据库hmbbs，再创建一个表hmbbs_logs_stat，表中有导出数据的5个字段：
logdate logdate varchar ,pv int, reguser int, ip int, jumper int

sqoop export
--connect jdbc:mysql://hadoop0:3306/hmbbs
--username root
--password admin
--table hmbbs_logs_stat
--fields-terminated-by '\001'
--export-dir ‘/hive/hmbbs_2013_05_30’