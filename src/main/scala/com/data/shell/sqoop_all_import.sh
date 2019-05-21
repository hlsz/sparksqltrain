#!/usr/bin/env bash
# !/bin/bash
# 功能:把源数据表加载到hive ods层
# 输入参数据:$1 源数据库 $2 源表名 $3 增全量 $4加载数据日期 $5增量字段1 $6增量字段2 $增量字段3
# 如果是全量 增量字段不需要输入
# $4加载日期如果不输入 则认为是加载昨日数据
# $5,$6,$7 根据实时情况输入
# 例：sh sqoop_mysql_hive_table.sh athletics c_banner i 2017-11-29 end_time start_time create_time


# 开始判断参数据 参数个数


if [ $# -lt 3 ];then
    echo "wrong arg[] number"
     exit 1
fi


#输入数据库名，表名
dbname=$1
tab_name=$2


if [ -z "$dbname" ] || [ -z "$tab_name" ]; then
         echo "The auth info of has not been configured"
          exit 1;
fi


# 判断日期输入 $7


if [  -z "$4" ];then
    yesterday=`date -d yesterday +%Y-%m-%d`
    stat_date=${yesterday}
 else
    stat_date="$4"
fi


"加载数据的日期是："$stat_date  &>> /disk4/azkaban/logs/sqoopimplog/${stat_date}_${tab_name}.log


# 判断增全量数据 并处理查询条件
if [ "$3" = "a" ];then
     contn=""
  elif [ "$3" = "i" ];then
    #处理增量参数据
     cont0=" WHERE 1=0 "
     if [ -z "$5"  ];then
       echo  $5" is null"
      else
       cont1="or date_format($5,'%Y-%m-%d')='$stat_date' "
     fi


     if [ -z "$6" ];then
         echo $6" is null"
      else
       cont2="or date_format($6,'%Y-%m-%d')='$stat_date' "
     fi


     if [ -z "$7" ];then
         echo $7" is null"
      else
       cont3="or date_format($7,'%Y-%m-%d')='$stat_date' "
     fi


     contn=$cont0$cont1$cont2$cont3


    echo $contn


  else
    echo "参数据错误!"
fi


#取源表数据库配置信息
function get_mql_params()
{
while read line;do
     eval "$line"
done < /disk4/azkaban/shell/cfg/.mysql_huihuahua_config


# 判断是否获取到对应数据库用户密码
if [ -z "$username" ] || [ -z "$password" ]; then
         echo "The auth info of has not been configured"
          exit 1;
fi

}
get_mql_params "mql"


mql_user=$username
mql_pwd=$password
mql_ip=$ip


#读取映射表
function get_map_table()
{
 mapna=`/usr/bin/mysql -u*** -p*** -h*** -P3306 --default-character-set=utf8 -Dmeta  -N -e"
 select concat(tableSchema,'#',tgtotablecn,'#',tgtstablecn)
from (
select tableSchema,tgtotablecn,tgtstablecn from t_map_dbs_cfg where lower(tableSchema) =lower('$dbname')
)tmx;
 "`
cfg_tableSchema=`echo $mapna | awk -F'#' '{print $1}'`
cfg_tgtotablecn=`echo $mapna | awk -F'#' '{print $2}'`
cfg_tgtstablecn=`echo $mapna | awk -F'#' '{print $3}'`




}


#处理数据加载到dc_ods层
function get_ods_table()
{
    hql1="insert overwrite table dc_ods.$cfg_tgtotablecn$tab_name   partition(dt='$stat_date') select t.*,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_time from dc_stage.$cfg_tgtstablecn$tab_name t;"
    echo $hql1


  hive -e "$hql1" &>> /disk4/azkaban/logs/sqoopimplog/${stat_date}_${tab_name}.log


}


#处理全量加载
function get_sqoop_all_table()
{
      hadoop fs -rm -r /user/hive/warehouse/dc_stage.db/$cfg_tgtstablecn$tab_name
      sqoop import  --connect jdbc:mysql://$mql_ip/$dbname \
                     --username $mql_user \
                     --password $mql_pwd \
                     --table $tab_name \
                     --target-dir  /user/hive/warehouse/dc_stage.db/$cfg_tgtstablecn$tab_name  \
                     --null-string 'null' \
                     --null-non-string 'null' \
                     --hive-drop-import-delims \
                     --fields-terminated-by "\0001" \
                     --lines-terminated-by "\n" \
                     --bindir /disk4/azkaban/shell/dirbld/ \
                     --outdir /disk4/azkaban/shell/dirout \
                     -m 1 &>> /disk4/azkaban/logs/sqoopimplog/${stat_date}_${tab_name}.log
if [ $? -eq 0 ] ; then
    echo `date +'%Y-%m-%d %H:%M:%S'` INFO Export data to local success  &>> /disk4/azkaban/logs/sqoopimplog/${stat_date}_${tab_name}.log
else
  echo `date +'%Y-%m-%d %H:%M:%S'` ERROR Export data to local failed   &>> /disk4/azkaban/logs/sqoopimplog/${stat_date}_${tab_name}.log
exit 1
fi


}


#处理增量加载
function get_sqoop_inc_table()
{
      hadoop fs -rm -r /user/hive/warehouse/dc_stage.db/$cfg_tgtstablecn$tab_name
      sqoop import  --connect jdbc:mysql://$mql_ip/$dbname \
                     --username $mql_user \
                     --password $mql_pwd \
                     --target-dir  /user/hive/warehouse/dc_stage.db/$cfg_tgtstablecn$tab_name \
                     --null-string 'null' \
                     --null-non-string 'null' \
                     --hive-drop-import-delims \
                     --delete-target-dir \
                     --fields-terminated-by "\0001" \
                     --lines-terminated-by "\n" \
                     --bindir /disk4/azkaban/shell/dirbld/ \
                     --outdir /disk4/azkaban/shell/dirout \
                     --query " SELECT * from ${tab_name} ${contn} and \$CONDITIONS " \
                     -m 1 &>> /disk4/azkaban/logs/sqoopimplog/${stat_date}_${cfg_tgtstablecn}${tab_name}.log
if [ $? -eq 0 ] ; then
    echo `date +'%Y-%m-%d %H:%M:%S'` INFO Export data to local success &>> /disk4/azkaban/logs/sqoopimplog/${stat_date}_${tab_name}.log
else
  echo `date +'%Y-%m-%d %H:%M:%S'` ERROR Export data to local failed  &>> /disk4/azkaban/logs/sqoopimplog/${stat_date}_${tab_name}.log
exit 1
fi
}


get_map_table


#全量导入
if [ "$3" = "a" ];then
     get_sqoop_all_table
     get_ods_table
fi
#增量导入
if [ "$3" = "i" ];then
     get_sqoop_inc_table
     get_ods_table
fi