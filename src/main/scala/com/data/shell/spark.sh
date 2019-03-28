#!/bin/bash

source /etc/profile

#求结果中的url路径长度，如果是4，表示这里的值是一个控制了(下面这两行是等效的)
#urlLength=`echo ${urlInfo} |jq '.data.urls[1]' | awk '{print length($0)}'`
#urlLength=$(echo ${urlInfo} |jq '.data.urls[1]' | awk '{print length($0)}')
#echo $urlLength

#引用公共文件中定义的参数变量
source $PWD/env.sh


#json数据所在的文件目录（相对于脚本所在的相对路径）
urlPrefix=
#Json数据文件存放的实际目录
fileFolder=


#1、获取Json文件相对脚本的文件目录（相对路径）
#2、获取Json数据文件在磁盘上的绝对路径
function getJsonFolderPath() {
    #查看指定文件是否存在
    urlPrefix=`cat $tradeInfoJsonUrlPrefix`
    #数据文件所在位置
    fileFolder=$current$urlPrefix
}

#是否执行过初始化程序了的控制逻辑
function isInited() {
   #如果文件存在，读取相应的数据类型
   if [[ `ls $tradeInfoVariableFile | grep tradeInfoVariableFile | grep -v grep` != "" ]];then
		dataType=`cat $tradeInfoVariableFile | grep sparkInited | sed 's/sparkInited=//g'`
	    #如果没有，说明这个Spark程序还没有初始化过
		if [[ $dataType == "" ]];then
		    echo -e "\n" >> $tradeInfoVariableFile
			echo "sparkInited=inited" >> $tradeInfoVariableFile
			return 0;
		else
		    return 1;
		fi
	else
	    mkdir -p $current/tmpfile
		cd $current/tmpfile
	    #如果没有这个文件，则是在这个文件中添加
		echo "sparkInited=inited" > $tradeInfoVariableFile
		return 0;
	fi
}

function mergeFiles() {
    #上传数据文件到HDFS中
    cd $current
    getJsonFolderPath

    isInited

    if [[ $? == 1 ]];then
        echo "开始合并小文件为大文件"
        hdfs dfs -getmerge $urlPrefix $PWD/tradeInfo
        #删除$urlPrefix 下的文件
        hdfs dfs -rm $urlPrefix/*
        #将文件上传到指定的位置
        hdfs dfs -put $PWD/tradeInfo $urlPrefix
        echo $urlPrefix"tradeInfo" > $tradeInfoJsonUrls
        echo "文件合并完成，并且已经将新文件路径写入文件"
        rm -rf $PWD/tradeInfo
        echo "删除存储在本地的文件"
    fi
}

#Spark处理
function sparkHandler() {
    #上传数据文件到HDFS中
    cd $current
    getJsonFolderPath

	if [[ $urlPrefix == "" ]];then
	    echo "当天没有数据文件，直接返回"
	    return 0;
	fi

	isInited
	if [[ $? == 0 ]];then
	    #由于是全量数据，在处理之前，删除hive库中的所有数据
		echo '开始drop hive中的tb_trade_info表'
		hive -e "
			use data_center;
			drop table if EXISTS tb_trade_info;

			CREATE TABLE IF NOT EXISTS tb_trade_info (
			createDate bigint comment '交易订单创建天，时间格式为yyyyMMdd的integer值，分区时间'
			)
			partitioned by(pt_createDate integer comment '创建天，时间格式为yyyyMMdd的integer值，分区时间')
			ROW FORMAT DELIMITED
			FIELDS TERMINATED BY '\t'
			LINES TERMINATED BY '\n'
			STORED AS TEXTFILE;
		"
		echo 'drop hive中的tb_trade_info表 完成'
	fi

    #下面是上传文件到hdfs中
    for line in `cat $tradeInfoJsonUrls`
    do
	    #执行Spark程序来
        echo $line
		cd $SPARK_HOME
	    bin/spark-submit $sparkArgs xxx.xxx.xxx.xxx.xxx.TradeDataClean $programPrefixPath/trade-info/trade-info-1.0.1-SNAPSHOT.jar $line
    done
    echo "完成执行Spark程序"
}

mergeFiles

#上传数据文件到HDFS中
sparkHandler

#清理Linux系统中不用的垃圾暂用的内存
sync
echo 3 > /proc/sys/vm/drop_caches

