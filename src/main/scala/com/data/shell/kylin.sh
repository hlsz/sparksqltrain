#!/bin/bash

#kylin的参数
export kylinUserInfo="--user ADMIN:KYLIN"
export kylinCubeUrl="http://xxx:7070/kylin/api/cubes/"
export kylinJobsUrl="http://xxxx:7070/kylin/api/jobs/"
export startTime="2015-01-01 00:00"
export startTimeTimeStamp=`date -d "$startTime" +%s`
export startTimeTimeStampMs=$(($startTimeTimeStamp * 1000))
export endTime=`date +%Y-%m-%d -d "+1days"`
export endTimeTimeStamp=`date -d "$endTime" +%s`
#将时间戳编程毫秒值
export endTimeTimeStampMs=$(($endTimeTimeStamp * 1000))

export tradeInfoArgs="dataName=tradeInfo&dataType="    #$dataType"&dataTime="$yesterday
#json的url信息存储的文件路径
export tradeInfoJsonUrls=$current/tmpfile/tradeInfoJsonUrls
#json的url存储位置前缀
export tradeInfoJsonUrlPrefix=$current/tmpfile/tradeInfoJsonUrlPrefix
export tradeAnalyzeCubeName="xxxx"
export tradeCollectMoneyCubeName="xxxx"
#用于存储是否下载了的变量文件
export tradeInfoVariableFile=$current/tmpfile/tradeInfoVariableFile

#!/bin/bash

source /etc/profile

#引用公共文件中定义的参数变量
source $PWD/env.sh

jobId=

#是否执行过初始化程序了的控制逻辑
function isInited() {
   #如果文件存在，读取相应的数据类型
   if [[ `ls $tradeInfoVariableFile | grep tradeInfoVariableFile | grep -v grep` != "" ]];then
		dataType=`cat $tradeInfoVariableFile | grep kylinTradeAnalyzeCubeInited | sed 's/kylinTradeAnalyzeCubeInited=//g'`
	    #如果没有，说明这个Spark程序还没有初始化过
		if [[ $dataType == "" ]];then
		    echo -e "\n" >> $tradeInfoVariableFile
			echo "kylinTradeAnalyzeCubeInited=inited" >> $tradeInfoVariableFile
			return 0;
		else
		    return 1;
		fi
	else
	    mkdir -p $current/tmpfile
		cd $current/tmpfile
	    #如果没有这个文件，则是在这个文件中添加
		echo "kylinTradeAnalyzeCubeInited=inited" > $tradeInfoVariableFile
		return 0;
	fi
}

#Spark处理
function kylinHandler() {
    isInited
	if [[ $? == 0 ]];then
	    #上传数据文件到HDFS中
		cd $current
		#1、Disable Cube
		curl -X PUT $kylinUserInfo -H "Content-Type: application/json;charset=utf-8" $kylinCubeUrl$tradeAnalyzeCubeName/disable
		echo ""
		echo ""

		#2、Purge Cube
		curl -X PUT $kylinUserInfo -H "Content-Type: application/json;charset=utf-8" $kylinCubeUrl$tradeAnalyzeCubeName/purge
		echo ""
		echo ""

		#3、Enable Cube
		curl -X PUT $kylinUserInfo -H "Content-Type: application/json;charset=utf-8" $kylinCubeUrl$tradeAnalyzeCubeName/enable
		echo ""
		echo ""

		#4、Build cube
		cubeBuildInfo=`curl -X PUT $kylinUserInfo -H "Content-Type: application/json;charset=utf-8" -d '{ "startTime":'$startTimeTimeStampMs',"endTime":'$endTimeTimeStampMs', "buildType": "BUILD"}' $kylinCubeUrl$tradeAnalyzeCubeName/build`
		echo ""
		echo ""
	else
	    cubeBuildInfo=`curl -X PUT $kylinUserInfo -H "Content-Type: application/json;charset=utf-8" -d '{"endTime":'$endTimeTimeStampMs', "buildType": "BUILD"}' $kylinCubeUrl$tradeAnalyzeCubeName/rebuild`
		echo ""
		echo ""
	fi


	echo "cube build的状态结果:"
	echo $cubeBuildInfo
	echo ""
	echo ""
	#查看是否build好了，如果build好了，发现last_build_time变成了build的最后时间了。
	jobId=$(echo $cubeBuildInfo |jq '.uuid')
	echo $jobId > $jobId
	sed -i 's/"//g' $jobId
	realJobId=`cat $jobId`
	echo $realJobId
	rm -rf $jobId
	echo ""
	echo ""

	while :
	do
	    sleep 1m
	    cubeJobInfo=`curl -X GET --user ADMIN:KYLIN $kylinJobsUrl$realJobId`
		echo "获取cube job运行的状态"
		echo $cubeJobInfo
		echo ""
	    echo ""

	    jobStatus=$(echo $cubeJobInfo | jq ".job_status")
		echo "jobStatus"
		echo $jobStatus > $realJobId
		sed -i 's/"//g' $realJobId
		realJobStatus=`cat $realJobId`
		echo "$realJobStatus"
	    echo ""


		if [[ $realJobStatus == "NEW" ]];then
		    echo "kylin cube build job status：NEW; sleep 1m;"
		elif [[ $realJobStatus == "PENDING" ]];then
		    echo "kylin cube build job status：PENDING; sleep 1m;"
		elif [[ $realJobStatus == "RUNNING" ]];then
		    echo "kylin cube build job status：RUNNING; sleep 1m;"
		elif [[ $realJobStatus == "STOPPED" ]];then
		    echo "kylin cube build job status：STOPPED"
			#如果stop了，停掉kylin脚本的运行
			break;
		elif [[ $realJobStatus == "FINISHED" ]];then
		    echo "kylin cube build job status：FINISHED"
			break;
	    elif [[ $realJobStatus == "ERROR" ]];then
		    echo "kylin cube build job status：ERROR"
			break;
	    elif [[ $realJobStatus == "DISCARDED" ]];then
		    echo "kylin cube build job status：DISCARDED"
			break;
		else
		    echo "kylin cube build job status：OTHER UNKNOWN STATUS"
			break;
		fi
	done

	#删除文件
	rm -rf $realJobId
}

#上传数据文件到HDFS中
kylinHandler

#清理Linux系统中不用的垃圾暂用的内存
sync
echo 3 > /proc/sys/vm/drop_caches
