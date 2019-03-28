#!/bin/bash

#env.sh

#定义接口请求url地址
export webUrl='http://xxx/xxx/xxx/xxxx'
export backUpWebUrl='http://xxxx/xxxx/xxxx/xxxx'

#echo ${webUrl}
#设置默认的数据类型,默认下载全量数据
export dataType="full"

#昨天时间(时间格式类:2018-10-24)
export yesterday=`date --date='1 days ago' +%Y-%m-%d`
export today=`date +%Y-%m-%d`
#1周前数据（用于保留7天数据）
export aweekAgo=`date --date='7 days ago' +%Y-%m-%d`
export aweekAgoFolder=
#echo $yesterday

#oss中json的位置
export ossUrl="https://ossurl"

#当前路径
export current=$PWD

#Spark运行所需的参数配置等信息
export sparkArgs="--jars /xxxx/apache-phoenix-4.14.1-HBase-1.4-bin/phoenix-spark-4.14.1-HBase-1.4.jar,/xxx/apache-phoenix-4.14.1-HBase-1.4-bin/phoenix-4.14.1-HBase-1.4-client.jar --master spark://xxxx:7077 --executor-memory 2g --total-executor-cores 6 --class "
#Spark程序所在位置
export programPrefixPath="/xxxx/program"


#kylin的参数
export kylinUserInfo="--user ADMIN:KYLIN"
export kylinCubeUrl="http://xxxx:7070/kylin/api/cubes/"
export kylinJobsUrl="http://xxxx:7070/kylin/api/jobs/"
export startTime="2015-01-01 00:00"
export startTimeTimeStamp=`date -d "$startTime" +%s`
export startTimeTimeStampMs=$(($startTimeTimeStamp * 1000))
export endTime=`date +%Y-%m-%d -d "+1days"`
export endTimeTimeStamp=`date -d "$endTime" +%s`
#将时间戳编程毫秒值
export endTimeTimeStampMs=$(($endTimeTimeStamp * 1000))

#phoenix对应的ZkUrl
#export phoenixZkUrl="jdbc:phoenix:ip地址:2181"

#########################################################
#3、订单交易数据请求参数(第一次全量，后续增量)
#请求地址：curl -d "dataName=tradeInfo&dataType=full&dataTime=2018-10-23" http://xxxxx/oss/selectList
export tradeInfoArgs="dataName=tradeInfo&dataType="  #$dataType"&dataTime="$yesterday
#json的url信息存储的文件路径
export tradeInfoJsonUrls=$current/tmpfile/tradeInfoJsonUrls
#json的url存储位置前缀
export tradeInfoJsonUrlPrefix=$current/tmpfile/tradeInfoJsonUrlPrefix
export tradeAnalyzeCubeName="tb_trade_analyze_cube"
export tradeCollectMoneyCubeName="tb_trade_collect_money_cube"
#用于存储是否下载了的变量文件
export tradeInfoVariableFile=$current/tmpfile/tradeInfoVariableFile


#!/bin/bash

source /etc/profile

#求结果中的url路径长度，如果是4，表示这里的值是一个控制了(下面这两行是等效的)
#urlLength=`echo ${urlInfo} |jq '.data.urls[1]' | awk '{print length($0)}'`
#urlLength=$(echo ${urlInfo} |jq '.data.urls[1]' | awk '{print length($0)}')
#echo $urlLength

#引用公共文件中定义的参数变量
source $PWD/env.sh


#定义变量
urlInfo=
#定义尝试次数
retryTimes=1
#json数据所在的文件目录（相对于脚本所在的相对路径）
urlPrefix=
#Json数据文件存放的实际目录
fileFolder=
#最新的数据下载位置
newUrlArgs=

#传递变量存储的路径的位置，返回当前当前数据类型
function resetArgs() {
   #如果文件存在，读取相应的数据类型
   if [[ `ls $tradeInfoVariableFile | grep tradeInfoVariableFile | grep -v grep` != "" ]];then
		#存在这个文件的时候，返回存储在文件中的这个类型的值
		#获取数据类型，然后读取出文件中dataType的值,将dataType=变成空值
		dataType=`cat $tradeInfoVariableFile | grep dataType | sed 's/dataType=//g'`
		newUrlArgs=$tradeInfoArgs$dataType"&dataTime="$yesterday

		#并返回dataType
		#return $dataType
	else
	    mkdir -p $current/tmpfile
		cd $current/tmpfile
	    #不存在这个文件的时候，返回0，并创建这个文件，将变量的类型的值写入到文件中
		#将数据类型写入进去，表示后续都是按照增量的方式进行计算
		echo "dataType=increment" > $tradeInfoVariableFile
		newUrlArgs=$tradeInfoArgs"full&dataTime="$yesterday

		#return "full"
	fi
}

#获取代理商和区域的数据json url地址信息
function getUrlInfo() {
    resetArgs

	echo $newUrlArgs

    #获取代理商的地址信息
    urlInfo=`curl -d $newUrlArgs $webUrl`
}

#获取url参数
#返回值
#1:请求url的结果为200，且成功做了相关操作
#0:请求url的结果不为为200
function getUrlsArray() {
    code=$(echo ${urlInfo} |jq '.code')
    if [[ "$code" = 200 ]];then
        echo "状态码为200"
		mkdir -p $current/tmpfile
        #删除上次生成的临时的json url地址
        rm -rf $tradeInfoJsonUrls
        rm -rf $tradeInfoJsonUrlPrefix
        touch $tradeInfoJsonUrls
        touch $tradeInfoJsonUrlPrefix

		dataInfo=$(echo ${urlInfo} |jq '.data')
		if [[ $dataInfo == "" ]];then
            return 1
		fi

        #获取url的前缀
        echo "===============开始获取 json url 路径前缀==========================="
        echo ${urlInfo} |jq '.data.urlPrefix' > $tradeInfoJsonUrlPrefix
        sed -i 's/"//g' $tradeInfoJsonUrlPrefix
        echo "===============获取 json url 路径前缀结束==========================="

        echo "===============开始获取 json url ==================================="
        #do while方式获取url的列表，然后把结果存入新的数组中
        #定义数组的角标
        index=0
        while :
        do
            #获取url
            url=$(echo ${urlInfo} |jq '.data.urls['$index']')
            #查看字符串中是否有指定字符串
            hasBplan=$(echo $url | grep "bplan/data-center")
            #如果url中有bplan/data-center这样的表示，将这些url存入到临时文件中
            if [[ "$hasBplan" != "" ]]
            then
                echo $url >> $tradeInfoJsonUrls
                index=`expr $index + 1`
            else
                break
            fi
        done

        #将文本中的所有的字符串中的引号去除掉
        sed -i 's/"//g' $tradeInfoJsonUrls
        echo "===============获取 json url 成功==================================="

        #如果最终成功，返回1
        return 1
    else
        #如果没有得到url的值，返回0，表示失败
		webUrl=$backUpWebUrl
        return 0
    fi
}

#如果获取url的过程失败，则一直失败重试，直到程序被处理好了
function getUrlRetry() {
    while :
    do
        echo "开始执行第${retryTimes}次任务，结果如下："

        #调用方法
        getUrlInfo
        getUrlsArray
        #判断本地执行是否成功
        if [[ $? = 1 ]];then
            echo "第${retryTimes}次执行之后,处理json数据成功了，接着处理后续任务"
            break
        else
            echo "第${retryTimes}次执行程序失败,休眠5分钟后再次重试，知道144次之后停止"

            #重试144次，即144 * 5 = 720min (半天)
            if [[ "$retryTimes" -eq 144 ]];then
                echo "已经执行了${retryTimes}次,程序达到预定次数,后续停止执行"
                break
            else
                retryTimes=`expr $retryTimes + 1`
                #休眠5分钟
                sleep 5m
                #再次执行这个函数
            fi
        fi

        #为了让打印的日志显示好看一些，空3行
        echo ""
        echo ""
        echo ""
    done
}

#1、获取Json文件相对脚本的文件目录（相对路径）
#2、获取Json数据文件在磁盘上的绝对路径
function getJsonFolderPath() {
    #查看指定文件是否存在
    urlPrefix=`cat $tradeInfoJsonUrlPrefix`
    #数据文件所在位置
    fileFolder=$current$urlPrefix
}

#下载Json文件
function downloadJsons() {
    #获取到url路径前缀
    echo "开始下载Json文件"
    #urlPrefix=`cat $tradeInfoJsonUrlPrefix`
    #echo $current$urlPrefix
    #最终下载的文件存放位置在下面
    #fileFolder=$current$urlPrefix
    getJsonFolderPath

	if [[ $urlPrefix == "" ]];then
	    echo "当天没有数据文件，直接返回"
	    return 0;
	fi

    mkdir -p $fileFolder
    #删除指定目录下的文件，然后删除
    rm -rf $fileFolder/*

    #进入$current$urlPrefix，开始循环下载json文件
    cd $fileFolder
    #开始循环文件，然后下载文件
    for line in `cat $tradeInfoJsonUrls`
    do
        jsonOssPath=$ossUrl$line
        echo $jsonOssPath
        wget $jsonOssPath
        echo "文件路径:"$current$line
		newPath=`echo $line |sed 's/_//g'`
		mv $current$line $current$newPath
    done
	#修改替换文件中文件名称
	sed -i 's/_//g' $tradeInfoJsonUrls
    echo "下载json文件结束"
}

#上传json文件到HDFS中
function putJsonFile2Hdfs() {
    #上传数据文件到HDFS中
    cd $current
    getJsonFolderPath

	if [[ $urlPrefix == "" ]];then
	    echo "当天没有数据文件，直接返回"
	    return 0;
	fi

    echo "hdfs中的文件路径"
    echo $urlPrefix
    hdfs dfs -rm -r $urlPrefix
    hdfs dfs -mkdir -p $urlPrefix

    #下面是上传文件到hdfs中
    for line in `cat $tradeInfoJsonUrls`
    do
        echo $current$line
        #将文件上传到指定的目录中
        hdfs dfs -put $current$line $urlPrefix
        #上传完成之后，删除留在本地的Json文件
        rm -rf $current$line
    done
    echo "上传json文件到HDFS中完成"
}

#获取数据json文件路径，前缀等信息
getUrlRetry

#下载json数据到指定目录
downloadJsons

#上传数据文件到HDFS中
putJsonFile2Hdfs

#清理Linux系统中不用的垃圾暂用的内存
sync
echo 3 > /proc/sys/vm/drop_caches

