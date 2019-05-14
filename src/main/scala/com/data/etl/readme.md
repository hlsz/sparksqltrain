客户端与集群时间同步

关闭selinux  
[root@localhost ~]$ vim /etc/sysconfig/selinux
SELINUX=disabled

[root@localhost ~]# vi /etc/hosts
192.168.48.129    hadoop-master
192.168.48.128    hadoop-slave1
192.168.48.127    hadoop-slave2


安装Jdk 同服务端版本一致  1.8.0_151
java -version


[root@localhost ~]# useradd  hadoop
[root@localhost ~]# passwd hadoop
password : admin
 

本地创建目录/opt/cloudera/parcels
mkdir –R /opt/cloudera/parcels

拷贝组件包CDH-5.12.0-1.cdh5.12.0.p0.29到目录/opt/cloudera/parcels
进入目录建立软连接
cd /opt/cloudrea/parcels
ln –s CDH-5.12.0-1.cdh5.12.0.p0.29 CDH

拷贝集群内hadoop相关配置文件到客户端
创建目录/etc/hadoop,将/etc/hadoop/conf文件夹放入该目录,hadoop-master为集群内节点

mkdir /etc/hadoop
scp -r hadoop-master:/etc/hadoop/conf /etc/hadoop

创建目录/etc/hive,将/etc/hive/conf文件夹放入该目录
mkdir /etc/hive
scp -r hadoop-master:/etc/hive/conf /etc/hive

创建目录/etc/spark,将/etc/spark/conf文件夹放入该目录
mkdir /etc/spark
scp -r hadoop-master:/etc/spark/conf /etc/spark
如需使用spark，需要安装Scala, 同服务器的版本一致


设置环境变量
su - root
vi /etc/profile

JAVA_HOME=/usr/java/jdk1.8.0_151
HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
HADOOP_CONF=/etc/hadoop/conf
HADOOP_CONF_DIR=/etc/hadoop/conf
YARN_CONF_DIR=/etc/hadoop/conf
SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
SPARK_CONF_DIR=/etc/spark/conf
CDH_HOME=/opt/cloudera/parcels/CDH
CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
PATH=$JAVA_HOME/bin:$CDH_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH

设置生效（重要）
[root@localhost ~]# source /etc/profile



运行客户端脚本client.sh,文件内容如下：
export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
export HADOOP_CONF=/etc/hadoop/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export SPARK_CONF_DIR=/etc/spark/conf
#export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
export CDH_HOME=/opt/cloudera/parcels/CDH
export PATH=$CDH_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin/:$PATH
##beeline 连接hive进行sql查询
cd /opt/cloudera/parcels/CDH/bin
./beeline -u "jdbc:hive2://cz-bigdata-nnode1:10000/;principal=hive/cz-bigdata-nnode1@HAHADOOP.COM" --config /etc/hive/conf

 ./beeline
 !connect jdbc:hive2://cz-bigdata-nnode1:10000/;principal=hive/cz-bigdata-nnode1@HAHADOOP.COM

SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
 
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath):$CLASSPATH

export SPARK_DIST_CLASSPATH=$(hadoop classpath)//非常重要的变量。可参考下面的链接解释

export SPARK_DIST_CLASSPATH=$SPARK_DIST_CLASSPATH:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar


spark2-submit --class org.apache.spark.examples.SparkPi --master yarn /opt/cloudera/parcels/SPARK2/lib/spark2/examples/jars/spark-examples_2.11-2.2.1.jar  

在客户端上执行以下模拟Pi的示例程序：
 
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar pi 10 100

通过YARN的Web管理界面也可以看到MapReduce的执行状态：

下载CDH客户端，与集群版本相同
http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.6.0-cdh5.12.0.tar.gz
http://archive.cloudera.com/cdh5/cdh/5/hbase-1.2.0-cdh5.12.0.tar.gz
http://archive.cloudera.com/cdh5/cdh/5/hive-1.1.0-cdh5.12.0.tar.gz



 