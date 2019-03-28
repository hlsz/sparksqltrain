package scala.service

import java.util.Properties

import com.data.utils.{ClientUtils, HDFSUtilScala}
import org.apache.hadoop.fs.FileSystem

/**
  * package: com.cloudera.hdfs
  * describe: Scala访问Kerberos环境下的HDFS示例
  * creat_user: Fayson
  * email: [url=mailto:htechinfo@163.com]htechinfo@163.com[/url]
  * creat_date: 2018/11/13
  * creat_time: 下午9:02
  * 公众号：Hadoop实操
  */
object OperatorHDFSByAPI {

  def main(args: Array[String]): Unit = {
    //加载客户端配置参数
    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream("src/Resources/client.properties"))

    //初始化HDFS Configuration 配置
    val configuration = ClientUtils.initConfiguration()

    //集群启用Kerberos，代码中加入Kerberos环境
    ClientUtils.initKerberosENV(configuration, false, properties)

    val fileSystem = FileSystem.get(configuration)

    val testPath = "/fayson/test"
    //创建HDFS目录
    //    HDFSUtils.mkdir(fileSystem, testPath)
    //设置目录属主及组
    HDFSUtilScala.setowner(fileSystem, testPath, "hive", "hive")
    //设置指定HDFS路径的权限
    HDFSUtilScala.setPermission(fileSystem, testPath, "771")
    //设置指定HDFS目录的ACL
    HDFSUtilScala.setAcl(fileSystem, testPath)
    //递归指定路径下所有目录及文件
    HDFSUtilScala.recursiveDir("/user/hive/warehouse/test.db/", fileSystem)

    fileSystem.close()
  }

}
