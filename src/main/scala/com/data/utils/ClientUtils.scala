package com.data.utils

import java.io.IOException
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

/**
  * package: com.cloudera.utils
  * describe: 客户端访问HDFS工具类
  * creat_user: Fayson
  * email: [url=mailto:htechinfo@163.com]htechinfo@163.com[/url]
  * creat_date: 2018/11/13
  * creat_time: 下午9:16
  * 公众号：Hadoop实操
  */
object ClientUtils {
  /**
      * 初始化HDFS的Configuration
    * @return
    */
    def initConfiguration(): Configuration = {
        val configuration = new Configuration
        configuration.addResource(this.getClass().getResourceAsStream("src/Resources/hdfs-client-kb/core-site.xml"))
        configuration.addResource(this.getClass().getResourceAsStream("src/Resources/hdfs-client-kb/hdfs-site.xml"))
        configuration
    }

   /**
      * 初始化访问Kerberos访问
    * @param configuration
    * @param debug 是否启用Kerberos的Debug模式
    * @param properties 客户端配置信息
    */
    def initKerberosENV(configuration: Configuration, debug: Boolean, properties: Properties):Unit = {
      System.setProperty("java.security.krb5.conf", properties.getProperty("krb5.conf"))
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
      if (debug) System.setProperty("sun.security.krb5.debug", "true")
      try {
        UserGroupInformation.setConfiguration(configuration)
        UserGroupInformation.loginUserFromKeytab(properties.getProperty("kerberos.principal"), properties.getProperty("kerberos.keytab"))
        System.out.println(UserGroupInformation.getCurrentUser)
        } catch {
            case e: IOException => {
              e.printStackTrace()
            }
          }
    }
}
