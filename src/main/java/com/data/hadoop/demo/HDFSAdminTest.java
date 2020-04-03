package com.data.hadoop.demo;

import com.data.utils.HDFSAdminUtils;
import com.data.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class HDFSAdminTest {

    private static String confPath = System.getProperty("user.dir") + File.separator + "conf";

    public static void main(String[] args) {

        try{
            File file = new File(confPath + File.separator + "conf.properties");
            if (!file.exists()) {
                System.out.println("配置文件不存在");
                System.exit(0);
            }
            Properties properties = new Properties();
            properties.load(new FileInputStream(file));

            Configuration configuration = HDFSUtils.initConfiguration(confPath);
            if(properties.getProperty("berberos.isenable").equals("true")){
                initKerberosENV(configuration, properties);
            }

            FileSystem fileSystem = FileSystem.get(configuration);
            HdfsAdmin hdfsAdmin = new HdfsAdmin(fileSystem.getUri(), configuration);

            String operation = args[0];
            switch (operation){
                case "setQuota":
                    HDFSAdminUtils.setQuota(hdfsAdmin, new Path(args[1]), Long.parseLong(args[2]));
                    break;
                case "setSpaceQuota":
                    HDFSAdminUtils.setSpaceQuota(hdfsAdmin, new Path(args[1]), Long.parseLong(args[2]));
                    break;
                case "clrAllQuota":
                    HDFSAdminUtils.clrAllQuota(hdfsAdmin, new Path(args[1]));
                    break;
                default:
                    System.out.println("操作错误");
                    break;
            }

        }catch (Exception e){
            e.printStackTrace();
        }


    }

    public static void initKerberosENV(Configuration conf, Properties properties){
        System.setProperty("java.security.krb5.conf", confPath + File.separator + "krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", properties.getProperty("kerberos.debug"));
        try{
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(properties.getProperty("kerberos.principal"),
                    confPath + File.separator + properties.getProperty("kerberos.keytab"));
            System.out.println(UserGroupInformation.getCurrentUser());
        }catch (Exception e){
            e.printStackTrace();
        }
    }



}
