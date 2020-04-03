package com.data.hadoop.kerberos;

import com.data.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;

public class KbHdfsTest {

    private static String confPath = System.getProperty("user.dir") + File.separator + "conf";

    public static void main(String[] args) {

        System.out.println("+++++++++++++++++++confpath+++++++++++"+confPath);

        Configuration configuration = HDFSUtils.initConfiguration(confPath);
        initKerberosENV(configuration);

        try{
            FileSystem fileSystem = FileSystem.get(configuration);

            // 查看文件
            HDFSUtils.readFile(fileSystem, "/user/hive/srctabs/dc/raw/hs08_his_price/hs08_his_price.csv");
            //创建文件
            HDFSUtils.mkdir(fileSystem, "/user/test");
            //上传文件
            HDFSUtils.uploadFile(fileSystem, "/user/hs08_his_price.csv", "/user/hive/srctabs/dc/raw/hs08_his_price/hs08_his_price.csv");


        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void initKerberosENV(Configuration conf){
        System.setProperty("java.security.krb5.conf", confPath + File.separator + "krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        try{
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdfs@HADOOP.COM",
                    confPath + File.separator + "hdfs.keytab");
            System.out.println(UserGroupInformation.getCurrentUser());
        }catch (Exception e){
            e.printStackTrace();
        }
    }




}
