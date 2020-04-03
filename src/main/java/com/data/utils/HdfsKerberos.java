package com.data.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import sun.security.krb5.Config;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class HdfsKerberos {

    public static final String KEYTAB_FILE_KEY="hdfs.keytab";
    public static final String USER_NAME_KEY= "hdfs.kerberos.principal";
    private static Configuration conf = new Configuration();

    private static String confPath = System.getProperty("user.dir")+
            File.separator+ "kb-conf";

    public static void main(String[] args) throws IOException {

        System.setProperty("java.security.krb5.conf", confPath+ File.separator+"krb5.conf");
        System.setProperty("java.security.auth.useSubjectCredsOnly", "false");

        conf.set(KEYTAB_FILE_KEY, confPath + File.separator + "hdfs.keytab");
        conf.set(USER_NAME_KEY , "hdfs@HADOOP.COM");

        /**
         * 启动KERBEROS认证
         *
         */
        conf.setBoolean("hadoop.security.authorization", true);
        conf.set("hadoop.security.authentication", "Kerberos");//首字母大写

        conf.set("fs.default.name","hdfs://vt-hadoop2:8020");
        conf.set("mapred.job.tracker","hdfs://vt-hadoop2:8030");

        conf.set("df.namenode.kerberos.principal","hdfs@HADOOP.COM");
        conf.set("fs.hdfs.impl","DistributeFileSystem.class.getName()");
        conf.set("fs.file.impl","LocalFileSystem.class.getName()");

        /**
         * 添加hdfs-site.xml, core-site.xml
         *
         */
        conf.addResource(new Path(confPath + "core-site.xml"));
        conf.addResource(new Path(confPath +  "hdfs-site.xml"));
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("hdfs@HADOOP.COM", confPath + File.separator + "hdfs.keytab");

        login(conf);

        System.out.println(loadHdfsFile("/user/hive/srctabs/dc/raw/hs08/hs08_his_price/hs08_his_price.csv"));;

    }


    public static List<String> loadHdfsFile(String filePath) {

        List<String> resultList = new ArrayList<>();
        FileSystem fileSystem = null;

        try{
            fileSystem = FileSystem.get(conf);

            FSDataInputStream fs = fileSystem.open(new Path(filePath));
            BufferedReader bis = new BufferedReader(new InputStreamReader(fs, "UTF-8"));
            String line;
            while((line = bis.readLine()) != null){
                resultList.add(line);
            }
            fileSystem.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }

    public static void login(Configuration hdfsConfig) throws IOException {
        boolean securityEnabled = UserGroupInformation.isSecurityEnabled();
        if (securityEnabled) {
            String keytab = conf.get(KEYTAB_FILE_KEY);
            if(keytab != null) {
                hdfsConfig.set(KEYTAB_FILE_KEY, keytab);
            }
            String userName = conf.get(USER_NAME_KEY);
            if(userName != null) {
                hdfsConfig.set(USER_NAME_KEY, userName);
            }
            SecurityUtil.login(hdfsConfig, KEYTAB_FILE_KEY, USER_NAME_KEY);
        }
    }




}
