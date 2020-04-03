package com.java.test;

import com.data.utils.HDFSUtil;
import com.data.utils.PropertiesUtil;
import com.data.utils.RemoteUtil;

import java.util.List;

public class UtilTest {
    public static void main(String[] args) {


        PropertiesUtil propertiesUtil = new PropertiesUtil("system.properties");
        String host = propertiesUtil.readPropertiesByKey("hostName");
        String userName = propertiesUtil.readPropertiesByKey("userName");
        String userPwd = propertiesUtil.readPropertiesByKey("userPwd");
        HDFSUtil hdfsUtil = new HDFSUtil(host);
        List<String> list = hdfsUtil.getFileInfo("/");
        for(String string: list){
            System.out.println(string);
        }
        RemoteUtil remoteUtil = new RemoteUtil(host, userName, userPwd);
        String bin = propertiesUtil.readPropertiesByKey("hadoopBinHome");

        String result = remoteUtil.execute(bin+ "/hdfs dfs -ls /");
        System.out.println(result);
        result = remoteUtil.execute("source .bash_profile && hdfs dfs -ls /");
        System.out.println(result);


    }
}
