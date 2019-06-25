package com.data.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

public class HadoopService {

    public static Configuration conf;
    private static  String namenode;
    private static String resourcenode;
    private static String schedulernode;
    private static String jobhistorynode;

    public static Configuration getConf() {
        if(conf == null){
            conf = new Configuration();
            conf.setBoolean("mapreduce.app-submission.cross-platform",true);
            conf.set("fs.defaultFS","hdfs://"+namenode + ":8020");
            conf.set("mapreduce.framework.name", "yarn"); // 指定使用yarn框架
            conf.set("yarn.resourcemanager.address", resourcenode + ":8032"); // 指定resourcemanager
            conf.set("yarn.resourcemanager.scheduler.address", schedulernode + ":8030");// 指定资源分配器
            conf.set("mapreduce.jobhistory.address", jobhistorynode + ":10020");// 指定historyserver
        }
        return conf;
    }








}
