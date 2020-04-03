package com.data.hadoop.demo;

import com.data.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;

public class Lauch {

    public static String confPath = System.getProperty("user.dir"+ File.separator + "hdfs-conf");

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        try{
            FileSystem fileSystem = FileSystem.get(configuration);
            String operation = null;
            switch(operation){
                case "mkdir":
                    HDFSUtils.mkdir(fileSystem, args[0]);
                    break;
                case "upload":
                    HDFSUtils.uploadFile(fileSystem, args[1], args[2]);
                    break;
                case "createFile":
                    HDFSUtils.createFile(fileSystem, args[1], args[2]);
                    break;
                case "rename":
                    HDFSUtils.rename(fileSystem, args[1], args[2]);
                    break;
                case "readFile":
                    HDFSUtils.readFile(fileSystem, args[1]);
                    break;
                case "deleteFile":
                    HDFSUtils.delete(fileSystem, args[1]);
                    break;
                default:
                        System.out.println("操作错误");
                        break;
            }
            fileSystem.close();
        }catch (IOException e){
            e.printStackTrace();
        }


    }


}
