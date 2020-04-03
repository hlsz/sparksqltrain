package com.data.utils;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HadoopUtil{

    public static void addJarsToDistributedCache(Job job, String path){

    }

    public static List<String> listDirectoryAsListOfString(String inputDir, FileSystem fs  ) throws IOException {
        FileSystem[] fss = fs.getChildFileSystems();
        List<String> lists = new ArrayList<>();
        for (int i = 0; i < fss.length; i++) {
            lists.add(fss[i].getUri()+":"+fss[i].getName());
        }
        return lists;
    }

    public static boolean pathExists(Path path, FileSystem fs) {
        return false;
    }
}
