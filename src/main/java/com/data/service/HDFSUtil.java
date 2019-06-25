package com.data.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class HDFSUtil {

    public static void read(String path) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = null;
        try{
            FSDataInputStream fsinput = fs.open(new Path(path));
            reader = new BufferedReader(new InputStreamReader(fsinput, "UTF-8"), 256);
            String line = null;
            while (true){
                line = reader.readLine();
                if(line == null){
                    break;
                }
                System.out.println(line);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(reader);
        }
    }

    public static void create(String parent, String file) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        BufferedWriter writer = null;
        try{
            FSDataOutputStream fsOut = fs.create(new Path(parent, file), true);
            writer = new BufferedWriter(new OutputStreamWriter(fsOut, "UTF-8"));
            writer.write("hello:hadoop");
            writer.newLine();
            writer.write("Hello:HDFS");
            writer.flush();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            writer.close();
        }
    }



    public void copyFile(String src,String dst){
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "localhost:9001");
        conf.set("fs.default.name", "hdfs://localhost:9000");
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(conf);
            Path srcPath = new Path(src);
            Path dstPath = new Path(dst);
            hdfs.copyFromLocalFile(srcPath, dstPath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally{
            if(hdfs != null){
                try {
                    hdfs.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    //创建新文件
    public void createFile(String dst){
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker","localhost:9001");
        conf.set("fs.default.name","hdfs://localhost:9000");
        FileSystem fs = null;
        FSDataOutputStream out = null;
        try{
            String content = "hello hadoop";
            Path path = new Path(dst);
            fs = FileSystem.get(conf);
            out = fs.create(path);
            out.write(content.getBytes());
            out.flush();
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            if(out != null){
                try {
                    out.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if(fs != null){
                try {
                    fs.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public void renameFile(String str,String dst){
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker","localhost:9001");
        conf.set("fs.default.name","hdfs://localhost:9000");
        FileSystem fs = null;
        try{
            fs = FileSystem.get(conf);
            Path srcPath = new Path(str);
            Path dstPath = new Path(dst);
            fs.rename(srcPath, dstPath);
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            if(fs != null){
                try {
                    fs.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public void deleteFile(String src){
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker","localhost:9001");
        conf.set("fs.default.name","hdfs://localhost:9000");
        FileSystem fs = null;
        try{
            fs = FileSystem.get(conf);
            fs.delete(new Path(src));
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            if(fs != null){
                try {
                    fs.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args){
        System.setProperty("HADOOP_USER_NAME", "pijing");
        String src = "/home/pijing/input/test1";
        String dst = "/";
        HDFSUtil learn = new HDFSUtil();
//		learn.copyFile(src, dst);
//		System.out.println("copy file from local file system to HDFS");
//		learn.createFile("/test2");
//		System.out.println("create file test2");
//		String org = "/test2";
//		String lasted = "/test3";
//		learn.renameFile(org, lasted);
//		System.out.println("rename file /test2 to /test3");
        learn.deleteFile("/test3");
        System.out.println("delete file /test3");
    }

    
}
