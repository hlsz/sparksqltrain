package com.data.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {


    public static String URI = null;
    public static Configuration conf = null;
    private static FileSystem fs;

    static  {
        try {
            URI = "hdfs://hadoop02:9000";
            conf = new Configuration();
            fs = FileSystem.get(new URI(URI), conf, "zjl");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        //Configuration conf = new Configuration();
        conf.set("fs.defaultFs",URI);
        conf.set("dfs.replication","2");

//        FileSystem fs= FileSystem.get(conf);
//        FileSystem fs = FileSystem.get(new URI(URI), conf, "zjl");

        fs.mkdirs(new Path("/user/zjl"));

        fs.close();

        System.out.println("close");
    }


    /**
     * 上传文件
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void copyFromLocalFile() throws  IOException {
//        Configuration conf = new Configuration();
        conf.set("dfs.replication","2");
//        FileSystem fs = FileSystem.get(new URI(URI), conf, "zjl");
        fs.copyFromLocalFile(new Path("/aaa.txt"), new Path("/user/zjl/"));

        fs.close();
    }

    /**
     * 文件下载
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void copyLToLocalFile() throws  IOException {
//        Configuration conf = new Configuration();

//        FileSystem fs = FileSystem.get(new URI(URI), conf, "zjl");

        fs.copyToLocalFile(true, new Path("/user/zjl/aaa.txt"), new Path("/home/zjl/aaa.txt"), true);

//        fs.copyToLocalFile(new Path("/user/zjl/aaa.txt"), new Path("/home/zjl/aaa.txt"));

        fs.close();
    }

    /**
     * 文件删除
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void deleteFile() throws   IOException{

        fs.delete(new Path("/user/zjl"), true);
        fs.close();
    }

    /**
     * 重命名
     * @throws IOException
     */
    public void renameFile()  throws IOException {
        fs.rename(new Path("/user/zjl/aaa.txt"), new Path("/user/zjl/bbb.txt"));
        fs.close();
    }

    /**
     * 查看文件信息
     * @throws IOException
     */
    public void listFile() throws IOException {

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/usr/zjl"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName()+"\r\n"+fileStatus.getPermission()+"\r\n"+fileStatus.getLen() );

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("------------分隔线--------------");
        }
        fs.close();
    }

    /**
     * 检查文件是文件还是文件夹
     * @throws IOException
     */
    public void listStatus() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/user/zjl"));

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()){
                System.out.println("f:"+fileStatus.getPath().getName());
            }else{
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        fs.close();
    }

    /**
     * 文件上传下载
     */
    public void putFileToHDFS() throws IOException {

        //获取输入流
        FileInputStream fileInputStream = new FileInputStream(new File("/user/anna/aaa.txt"));

        //获取输出流
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/user/zjl/aaa.txt"));

        //流的对拷
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream,conf);

        //关闭资源
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fsDataOutputStream);
        fs.close();
    }


    /**
     * 下载
     */
    public void getFileFromHDFS () throws IOException {

        //获取输入流
        FSDataInputStream open = fs.open(new Path("/user/zjl/aaa.txt"), 1);

        //获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/user/anna/aaa.txt"));

        //流的拷贝
        IOUtils.copyBytes(open, fileOutputStream, conf);

        //闭资源
        IOUtils.closeStream(open);
        IOUtils.closeStream(fileOutputStream);
        fs.close();

    }

    /**
     * 定位文件读取
     */
    public void readFileSeek1() throws IOException{

        // 获取输入流
        FSDataInputStream open = fs.open(new Path("/user/zjl/aaa.txt"));

        //获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/user/anna/aaa.txt"));

        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            open.read(buf);
            fileOutputStream.write(buf);
        }

        IOUtils.closeStream(open);
        IOUtils.closeStream(fileOutputStream);
        fs.close();
    }


    /**
     *
     * @throws IOException
     */
    public void readFileSeek2() throws IOException{

        FSDataInputStream fsDataInputStream = fs.open(new Path("/user/zjl/aaa.txt"));

        //设置指定读取的起点
        fsDataInputStream.seek(1024 * 1024 * 128);

        FileOutputStream fileOutputStream = new FileOutputStream(new File("/user/anna/aaa.txt.2"));

        IOUtils.copyBytes(fsDataInputStream, fileOutputStream, conf);

        IOUtils.closeStream(fsDataInputStream);
        IOUtils.closeStream(fileOutputStream);
        fs.close();
    }


    /**
     *
     */









}
