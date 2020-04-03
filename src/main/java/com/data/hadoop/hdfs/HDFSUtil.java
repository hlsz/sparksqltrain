package com.data.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 *
 * @ClassName: HDFSUtil
 *
 * @Description: TODO  HDFS系统与本地文件交互
 *
 * @author kngines
 *
 * @date 2018年4月15日
 */

public class HDFSUtil {

    private static String _URI = "hdfs://192.168.1.103:9000";

    public static void main(String[] args) {
        try {
            HDFSUtil.uploadFileFromLocation("local.txt", "/user/hadoop/cloud.txt");  // 本地文件上传
            HDFSUtil.downFileFromHDFS("cloudFromHDFS.txt", "/user/hadoop/cloud.txt");  // 远程文件下载
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    /**
     *
     * @Title: uploadFileFromLocation
     * @Description: TODO 本地文件上传至 HDFS
     * @param localFName  本地文件，eclispe项目根目录下
     * @param hdfsFName  HDFS系统下文件
     * @throws IOException void
     * @author Khgines
     * @date 2018年4月15日下午11:52:43
     */
    public static void uploadFileFromLocation(String localFName, String hdfsFName) throws IOException {

        // 云端HDFS文件路径 user/hadoop
        String hdfsURI = _URI + hdfsFName;
        InputStream in = new BufferedInputStream(new FileInputStream(localFName));

        Configuration conf = new Configuration();  // 定义conf对象
        FileSystem fs = FileSystem.get(URI.create(hdfsURI), conf);  // 创建文件系统 对象
        OutputStream out = fs.create(new Path(hdfsURI), new Progressable() {  // 输出流
            @Override
            public void progress() {
                System.out.println("上传完成一个文件到HDFS");
            }
        });
        IOUtils.copyBytes(in, out, 1024, true);  // 连接两个流，形成通道，使输入流向输出流传输数据

        in.close();
        fs.close();
        out.close();
    }

    /**
     *
     * @Title: downFileFromHDFS
     * @Description: TODO HDFS 文件下载
     * @param localFName  本地文件路径
     * @param hdfsFName  HDFS 文件路径
     * @throws FileNotFoundException
     * @throws IOException void
     * @author Khgines
     * @date 2018年4月15日下午11:53:39
     */
    private static void downFileFromHDFS(String localFName, String hdfsFName) throws FileNotFoundException, IOException {

        String hdfsFPath = _URI+hdfsFName;
        Configuration conf = new Configuration();  // 获取conf配置
        FileSystem fs = FileSystem.get(URI.create(hdfsFPath), conf);  // 创建文件系统对象
        FSDataInputStream outHDFS = fs.open(new Path(hdfsFPath));  // 从HDFS读出文件流
        OutputStream inLocal = new FileOutputStream(localFName);  // 写入本地文件
        IOUtils.copyBytes(outHDFS, inLocal, 1024, true);

        fs.close();
        outHDFS.close();
        inLocal.close();
    }

}