package com.data.service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
/**
 * @function 将指定格式的多个文件上传至 HDFS
 * 使用文件模式，实现多文件上传至HDFS
 * @author 小讲
 *
 */
@SuppressWarnings("unused")
public class CopyManyFilesToHDFS {

    private static FileSystem fs = null;//FileSystem实例对象，即fs
    private static FileSystem local = null;//FileSystem实例对象，即Local,本地文件系统

    /**
     * @function Main 方法
     * @param args
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void main(String[] args) throws IOException,URISyntaxException {
        //文件上传路径
//        Path dstPath = new Path("hdfs://djt002:9000/outData/copyManyFilesToHDFS/");//这样会在这个默认的copyManyFilesToHDFS.txt里
        Path dstPath = new Path("hdfs://djt002:9000/outCopyManyFilesToHDFS/");//要么，你先可以新建好outCopyManyFilesToHDFS这个目录


        //调用文件上传 list 方法
        list(dstPath);
    }

    /**
     * function 过滤文件格式   将多个文件上传至 HDFS
     * @param dstPath 目的路径
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void list(Path dstPath) throws IOException, URISyntaxException {
        //读取hadoop文件系统的配置
        Configuration conf = new Configuration();
        //HDFS 接口
        URI uri = new URI("hdfs://djt002:9000");

//        URL、URI与Path三者的区别
//        Hadoop文件系统中通过Hadoop Path对象来代表一个文件
//        URL（相当于绝对路径）    ->   （文件） ->    URI（相当于相对路径，即代表URL前面的那一部分）
//        URI：如hdfs://dajiangtai:9000
//        如，URL.openStream


        //获得FileSystem实例fs
        fs = FileSystem.get(uri, conf);
//        返回类型是FileSystem，等价于  FileSystem fs = FileSystem.get(uri, conf);


        //获得FileSystem实例，即Local
        local = FileSystem.getLocal(conf);
//        返回类型是LocalFileSystem，等价于  LocalFileSystem  local = FileSystem.getLocal(conf);

//        为什么要获取到Local呢，因为，我们要把本地D盘下data/74目录下的文件要合并后，上传到HDFS里，所以，我们需先获取到Local，再来做复制工作啦！


        //只上传data/testdata 目录下 txt 格式的文件
        FileStatus[] localStatus = local.globStatus(new Path("D://data/74/*"),new RegexAcceptPathFilter("^.*txt$"));
//        FileStatus[] localStatus = local.globStatus(new Path("./data/copyManyFilesToHDFS/*"),new RegexAcceptPathFilter("^.*txt$"));
//        ^表示匹配我们字符串开始的位置               *代表0到多个字符                        $代表字符串结束的位置
//        RegexAcceptPathFilter来只接收我们需要的，即格式
//        RegexAcceptPathFilter这个方法我们自己写

//        但是我们，最终是要处理文件里的东西，最终是要转成Path类型，因为Path对象f,它对应着一个文件。

        //获取74目录下的所有文件路径，注意FIleUtil中stat2Paths()的使用，它将一个FileStatus对象数组转换为Path对象数组。
        Path[] listedPaths = FileUtil.stat2Paths(localStatus);//localStatus是FileStatus数组类型

        for(Path p:listedPaths){//for星型循环，即将listedPaths是Path对象数组，一一传给Path p
            //将本地文件上传到HDFS
            fs.copyFromLocalFile(p, dstPath);
            //因为每一个Path对象p，就是对应本地下的一个文件，

        }
    }

    /**
     * @function 只接受 txt 格式的文件aa
     * @author 小讲
     *
     */
    public static class RegexAcceptPathFilter implements PathFilter {
        private final String regex;//变量

        public RegexAcceptPathFilter(String regex) {
            this.regex = regex;//意思是String regex的值，赋给当前类RegexAcceptPathFilter所定义的private final String regex;
        }

        public boolean accept(Path path) {//主要是实现accept方法
            // TODO Auto-generated method stub
            boolean flag = path.toString().matches(regex);//匹配正则表达式，这里是^.*txt$
            //只接受 regex 格式的文件
            return flag;//如果要接收 regex 格式的文件，则accept()方法就return flag; 如果想要过滤掉regex格式的文件，则accept()方法就return !flag。
        }
    }
}
