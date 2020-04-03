package com.java.test;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.Test;
import sun.security.krb5.Config;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

public class HDFSDemo {

    FileSystem fs;
    String uri="hdfs://localhost:9000"; //对应于core-site.xml中的FS.default

    /**
     获取FileSystem
     * FileSystem是HDFS的一个抽象，用于操作HDFS
     * 里面的操作等同于HADOOP_HOME/bin/hdfs dfs里的操作
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    public void getFileSystem() throws  IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        // 可通过指定的方式来获取， 也可通过加载core-site.xml来获取
        // hdfs 为超级用户
        fs = FileSystem.get(new URI(uri), conf, "hdfs");
    }

    @Test
    public void testConnect() throws  Exception {
        Configuration configuration = new Configuration();
        //还可以通过conf.set(name,value)指定属性的的值，如果指定了以它为主。
        //conf.set("dfs.replication", "1");
        //如果不指定则使用Configuration默认值
        //FileSystem是Hadoop的文件系统的抽象类，HDFS分布式文件系统只是Hadoop文件系统中的一种，对应的实现类：
        //org.apache.hadoop.hdfs.DistributedFileSystem。HDFS是Hadoop开发者最常用的文件系统，
        //因为HDFS可以和MapReduce结合，从而达到处理海量数据的目的
        //Hftp：基于HTTP方式去访问HDFS文件提供（只读）
        //hdfs:可以读取、写入、改等操作。
        //get(URI:制定位置"ip:9000",conf)

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.22.33:9000"), configuration);
        System.out.println(fs);
        fs.close();
    }

    @Test
    public void testMkdir() throws Exception {
        FileSystem fs = FileSystem.get(
                new URI("hdfs://localhost:9000"),
                new Configuration(),
                "hdfs");

        fs.mkdirs(new Path("/user/analyai"));
        fs.close();
    }

    @Test
    public void testCopyToLocal() throws  Exception {
        FileSystem fs = FileSystem.get(
                new URI("hdfs://localhost:9000"),
                new Configuration(),
                "hdfs"
        );

        FSDataInputStream fsin = fs.open(new Path("/user/analyai/tt.txt"));
        OutputStream out = new FileOutputStream(
                new File("1.txt")
        );

        int len = -1;
        byte[] bts = new byte[1024];
        while((len = fsin.read(bts)) != -1){
            out.write(bts, 0, len);
        }

        IOUtils.copy(fsin, out);

        out.close();
        fsin.close();
        fs.close();

    }

    @Test
    public void testCopyFromLocal() throws  Exception {
        Configuration conf = new Configuration();
        //如果没有通过conf.set("dfs.replication","1")指定副本的数量，
        //默认是3份（即使在hdfs-site.xml中配置了1）

        conf.set("dfs.replication", "1");
        FileSystem fs = FileSystem.get(
                new URI("hdfs://localhost:9000"),
                conf,
                "hdfs"
        );
        FSDataOutputStream out = fs.create(new Path("/user/analyai/test/1.txt"));
        InputStream in = new FileInputStream("1.txt");
        IOUtils.copy(in, out);
        in.close();
        out.close();
        fs.close();

    }

    public void createDir() throws  IOException {
        String uri = "/user/analyai/output";
        Path path = new Path(uri);
        fs.mkdirs(path);
    }

    public void createFile() throws IOException{
        Path path = new Path("/user/analyai/output/ai.txt");
        FSDataOutputStream out = fs.create(path);
        String data = "Hello world";
        out.writeChars(data);
    }

    public void deleteFile() throws IOException {
        Path path = new Path("/user/analyai/output/ai.txt");
        if(fs.exists(path)){
            fs.delete(path,false);
        }else{
            System.out.println(path.getName()+" is not exits");
        }
    }

    /**
     * 删除文件夹或文件
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public void readFile() throws  IOException, IllegalArgumentException {
        Path path  = new Path("/user/analyai/output/ai.txt");

        if(fs.isFile(path)){
            ByteBuffer buf = ByteBuffer.allocate(1024);
            FSDataInputStream file = fs.open(path);
            int read = 0;
            while((read = file.read(buf)) != -1) {
                System.out.println(new String(buf.array()));
                buf.clear();
            }
        }
    }

    /**
     * 展示列表文件
     * @throws Exception
     */
    public void listFiles() throws FileNotFoundException, IOException {
        Path path = new Path("/user/analyai/outpu/ai.txt");

        FileStatus[] listStatus = fs.listStatus(path);
        for(FileStatus fileStatus: listStatus){
            System.out.println(fileStatus);
        }
        //展示所有的文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(path, false);
        LocatedFileStatus next = null;
        while(listFiles.hasNext()){
            next = listFiles.next();
            System.out.println(next);
        }
    }

    public void queryPosition() throws  IOException {
        Path path = new Path("/user/analyai/output/ai.txt");
        FileStatus fileStatus = fs.getFileStatus(path);

        //获取文件所在集群位置
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for(BlockLocation blockLocation: fileBlockLocations){
            System.out.println(blockLocation); //0,120,hadoop
        }

        //获取checksum
        FileChecksum fileChecksum = fs.getFileChecksum(path);
        // MD5-of-0MD5-of-512CRC32C:cb95b700877b44dab0fcfeb617d7f95d
        System.out.println(fileChecksum);

        //获取集群中的所有节点信息
        DistributedFileSystem dfs = (DistributedFileSystem)fs;
        DatanodeInfo[] dataNodeStats = dfs.getDataNodeStats();
        for(DatanodeInfo datanodeInfo: dataNodeStats){
            System.out.println(datanodeInfo);//192.168.123.131:50010
        }
    }

    @Test
    public void readHDFSFile(){
        HDFSDemo d = new HDFSDemo();
        try{

            d.getFileSystem();
            d.deleteFile();
            d.createDir();
            d.createFile();
            d.listFiles();
            d.readFile();
            d.queryPosition();
        }catch (IllegalArgumentException e){
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }catch (URISyntaxException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }







}
