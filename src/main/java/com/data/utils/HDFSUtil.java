package com.data.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class HDFSUtil {
    FileSystem fs;
    String host;

    public HDFSUtil(String host){
        this.host = host;
    }

    /**
     * 删除某一路径
     * @param path 需要删除的路径
     * @param recursive 指定为true删除目录中全部文件，false时可以删除空目录和单个文件
     * @return
     */
    public boolean delete(String path, boolean recursive) {
        boolean result = false;
        if (recursive) {
            try{
                result = fs.delete(new Path(path), true);
            } catch (Exception e){
                e.printStackTrace();
                result  = false;
            }
            return result;
        }else{
            try{
                result = fs.delete(new Path(path), false);
            } catch (Exception e){
                e.printStackTrace();
                result = false;
            }
            return result;
        }
    }

    /**
     * 获得某一路径下的文件信息
     * @param path 待查看路径
     * @return 文件信息列表-包含文件类型，文件大小，所有者，所在组，文件名称
     */
    public List<String> getFileInfo(String path) {
        List<String> infos = new ArrayList<>();
        try {
            FileStatus[] fileStatuses = fs.listStatus(new Path(path));
            for (FileStatus temp : fileStatuses){
                String info = "";
                if(temp.isDirectory()){
                    info += "目录\t"+"0"+"\t";
                }else{
                    info +="文件\t"+sizeFormat(temp.getLen())+"\t";
                }
                //拼接文件信息
                info += temp.getOwner()+"\t"+temp.getGroup()+"\t"+temp.getPath().getName();
                infos.add(info);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return infos;
    }

    /**
     * 文件大小单位换算
     * @param length 默认获得的文件大小单位为Byte字节
     * @return
     */
    private String sizeFormat(long length){
        long result = length;
        if (result / 1024 == 0){
            return result + "B";
        }else{
            result /= 1024;
            if (result / 1024 ==0 ){
                return result +"KB";
            }else{
                return result/1024 +"MB";
            }
        }
    }

    public boolean write(String src, String parentDir, String fileName, boolean overwrite){
        if (!new File(src).exists()) {
            System.out.println("源文件不存在");
            return false;
        }
        FSDataOutputStream fsDataOutputStream = null;
        boolean isDir = false;
        try{
            //由于HDFS的特殊性，必须保证父级路径是一个目录，而不能只判断是否存在
            isDir = fs.isDirectory(new Path(parentDir));
        }catch (Exception e){
            e.printStackTrace();
        }
        if(!isDir){ //false 可能为文件也可能不存在
            try{
                //尝试创建父级目录
                fs.mkdirs(new Path(parentDir));
            }catch (Exception e){
                //出现异常说明该路径已经存了文件 与目标文件夹文件相同
                e.printStackTrace();
                System.out.println("该路径不可用");
                return false;
            }
        }
        Path destPath = new Path(parentDir + File.separator + fileName);
        if (overwrite){
            try{
                //覆盖写入时使用create 方法进行创建，指定覆盖参数true
                fsDataOutputStream =fs.create(destPath, true);
            }catch (Exception e){
                e.printStackTrace();
            }
        }else {
            try{
                //保证文件一定存在，如果已经存在返回false，不会重新创建
                fs.createNewFile(destPath);
                //追加写入时使用append方法进行创建
                fsDataOutputStream = fs.append(destPath);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        //初始化输入流，指定编码
        BufferedReader bufferedReader = null;
        Writer writer = null;
        try{
            bufferedReader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(new File(src)), "UTF-8"));
            writer = new OutputStreamWriter(fsDataOutputStream, "UTF-8");
        }catch (Exception e){
            e.printStackTrace();
        }

        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        String temp = null;
        int line  =0;
        try{
            while((temp = bufferedReader.readLine()) != null){
                bufferedWriter.write(temp);
                bufferedWriter.newLine();
                line ++;
                //每一千行写入一次数据
                if (line % 1000 == 0){
                    bufferedWriter.flush();
                }
            }
        }catch (IOException e){
            e.printStackTrace();
            return false;
        }
        try{
            bufferedWriter.flush();
            bufferedWriter.close();
            writer.close();
            bufferedReader.close();
            fsDataOutputStream.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return true;
    }

    public static void read(String path) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = null;
        try{
            //使用Open方法获得一个输入流
            FSDataInputStream fsinput = fs.open(new Path(path));
            //使用缓冲流读取文件内容
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
        String host = "localhost";
        HDFSUtil learn = new HDFSUtil(host);
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
