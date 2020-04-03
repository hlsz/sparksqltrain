package com.data.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class HdfsDemo {

    public static void main(String[] args)  throws IOException {

        Configuration configuration = new Configuration();
        String uri = args[0];
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);

        InputStream inputStream = null;

        try{
            inputStream = fs.open(new Path(uri));
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(inputStream);
        }

    }


}
