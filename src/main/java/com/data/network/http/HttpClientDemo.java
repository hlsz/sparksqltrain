package com.data.network.http;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpClientDemo {

    @Test
    public void urlTest() throws IOException {

        long begintime = System.currentTimeMillis();

        URL url = new URL("http://www.baidu.com");
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.connect();
        InputStream is = urlConnection.getInputStream();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
        StringBuffer bs = new StringBuffer();
        String l = null;
        while((l = bufferedReader.readLine()) !=null){
            bs.append(l).append("\n");
        }
        System.out.println(bs.toString());

        System.out.println("总执行时间："+(System.currentTimeMillis() - begintime)+ "毫秒");

    }
}
