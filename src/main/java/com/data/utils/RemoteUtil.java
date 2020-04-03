package com.data.utils;


import bsh.StringUtil;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import com.ning.compress.BufferRecycler;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class RemoteUtil {

    private static String DEFAULTCHART = "UTF-8";
    private Connection conn;
    private String host;
    private String userName;
    private String userPwd;

    public RemoteUtil(String host, String userName, String userPwd){
        this.host = host;
        this.userName = userName;
        this.userPwd = userPwd;

    }

    public boolean login(){
        boolean flag = false;
        try{
            conn = new Connection(host);
            conn.connect();
            flag = conn.authenticateWithPassword(userName, userPwd);
        }catch (Exception e){
            e.printStackTrace();
        }
        return flag;
    }

    public String execute(String cmd){
        String result = "";
        try{
            if(login()){
                System.out.println("登录成功");
                Session session = conn.openSession();
                session.execCommand(cmd);
//                session.getStdout();
                result = processStdout(session.getStdout(), DEFAULTCHART);
                if (StringUtils.isBlank(result)){
                    result = processStdout(session.getStderr(), DEFAULTCHART);
                }
                conn.close();
                session.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return result;
    }

    private String processStdout(InputStream in, String charset){
        // 接收目标服务器上的控制台返回结果
        InputStream stdout = new StreamGobbler(in);
        StringBuffer buffer = new StringBuffer();
        try{
            //将控制台的返回结果包装成BufferedReader
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout,charset));
            String line = null;
            while((line = br.readLine()) != null){
                buffer.append(line + "\n");
            }
            br.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return buffer.toString();
    }

    public static  void setCharset(String charset){
        DEFAULTCHART = charset;
    }

}
