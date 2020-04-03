package com.data.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class Util {

    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    /**mapping.properties file name*/
    public static final Properties props = new Properties();
    public static final String CONF_PROPERTIES_FILE= "mapping.properties";
    public static final String CONF = "RUN_CONF";

    static{
        Log4iConfigurer.initLogger();
        getProperties();
    }

    public static String GetString(String key){
        String value = null;
        value = props.getProperty(key);
        return value;
    }

    //读取配置文件
    private static File getConfProperties(){
        String confHome = System.getProperty(CONF);
        if(!StringUtils.isEmpty(confHome)){
            logger.info("Use CONF="+confHome);
            return getPropertiesFile(confHome);
        }

        logger.warn("Conf property was not set ,will seek conf env variable");

        String runHome = getRunHome();
        if(StringUtils.isEmpty(runHome)){
            throw new RuntimeException("didn't find project runpath,please set");
        }
        String path = runHome+File.separator+"conf";
        return getPropertiesFile(path);
    }

    public static void getProperties(){
        File propFile = getConfProperties();
        if(propFile == null||!propFile.exists()){
            logger.info("fail to load properties");
            throw new RuntimeException("fail to load properties");
        }
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(propFile);
            props.load(fis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    private static File getPropertiesFile(String path){
        if(path == null){
            return null;
        }
        return new File(path,CONF_PROPERTIES_FILE);
    }

    private static String getRunHome(){
        String runHome = System.getProperty("runpath");
        if(StringUtils.isEmpty(runHome)){
            logger.warn("run home was not set");
        }
        return runHome;
    }


    public static void main(String[] args)   {
//        System.out.println(System.getenv("PATH"));
//        System.out.println(System.getProperty("conf"));
//        System.setProperty("run.home", "/home/dinpay/mappinghome");
//        System.out.println(System.getProperty("run.home"));
        JSONArray jsonArray = new JSONArray();
        JSONObject jb = new JSONObject();
        jb.put("returns", 0.22);
        jb.put("vari",0.2344);
        jb.put("weight",new double[]{0.1,0.23,0.3});
        jsonArray.add(jb);
        JSONObject j1 = new JSONObject();
        j1.put("returns",0.89);
        j1.put("vari",0.1234);
        j1.put("weigth",new double[]{0.21,0.31,0.56});
        jsonArray.add(j1);
        StringBuffer stringBuffer = new StringBuffer();
        jsonArray.stream().forEach(jsonObject->arrayIdToString((JSONObject) jsonObject, stringBuffer));

    }

    private static StringBuffer arrayIdToString(JSONObject jsonObject,
                                                StringBuffer stringBuffer) {
        return stringBuffer.append(jsonObject.getDouble("returns")).append(",");
    }

    public static void f2(JSONArray ja) {
        for(int i=0;i<ja.size();i++) {
            System.out.println(ja.getJSONObject(i).get("id"));
        }
    }
    public String listToString(List list, char separator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i)).append(separator);
        }
        return sb.toString().substring(0, sb.toString().length() - 1);
    }
    public String listToString2(List list, char separator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            if (i == list.size() - 1) {
                sb.append(list.get(i));
            } else {
                sb.append(list.get(i));
                sb.append(separator);
            }
        }
        return sb.toString();
    }

    public String listToString3(List list, char separator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i));
            if (i < list.size() - 1) {
                sb.append(separator);
            }
        }
        return sb.toString();
    }

    public String listToString4(List<String> list, char separator) {
        StringBuilder sb = new StringBuilder();
        for (String s : list) {
            if (s != null && !"".equals(s)) {
                sb.append(separator).append(s);
            }
        }
        return sb.toString();
    }

    public String listToString5(List list, char separator) {
        return org.apache.commons.lang.StringUtils.join(list.toArray(), separator);
    }

}
