package com.data.utils;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtil {


    private String fileName;
    private Properties properties = new Properties();

    public PropertiesUtil(String fileName){
        this.fileName = fileName;
        open();
    }

    private void open(){
        try{
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName));
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public String readPropertiesByKey(String key){
        return properties.getProperty(key);
    }

}
