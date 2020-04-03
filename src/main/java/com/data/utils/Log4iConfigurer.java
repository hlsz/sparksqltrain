package com.data.utils;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.InputStream;
import java.util.Enumeration;

//echo start elasticsearch-mapping
//        java -Drunpath=/home/dinpay/esmap -Dlog4j.configuration=conf/log4j.properties -classpath .:lib/* com.dinpay.bdp.rcp.service.Start
//echo "create elasticsearch mapping successfully!"

public class Log4iConfigurer {

    private static boolean INITIALIZED = false;

    public static void initLogger(){
        if(!INITIALIZED&&!isConfigured()){
            InputStream is =Log4iConfigurer.class.getClassLoader().getResourceAsStream("log4j.properties");
            PropertyConfigurator.configure(is);
            IOUtils.closeQuietly(is);
        }
    }

    private static boolean isConfigured() {
        if(LogManager.getRootLogger().getAllAppenders().hasMoreElements()){
            return true;
        }else{
            Enumeration<?> loggers =LogManager.getCurrentLoggers();
            while(loggers.hasMoreElements()){
                Logger logger= (Logger)loggers.nextElement();
                if(logger.getAllAppenders().hasMoreElements()){
                    return true;
                }
            }
        }
        return false;
    }

}
