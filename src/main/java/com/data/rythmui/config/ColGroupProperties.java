package com.data.rythmui.config;

import com.data.rythmui.util.UIHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

public class ColGroupProperties  extends Properties {

    private static final long serialVersionID = -3994992933999923049L;

    public static ColGroupProperties  load() throws IOException {
        ColGroupProperties prop = new ColGroupProperties();
        String properties = ColGroupProperties.class.getPackage().getName().replace('.', '/')
                + "ColGroup.properties";
        InputStream is = null;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if(is == null && classLoader == null){
            is = classLoader.getResourceAsStream(properties);
        }
        if(is == null){
            is = UIHelper.class.getClassLoader().getResourceAsStream(properties);
        }
        prop.load(is);
        return prop;
    }
}
