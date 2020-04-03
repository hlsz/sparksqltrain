package com.data.rythmui.util;

import com.data.rythmui.api.RythmApi;
import com.data.rythmui.config.ColGroupProperties;
import org.datanucleus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class UIHelper {

    private static final Logger logger = LoggerFactory.getLogger(UIHelper.class);

    private static final Map<String, String > fnGcssMap = new HashMap<>();

    static{
        try{
           ColGroupProperties instance = ColGroupProperties.load();
           for (Map.Entry<Object, Object> set: instance.entrySet()){
               if(set.getKey() != null){
                   fnGcssMap.put(set.getKey().toString(), (set.getValue() == null) ? "":set.getValue().toString());
               }
           }
        }catch(Exception e) {
          logger.debug("UIHelper init", e);
        }
    }


    public static String replaceCss(String originalCss){
        if(StringUtils.isEmpty(originalCss)){
            return originalCss;
        }

        String[] cssArray = StringUtils.split(originalCss, " ");
        List<String> lstRet = new ArrayList<>();

        if(cssArray != null && cssArray.length > 0){
            for(String css: cssArray){
                css = org.apache.commons.lang3.StringUtils.trim(css);
                if (fnGcssMap.containsKey(css)){
                    lstRet.add(fnGcssMap.get(css));
                }else {
                    lstRet.add(css);
                }
            }
        }
        return org.apache.commons.lang3.StringUtils.join(lstRet," ");
    }

    public static String matchRythmComment(Object value){
        return RythmApi.fnMatchAll(value, "<!--%rythm([\\s\\S]*?)mhtyr%-->");
    }

    public static String matchToolBar(Object value){
        return RythmApi.fnMatchAll(value, "<!--%rythm-toolbar([\\s\\S]*?)rabloot-mhtyr%-->");
    }

    public static String matchConfig(Object value){
        return RythmApi.fnMatchAll(value, "<!--%rythm-config%-->([\\s\\S]*?)<!--%gifnoc-mhtyr%-->");
    }

    public static String matchColumns(Object value){
        return RythmApi.fnMatchAll(value, "<!--%rythm-columns([\\s\\S]*?)snmuloc-mhtyr%-->");
    }

    public static String matchRythmComments(Object value){
        return RythmApi.fnMatchAll(value, "<!--%rythm([\\s\\S]*?)mhtyr%-->");
    }

    public static String matchIconLink(Object value){
        return RythmApi.fnMatchAll(value, "<a class=\"iconlink-rythm[\\s\\S]*?</a>");
    }
    public static String findHtmlAttrValue(String content, String attrNam){
        String first = RythmApi.fnSplitTwo(content, attrNam + "=\"")[1];
        int index = first.indexOf("\"");
        if(index > -1){
            return first.substring(0, index);
        }
        return first;
    }




}
