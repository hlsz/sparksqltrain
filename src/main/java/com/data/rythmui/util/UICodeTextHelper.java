package com.data.rythmui.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public abstract  class UICodeTextHelper {

    private static final Logger logger = LoggerFactory.getLogger(UICodeTextHelper.class);

    public static List<UICodeText> convertObjectToCodeText(final Object value, final String codeField,
                                                           final String textField){
        List<UICodeText> retList = new ArrayList<>();
        if(value == null){
            return null;
        }
        if(value instanceof Map) {
            retList.addAll(convertMapToCodeText((Map<String, String>)value));
        }else if(value instanceof  String) {
            String strValue = (String) value;
            if(strValue.startsWith("{")){
                retList.addAll(convertJsonStrToCodeText(strValue));
            }else if(strValue.startsWith("[")){
                retList.addAll(convertJsonArrayStrToCodeText(strValue, codeField, textField));
            }else {
                retList.addAll(convertStrToCodeText(strValue,codeField,textField));
            }
        }else if(value instanceof String[]){
            for (String str : (String[])value) {
                retList.add(new UICodeText.Impl(str, str));
            }
        }else if(value instanceof Collection) {
            retList.addAll(convertCollectionToCodeText((Collection<?>) value, codeField, textField));
        }else{

        }
        return retList;
    }

    public static List<UICodeText> convertStrToCodeText(final String value, final String codeField,
                                                        String textField) {
        List<UICodeText> retList = new ArrayList<>();
        if(value == null) {
            return retList;
        }
        String[] valueArray = StringUtils.split(value, ",");
        if(valueArray != null){
            for (String str : valueArray) {
                retList.add(new UICodeText.Impl(str, str));
            }
        }
        return retList;
    }

    public static List<UICodeText> convertJsonStrToCodeText(final String value) {
        List<UICodeText> retList = new ArrayList<>();
        if(value == null) {
            return retList;
        }
        LinkedHashMap<String, String> jsonMap  = JSON.parseObject(value,
                new TypeReference<LinkedHashMap<String, String>>(){
                });

        for (Map.Entry<String, String> entry : jsonMap.entrySet()) {
            retList.add(new UICodeText.Impl(entry.getKey(), entry.getValue()));
        }
        return retList;
    }

    public static List<UICodeText> convertMapToCodeText(final Map<String,String> value) {
        List<UICodeText> retList = new ArrayList<>();
        if(value == null) {
            return retList;
        }
        for (Map.Entry<String, String> entry : value.entrySet()) {
            retList.add(new UICodeText.Impl(entry.getKey(), entry.getValue()));
        }
        return retList;
    }

    public static List<UICodeText> convertJsonArrayStrToCodeText(final String  value,
                                                                 final String codeField,
                                                                 final String textField) {
        List<UICodeText> retList = new ArrayList<UICodeText>();
        if(value == null) {
            return retList;
        }
        JSONArray jsonArray = JSON.parseArray(value);
        if(jsonArray ==null){
            return retList;
        }
        for (Object obj : jsonArray) {
            if(obj != null && obj  instanceof JSONObject) {
                JSONObject jsonObject = (JSONObject) obj;
                String code = jsonObject.getString(codeField);
                String text = jsonObject.getString(textField);
                retList.add(new UICodeText.Impl(code, text));
            }
        }
        return retList;
    }

    public static List<UICodeText> convertCollectionToCodeText(final Collection<?>  value,
                                                                 final String codeField,
                                                                 final String textField) {
        List<UICodeText> retList = new ArrayList<UICodeText>();
        if(value == null) {
            return retList;
        }
        for (Object obj : (Collection<?> ) value) {
            if(obj == null ) {
                continue;
            }
            if (obj instanceof  UICodeText){
                retList.add((UICodeText) obj);
                continue;
            }
            try{
                String code = BeanUtils.getProperty(obj, codeField);
                String text = BeanUtils.getProperty(obj, textField);
                retList.add(new UICodeText.Impl(code, text));
            }catch(Exception e) {
               logger.debug("UI Exception: {}", e.getMessage());
            }
        }
        return retList;
    }






}
