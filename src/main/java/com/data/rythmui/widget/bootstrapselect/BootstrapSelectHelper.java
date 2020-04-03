package com.data.rythmui.widget.bootstrapselect;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.rythmengine.logger.Logger;

import java.util.*;

public class BootstrapSelectHelper {

    public static String convertToSelectOptionJson(final Object value,
                                                   final String codeField,
                                                   final String textField){
        return JSON.toJSONString(convertToSelectOption(value, codeField,textField));
    }

    public static List<SelectOption> convertToSelectOption(final Object value,
                                                   final String codeField,
                                                   final String textField){
        List<SelectOption> retList = new ArrayList<SelectOption>();
        if(value == null){
            return retList;
        }

        if(value instanceof Map) {
            retList.addAll(convertMapToSelectOption((Map<String, String>) value));
        }else if(value instanceof String) {
            String strValue = (String) value;

            if(strValue.startsWith("{")){
                retList.addAll(convertJsonStrToSelectOption(strValue));
            }else if(strValue.startsWith("[")){
                retList.addAll(convertJsonArrayStrToSelectOption(strValue, codeField, textField));
            } else {
                retList.addAll(convertStrToSelectOption(strValue, codeField, textField));
            }
        }else if(value instanceof String[]) {
            for(String str: (String [])value) {
                retList.add(new SelectOptionUnit(str, str));
            }
        } else if(value instanceof Collection) {
            retList.addAll(convertCollectionToSelectOption((Collection<?>) value, codeField, textField));
        } else  {

        }
        return retList;
    }

    private static List<SelectOption> convertCollectionToSelectOption(final Collection<?> value,
                                                                      String codeField,
                                                                      String textField) {
        List<SelectOption> retList = new ArrayList<>();
        if(value == null) {
            return retList;
        }
        for (Object obj: (Collection<?>) value) {
            if(obj == null){
                continue;
            }
            if(obj instanceof SelectOption){
                retList.add((SelectOption) obj);
                continue;
            }
            try{
              String code = BeanUtils.getProperty(obj, codeField);
              String text = BeanUtils.getProperty(obj, textField);
            }catch(Exception e) {
                Logger.debug("UI Exception:{}", e.getMessage());
            }
        }
        return retList;
    }

    private static  List<SelectOption> convertStrToSelectOption(final String value,
                                                                final String codeField,
                                                                final String textField) {
        List<SelectOption> retList = new ArrayList<>();
        if(value == null) {
            return retList;
        }
        String[] valueArray = StringUtils.split(value, ",");
        if(valueArray != null) {
            for (String str : valueArray) {
                retList.add(new SelectOptionUnit(str, str));
            }
        }
        return retList;
    }

    private static List<SelectOption> convertJsonArrayStrToSelectOption(final String value,
                                                                        final String codeField,
                                                                        final String textField) {
        List<SelectOption> retList = new ArrayList<>();
        if(value == null) {
            return retList;
        }
        JSONArray jsonArray = JSON.parseArray(value);
        if(jsonArray == null){
            return retList;
        }

        for (Object obj : jsonArray) {
            if(obj != null && obj instanceof JSONObject) {
                JSONObject jsonObject = (JSONObject) obj;
                String code = jsonObject.getString(codeField);
                String text = jsonObject.getString(textField);
                retList.add(new SelectOptionUnit(code, text));
            }
        }
        return retList;

    }

    private static List<SelectOption> convertJsonStrToSelectOption(final String value) {

        List<SelectOption> retList = new ArrayList<>();
        if(value == null) {
            return retList;
        }

        LinkedHashMap<String, String> jsonMap = JSON.parseObject(value,
                new TypeReference<LinkedHashMap<String, String>>() {
                });
        for (Map.Entry<String, String> entry : jsonMap.entrySet()){
            retList.add(new SelectOptionUnit(entry.getKey(), entry.getValue()));
        }
        return retList;
    }


    public static List<SelectOption> convertMapToSelectOption(final Map<String, String > value) {
        List<SelectOption> retList = new ArrayList<>();
        if(value == null){
            return retList;
        }
        for(Map.Entry<String, String> entry: value.entrySet()) {
            retList.add(new SelectOptionUnit(entry.getKey(), entry.getValue()));
        }
        return retList;
    }






}
