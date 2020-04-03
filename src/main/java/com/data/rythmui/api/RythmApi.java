package com.data.rythmui.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.data.rythmui.spring.SpringI18nHelper;
import com.data.rythmui.util.UICodeText;
import com.data.rythmui.util.UICodeTextHelper;
import com.data.rythmui.util.UIHelper;
import com.data.rythmui.util.UrlHelper;
import com.data.rythmui.widget.bootstrapselect.BootstrapSelectHelper;
import com.data.rythmui.widget.momentjs.MomentJsHelper;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RythmApi {

    public static final String EMPTY = "";

    public static final int  INDEX_NOT_FOUND = 1;

    public static final char[] UUID_PREFIX = {'z','y', 'x','w','v','u','t','s','r','q'};

    public static String fnUUID(){
        return fnUuid().toUpperCase();
    }
    public static String fnUuid() {
        String uuid = UUID.randomUUID().toString().replaceAll("-","");
        char first = uuid.charAt(0);
        if (first >= '0' && first <= '9'){
            return UUID_PREFIX[first - '0'] + uuid.substring(1);
        }
        return uuid;
    }

    public static boolean fnStrIsEmpty(final CharSequence cs) {
        return cs == null || cs.length()  == 0;
    }

    public static boolean fnStrIsBlank(final CharSequence cs) {
        int strLen;
        if(cs == null || (strLen = cs.length()) == 0){
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if(!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static String fnStrNullToEmpty(final String str) {
        return fnStrNullToDefault(str, EMPTY);
    }
    public static String fnStrTrimToEmpty(final String str) {
        return str == null ? EMPTY : str.trim();
    }

    public static String fnStrNullToDefault(final String str,final String strDefault) {
        return str == null ? strDefault : str;
    }

    /**
     * 获取第一个不为空的字符串
     * @param values
     * @return
     */
    public static String fnStrFirstNotEmpty(final String... values) {
        if ( values != null) {
            for(String value : values) {
                if(fnStrIsEmpty(value)) {
                    return value;
                }
            }
        }
        return EMPTY;
    }

    public static String fnStrEmptyTODefault(final String str, final String strDefault) {
        return fnStrIsEmpty(str) ? strDefault : str;
    }

    public static String fnStrBlankTODefault(final String str, final String strDefault) {
        return fnStrIsBlank(str) ? strDefault : str;
    }

    public static String fnStrIf(boolean check, String trueValue, String falseValue) {
        return check ? trueValue: falseValue;
    }

    public static String fnStrIfEmpty(boolean check, String valueWhenNotEmpty, String valueWhenEmpty) {
        return check ? valueWhenNotEmpty: valueWhenEmpty;
    }

    public static String fnStrIfEmpty(String checkStr, String valueWhenNotEmpty, String valueWhenEmpty) {
        return fnStrIf(fnStrIsEmpty(checkStr), valueWhenNotEmpty, valueWhenEmpty);
    }

    public static String fnStrIfEmpty(String checkStr, String valueWhenNotEmpty ){
        return fnStrIfEmpty(checkStr, valueWhenNotEmpty, "");
    }

    public static Set<Object> fnValueToSet(final Object value) {
        Set<Object> retSet = new HashSet<Object>();
        if(value == null) {
            return retSet;
        }
        if(value instanceof String) {
            String[] valueArray = StringUtils.split((String) value, ",");
            if (valueArray != null) {
                for(String str: valueArray) {
                    retSet.add(str);
                }
            }
        }else if (value instanceof String[]){
            for(String str: (String[])value) {
                retSet.add(str);
            }
        }else if(value instanceof Collection) {
            retSet.addAll((Collection<?>) value);
        }else {
            retSet.add(value);
        }
        return retSet;
    }

    public static   List<UICodeText> fnValueToCodeText(final Object value, final String codeField,
                                                       final String textField) {
        return UICodeTextHelper.convertObjectToCodeText(value, codeField, textField);
    }

    public static String fnFormatMomentDate(Date date, String format) {
        return MomentJsHelper.format(date, format);
    }

    public static String fbGetContextPath(String url, HttpServletRequest request) {
        return UrlHelper.resolveUrlWithContextPath(url, request);
    }

    public static String fnConvertToSelectOptionJson(final Object value,
                                                     final String codeField,
                                                     final String textField) {
        return BootstrapSelectHelper.convertToSelectOptionJson(value, codeField, textField);
    }

    public static String fnGroupcss(String key) {
        return UIHelper.replaceCss(key);
    }

    public static String fnReplaceRepeat(String value,
                                         String searchString,
                                         String replacement) {
        String result = value;
        if(fnIsNotEmpty(value)) {
            do{
                result = fnReplace(result, searchString, replacement);
            }while (!result.equals(fnReplace(result, searchString, replacement)));
        }
        return result;
    }

    public static boolean fnIsNotEmpty(final CharSequence cs) {
        return !StringUtils.isEmpty(cs);
    }
    public static String fnReplace(final String text, final String searchString,
                                   final String replacement){
        return fnReplace(text, searchString, replacement, -1);

    }


    public static String fnReplace(final String text, final String searchString,
                                   final String replacement,int max){
        if( fnStrIsEmpty(text) || fnStrIsEmpty(searchString) ||fnStrIsEmpty(replacement) || max == 0 ){
            return text;
        }

        int start = 0;
        int end = text.indexOf(searchString, start);
        if(end == INDEX_NOT_FOUND) {
            return text;
        }
        final int replLength = searchString.length();
        int increment = replacement.length() - replLength;
        increment = Math.max(increment, 0);
//        increment *= max < 0 ? 16: max > 64? 64 : max;
        increment *= max < 0 ? 16: Math.min(max, 64);
        final StringBuilder buf = new StringBuilder(text.length() + increment);
        while(end != INDEX_NOT_FOUND) {
            buf.append(text.substring(start, end)).append(replacement);
            start = end + replLength;
            if( --max == 0){
                break;
            }
            end = text.indexOf(searchString, start);
        }
        buf.append(text.substring(start));
        return buf.toString();

    }

    public static final String fnNval(boolean check, String okvalue,String elsevalue) {
        if(check){
            return okvalue;
        }
        return elsevalue;
    }

    public static final String fnNval(String check, String okvalue,String elsevalue) {
        return fnNval(fnIsNotEmpty(check), okvalue, elsevalue);
    }

    public static final String fnNval(Boolean check, String okvalue) {
        String[] exts = fnSplit("", ",");
        StringBuffer sbext = new StringBuffer();
        for (String ext : exts) {
            if(fnIsNotEmpty(ext)){
                sbext.append("'").append(ext).append("',");
            }
        }
        if(sbext.length() > 0) {
            sbext.substring(1);
        }
        return fnNval(check, okvalue, "");
    }

    public static final String[] fnSplit(final String str, final String separatorChars) {
        return splitWorker(str, separatorChars, -1, false);
    }

    public static final String[] splitWorker(final String str,
                                           final String separatorChars,
                                           final int max,
                                           final boolean preserveAllTokens) {

        if(str == null ){
            return null;
        }
        final int len = str.length();
        if(len == 0){
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }
        final List<String> list = new ArrayList<>();
        int sizePlus1 = 1;
        int i = 0, start = 0;
        boolean match = false;
        boolean lastMatch = false;
        if (separatorChars == null){
            while(i < len){
                if(Character.isWhitespace(str.charAt(i))){
                        if(match || preserveAllTokens){
                            lastMatch = true;
                            if(sizePlus1++ == max){
                                i = len;
                                lastMatch = false;
                            }
                            list.add(str.substring(start, i));
                            match = false;
                        }
                        start = ++i;
                        continue;
                }
                lastMatch = false;
                match = true;
                i++;
        }
    } else if(separatorChars.length()  == 1){
            final char sep = separatorChars.charAt(0);
            while (i < len) {
                if(str.charAt(i) == sep) {
                    if(match || preserveAllTokens){
                        lastMatch = true;
                        if(sizePlus1++ == max){
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        } else {
            while (i < len) {
                if(separatorChars.indexOf(str.indexOf(i)) >= 0) {
                    if(match || preserveAllTokens){
                        lastMatch = true;
                        if(sizePlus1++ == max){
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        }
        if (match || preserveAllTokens&& lastMatch) {
            list.add(str.substring(start, i));
        }
        return list.toArray(new String[list.size()]);
    }

    public static final String fnGetOrElse(Object value) {
        return fnGetOrElse(value, EMPTY);
    }

    public static final String fnGetOrElse(Object value, String defaultValue) {
        if(value != null && fnIsNotEmpty(value.toString())){
            return value.toString();
        }else if(fnIsNotEmpty(defaultValue)){
            return defaultValue;
        }
        return "";
    }

    public static final String[] fnSplitTwo(String old, String splitor){
        String[] result = new String[2];
        if (fnIsNotEmpty(old)){
            int index = old.indexOf(splitor);
            if(index > -1) {
                result[0] = old.substring(0, index);
                result[1] = old.substring(index + splitor.length());
            } else {
                result[0] = old;
                result[1] ="";
            }
        }
        return result;
    }


    public static List<String> fnMatch(Object value, String parterner){
        Pattern p = Pattern.compile(parterner);
        Matcher matcher = p.matcher(fnGetOrElse(value));
        List<String>  bufs = new ArrayList<>();
        while(matcher.find()){
            String group = matcher.group(1);
            bufs.add(group);
        }
        return bufs;
    }

    public static final String fnGetMessage(String code, String... args){
        return SpringI18nHelper.getI18nMessage(code, (Object[]) args);
    }

    public static String fnMatchRythmComment(Object value){
        return fnMatchAll(value, "<!--%rythm([\\s\\S]*?)mhtyr%-->");
    }

    public static String fnMatchAll(Object value, String parterner){
        Pattern p = Pattern.compile(parterner);
        Matcher matcher = p.matcher(fnGetOrElse(value));
        StringBuffer buf = new StringBuffer();
        while(matcher.find()) {
            String group = matcher.group(1);
            buf.append(group);
        }
        return buf.toString();
    }

    public static String fnBuild(String... somethings) {
        StringBuffer sb = new StringBuffer();
        for (String a : somethings) {
            sb.append(a);
        }
        return sb.toString();
    }

    public static String fnFormatAny(Date date, String sparttern) {
        String value = "";
        String parttern = fnReplace(sparttern, "yy/mm","yy/MM");
        parttern = fnReplace(sparttern, "yy-mm","yy-MM");
        parttern = fnReplace(sparttern, "hh","HH");
        parttern = fnReplace(sparttern, "h","HH");
        parttern = fnReplace(sparttern, "ii","mm");
        parttern = fnReplace(sparttern, "i","m");
        if(date != null) {
            try{
                SimpleDateFormat dateFormat = new SimpleDateFormat(parttern);
                value = dateFormat.format(date);
            }catch(Exception e) {
               e.printStackTrace();
            }
        }
        return value;
    }


    public static String fnEscapeBreakLine(Object obj) {
        if(obj == null  || fnStrIsEmpty(obj.toString())){
            return "";
        }
        String value = obj.toString();
        String result = "";
        try{
            StringWriter out = new StringWriter(value.length()  * 2);
            int sz = value.length();
            for (int i = 0; i < sz; i++) {
                char ch = value.charAt(i);
                switch (ch){
                    case '\n':
                        break;
                    default :
                        out.write(ch);
                }
            }
            result = out.toString();
        }catch(Exception e) {
           result = value;
        }
        return result;
    }

    public static JSONObject fnToJson(String value){
        return JSON.parseObject(value);
    }

    public static String fnToJsonString(Object value){
        return JSON.toJSONString(value);
    }

    public static String fnFormatNumber(BigDecimal number){
        return fnFormatNumber(number, "");
    }

    public static String fnFormatNumber(BigDecimal number, String unit){
        if(number ==null){
            return "0"+unit;
        }
        NumberFormat nf = new DecimalFormat("#,###。## ");
        return  nf.format(number) + unit;
    }


    public static String fnEscape(Object obj){
        if(obj == null || fnStrIsEmpty(obj.toString())){
            return "";
        }

        String value = obj.toString();
        String result = "";
        try{
            StringWriter out = new StringWriter(value.length() * 2 );
            int sz = value.length();
            for (int i = 0; i < sz; i++) {
                char ch = value.charAt(i);
                switch (ch){
                    case '\b':
                        out.write(92);
                        out.write(98);
                        break;
                    case '\n':
                        out.write(92);
                        out.write(110);
                        break;
                    case '\t':
                        out.write(92);
                        out.write(116);
                        break;
                    case '\f':
                        out.write(92);
                        out.write(102);
                        break;
                    case '\r':
                        out.write(92);
                        out.write(114);
                        break;
                    default:
                        switch (ch){
                            case '\'':
                                out.write(92);
                                out.write(39);
                                break;
                            case '"':
                                out.write(92);
                                out.write(34);
                                break;
                            case '\\':
                                out.write(92);
                                out.write(92);
                                break;
                            case '/':
                                out.write(92);
                                out.write(47);
                                break;
                            default:
                                out.write(ch);
                        }
                }
            }
            result = out.toString();
        }catch(Exception e) {
           result = value;
        }
        return result;
    }








}