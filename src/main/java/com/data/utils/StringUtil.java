package com.hazq.ccg.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {

    /**
     * 去掉指定字符串的开头的指定字符
     * @param stream 原始字符串
     * @param trim 要删除的字符串
     * @return
     */
    public static String StringStartTrim(String stream, String trim) {
        // null或者空字符串的时候不处理
        if (stream == null || stream.length() == 0 || trim == null || trim.length() == 0) {
            return stream;
        }
        // 要删除的字符串结束位置
        int end;
        // 正规表达式
        String regPattern = "[" + trim + "]*+";
        Pattern pattern = Pattern.compile(regPattern, Pattern.CASE_INSENSITIVE);
        // 去掉原始字符串开头位置的指定字符
        Matcher matcher = pattern.matcher(stream);
        if (matcher.lookingAt()) {
            end = matcher.end();
            stream = stream.substring(end);
        }
        // 返回处理后的字符串
        return stream;
    }

    public static String StringStartTrim(String stream ) {
        return StringStartTrim(stream, "-|=");
    }

    public static void main(String[] args) {
        System.out.println(StringStartTrim("-----===sdfsdfafs=------=======    cmd / a"));
    }


}
