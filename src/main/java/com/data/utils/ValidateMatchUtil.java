package com.data.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;

/**
 * 通过正则表达判断是否正确的手机号,固定电话,身份证,邮箱,url,车牌号,日期,ip地址,mac,人名等.
 */
public class ValidateMatchUtil {
    /**
     * 正则：手机号（简单）, 1字头＋10位数字即可.
     */
    private static final String REGEX_MOBILE_SIMPLE = "^[1]\\d{10}$";
    private static final Pattern PATTERN_REGEX_MOBILE_SIMPLE = Pattern.compile(REGEX_MOBILE_SIMPLE);

    /**
     * 正则：手机号（精确）, 已知3位前缀＋8位数字
     * <p>
     * 移动：134(0-8)、135、136、137、138、139、147、150、151、152、157、158、159、178、182、183、184、187、188
     * </p>
     * <p>
     * 联通：130、131、132、145、155、156、175、176、185、186
     * </p>
     * <p>
     * 电信：133、153、173、177、180、181、189
     * </p>
     * <p>
     * 全球星：1349
     * </p>
     * <p>
     * 虚拟运营商：170
     * </p>
     */
    private static final String REGEX_MOBILE_EXACT = "^((13[0-9])|(14[5,7])|(15[0-3,5-9])|(17[0,3,5-8])|(18[0-9])|(147))\\d{8}$";
    private static final Pattern PATTERN_REGEX_MOBILE_EXACT = Pattern.compile(REGEX_MOBILE_EXACT);

    /**
     * 正则：固定电话号码,可带区号,然后至少6,8位数字
     */
    private static final String REGEX_TEL = "^(\\d{3,4}-)?\\d{6,8}$";
    private static final Pattern PATTERN_REGEX_TEL = Pattern.compile(REGEX_TEL);

    /**
     * 正则：身份证号码15位, 数字且关于生日的部分必须正确
     */
    private static final String REGEX_ID_CARD15 = "^[1-9]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}$";
    private static final Pattern PATTERN_REGEX_ID_CARD15 = Pattern.compile(REGEX_ID_CARD15);

    /**
     * 正则：身份证号码18位, 数字且关于生日的部分必须正确
     */
    private static final String REGEX_ID_CARD18 = "^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}([0-9Xx])$";
    private static final Pattern PATTERN_REGEX_ID_CARD18 = Pattern.compile(REGEX_ID_CARD18);

    /**
     * 正则：邮箱, 有效字符(不支持中文), 且中间必须有@,后半部分必须有.
     */
    private static final String REGEX_EMAIL = "^\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*$";
    private static final Pattern PATTERN_REGEX_EMAIL = Pattern.compile(REGEX_EMAIL);

    /**
     * 正则：URL, 必须有"://",前面必须是英文,后面不能有空格
     */
    private static final String REGEX_URL = "[a-zA-z]+://[^\\s]*";
    private static final Pattern PATTERN_REGEX_URL = Pattern.compile(REGEX_URL);

    /**
     * 正则：yyyy-MM-dd格式的日期校验,已考虑平闰年
     */
    private static final String REGEX_DATE = "^(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)$";
    private static final Pattern PATTERN_REGEX_DATE = Pattern.compile(REGEX_DATE);

    /**
     * 正则：IP地址
     */
    private static final String REGEX_IP = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
    private static final Pattern PATTERN_REGEX_IP = Pattern.compile(REGEX_IP);


    /**
     * 正则：车牌号
     */
    private static final String REGEX_CAR = "^[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼使领A-Z]{1}[A-Z]{1}[A-Z0-9]{3,4}[A-Z0-9挂学警港澳]{1}$";
    private static final Pattern PATTERN_REGEX_CAR = Pattern.compile(REGEX_CAR);


    /**
     * 正则：人名
     */
    private static final String REGEX_NAME = "^([\\u4e00-\u9fa5]{1,20}|[a-zA-Z\\.\\s]{1,20})$";
    private static final Pattern PATTERN_REGEX_NAME = Pattern.compile(REGEX_NAME);

    /**
     * 正则：mac地址
     */
    private static final String REGEX_MAC = "([A-Fa-f0-9]{2}-){5}[A-Fa-f0-9]{2}";
    private static final Pattern PATTERN_REGEX_MAC = Pattern.compile(REGEX_MAC);


    //封装方法：
    /**
     * 验证手机号（简单）
     */
    public static boolean isMobileSimple( String str) {
        return isMatch(PATTERN_REGEX_MOBILE_SIMPLE, str);
    }

    /**
     * 验证手机号（精确）
     */
    public static boolean isMobileExact( String str) {
        return isMatch(PATTERN_REGEX_MOBILE_EXACT, str);
    }

    /**
     * 验证固定电话号码
     */
    public static boolean isTel( String str) {
        return isMatch(PATTERN_REGEX_TEL, str);
    }

    /**
     * 验证15或18位身份证号码
     */
    public static boolean isIdCard( String str) {
        return isMatch(PATTERN_REGEX_ID_CARD15, str) || isMatch(PATTERN_REGEX_ID_CARD18, str);
    }

    /**
     * 验证邮箱
     */
    public static boolean isEmail( String str) {
        return isMatch(PATTERN_REGEX_EMAIL, str);
    }

    /**
     * 验证URL
     */
    public static boolean isUrl( String str) {
        return isMatch(PATTERN_REGEX_URL, str);
    }

    /**
     * 验证yyyy-MM-dd格式的日期校验,已考虑平闰年
     */
    public static boolean isDate( String str) {
        return isMatch(PATTERN_REGEX_DATE, str);
    }

    /**
     * 验证IP地址
     */
    public static boolean isIp( String str) {
        return isMatch(PATTERN_REGEX_IP, str);
    }

    /**
     * 验证车牌号
     */
    public static boolean isCar( String str) {
        return isMatch(PATTERN_REGEX_CAR, str);
    }

    /**
     * 验证人名
     */
    public static boolean isName( String str) {
        return isMatch(PATTERN_REGEX_NAME, str);
    }

    /**
     * 验证mac
     */
    public static boolean isMac( String str) {
        return isMatch(PATTERN_REGEX_MAC, str);
    }

    public static boolean isMatch(Pattern pattern, String str) {
        return StringUtils.isNotEmpty(str) && pattern.matcher(str).matches();
    }


    /**
     *校验邮箱格式
     */
    public static boolean checkEmail(String value){
        boolean flag=false;
        Pattern p1 = null;
        Matcher m = null;
        p1 = Pattern.compile("\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*");
        m = p1.matcher(value);
        flag = m.matches();
        return flag;
    }
    /**
     * @param checkType 校验类型：0校验手机号码，1校验座机号码，2两者都校验满足其一就可
     * @param phoneNum
     * */
    public static boolean validPhoneNum(String checkType,String phoneNum){
        boolean flag=false;
        Pattern p1 = null;
        Pattern p2 = null;
        Matcher m = null;
        p1 = Pattern.compile("^(((13[0-9]{1})|(15[0-9]{1})|(18[0-9]{1})|(17[0-9]{1}))+\\d{8})?$");
        p2 = Pattern.compile("^(0[0-9]{2,3}\\-)?([1-9][0-9]{6,7})$");
        if("0".equals(checkType)){
            System.out.println(phoneNum.length());
            if(phoneNum.length()!=11){
                return false;
            }else{
                m = p1.matcher(phoneNum);
                flag = m.matches();
            }
        }else if("1".equals(checkType)){
            if(phoneNum.length()<11||phoneNum.length()>=16){
                return false;
            }else{
                m = p2.matcher(phoneNum);
                flag = m.matches();
            }
        }else if("2".equals(checkType)){
            if(!((phoneNum.length() == 11 && p1.matcher(phoneNum).matches())||(phoneNum.length()<16&&p2.matcher(phoneNum).matches()))){
                return false;
            }else{
                flag = true;
            }
        }
        return flag;
    }
    /**
     * 功能：身份证的有效验证
     */
    public static boolean IDCardValidate(String IDStr) throws ParseException {
        IDStr = IDStr.trim().toUpperCase();
        String errorInfo = "";// 记录错误信息
        String[] ValCodeArr = { "1", "0", "X", "9", "8", "7", "6", "5", "4", "3", "2" };
        String[] Wi = { "7", "9", "10", "5", "8", "4", "2", "1", "6", "3", "7", "9", "10", "5", "8", "4", "2" };
        String Ai = "";
        // ================ 号码的长度 15位或18位 ================
        if (IDStr.length() != 15 && IDStr.length() != 18) {
            //身份证号码长度应该为15位或18位
            return false;
        }
        // =======================(end)========================

        // ================ 数字 除最后以为都为数字 ================
        if (IDStr.length() == 18) {
            Ai = IDStr.substring(0, 17);
        } else if (IDStr.length() == 15) {
            Ai = IDStr.substring(0, 6) + "19" + IDStr.substring(6, 15);
        }
        if (isNumeric(Ai) == false) {
            //身份证15位号码都应为数字 ; 18位号码除最后一位外，都应为数字。
            return false;
        }
        // =======================(end)========================

        // ================ 出生年月是否有效 ================
        String strYear = Ai.substring(6, 10);// 年份
        String strMonth = Ai.substring(10, 12);// 月份
        String strDay = Ai.substring(12, 14);// 月份
        if (isDataFormat(strYear + "-" + strMonth + "-" + strDay) == false) {
            //身份证生日无效。
            return false;
        }
        GregorianCalendar gc = new GregorianCalendar();
        SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
        if ((gc.get(Calendar.YEAR) - Integer.parseInt(strYear)) > 150
                || (gc.getTime().getTime() - s.parse(strYear + "-" + strMonth + "-" + strDay).getTime()) < 0) {
            //身份证生日不在有效范围。
            return false;
        }
        if (Integer.parseInt(strMonth) > 12 || Integer.parseInt(strMonth) == 0) {
            //身份证月份无效
            return false;
        }
        if (Integer.parseInt(strDay) > 31 || Integer.parseInt(strDay) == 0) {
            //身份证日期无效
            return false;
        }
        // =====================(end)=====================

        // ================ 地区码时候有效 ================
        Hashtable h = GetAreaCode();
        if (h.get(Ai.substring(0, 2)) == null) {
            //身份证地区编码错误。
            return false;
        }
        // ==============================================

        // ================ 判断最后一位的值 ================
        int TotalmulAiWi = 0;
        for (int i = 0; i < 17; i++) {
            TotalmulAiWi = TotalmulAiWi + Integer.parseInt(String.valueOf(Ai.charAt(i))) * Integer.parseInt(Wi[i]);
        }
        int modValue = TotalmulAiWi % 11;
        String strVerifyCode = ValCodeArr[modValue];
        Ai = Ai + strVerifyCode;

        if (IDStr.length() == 18) {
            if (Ai.equals(IDStr) == false) {
                //身份证无效，不是合法的身份证号码
                return false;
            }
        } else {
            return true;
        }
        // =====================(end)=====================
        return true;
    }

    /**
     * 功能：设置地区编码
     */
    private static Hashtable GetAreaCode() {
        Hashtable hashtable = new Hashtable();
        hashtable.put("11", "北京");
        hashtable.put("12", "天津");
        hashtable.put("13", "河北");
        hashtable.put("14", "山西");
        hashtable.put("15", "内蒙古");
        hashtable.put("21", "辽宁");
        hashtable.put("22", "吉林");
        hashtable.put("23", "黑龙江");
        hashtable.put("31", "上海");
        hashtable.put("32", "江苏");
        hashtable.put("33", "浙江");
        hashtable.put("34", "安徽");
        hashtable.put("35", "福建");
        hashtable.put("36", "江西");
        hashtable.put("37", "山东");
        hashtable.put("41", "河南");
        hashtable.put("42", "湖北");
        hashtable.put("43", "湖南");
        hashtable.put("44", "广东");
        hashtable.put("45", "广西");
        hashtable.put("46", "海南");
        hashtable.put("50", "重庆");
        hashtable.put("51", "四川");
        hashtable.put("52", "贵州");
        hashtable.put("53", "云南");
        hashtable.put("54", "西藏");
        hashtable.put("61", "陕西");
        hashtable.put("62", "甘肃");
        hashtable.put("63", "青海");
        hashtable.put("64", "宁夏");
        hashtable.put("65", "新疆");
        hashtable.put("71", "台湾");
        hashtable.put("81", "香港");
        hashtable.put("82", "澳门");
        hashtable.put("91", "国外");
        return hashtable;
    }

    /**
     * 验证日期字符串是否是YYYY-MM-DD格式
     */
    public static boolean isDataFormat(String str) {
        boolean flag = false;
        // String
        // regxStr="[1-9][0-9]{3}-[0-1][0-2]-((0[1-9])|([12][0-9])|(3[01]))";
        String regxStr = "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s(((0?[0-9])|([1-2][0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$";
        Pattern pattern1 = Pattern.compile(regxStr);
        Matcher isNo = pattern1.matcher(str);
        if (isNo.matches()) {
            flag = true;
        }
        return flag;
    }

    /**
     * 功能：判断字符串是否为数字
     */
    private static boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if (isNum.matches()) {
            return true;
        } else {
            return false;
        }
    }
// 身份证号码验证：end

    //测试方法
    public static void main(String[]args){
        String car = "137888888888";
        System.out.println(isMobileSimple(car));
    }
}
