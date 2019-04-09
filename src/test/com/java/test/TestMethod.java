package com.java.test;

import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Date;

public class TestMethod {
    public static Date parseDate(String inputDate) {
        Date outputDate = null;
        String[] possibleDateFormats =
                {
                        "yyyy-MM-dd",
                        "yyyyMMdd",
                        "yyyy/MM/dd",
                        "yyyy年MM月dd日",
                        "yyyy MM dd"
                };

        try {
            outputDate = DateUtils.parseDate(inputDate, possibleDateFormats);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return outputDate;
    }

    public static void main(String[] args) {
        String[] possiblePatterns =
                {
                        "yyyy-MM-dd",
                        "yyyy-MM-dd HH:mm:ss",
                        "yyyyMMdd",
                        "yyyy/MM/dd",
                        "yyyy年MM月dd日",
                        "yyyy MM dd"
                };

        String inputDate1 = "2018-01-01";
        String inputDate2 = "2018-01-01 12:12:12";
        String inputDate3 = "20180101";
        String inputDate4 = "2018/01/01";
        String inputDate5 = "2018年01月01日";
        String inputDate6 = "2018 01 01";
        System.out.println(parseDate(inputDate6));
//        System.out.println(parseDate(inputDate1,possiblePatterns));
//        System.out.println(parseDate(inputDate2,possiblePatterns));
        System.out.println(parseDate(inputDate3));
//        System.out.println(parseDate(inputDate4,possiblePatterns));
//        System.out.println(parseDate(inputDate5,possiblePatterns));
//        System.out.println(parseDate(inputDate6,possiblePatterns));

    }
}
