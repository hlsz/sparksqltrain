package com.data.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
public class UDFDateTimeDiff extends UDF
{
    private LongWritable defaultValue = new LongWritable(Long.MAX_VALUE);
    private final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private String secondType = "second";
    private String yearType = "year";
    double result = 0.0;
    double diff = 0.0;
    DecimalFormat df = new DecimalFormat("0");
    LongWritable returnvalue = new LongWritable();
    long unixtime = 0l;
    // convert seconds to milliseconds
    Date date = new Date();
    Calendar cal = Calendar.getInstance();
    int yearNow = 0;
    int monthNow = 0;
    int dayOfMonthNow = 0;
    int yearBirth = 0;
    int monthBirth = 0;
    int dayOfMonthBirth = 0;
    int age = 0;
    String strTimeS;
    String strTimeE;
    HashMap<Integer, String> sdfMap = new HashMap<Integer, String>();
    public UDFDateTimeDiff()
    {
        sdfMap.put(23, "");
        sdfMap.put(22, "0");
        sdfMap.put(21, "00");
        sdfMap.put(19, ".000");
        sdfMap.put(10, " 00:00:00.000");
    }
    public LongWritable evaluate(Text birthday)
    {
        return getDiff(birthday.toString(), formatter.format(new Date()), yearType);
    }
    public LongWritable evaluate(Text stime, Text etime)
    {
        if (stime == null || etime == null)
            return defaultValue;
        return getDiff(stime.toString(), etime.toString(), secondType);
    }
    public LongWritable evaluate(Text stime, Text etime, Text defaultType)
    {
        if (stime == null || etime == null || defaultType == null)
            return defaultValue;
        return getDiff(stime.toString(), etime.toString(), defaultType.toString());
    }
    public LongWritable getDiff(String birthday)
    {
        if (StringUtils.isBlank(birthday))
            return defaultValue;
        return getDiff(birthday, formatter.format(new Date()), yearType);
    }
    public LongWritable getDiff(String stime, String etime)
    {
        if (StringUtils.isBlank(stime) || StringUtils.isBlank(etime))
            return defaultValue;
        return getDiff(stime, etime, secondType);
    }
    public LongWritable getDiff(String strTimeS, String strTimeE, String defaultType)
    {
        if (StringUtils.isBlank(strTimeS) || StringUtils.isBlank(strTimeE))
            return defaultValue;
        // 根据时间的长度，补齐为全格式
        strTimeS = strTimeS + sdfMap.get(strTimeS.length());
        strTimeE = strTimeE + sdfMap.get(strTimeE.length());
        if (strTimeS.length() != 23 || strTimeE.length() != 23)
            throw new RuntimeException("时间格式不符合要求：" + strTimeS + "\t" + strTimeE + "\t");
        try
        {
            diff = formatter.parse(strTimeE).getTime() - formatter.parse(strTimeS).getTime();
            if (defaultType.toString().equals("year"))
            {
                result = getAge(formatter.parse(strTimeS), formatter.parse(strTimeE));
            } else if (defaultType.toString().equals("month"))
            {
                result = getMonth(formatter.parse(strTimeS), formatter.parse(strTimeE));
            } else if (defaultType.toString().equals("day"))
            {
                result = diff / (1000 * 60 * 60 * 24);
            } else if (defaultType.toString().equals("hour"))
            {
                result = diff / (1000 * 60 * 60);
            } else if (defaultType.toString().equals("minute"))
            {
                result = diff / (1000 * 60);
            } else if (defaultType.toString().equals("second"))
            {
                result = diff / 1000;
            } else
            {
                return defaultValue;
            }
        } catch (ParseException e)
        {
            System.out.println(e.getMessage());
            return defaultValue;
        }
        // 保留两位小数
        // 向下取整
        returnvalue.set((long) Math.floor(result));
        return returnvalue;
    }
    // 转换unix时间为yyyy-MM-dd HH:mm:ss格式，如1329926400
    public String eval(Text ttime) throws ParseException
    {
        unixtime = (long) Double.parseDouble(ttime.toString());
        // convert seconds to milliseconds
        date.setTime(unixtime * 1000L);

        return formatter.format(date);
    }

    public int getAge(Date birthDay, Date etime)
    {
        try
        {
            cal.setTime(etime);
            if (cal.before(birthDay))
            {
                throw new IllegalArgumentException("出生时间大于当前时间!");
            }
            yearNow = cal.get(Calendar.YEAR);
            monthNow = cal.get(Calendar.MONTH);// 注意此处，如果不加1的话计算结果是错误的
            dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);
            cal.setTime(birthDay);
            yearBirth = cal.get(Calendar.YEAR);
            monthBirth = cal.get(Calendar.MONTH);
            dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);
            age = yearNow - yearBirth;
            if (monthNow <= monthBirth)
            {
                if (monthNow == monthBirth)
                {
                    // monthNow==monthBirth
                    if (dayOfMonthNow < dayOfMonthBirth)
                    {
                        age--;
                    } else
                    {
                        // do nothing
                    }
                } else
                {
                    // monthNow>monthBirth
                    age--;
                }
            } else
            {
            }

            return age;
        } catch (Exception e)
        {
        }
        return 0;
    }

    public int getMonth(Date birthDay, Date etime)
    {
        try
        {
            cal.setTime(etime);
            if (cal.before(birthDay))
            {
                throw new IllegalArgumentException("出生时间大于当前时间!");
            }
            yearNow = cal.get(Calendar.YEAR);
            monthNow = cal.get(Calendar.MONTH)+1;// 注意此处，如果不加1的话计算结果是错误的
            dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);
            cal.setTime(birthDay);
            yearBirth = cal.get(Calendar.YEAR);
            monthBirth = cal.get(Calendar.MONTH);
            dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);
            age = (yearNow - yearBirth) * 12 + monthNow - monthBirth;

            if (dayOfMonthNow < dayOfMonthBirth)
            {
                age--;
            }
            return age;
        } catch (Exception e)
        {
        }
        return 0;
    }
    //直接在main函数里测试
    public static void main(String[] args)
    {
        UDFDateTimeDiff o = new UDFDateTimeDiff();

        String stime = "2016-02-26";
        String etime = "2016-02-26";
        //下面基于不同粒度计算stime和etime之间的时间差
        System.out.println("相差多少年:" + o.getDiff(stime, etime, "year"));
        System.out.println("相差多少月:" + o.getDiff(stime, etime, "month"));
        System.out.println("相差多少天:" + o.getDiff(stime, etime, "day"));
        System.out.println("相差多少小时:" + o.getDiff(stime, etime, "hour"));
        System.out.println("相差多少分钟:" + o.getDiff(stime, etime, "minute"));
        System.out.println("相差多少秒:" + o.getDiff(stime, etime, "second"));
        System.out.println("相差多少秒:" + o.getDiff(stime, etime));
    }
}
//输出结果如下：
//            相差多少年:1
//            相差多少月:13
//            相差多少天:366
//            相差多少小时:8784
//            相差多少分钟:527040
//            相差多少秒:31622400
//            相差多少秒:31622400
