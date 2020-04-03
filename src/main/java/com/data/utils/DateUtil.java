package com.data.utils;

import com.data.dao.DBConn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


/**
 * <p>Title: main </P>
 * <p>Description: TODO </P>
 * @param args
 * return void    返回类型
 * throws
 * date 2014-11-24 上午09:11:47
 */
public class DateUtil{
    public static void main(String[] args) {
        try {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            DBConn conn = new DBConn();
            Connection conn2 = conn.Conn();
            String sql= "select * from lian";
            PreparedStatement statement = conn2.prepareStatement(sql);

            ResultSet resultSet = statement.executeQuery();
            //定义集合封装立案时间
            List<Calendar> lians=new ArrayList<>();
            while(resultSet.next()){
                String holiday = resultSet.getString(2);
                Calendar calendar= Calendar.getInstance();
                Date date = df.parse(holiday);
                calendar.setTime(date);
                lians.add(calendar);
                System.out.println("所有立案时间:"+df.format(date));
            }
            for (Calendar calendar : lians) {
                //初始化集合
                initHolidayList();
                //10个工作日排除法定节日和双休后的日期
                String c = addDateByWorkDay(calendar,10);
//           System.out.println("开始日期:"+df.format(calendar));
                System.out.println("10个工作日排除法定节日和双休后的日期:"+c);

            }

        } catch ( Exception e) {
            // TODO: handle exception

            e.printStackTrace();
        }

    }

    private static List<Calendar> holidayList = new ArrayList<Calendar>();  //节假日列表

    /**
     *
     * <p>Title: addDateByWorkDay </P>
     * <p>Description: TODO  计算相加day天，并且排除节假日和周末后的日期</P>
     * @param calendar  当前的日期
     * @param day  相加天数
     * @return
     * return Calendar    返回类型   返回相加day天，并且排除节假日和周末后的日期
     * throws
     * date 2014-11-24 上午10:32:55
     */
    public static String addDateByWorkDay(Calendar calendar,int day){

        try {
            for (int i = 0; i < day; i++) {
                //天数加1
                calendar.add(Calendar.DAY_OF_MONTH, 1);
                //校验是否是休息日
                if(checkHoliday(calendar)){
                    //是,这天不算
                    i--;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return  df.format(calendar.getTime());
    }

    /**
     *
     * <p>Title: checkHoliday </P>
     * <p>Description: TODO 验证日期是否是节假日</P>
     * @param calendar  传入需要验证的日期
     * @return
     * return boolean    返回类型  返回true是节假日，返回false不是节假日
     * throws
     * date 2014-11-24 上午10:13:07
     */
    public static boolean checkHoliday(Calendar calendar) throws Exception{

        //判断日期是否是周六周日
        if(calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY ||
                calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY){
            return true;
        }
        //判断日期是否是节假日
        for (Calendar ca : holidayList) {
            if(ca.get(Calendar.MONTH) == calendar.get(Calendar.MONTH) &&
                    ca.get(Calendar.DAY_OF_MONTH) == calendar.get(Calendar.DAY_OF_MONTH)&&
                    ca.get(Calendar.YEAR) == calendar.get(Calendar.YEAR)){
                return true;
            }
        }

        return false;
    }

    /**
     *
     * <p>Title: initHolidayList </P>
     * <p>Description: TODO  把所有节假日放入list，验证前要先执行这个方法</P>
     * @param date  从数据库查 查出来的格式2014-05-09
     * return void    返回类型
     * throws
     * date 2014-11-24 上午10:11:35
     * @throws Exception
     */
    public static  void initHolidayList() throws Exception {
        //创建数据库连接
        DBConn conn = new DBConn();
        Connection connection = conn.Conn();
        //查询表
        String sql = "select * from fn_all_holiday";

        PreparedStatement statement = connection.prepareStatement(sql);
        //得到结果集
        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            //去结果集的时间字段
            String string = resultSet.getString(3);
            String[] da = string.split("-");
            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.YEAR, Integer.valueOf(da[0]));
            calendar.set(Calendar.MONTH, Integer.valueOf(da[1]) - 1);// 月份比正常小1,0代表一月
            calendar.set(Calendar.DAY_OF_MONTH, Integer.valueOf(da[2]));
            //把结果集放入集合
            holidayList.add(calendar);
        }

    }
}
