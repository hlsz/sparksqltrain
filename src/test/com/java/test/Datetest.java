package com.java.test;

import com.data.model.Days;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


public class Datetest {
    public static void main(String[] args) {
        //驱动程序名
        String driver = "com.mysql.jdbc.Driver";
        //要插入的数据库，表
        String url = "jdbc:mysql://127.0.0.1:3306/test";
        String user = "root";
        String password = "152152";
        try {
            //加载驱动程序
            Class.forName(driver);
            //连续MySQL 数据库
            Connection conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            //statement用来执行SQL语句
            Statement statement = conn.createStatement();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            java.util.Date start = sdf.parse("2017-01-01");//开始时间
            java.util.Date end = sdf.parse("2017-12-31");//结束时间
            List<Date> lists = dateSplit(start, end);

            //-------------------插入周末时间---------------
            if (!lists.isEmpty()) {
                for (Date date : lists) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date);
                    if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                        System.out.println("插入日期:" + sdf.format(date) + ",周末");
                        StringBuffer insertSql = new StringBuffer(" INSERT INTO fn_all_holiday (title,holiday_date) VALUES(");
                        insertSql.append("'周末',");
                        insertSql.append("'" + sdf.format(date) + "'");
                        statement.executeUpdate(insertSql.toString());
                    }
                }
            }

            //---------------插入节假日时间------------------
            List<Days> holidays = new ArrayList<Days>();

            holidays.add(new Days(1, "元旦", sdf.parse("2017-01-01")));
            holidays.add(new Days(2, "元旦", sdf.parse("2017-01-02")));
            holidays.add(new Days(3, "元旦", sdf.parse("2017-01-03")));

            holidays.add(new Days(4, "春节", sdf.parse("2017-02-07")));
            holidays.add(new Days(5, "春节", sdf.parse("2017-02-08")));
            holidays.add(new Days(6, "春节", sdf.parse("2017-02-09")));
            holidays.add(new Days(7, "春节", sdf.parse("2017-02-10")));
            holidays.add(new Days(8, "春节", sdf.parse("2017-02-11")));
            holidays.add(new Days(9, "春节", sdf.parse("2017-02-12")));
            holidays.add(new Days(10, "春节", sdf.parse("2017-02-13")));

            holidays.add(new Days(11, "清明节", sdf.parse("2017-04-02")));
            holidays.add(new Days(12, "清明节", sdf.parse("2017-04-03")));
            holidays.add(new Days(13, "清明节", sdf.parse("2017-04-04")));

            holidays.add(new Days(14, "劳动节", sdf.parse("2017-04-30")));
            holidays.add(new Days(15, "劳动节", sdf.parse("2017-05-01")));
            holidays.add(new Days(16, "劳动节", sdf.parse("2017-05-02")));

            holidays.add(new Days(17, "端午节", sdf.parse("2017-06-09")));
            holidays.add(new Days(18, "端午节", sdf.parse("2017-06-10")));
            holidays.add(new Days(19, "端午节", sdf.parse("2017-06-11")));

            holidays.add(new Days(20, "中秋节", sdf.parse("2017-09-15")));
            holidays.add(new Days(21, "中秋节", sdf.parse("2017-09-16")));
            holidays.add(new Days(22, "中秋节", sdf.parse("2017-09-17")));

            holidays.add(new Days(23, "国庆节", sdf.parse("2017-10-01")));
            holidays.add(new Days(24, "国庆节", sdf.parse("2017-10-02")));
            holidays.add(new Days(25, "国庆节", sdf.parse("2017-10-03")));
            holidays.add(new Days(26, "国庆节", sdf.parse("2017-10-04")));
            holidays.add(new Days(27, "国庆节", sdf.parse("2017-10-05")));
            holidays.add(new Days(28, "国庆节", sdf.parse("2017-10-06")));
            holidays.add(new Days(29, "国庆节", sdf.parse("2017-10-07")));
            for (Days day : holidays) {
                //跟周末冲突的，不重复插入
                String sql = "select count(1) as numbers from fn_all_holiday where holiday_date ='" + sdf.format(day.getDate()) + "'";
                //结果集
                ResultSet rs = statement.executeQuery(sql);
                boolean hasRecord = false;
                while (rs.next()) {

                    if (!"0".equals(rs.getString("numbers"))) {
                        hasRecord = true;
                    }
                }
                if (!hasRecord) {
                    System.out.println("插入日期：" + sdf.format(day.getDate()) + "," + day.getTitle());
                    String insertSql = "INSERT INTO fn_all_holiday (title,holiday_date) VALUES('" + day.getTitle() +
                            "','" + sdf.format(day.getDate()) + "')";
                    statement.executeUpdate(insertSql);
                }
            }
            conn.close();
        } catch (ClassNotFoundException e) {
            System.out.println("Sorry,can't find the Driver!");
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Date> dateSplit(java.util.Date start, Date end) throws Exception {
        if (!start.before(end))
            throw new Exception("开始时间应该在结束时间之后");
        Long spi = end.getTime() - start.getTime();
        Long step = spi / (24 * 60 * 60 * 1000);// 相隔天数

        List<Date> dateList = new ArrayList<Date>();
        dateList.add(end);
        for (int i = 1; i <= step; i++) {
            dateList.add(new Date(dateList.get(i - 1).getTime() - (24 * 60 * 60 * 1000)));// 比上一天减一
        }
        return dateList;
    }
}