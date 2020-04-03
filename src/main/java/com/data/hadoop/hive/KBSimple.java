package com.data.hadoop.hive;

import com.data.utils.JDBCUtils;
import org.apache.flink.table.expressions.E;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class KBSimple {


    private static String confPath = System.getProperty("user.dir") + File.separator + "conf";

    private static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String CONNECTION_URL="jdbc:hive2://vt-hadoop2:10000/;principal=hive/vt-hadoop2@HADOOP.COM";

    static {
        try{
            Class.forName(JDBC_DRIVER);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("通过JDBC连接连接kerberos环境下的hiveServer2");
        System.setProperty("java.security.krb5.conf", confPath+ File.separator+"krb5.conf");

        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication","Kerberos");
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("hive@HADOOP.COM",confPath+ File.separator+"hive.keytab");

        System.out.println(UserGroupInformation.getLoginUser());

        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try{
            connection = DriverManager.getConnection(CONNECTION_URL);
            ps= connection.prepareStatement("select * form test1");
            rs = ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getInt(1)+ "----"+ rs.getString(2));
            }
        }catch ( Exception e){
            e.printStackTrace();
        }finally {
            JDBCUtils.disconnect(connection, rs, ps);
        }

    }



}
