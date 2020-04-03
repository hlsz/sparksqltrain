package com.data.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCUtils {

    public static void disconnect(Connection connection, ResultSet rs, PreparedStatement ps){
        try{
            if(rs != null ) rs.close();
            if(ps != null) ps.close();
            if(connection != null) connection.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

}
