package com.scala.service

import java.sql.DriverManager

object SparkSQLThriftServer {
  def main(args: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val connection = DriverManager.getConnection("jdbc:hive2://localhost:10000","root","")
    connection.prepareStatement("use sid").execute()
    val pstmt = connection.prepareStatement("select id,name,salary,destination from emp")
    val rs = pstmt.executeQuery()
    while(rs.next()){
      print("id:"+rs.getInt("id")+",name"+
      rs.getString("name")+", salary:"+
        rs.getString("salary")
        +", destination: "+rs.getString("destination"))
    }

    rs.close()
    pstmt.close()
    connection.close()

  }

}
