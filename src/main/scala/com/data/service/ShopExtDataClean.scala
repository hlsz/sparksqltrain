package com.data.service

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.data.utils.SnowflakeUtils
import org.apache.spark.sql.SparkSession

object ShopExtDataClean {

  //  /**
  //    * 如果有参数，直接返回参数中的值，如果没有默认是前一天的时间
  //    * @param args        :系统运行参数
  //    * @param pattern     :时间格式
  //    * @return
  //    */
  //  def gainDayByArgsOrSysCreate(args: Array[String],pattern: String):String = {
  //    //如果有参数，直接返回参数中的值，如果没有默认是前一天的时间
  //    if(args.length > 1) {
  //      args(1)
  //    } else {
  //      val previousDay = DateUtils.addOrMinusDay(new Date(), -1);
  //      DateUtils.dateFormat(previousDay, "yyyy-MM-dd");
  //    }
  //  }

  /**
    * args(0)         :json数据
    * args(1)         :mysql的ip地址
    * args(2)         :mysql数据库的端口号
    * args(3)         :mysql数据库用户
    * args(4)         :mysql数据库密码
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ShopDataClean")
      //.master("local[*]")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate();

    //    val previousDayStr = gainDayByArgsOrSysCreate(args,"yyyy-MM-dd")

    //val df = spark.read.json("/bplan/data-center/shop/" + previousDayStr + "/shop.json");
    val df = spark.read.json(args(0));
    spark.sql("use data_center");
    df.createOrReplaceTempView("shop_ext_temp")

    val df2 = spark.sql("SELECT " +
      "   st.areaName as areaName, " +
      "   st.areaCode as areaCode, " +
      "   st.agentId as agentId, " +
      "   st.agentName as agentName, " +
      "   st.rootCategoryId as rootCategoryId, " +
      "   st.parentCategoryId as parentCategoryId, " +
      "   st.industryId as industryId, " +
      "   st.industryName as industryName, " +
      "   set.shopId as shopId, " +
      "   set.businessType as businessType, " +
      "   set.addTime as addTime, " +
      "   set.num as num " +
      "FROM " +
      "    shop_ext_temp set left join tb_shop st " +
      "ON " +
      "    set.shopId = st.shopId and st.storeType in(1, 20)")
    //"    set.shopId = st.shopId and st.storeType = 1 or st.storeType = 20")

    //    val previousDay = DateUtils.addOrMinusDay(new Date(), -1);
    //    //将临时的数据存入到实际的tb_shop表中
    //    val pt_createDate = DateUtils.dateFormat(previousDay, "yyyyMMdd")

    var conn: Connection = null;
    var ps: PreparedStatement = null;
    val sql = s"insert into tb_shop_ext(" +
      s"id," +
      s"area_name," +
      s"area_code," +
      s"agent_id," +
      s"agent_name," +
      s"root_category_id," +
      s"parent_category_id," +
      s"industry_id," +
      s"industry_name," +
      s"shop_id," +
      s"business_type," +
      s"add_time," +
      s"num) " +
      s"values (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(s"jdbc:mysql://" + args(1) + ":" + args(2) + "/data_center", args(3), args(4))
      ps = conn.prepareStatement(sql)

      //关闭自动提交，即开启事务
      conn.setAutoCommit(false)

      var i = 1;
      df2.collect().foreach(
        x => {
          ps.setLong(1, SnowflakeUtils.getId)
          ps.setString(2, x.get(0).toString)
          ps.setString(3, x.get(1).toString)
          ps.setString(4, x.get(2).toString)
          ps.setString(5, x.get(3).toString)
          ps.setString(6, x.get(4).toString)
          ps.setString(7, x.get(5).toString)
          ps.setString(8, x.get(6).toString)
          ps.setString(9, x.get(7).toString)
          ps.setString(10, x.get(8).toString)
          ps.setInt(11, x.get(9).toString.toInt)
          ps.setLong(12, x.get(10).toString.toLong)
          ps.setInt(13, x.get(11).toString.toInt)
          ps.addBatch()

          i += 1;
          if (i % 500 == 0) {
            ps.executeBatch()
          }
        }
      )
      //最后不足500条的，直接执行批量更新操作
      ps.executeBatch()
      ps.close()
      //执行完后，手动提交事务
      conn.commit()
      //再把自动提交打开
      conn.setAutoCommit(true)
    } catch {
      case e: Exception => {
        //先打印出异常
        e.printStackTrace()
        try {
          //发生异常，事务回滚
          if (conn != null && !conn.isClosed) {
            conn.rollback()
            conn.setAutoCommit(true)
          }
        } catch {
          case ex: Exception => ex.printStackTrace()
        }
      }
    } finally {
      if (ps != null) {
        try {
          ps.close()
        } catch {
          //下面两行等价 case e : Exception => e.printStackTrace()
          //case e : ClassNotFoundException => e.printStackTrace()
          //case e : SQLException => e.printStackTrace()
          case e: Exception => e.printStackTrace()
        }
      }
      if (conn != null) {
        try {
          conn.close()
        } catch {
          case ex: Exception => ex.printStackTrace()
        }
      }
    }

    spark.stop();
    //程序正常退出
    System.exit(0)
  }
}
