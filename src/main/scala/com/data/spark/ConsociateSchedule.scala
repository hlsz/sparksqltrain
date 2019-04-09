package com.data.spark

object ConsociateSchedule {


  def main(args: Array[String]): Unit = {

    new  GetTargetData().getTargetData("trade_get_data",20190101,20190401)

  }


}
