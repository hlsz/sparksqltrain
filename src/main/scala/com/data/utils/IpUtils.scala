package com.data.utils

import com.ggstar.util.ip.IpHelper

object IpUtils {
  def getCity(ip:String) ={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCity("58.30.15.255"))
  }
}
