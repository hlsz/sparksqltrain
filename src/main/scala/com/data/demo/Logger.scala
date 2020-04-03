package com.data.demo

// 单例对象
object Logger {

  def info(message: String) : Unit = println(s"INFO: $message")

}
