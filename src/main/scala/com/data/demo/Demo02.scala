package com.data.demo

import scala.util.Random

object Demo02 {

  def factorial(x: Int) :Int = {
    def fact(x: Int, accumulator: Int): Int = {
      if(x <= 1) accumulator
      else fact(x - 1, x * accumulator)
    }
    fact(x, 1)
  }

//  柯里化  多参数列表


  def main(args: Array[String]): Unit = {
    println("factorial of 2: " + factorial(2))

    //  柯里化  多参数列表
    val numbers = List(1,2,3,4,5,6,7,8,9,10)
    val res = numbers.foldLeft(0)((m,n) => m + n)
    println(res)

    val numberFunc = numbers.foldLeft(List[Int]())_

    val squares = numberFunc((xs, x) => xs:+ x*x)
    println(squares.toString())

    numbers.foldLeft(0)((sum, item) => sum + item)
    numbers.foldRight(0)((sum,item) => sum + item)

    numbers.foldLeft(0)(_+_)
    numbers.foldRight(0)(_+_)

    (0 /: numbers)(_+_)
    (numbers :\ 0)(_+_)

//    案例类在比较的时候是按值比较而非按引用比较：
    case class Message(sender: String, recipient: String, body: String)
    val message1 = Message("a","b","xxxxx")
    val message2 = Message("a","b","xxxxx")
    val messageCompare = message1 == message2

//    浅拷贝
    val message5 = message1.copy(sender = message1.recipient, recipient = "c")
    println(message5.body, message5.recipient, message5.sender)

//    模式匹配

    val x: Int = Random.nextInt(10)
    x match {
      case 0 => "zeor"
      case 1 => "one"
      case _ => "other"
    }

    def matchTest(x: Int) : String = x match {
      case 1 => "one"
      case 2 => "two"
      case _ => "other"
    }
    matchTest(3)

    abstract class Notification

    case class Email(sender:String, title: String, body: String) extends Notification
    case class SMS(caller: String, message: String) extends Notification
    case class VoiceRecording(contactName: String, link: String) extends Notification

    def showNotification(notification: Notification): String = {
      notification match {
        case Email(sender, title, _) =>
          s"you got an email"
        case SMS(number, message) =>
          s"you got an sms"
        case VoiceRecording(name, link) =>
          s"you received a voice"
      }
    }






  }

}
