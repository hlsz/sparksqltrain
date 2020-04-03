package com.data.demo

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
//服务端发送消息到客户端
case class ServerMessage(msg:String)
//客户端向服务器发送消息
case class ClientMessage(msg:String)


class Server extends Actor{
  override def receive: Receive = {
    case "start" => println("服务端已启动")
    case ClientMessage(msg) => {
      println(s"服务端收到:$msg")
      //延时3秒（自定义设置，测试用，也可不设置）
      Thread.sleep(3000)
      //sender返回给客户端响应消息
      sender ! ServerMessage("服务端已收到消息")
    }
  }
}
//创建服务端Actor
object Server extends App {
  //服务端的IP地址
  val host = "172.18.18.132"
  //服务端的端口号（自定义设置，只要端口不被占用即可）
  val port = 5555
  //通信协议
  val conf = ConfigFactory.parseString(
    s"""|akka.actor.provider="akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname=$host
        |akka.remote.netty.tcp.port=$port
      """.stripMargin
  )
  val actorSystem = ActorSystem("server",conf)
  val serverActorRef = actorSystem.actorOf(Props[Server],"server")
  serverActorRef ! "start" //启动本服务端，调用receive方法
}
