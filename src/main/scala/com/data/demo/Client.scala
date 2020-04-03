package com.data.demo

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.io.StdIn

class Client(host:String,port:Int) extends Actor{
  //创建服务端代理对象
  var serverActorRef:ActorSelection= _ //null,注明值的类型
  //在执行receive方法之前要调用的方法
  override def preStart(): Unit = {
    //server@${host}:${port}/user/server
    //第一个server是在服务端actorSystem设置时自己命名的
    //host服务端ip地址，port服务端的端口号
    //第二个server是在服务端actorSystem.actorOf()设置时自己命名的
    serverActorRef = context.actorSelection(s"akka.tcp://server@${host}:${port}/user/server")
  }
  override def receive: Receive = {
    case "start" => println("客户端已启动")
    case msg:String => {
      serverActorRef ! ClientMessage(msg) //在客户端向服务端发送消息
    }
    //服务端的响应消息
    case ServerMessage(msg) => {
      println(s"响应消息:$msg")
    }
  }
}

//创建客户端Actor
object Client extends App{
  //客户端的IP地址
  val host = "172.18.18.132"
  //客户端的端口号（自定义设置，只要端口不被占用即可）
  val port = 7777

  //目的（服务端）地址
  val serverHost = "172.18.18.132"
  //目的（服务端）端口号
  val serverPort = 5555

  val conf = ConfigFactory.parseString(
    s"""|akka.actor.provider="akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname=$host
        |akka.remote.netty.tcp.port=$port
      """.stripMargin
  )
  val  clientSystem = ActorSystem("client",conf)
  val actorRef = clientSystem.actorOf(Props(new Client(serverHost,serverPort)),"client")
  actorRef ! "start" //启动本客户端，调用receive方法
  while(true){
    print("客户端：")
    val str = StdIn.readLine()//强制读取控制台输入
    actorRef ! str
  }
}
