package com.data.demo


abstract class Device
sealed abstract class Furniture

case class Couch() extends Furniture
case class Chair() extends Furniture

case class Phone(model: String) extends Device {
  def screenOff = "Turning screen off"
}

case class Computer(model: String) extends Device {
  def screenSaveOn = "Turning screen saver on...."
}


class Demo04 {

  def goIdle(device: Device) = device match {
    //      仅匹配类型
    case p: Phone => p.screenOff
    case c: Computer => c.screenSaveOn
  }

  // 密封类
  // 特质和类可以用sealed标记为密封类，其所有子类都必须与之定
  // 义在相同文件中
  def findPlaceToSit(piece: Furniture): String = piece match  {
    case a: Couch => "lie on the courch"
    case b: Chair => "sit on the chair"
  }


}
