package com.data.demo


class Fruit(name:String) {
  def getFruitName():String = {name}

}

class Monkey(f:Fruit) {
  def say() = {
    {
      println("monkeylike" + f.getFruitName())
    }
  }
}


object Demo {

  implicit def fruitToMonkey(f:Fruit):Monkey = {new Monkey(f)  }

  def main(args: Array[String]): Unit = {
    val f:Fruit = new Fruit("banana")
    f.say()
  }





}
