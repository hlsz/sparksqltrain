package com.data.demo

trait Iterator[A] {
  def hasNext: Boolean
  def next(): A
}

class IntIterator(to: Int) extends Iterator[Int] {
  private var current = 0
  override def hasNext: Boolean = current  < to

  override def next(): Int = {
    if (hasNext) {
      val t = current
      current += 1
      t
    } else { 0 }
  }
}

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

    val iterator = new IntIterator(10)
    iterator.next()
    iterator.next()

  }





}
