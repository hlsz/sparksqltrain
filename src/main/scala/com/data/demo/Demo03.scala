package com.data.demo

class Student[T](val localId: T) {
  def getSchool(hukouId: T) = "Student-" + hukouId + "-" + localId
}

class Person(val name: String) {
  def sayHello = println("Hello, my name is "+ name)
  def makeFriends(p: Person): Unit = {
    sayHello
    p.sayHello
  }
}

class Dog (val name: String){
  def sayHello = println("wang wange , is "+ name)
}

class Parent(val name: String) {
}
class Child(name: String) extends Parent(name) {
}
class Student2(name: String) extends Person(name){
}

// 上边界Bounds
class Party[T <: Person](p1:T, p2:T) {
  def play = p1.makeFriends(p2)
}
// View Bounds
class Party2[T <% Person](p1:T, p2:T) {
  def play = p1.makeFriends(p2)
}

// Context Bounds
class Calculator[T:Ordering](val num1:T, num2:T){
  def getMax(implicit order: Ordering[T]) = if(order.compare(num1, num2) > 0 ) num1 else num2
}

//manifest conext bounds
class Meat(val name:String) {

}
class Vegetable(val name: String){

}



object Main {
  def main(args: Array[String]): Unit = {
    var s = new Student[Int](1111)
    println(s.getSchool(222))

    var s1 = new Student[String]("1111")
    println(s1.getSchool("3333"))

    var s2 = new Student("111")
    println(s2.getSchool("22222"))

    getCard[Int](100)
    getCard[String]("100")
    getCard(120)

    var s3 = new Student2("zhao hu")
    var s4 = new Student2("zhang long")
    var p = new Party[Student2](s3,s4)
    p.play

    var father = new Parent("parent")
    var child = new Child("child")
    var person = new Person("person")
    getIdCard(father)
    getIdCard(child)
    getIdCard(person)

    val s5 = new Student2("z j")
    val d = new Dog("d j")
    val person5 = dog2person(d)
    val party = new Party2(s5, person5)
    party.play

    var c = new Calculator(1,2)
    println(c.getMax)

    val meat1 = new Meat("yuxiangrousi")
    val meat2 = new Meat("xiaosurou")
    val meat3 = new Meat("hongshaorou")

    val vegetable1 = new Vegetable("xiaoqingcai")
    val vegetable2 = new Vegetable("baicai")
    val vegetable3 = new Vegetable("qincai")
    val objects2 = packageFood(vegetable1, vegetable2,
      vegetable3)
    println(objects2(0).getClass)



  }

  def packageFood[T:Manifest](food:T*) = {
    val packageFood = new Array[T](food.length)
    for(i <- 0 until food.length) {
      packageFood(i) = food(i)
    }
    packageFood
  }

  implicit def dog2person(dog: Object) : Person = {
    if (dog.isInstanceOf[Dog]) {
      val _dog = dog.asInstanceOf[Dog]
      new Person(_dog.name)
    }
    else
      Nil
  }


  //下边界Bounds
  def getIdCard[T >: Child](p:T) :Unit = {
    if(p.getClass == classOf[Child]) println("please tell me your parent name")
    else if(p.getClass == classOf[Parent]) println("please sign your parent naem to get your id card")
    else println("your can't get id card")

  }

  def getCard[T](content:T):Unit = {
    if (content.getClass == classOf[Integer]) println("int card")
    else if(content.getClass == classOf[String]) println("String card")
    else println("card: "+ content)
  }





}
