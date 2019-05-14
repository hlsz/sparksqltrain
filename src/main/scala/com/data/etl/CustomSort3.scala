package com.data.etl

object CustomSort3 {
  def main(args: Array[String]): Unit = {

  }

}
case class User3 (val id:Long,
                  val name:String,
                  val fv:Int,
                  val age:Int) extends Ordered[User3] {
  override def compare(that: User3): Int = {
    if(that.fv == this.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

  override def toString: String = super.toString
}
