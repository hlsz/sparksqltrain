package com.scala.test

import com.data.utils.DateUtils
import org.junit._

@Test
class AppTest {

    @Test
//    def testOK() = assertTrue(true)
  def test(): Unit =
  {
     val sttr =     DateUtils.intToDateStr(20180903)
    println(sttr)
  }

//    @Test
//    def testKO() = assertTrue(false)

}


