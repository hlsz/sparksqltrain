package com.data.demo

import com.data.demo.Logger.info

class Project(name: String, daysToComplete: Int)

class SingleClassTest {
  val project1 = new Project("Tps Report", 1)
  val project2 = new Project("webSite", 5)
  info("Created projects")
}
