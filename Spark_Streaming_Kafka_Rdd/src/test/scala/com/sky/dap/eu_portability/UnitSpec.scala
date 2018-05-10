package com.sky.dap.eu_portability

import org.scalatest._

abstract class UnitSpec(component: String) extends WordSpec with Matchers with BeforeAndAfter with GivenWhenThen with BeforeAndAfterAll {
  val SPARK_APP_NAME = "identity_listener"
  val SPARK_MASTER = "local[*]"

  val failed = 'failure
  val successful = 'success

  def beActive = afterWord("be active")
  def isRelatedTo = afterWord("is related to")
}

