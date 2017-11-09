package de.ag.sqala

object TestUtil {

  import org.scalatest.Assertion
  import org.scalatest.Assertions._

  def assertEquals(left: Any, right: Any): Assertion = assert(left === right)
}
