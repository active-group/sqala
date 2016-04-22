package de.ag.sqala

import minitest._

object TypeTests extends SimpleTestSuite {
  test("nullability") {
    val n = Type.string.toNullable()
    assert(!Type.string.isNullable)
    assert(n.isNullable)
    assertEquals(n, n.toNullable())
    assertEquals(Type.string.toNullable().toNonNullable(), Type.string)
  }
}
