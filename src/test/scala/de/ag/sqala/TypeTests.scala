package de.ag.sqala

import TestUtil.assertEquals

import org.scalatest.FunSuite

class TypeTests extends FunSuite {

  test("nullability") {
    val n = Type.string.toNullable()
    assert(!Type.string.isNullable)
    assert(n.isNullable)
    assertEquals(n, n.toNullable())
    assertEquals(Type.string.toNullable().toNonNullable(), Type.string)
  }
}
