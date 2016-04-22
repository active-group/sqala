package de.ag.sqala

import minitest._

object RelationalAlgebraTests extends SimpleTestSuite {

  val tbl1 = Query.makeBaseRelation("tb1",
                                    RelationalScheme.make(Seq(("one", Type.string), ("two", Type.integer))),
                                    "handle1")
  val tbl2 = Query.makeBaseRelation("tbl2",
                                    RelationalScheme.make(Seq(("one", Type.integer), ("two", Type.string))),
                                    "handle2")
  val tbl3 = Query.makeBaseRelation("tbl3",
                                    RelationalScheme.make(Seq(("three", Type.integer), ("four", Type.string))),
                                    "handle3")

  test("projections") {
    val p = tbl1.project(Seq(("two", Expression.makeAttributeRef("two")),
                             ("one", Expression.makeAttributeRef("one"))))
    assertEquals(p.getScheme(), RelationalScheme.make(Seq(("two", Type.integer), ("one", Type.string))))
    assertEquals(p.project(Seq()), tbl1.project(Seq()))
  }

  test("restrictions") {
    val p = tbl1.restrict(Expression.makeConst(Type.boolean, true))
    assertEquals(p.getScheme(), tbl1.getScheme())
  }

  test("extend") {
    val p = tbl1.extend(Seq(("three", Expression.makeConst(Type.integer, 3)),
                            ("four", Expression.makeConst(Type.integer, 4))))
    assertEquals(p.getScheme(),
                 RelationalScheme.make(Seq(("one", Type.string), ("two", Type.integer),
                                           ("three", Type.integer), ("four", Type.integer))))
  }
}