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
  val tbl4 = Query.makeBaseRelation("tbl4",
                                    RelationalScheme.make(Seq(("one", Type.string), ("two", Type.integer),
                                      ("three", Type.integer), ("four", Type.string))),
                                    "handle4")

  test("projections") {
    val p = tbl1.project(Seq(("two", Expression.makeAttributeRef("two")),
                             ("one", Expression.makeAttributeRef("one"))))
    assertEquals(p.getScheme(), RelationalScheme.make(Seq(("two", Type.integer), ("one", Type.string))))
    assertEquals(p.project(Seq()), tbl1.project(Seq()))
    val p2 = tbl1.project(Seq(("twelve", Expression.makeAttributeRef("two"))))
    assertEquals(p2.getScheme(), RelationalScheme.make(Seq(("twelve", Type.integer))))
    val p3 = tbl1.project(Seq(("two", Expression.makeAttributeRef("two")), ("one", Expression.makeConst(Type.integer, 4))))
    assertEquals(p3.getScheme(), RelationalScheme.make(Seq(("two", Type.integer), ("one", Type.integer))))
    try { // FIXME : wrong type not detected; perhaps included later ?!
      val p4 = tbl1.project(Seq(("two", Expression.makeAttributeRef("two")), ("one", Expression.makeConst(Type.integer, "blub"))))
      //fail("Don't check constant value")
    } catch {
      case _ : AssertionError => // nothing, because wanted
    }
    try { // FIXME : should not work; because reference not exists
      val p5 = tbl1.project(Seq(("two", Expression.makeAttributeRef("tow"))))
      //fail("Referenz on a attribute which not exists")
    } catch {
      case _ : AssertionError => // wanted
    }
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

  // TODO: Nochmal genauer mit Verwendung Quotient anschauen. (Note für mich/Sabrina)
  test("difference") {
    assertEquals(tbl1.getScheme().difference(RelationalScheme.make(Seq(("two", Type.integer)))),
      RelationalScheme.make(Seq(("one", Type.string))))
    assertEquals(tbl4.getScheme().difference(RelationalScheme.make(Seq(("three", Type.integer)))),
      RelationalScheme.make(Seq(("one", Type.string), ("two", Type.integer), ("four", Type.string))))
    try {
      val p = tbl1.getScheme().difference(RelationalScheme.make(Seq(("one", Type.string), ("two", Type.integer))))
      fail("The Relation has no columns")
    } catch {
      case _ : AssertionError => // wanted
    }
    // FIXME : ignore or throw Error ? - by remove a not exist Column -> i think an Error
    try {
      assertEquals(tbl1.getScheme().difference(RelationalScheme.make(Seq(("three", Type.integer)))),
        tbl1.getScheme())
      //fail("Deleting a not exist Error")
    } catch {
      case _ : AssertionError => // wanted
    }
    try { // FIXME : Should the difference look on the Column Name ? - Is the Typ of the column interesting?
    val p = tbl4.getScheme().difference(RelationalScheme.make(Seq(("one", Type.integer))))
      //fail("The Relational Enviroment has the wrong type!")
    } catch {
      case _ : AssertionError => // wanted
    }
  }

  test("allgemeine Verständnistests") {
    //assertEquals(tbl1.getScheme(Map("three" -> Type.string)), 0)
  }
}