package de.ag.scala

import de.ag.sqala.QueryMonad.State
import de.ag.sqala._
import minitest.SimpleTestSuite

object queryComprehensionTest extends SimpleTestSuite {

  // Versuche
  val tbl1 = Query.makeBaseRelation("tb1",
    RelationalScheme.make(Seq(("one", Type.string), ("two", Type.integer))),
    "handle1")
  val stat = QueryMonad.State(0, null)

  val qm1 = for {
    t <- QueryMonad.embed(tbl1)
    p <- QueryMonad.project(Seq(("onex", AttributeRef("one_0")))) // FixMe _0 wrong; ohne _0 wird nicht gefunden
  } yield p

  def getRel[A](stat: (A, State)) : Any = stat._1 match {
    case Relation(_, scheme: RelationalScheme) => scheme
  }

  test("first tests") {
    assertEquals(QueryMonad.embed(tbl1).run(),
      (Relation(0, RelationalScheme(Vector("one", "two"), Map("one" -> Type.string, "two" -> Type.integer), None)),
        State(1, Product(
          EmptyQuery,
          Projection(Seq(("one_0", AttributeRef("one")), ("two_0", AttributeRef("two"))),
            BaseRelation("tb1", RelationalScheme(Vector("one", "two"), Map("one" -> Type.string, "two" -> Type.integer), None),
              "handle1"))))))
    // Why ?! - need AttRef one_0 instead to one :-/
    // -> merkt sich die alten Refs nicht oder falsch ausgeführt ... ?
    assertEquals(QueryMonad.embed(tbl1).flatMap(m => QueryMonad.project(Seq(("onex", Expression.makeAttributeRef("one_0"))))).run(),
      (Relation(1, RelationalScheme(Vector("onex"), Map("onex" -> Type.string), None)),
        State(2,
          Projection(Vector(("one_0", AttributeRef("one_0")), ("two_0", AttributeRef("two_0")), ("onex_1", AttributeRef("one_0"))),
            Product(
            EmptyQuery,
            Projection(Seq(("one_0", AttributeRef("one")), ("two_0", AttributeRef("two"))),
              BaseRelation("tb1", RelationalScheme(Vector("one", "two"), Map("one" -> Type.string, "two" -> Type.integer), None),
                "handle1")))))))
    val test1 = QueryMonad.embed(tbl1).flatMap(m => QueryMonad.project(Seq(("onex", Expression.makeAttributeRef("one_0")))))
    assertEquals(getRel(test1.run()), RelationalScheme.make(Seq(("onex", Type.string))))
    assertEquals(getRel(QueryMonad.embed(tbl1).run()), RelationalScheme.make(Seq(("one", Type.string), ("two", Type.integer))))
    assertEquals(getRel(qm1.run()), RelationalScheme.make(Seq(("onex", Type.string))))
  }
}