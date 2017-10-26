package de.ag.scala

import de.ag.sqala.QueryMonad.State
import de.ag.sqala._
import minitest.SimpleTestSuite

object queryComprehensionTest extends SimpleTestSuite {

  // Versuche
  val tbl1 = Query.makeBaseRelation("tb1",
    RelationalScheme.make(Seq(("one", Type.string), ("two", Type.integer))),
    "handle1")
  val tbl3 = Query.makeBaseRelation("tbl3",
    RelationalScheme.make(Seq(("three", Type.integer), ("four", Type.string))),
    "handle3")
  val stat = QueryMonad.State(0, null)

  val qm1 = for {
    t <- QueryMonad.embed(tbl1)
    p <- QueryMonad.project(Seq(("onex", t.!("one"))))
  } yield p

  def getRel[A](stat: (A, State)) : Any = stat._1 match {
    case Relation(_, scheme: RelationalScheme) => scheme
  }

  def getQuery(stat: (Any, State)) : Query = stat._2.query

  test("first tests - projektion") {
    assertEquals(QueryMonad.embed(tbl1).run(),
      (Relation(0, RelationalScheme(Vector("one", "two"), Map("one" -> Type.string, "two" -> Type.integer), None)),
        State(1, Product(
          EmptyQuery,
          Projection(Seq(("one_0", AttributeRef("one")), ("two_0", AttributeRef("two"))),
            BaseRelation("tb1", RelationalScheme(Vector("one", "two"), Map("one" -> Type.string, "two" -> Type.integer), None),
              "handle1"))))))
    assertEquals(getRel(QueryMonad.embed(tbl1).run()), RelationalScheme.make(Seq(("one", Type.string), ("two", Type.integer))))
    assertEquals(getRel(qm1.run()), RelationalScheme.make(Seq(("onex", Type.string))))

    assertEquals(getRel((for {
      t <- QueryMonad.embed(tbl1)
      p <- QueryMonad.project(Seq(("one", t.!("two")), ("new", Expression.makeConst(Type.boolean, true)), ("two", t.!("one"))))
    } yield p).run()), RelationalScheme.make(Seq(("one", Type.integer), ("new", Type.boolean), ("two", Type.string))))
  }

  test("test with restrict on one table") {
    assertEquals(getRel((for {
      t <- QueryMonad.embed(tbl1)
      res <- QueryMonad.restrict(Application(
        Rator("any", _.head),  Seq(Expression.makeConst(Type.boolean, "a"), Expression.makeConst(Type.boolean, "b"))))
      p <- QueryMonad.project(Seq(("one", t.!("one"))))
    } yield p).run()), RelationalScheme.make(Seq(("one", Type.string))))
    assertEquals(getRel((for {
      t <- QueryMonad.embed(tbl1)
      res <- QueryMonad.restrict(Application(
        Rator("any2", _.head), Seq(Expression.makeConst(Type.boolean, "a"), t.!("one"))))
      p <- QueryMonad.project(Seq(("x", t.!("one"))))
    } yield p).run()), RelationalScheme.make(Seq(("x", Type.string))))
  }

  def RatEq(seq : Seq[Type]) : Type = {
    if(seq.size != 2)
      throw new AssertionError("Invalid Expression size in '='")
    else if(seq(0) == seq(1))
      Type.boolean
    else
      throw new AssertionError("The Types must be the same")
  }

  val join1 = for {
    t <- QueryMonad.embed(tbl1)
    t2 <- QueryMonad.embed(tbl3)
    r <- QueryMonad.restrict(Application(
      Rator("=", s => RatEq(s)), Seq(t.!("two"), t2.!("three"))
    ))
    p <- QueryMonad.project(Seq(("Wert1", t.!("one")), ("Wert2", t2.!("four"))))
  } yield p

  test("join two tables") {
    assertEquals(getRel(join1.run()), RelationalScheme.make(Seq(("Wert1", Type.string), ("Wert2", Type.string))))
    //join1.run(),
/*->(Relation(2, RelationalScheme(Vector("Wert1", "Wert2"),Map("Wert1" -> Type.string, "Wert2" -> Type.string),None)),
        State(3, Projection(Vector(("one_0",AttributeRef("one_0")), ("two_0",AttributeRef("two_0")), ("three_1",AttributeRef("three_1")), ("four_1",AttributeRef("four_1")), ("Wert1_2",AttributeRef("one_0")), ("Wert2_2",AttributeRef("four_1"))),
          Restriction(Application(Rator("=",s => RatEq(s)),List(AttributeRef("two_0"), AttributeRef("three_1"))),
            Product(
              Product(
                EmptyQuery,
                Projection(
                  Vector(("one_0",AttributeRef("one")), ("two_0",AttributeRef("two"))),
                  BaseRelation("tb1",RelationalScheme(Vector("one", "two"),Map("one" -> Type.string, "two" -> Type.integer),None),"handle1"))),
              Projection(Vector(("three_1",AttributeRef("three")), ("four_1",AttributeRef("four"))),
                BaseRelation("tbl3", RelationalScheme(Vector("three", "four"), Map("three" -> Type.integer, "four" -> Type.string),None),"handle3"))))))) */
  }

  test("subquery") {
    def q1(e: Expression) =
      for {
        t3 <- QueryMonad.embed(tbl3)
        _ <- QueryMonad.restrict(e)
      } yield t3
    val a2 =
      for {
        t <- QueryMonad.embed(tbl1)
        sq <- QueryMonad.subquery(q1(t ! "one"))
        _ <- QueryMonad.restrict(Expression.makeScalarSubquery(sq))
      } yield t
    assertEquals(a2.buildQuery(), null)
  }


  test("Translation to SqlSelect") {
    assertEquals(EmptyQuery.toSqlSelect(), SqlSelectEmpty)
    assertEquals(tbl1.toSqlSelect(), SqlSelectTable("tb1", RelationalScheme.make(Seq(("one", Type.string), ("two", Type.integer)))))
    assertEquals(QueryMonad.embed(tbl1).buildQuery().toSqlSelect().toSQL,
      ("SELECT one_0 AS one, two_0 AS two FROM (SELECT one AS one_0, two AS two_0 FROM tb1)", Seq()))
    //assertEquals(qm1.run()._2, 0)
    assertEquals(qm1.buildQuery().toSqlSelect().toSQL,
      ("SELECT onex_1 AS onex FROM (SELECT one_0, two_0, one_0 AS onex_1 FROM (SELECT one AS one_0, two AS two_0 FROM tb1))", Seq()))
  }
}
