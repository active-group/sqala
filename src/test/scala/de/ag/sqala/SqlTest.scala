package de.ag.sqala

import TestUtil.assertEquals

import org.scalatest.FunSuite

class SQLTest extends FunSuite {

  test("SQLExpressionApp::toSQL works as expected") {
    val testArity = new SQLOperatorArity {
      override def toSQL(op: SQLOperator, rands: Seq[SQL.Return]) = rands.head // nonsense of course
    }
    val testOperator = SQLOperator("test", testArity)
    val testArg1 = SQLExpressionColumn("foo")
    val testArg2 = SQLExpressionColumn("bar")
    assert(SQLExpressionApp(testOperator, Seq(testArg1, testArg2)).toSQL === SQLOperator.toSQL(testOperator, Seq(testArg1.toSQL, testArg2.toSQL)))
  }

  test("SQLOperator::toSQL works as expected") {
    val testArity = new SQLOperatorArity {
      override def toSQL(op: SQLOperator, rands: Seq[SQL.Return]) = rands.head // nonsense of course
    }
    val testOperator = SQLOperator("test", testArity)
    val testArg1 = ("foo", Seq.empty)
    val testArg2 = ("bar", Seq.empty)

    assert(SQLOperator.toSQL(testOperator, Seq(testArg1, testArg2)) === testArity.toSQL(testOperator, Seq(testArg1, testArg2)))
  }

  test("SQLOperatorArity.Postfix works as expected") {
    val arity = SQLOperatorArity.Postfix
    val testOperator = SQLOperator("test", arity)
    val params = Seq((Type.string, "baz"))
    val testArg = ("foo", params)
    assert(arity.toSQL(testOperator, Seq(testArg)) === ("(foo) test", params))
  }

  test("SQLOperatorArity.Prefix works as expected") {
    val arity = SQLOperatorArity.Prefix
    val testOperator = SQLOperator("test", arity)
    val params = Seq((Type.string, "baz"))
    val testArg = ("foo", params)
    assert(arity.toSQL(testOperator, Seq(testArg)) === ("test(foo)", params))
  }

  test("SQLOperatorArity.Prefix2 works as expected") {
    val arity = SQLOperatorArity.Prefix2
    val testOperator = SQLOperator("test", arity, Some("::"))
    val params = Seq((Type.string, "baz"))
    val testArgs = Seq(("foo", params), ("42", Seq.empty))
    assert(arity.toSQL(testOperator, testArgs) === ("test(foo::42)", params))
  }

  test("SQLOperatorArity.Prefix3 works as expected") {
    val arity = SQLOperatorArity.Prefix3
    val testOperator = SQLOperator("test", arity, Some("::"))
    val params1 = Seq((Type.string, "baz"))
    val params2 = Seq((Type.string, "bar"))
    val testArgs = Seq(("foo", params1), ("42", Seq.empty), ("xxx", params2))
    assert(arity.toSQL(testOperator, testArgs) === ("(foo test 42 :: xxx)", params1 ++ params2))
  }

  test("SQLOperatorArity.PrefixN works as expected") {
    val arity = SQLOperatorArity.PrefixN
    val testOperator = SQLOperator("test", arity)
    val params1 = Seq((Type.string, "baz"))
    val params2 = Seq((Type.string, "bar"))
    val testArgs = Seq(("foo", params1), ("42", Seq.empty), ("xxx", params2))
    assert(arity.toSQL(testOperator, testArgs) === ("(foo test (42, xxx))", params1 ++ params2))
  }

  test("SQLOperatorArity.Infix works as expected") {
    val arity = SQLOperatorArity.Infix
    val testOperator = SQLOperator("test", arity)
    val params = Seq((Type.string, "baz"))
    val testArgs = Seq(("foo", params), ("42", Seq.empty))
    assert(arity.toSQL(testOperator, testArgs) === ("(foo test 42)", params))
  }

  import SQLTest._

  val tbl1 = SQLSelectTable("personen", RelationalScheme.make(Seq(("id", Type.integer), ("first", Type.string), ("last", Type.string))))
  val firmAddr = SQLSelectTable("firm_address", RelationalScheme.make(Seq(("orte", Type.string))))
  val standorte = SQLSelectTable("standorte", RelationalScheme.make(Seq(("orte", Type.string))))

  val select1 = SQL.makeSQLSelect(Seq.empty, Seq((None, tbl1)))

  val tbl3 = SQL.makeSQLSelect(Seq(("city", SQLExpressionColumn("orte")), ("xx", SQLExpressionConst(Type.string, "Nn"))), Seq((None, standorte)))
  val tbl3S = ("(SELECT orte AS city, ? AS xx FROM standorte AS __dummy0)", Seq((Type.string, "Nn")))

  test("SQLExpressionColumn::toSQL works as expected") {
    assert(SQLExpressionColumn("blub").toSQL === ("blub", Seq.empty))
  }

  test("SQLExpressionConst::toSQL works as expected") {
    assert(SQLExpressionConst(Type.integer, 5).toSQL === ("?", Seq((Type.integer, 5))))
  }

  test("SQLExpressionTuple::toSQL works as expected") {
    // TODO: cleanup, surroundSQL should be tested separately, and putJoiningInfix too
    assertEquals(SQLExpressionTuple(Seq(SQLExpressionConst(Type.integer, 5))).toSQL, ("(?)", Seq((Type.integer, 5))))
    assertEquals(SQLExpressionTuple(Seq(
        SQLExpressionConst(Type.integer, 5),
        SQLExpressionTuple(Seq(
          SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("age"), SQLExpressionConst(Type.integer, 95))),
          SQLExpressionConst(Type.string, "Alter"))),
        SQLExpressionColumn("name"))).toSQL,
      ("(?, ((age = ?), ?), name)", Seq((Type.integer, 5), (Type.integer, 95), (Type.string, "Alter"))))
  }

  test("SQLExpressionCase::toSQL works as expected") {
    // TODO: cleanup
    assertEquals(SQLExpressionCase(None, Seq(
      (SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("age"), SQLExpressionConst(Type.integer, 30))),
        SQLExpressionColumn("gebjahr"))),
      None).toSQL,
      ("(CASE WHEN (age = ?) THEN gebjahr END)", Seq((Type.integer, 30))))
    assertEquals(SQLExpressionCase(None, Seq(
      (SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("age"), SQLExpressionConst(Type.integer, 30))),
        SQLExpressionColumn("gebjahr")),
      (SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("age"), SQLExpressionConst(Type.integer, 35))),
        SQLExpressionApp(SQLOperator.plus, Seq(SQLExpressionColumn("gebjahr"), SQLExpressionConst(Type.integer, 1))))),
      Some(SQLExpressionApp(SQLOperator.minus, Seq(SQLExpressionColumn("gebjahr"), SQLExpressionConst(Type.integer, 10))))).toSQL,
      ("(CASE WHEN (age = ?) THEN gebjahr WHEN (age = ?) THEN (gebjahr + ?) ELSE (gebjahr - ?) END)",
        Seq((Type.integer, 30), (Type.integer, 35), (Type.integer, 1), (Type.integer, 10))))
    assertEquals(SQLExpressionCase(Some(SQLExpressionColumn("flag")), Seq(
      (SQLExpressionConst(Type.integer, 1), SQLExpressionColumn("wert1")),
      (SQLExpressionConst(Type.integer, 2), SQLExpressionColumn("wert2")),
      (SQLExpressionConst(Type.integer, 3), SQLExpressionConst(Type.integer, -1))),
      Some(SQLExpressionConst(Type.integer, -2))).toSQL,
      ("(CASE flag WHEN ? THEN wert1 WHEN ? THEN wert2 WHEN ? THEN ? ELSE ? END)",
        Seq((Type.integer, 1), (Type.integer, 2), (Type.integer, 3), (Type.integer, -1), (Type.integer, -2))))
    try {
      val t1 = SQLExpressionCase(Some(SQLExpressionColumn("blub")), Seq.empty, Some(SQLExpressionColumn("foo"))).toSQL
      // fail because the WHEN-THEN part/seq is empty!
      fail()
    } catch {
      case _ : AssertionError => // wanted
    }
  }

  test("SQLExpressionExists::toSQL works as expected") {
    val q = SQL.makeSQLSelect(Seq(("city", SQLExpressionColumn("orte")), ("xx", SQLExpressionConst(Type.string, "Nn"))), Seq((None, standorte)))
    assert(SQLExpressionExists(q).toSQL === ("EXISTS (" + q.toSQLText._1 +")", q.toSQLText._2))
  }

  test("SQLExpressionSubquery::toSQL works as expected") {
    val q = SQL.makeSQLSelect(Seq(("city", SQLExpressionColumn("orte")), ("xx", SQLExpressionConst(Type.string, "Nn"))), Seq((None, standorte)))
    assert(SQLExpressionSubquery(q).toSQL === ("(" + q.toSQLText._1 +")", q.toSQLText._2))
  }

  test("SQLExpressionAnd::toSQL works as expected") {
    val expr1 = SQLExpressionConst(Type.integer, 3)
    val expr2 = SQLExpressionColumn("blub")
    assert(SQLExpressionAnd(Seq(expr2)).toSQL === expr2.toSQL)
    assert(SQLExpressionAnd(Seq(expr1, expr2)).toSQL === ("(" + expr1.toSQL._1 + " AND " + expr2.toSQL._1 + ")", expr1.toSQL._2 ++ expr2.toSQL._2))
  }

  test("SQLExpressionOr::toSQL works as expected") {
    val expr1 = SQLExpressionConst(Type.integer, 3)
    val expr2 = SQLExpressionColumn("blub")
    assert(SQLExpressionOr(Seq(expr2)).toSQL === expr2.toSQL)
    assert(SQLExpressionOr(Seq(expr1, expr2)).toSQL === ("(" + expr1.toSQL._1 + " OR " + expr2.toSQL._1 + ")", expr1.toSQL._2 ++ expr2.toSQL._2))
  }


  test("SelectCombineOperations work as expected") {
    // Test all known CombineOpterators
    def withAll(sqlI1: SQL, sqlI2: SQL, strLeft: String, strRight: String,
                paramsLeft: Seq[(Type, String)], paramsRight: Seq[(Type, String)]) = {
      val check : Seq[(SQLCombineOperator, String)] = Seq(
        (SQLCombineOperator.Union, " UNION ALL "),
        (SQLCombineOperator.Intersection, " INTERSECT "),
        (SQLCombineOperator.Difference, " EXCEPT "))
      for (elem <- check) {
        assertEquals(SQLSelectCombine(elem._1, sqlI1, sqlI2).toSQLText,
          (strLeft+elem._2+strRight, paramsLeft++paramsRight))
        // should also work the other way round; is symetrisch
        // -> Result of the Query could be different but the way get the Query is the same
        assertEquals(SQLSelectCombine(elem._1, sqlI2, sqlI1).toSQLText,
          (strRight+elem._2+strLeft, paramsRight++paramsLeft))
      }
    }

    withAll(adr1, firmAddr, "(SELECT * FROM addresses)", "(SELECT * FROM firm_address)", Seq.empty, Seq.empty)
    withAll(tbl2, tbl3, tbl2S._1, tbl3S._1, tbl2S._2, tbl3S._2)
  }


  test("toSQL / simple Querys") {
    assertEquals(SQLSelectEmpty.toSQLText, ("SELECT * FROM (SELECT 1) AS TBL WHERE 2=3", Seq.empty))
    assertEquals(tbl1.toSQLText, ("SELECT * FROM personen", Seq.empty))
    assertEquals(select1.toSQLText, ("SELECT * FROM personen AS __dummy0", Seq.empty))
    assertEquals(SQL.makeSQLSelect(Seq.empty, Seq((Some("personen"), tbl1))).toSQLText,
      ("SELECT * FROM personen AS personen", Seq.empty))
    assertEquals(SQL.makeSQLSelect(Seq("DISTINCT"), Seq(("id", SQLExpressionColumn("gzd"))), Seq((None, SQLSelectTable("baz", null)))).toSQLText,
      ("SELECT DISTINCT gzd AS id FROM baz AS __dummy0", Seq.empty))
    assertEquals(SQLSelect(Some(Seq("DISTINCT")), Seq(("id", SQLExpressionColumn("gzd"))), Seq((None, SQLSelectTable("baz", null))),
      Seq.empty, Seq(SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("foo"), SQLExpressionConst(Type.string, "bla")))),
      Seq.empty,None, None, Some(Seq(("b", SQLOrderAscending))), None
      ).toSQLText,
      ("SELECT DISTINCT gzd AS id FROM baz AS __dummy0 WHERE (foo = ?) ORDER BY b ASC", Seq((Type.string, "bla"))))
  }



  test("complex SQL") {
    assertEquals(SQLSelect(None, Seq.empty,
      Seq((Some("one"), tbl1), (None, firmAddr)), Seq((Some("bl"), tbl2)),
      Seq(SQLExpressionApp(SQLOperator.isNull, Seq(SQLExpressionColumn("fabbs")))),
      Seq(SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("bab"), SQLExpressionColumn("blad")))),
      None, None, None, Some(Seq("LIMIT 5"))).toSQLText,
      ("SELECT * FROM (SELECT * FROM personen AS one, firm_address AS __dummy1) LEFT JOIN (SELECT city, ? AS xx FROM addresses AS __dummy0) AS bl "
        +"ON (bab = blad) WHERE (fabbs) IS NULL LIMIT 5", Seq((Type.string, "BlX"))))
    assertEquals(SQLSelect(None, Seq(("idc", SQLExpressionColumn("idc")), ("countThem", SQLExpressionApp(SQLOperator.count, Seq(SQLExpressionColumn("idc"))))),
      Seq((Some("one"), tbl1)), Seq((Some("bl"), tbl2)),
      Seq(SQLExpressionApp(SQLOperator.isNull, Seq(SQLExpressionColumn("fabbs")))),
      Seq(SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("bab"), SQLExpressionColumn("blad")))),
      Some(Set("idc")),
      Some(Seq(SQLExpressionApp(SQLOperator.gt, Seq(SQLExpressionApp(SQLOperator.count, Seq(SQLExpressionColumn("idc"))), SQLExpressionConst(Type.integer, 9))))),
      Some(Seq(("countThem", SQLOrderDescending))), Some(Seq("LIMIT 5"))).toSQLText,
      ("SELECT idc, COUNT(idc) AS countThem"
        +" FROM personen AS one LEFT JOIN (SELECT city, ? AS xx FROM addresses AS __dummy0) AS bl"
        +" ON (bab = blad) WHERE (fabbs) IS NULL"
        +" GROUP BY idc HAVING (COUNT(idc) > ?)"
        +" ORDER BY countThem DESC LIMIT 5", Seq((Type.string, "BlX"), (Type.integer, 9))))

  }

  test("fromQuery works") {
    val sch1 = RelationalScheme.make(Seq("c1" -> Type.integer, "c2" -> Type.string))
    val base = Query.makeBaseRelation("test", sch1, ())
    assert(SQL.fromQuery(base) === SQLSelectTable("test", sch1))
    val expr1 = Expression.makeConst(Type.boolean, false)
    val restrict1 = base.restrict(expr1)
    assert(SQL.fromQuery(restrict1) === SQLSelect.make(tables = Seq((None, SQLSelectTable("test", sch1))), criteria = Seq(SQLExpression.fromExpression(expr1))))
    val expr2 = Expression.makeConst(Type.boolean, true)
    val restrict2 = restrict1.restrict(expr2)
    assert(SQL.fromQuery(restrict2) !== SQL.fromQuery(restrict1))
    // TODO? assert(SQL.fromQuery(restrict2) === SQLSelect.make(tables = Seq((None, SQLSelectTable("test", sch1))), criteria = Seq(SQLExpression.fromExpression(expr1), SQLExpression.fromExpression(expr2))))
  }
}

object SQLTest {

  val adr1 = SQLSelectTable("addresses", RelationalScheme.make(Seq(("city", Type.string))))
  val tbl2 = SQL.makeSQLSelect(Seq(("city", SQLExpressionColumn("city")), ("xx", SQLExpressionConst(Type.string, "BlX"))), Seq((None, adr1)))
  val tbl2S = ("(SELECT city, ? AS xx FROM addresses AS __dummy0)", Seq((Type.string, "BlX")))

  val longExpr = SQLExpressionOr(Seq(
    SQLExpressionExists(tbl2),
    SQLExpressionAnd(Seq(
      SQLExpressionApp(SQLOperator.isNotNull, Seq(SQLExpressionColumn("house"))),
      SQLExpressionApp(SQLOperator.neq, Seq(SQLExpressionColumn("door"), SQLExpressionConst(Type.string, "closed")))
    )),
    SQLExpressionApp(SQLOperator.isNull, Seq(SQLExpressionColumn("house")))
  ))

  val longExpr1T = ("(EXISTS "+tbl2S._1+" OR ((house) IS NOT NULL AND (door <> ?)) OR (house) IS NULL)", tbl2S._2++Seq((Type.string, "closed")))

}
