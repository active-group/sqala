package de.ag.sqala

import TestUtil.assertEquals

import org.scalatest.FunSuite

class SQLTest extends FunSuite {

  import SQLTest._

  val tbl1 = SQLSelectTable("personen", RelationalScheme.make(Seq(("id", Type.integer), ("first", Type.string), ("last", Type.string))))
  val firmAddr = SQLSelectTable("firm_address", RelationalScheme.make(Seq(("orte", Type.string))))
  val standorte = SQLSelectTable("standorte", RelationalScheme.make(Seq(("orte", Type.string))))

  val select1 = SQL.makeSQLSelect(Seq.empty, Seq((None, tbl1)))

  val tbl3 = SQL.makeSQLSelect(Seq(("city", SQLExpressionColumn("orte")), ("xx", SQLExpressionConst(Type.string, "Nn"))), Seq((None, standorte)))
  val tbl3S = ("(SELECT orte AS city, ? AS xx FROM standorte)", Seq((Type.string, "Nn")))


  test("Expression (App) / simple Tests") {
    assertEquals(SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionConst(Type.integer, 4), SQLExpressionConst(Type.integer, 5))).toSQL,
      ("(? = ?)", Seq((Type.integer, 4), (Type.integer, 5))))
    assertEquals(SQLExpressionApp(SQLOperator.isNotNull, Seq(SQLExpressionColumn("busy"))).toSQL,
      ("(busy) IS NOT NULL", Seq.empty))
    assertEquals(SQLExpressionApp(SQLOperator.bitNot, Seq(SQLExpressionColumn("set"))).toSQL,
      ("~(set)", Seq.empty))
    assertEquals(SQLExpressionApp(SQLOperator.between, Seq(SQLExpressionColumn("age"), SQLExpressionConst(Type.integer, 20), SQLExpressionConst(Type.integer, 40))).toSQL,
      ("(age BETWEEN ? AND ?)", Seq((Type.integer, 20), (Type.integer, 40))))
    assertEquals(SQLExpressionApp(SQLOperator.concat, Seq(SQLExpressionConst(Type.string, "xx"), SQLExpressionColumn("arg"))).toSQL,
      ("CONCAT(?,arg)", Seq((Type.string, "xx"))))
    assertEquals(SQLExpressionApp(SQLOperator.gt, Seq(
      SQLExpressionApp(SQLOperator.sum, Seq(SQLExpressionColumn("stueck"))),
      SQLExpressionConst(Type.integer, 10))).toSQL,
      ("(SUM(stueck) > ?)", Seq((Type.integer, 10))))
    assertEquals(longExpr.toSQL, longExpr1T)
    assertEquals(SQLExpressionOr(Seq(
      longExpr,
      SQLExpressionExists(tbl1),
      SQLExpressionApp(SQLOperator.leq, Seq(
        SQLExpressionColumn("v"),
        SQLExpressionSubquery(SQL.makeSQLSelect(
          Seq(("c", SQLExpressionApp(SQLOperator.count, Seq(SQLExpressionColumn("c"))))),
          Seq((None, standorte))))
      )))).toSQL,
      ("("+longExpr1T._1+" OR EXISTS (SELECT * FROM personen) OR (v <= (SELECT COUNT(c) AS c FROM standorte)))", longExpr1T._2))
  }


  test("Expression (others)") {
    assertEquals(SQLExpressionColumn("blub").toSQL, ("blub", Seq.empty))
    assertEquals(SQLExpressionConst(Type.integer, 5).toSQL, ("?", Seq((Type.integer, 5))))

    // Tuple
    assertEquals(SQLExpressionTuple(Seq(SQLExpressionConst(Type.integer, 5))).toSQL, ("(?)", Seq((Type.integer, 5))))
    assertEquals(SQLExpressionTuple(Seq(
        SQLExpressionConst(Type.integer, 5),
        SQLExpressionTuple(Seq(
          SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("age"), SQLExpressionConst(Type.integer, 95))),
          SQLExpressionConst(Type.string, "Alter"))),
        SQLExpressionColumn("name"))).toSQL,
      ("(?, ((age = ?), ?), name)", Seq((Type.integer, 5), (Type.integer, 95), (Type.string, "Alter"))))

    // Case
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

    // Exists
    assertEquals(SQLExpressionExists(adr1).toSQL, ("EXISTS (SELECT * FROM addresses)", Seq.empty))
    assertEquals(SQLExpressionExists(tbl3).toSQL, ("EXISTS "+tbl3S._1, tbl3S._2))

    // subQuery
    assertEquals(SQLExpressionSubquery(adr1).toSQL, ("(SELECT * FROM addresses)", Seq.empty))
    assertEquals(SQLExpressionSubquery(tbl3).toSQL, tbl3S)

    // AND and OR
    assertEquals(longExpr.toSQL, longExpr1T)
  }





  test("SelectCombineOperations") {
    // Test all known CombineOpterators
    def withAll(sqlI1: SQLInterpretations, sqlI2: SQLInterpretations, strLeft: String, strRight: String,
                paramsLeft: Seq[(Type, String)], paramsRight: Seq[(Type, String)]) = {
      val check : Seq[(SQLCombineOperator, String)] = Seq(
        (SQLCombineOperator.Union, " UNION "),
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
    assertEquals(SQLSelectEmpty.toSQLText, ("", Seq.empty))
    assertEquals(tbl1.toSQLText, ("SELECT * FROM personen", Seq.empty))
    assertEquals(select1.toSQLText, ("SELECT * FROM personen", Seq.empty))
    assertEquals(SQL.makeSQLSelect(Seq.empty, Seq((Some("personen"), tbl1))).toSQLText,
      ("SELECT * FROM personen AS personen", Seq.empty))
    assertEquals(SQL.makeSQLSelect(Seq("DISTINCT"), Seq(("id", SQLExpressionColumn("gzd"))), Seq((None, SQLSelectTable("baz", null)))).toSQLText,
      ("SELECT DISTINCT gzd AS id FROM baz", Seq.empty))
    assertEquals(SQLSelect(Some(Seq("DISTINCT")), Seq(("id", SQLExpressionColumn("gzd"))), Seq((None, SQLSelectTable("baz", null))),
      Seq.empty, Seq(SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("foo"), SQLExpressionConst(Type.string, "bla")))),
      Seq.empty,None, None, Some(Seq(("b", SQLOrderAscending))), None
      ).toSQLText,
      ("SELECT DISTINCT gzd AS id FROM baz WHERE (foo = ?) ORDER BY b ASC", Seq((Type.string, "bla"))))
  }



  test("complex SQL") {
    assertEquals(SQLSelect(None, Seq.empty,
      Seq((Some("one"), tbl1), (None, firmAddr)), Seq((Some("bl"), tbl2)),
      Seq(SQLExpressionApp(SQLOperator.isNull, Seq(SQLExpressionColumn("fabbs")))),
      Seq(SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("bab"), SQLExpressionColumn("blad")))),
      None, None, None, Some(Seq("LIMIT 5"))).toSQLText,
      ("SELECT * FROM (SELECT * FROM personen AS one, firm_address) LEFT JOIN (SELECT city, ? AS xx FROM addresses) AS bl "
        // FixMe: Bei PostgreSQL könnte es zu Problemen kommen, da die innere Query ein Alias haben sollte
        +"ON (bab = blad) WHERE (fabbs) IS NULL LIMIT 5", Seq((Type.string, "BlX"))))
    assertEquals(SQLSelect(None, Seq(("idc", SQLExpressionColumn("idc")), ("countThem", SQLExpressionApp(SQLOperator.count, Seq(SQLExpressionColumn("idc"))))),
      Seq((Some("one"), tbl1)), Seq((Some("bl"), tbl2)),
      Seq(SQLExpressionApp(SQLOperator.isNull, Seq(SQLExpressionColumn("fabbs")))),
      Seq(SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("bab"), SQLExpressionColumn("blad")))),
      Some(Seq("idc")),
      Some(Seq(SQLExpressionApp(SQLOperator.gt, Seq(SQLExpressionApp(SQLOperator.count, Seq(SQLExpressionColumn("idc"))), SQLExpressionConst(Type.integer, 9))))),
      Some(Seq(("countThem", SQLOrderDescending))), Some(Seq("LIMIT 5"))).toSQLText,
      ("SELECT idc, COUNT(idc) AS countThem"
        +" FROM personen AS one LEFT JOIN (SELECT city, ? AS xx FROM addresses) AS bl"
        +" ON (bab = blad) WHERE (fabbs) IS NULL"
        +" GROUP BY idc HAVING (COUNT(idc) > ?)"
        +" ORDER BY countThem DESC LIMIT 5", Seq((Type.string, "BlX"), (Type.integer, 9))))
  }


}

object SQLTest {

  val adr1 = SQLSelectTable("addresses", RelationalScheme.make(Seq(("city", Type.string))))
  val tbl2 = SQL.makeSQLSelect(Seq(("city", SQLExpressionColumn("city")), ("xx", SQLExpressionConst(Type.string, "BlX"))), Seq((None, adr1)))
  val tbl2S = ("(SELECT city, ? AS xx FROM addresses)", Seq((Type.string, "BlX")))

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
