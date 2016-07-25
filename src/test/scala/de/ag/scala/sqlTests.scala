package de.ag.scala

import de.ag.sqala._
import minitest._

object SqlTests extends SimpleTestSuite {

  val tbl1 = SqlSelectTable("personen", RelationalScheme.make(Seq(("id", Type.integer), ("first", Type.string), ("last", Type.string))))
  val adr1 = SqlSelectTable("addresses", RelationalScheme.make(Seq(("city", Type.string))))
  val firmAddr = SqlSelectTable("firm_address", RelationalScheme.make(Seq(("orte", Type.string))))
  val standorte = SqlSelectTable("standorte", RelationalScheme.make(Seq(("orte", Type.string))))

  val select1 = SQL.makeSqlSelect(Seq.empty, Seq((None, tbl1)))

  val tbl2 = SQL.makeSqlSelect(Seq(("city", SqlExpressionColumn("city")), ("xx", SqlExpressionConst(Type.string, "BlX"))), Seq((None, adr1)))
  val tbl2S = ("(SELECT city, ? AS xx FROM addresses)", Seq((Type.string, "BlX")))
  val tbl3 = SQL.makeSqlSelect(Seq(("city", SqlExpressionColumn("orte")), ("xx", SqlExpressionConst(Type.string, "Nn"))), Seq((None, standorte)))
  val tbl3S = ("(SELECT orte AS city, ? AS xx FROM standorte)", Seq((Type.string, "Nn")))

  val longExpr = SqlExpressionOr(Seq(
    SqlExpressionExists(tbl2),
    SqlExpressionAnd(Seq(
      SqlExpressionApp(SqlOperator.isNotNull, Seq(SqlExpressionColumn("house"))),
      SqlExpressionApp(SqlOperator.neq, Seq(SqlExpressionColumn("door"), SqlExpressionConst(Type.string, "closed")))
    )),
    SqlExpressionApp(SqlOperator.isNull, Seq(SqlExpressionColumn("house")))
  ))
  val longExpr1T = ("(EXISTS "+tbl2S._1+" OR ((house) IS NOT NULL AND (door <> ?)) OR (house) IS NULL)", tbl2S._2++Seq((Type.string, "closed")))


  test("Expression (App) / simple Tests") {
    assertEquals(SqlExpressionApp(SqlOperator.eq, Seq(SqlExpressionConst(Type.integer, 4), SqlExpressionConst(Type.integer, 5))).toSQL,
      ("(? = ?)", Seq((Type.integer, 4), (Type.integer, 5))))
    assertEquals(SqlExpressionApp(SqlOperator.isNotNull, Seq(SqlExpressionColumn("busy"))).toSQL,
      ("(busy) IS NOT NULL", Seq.empty))
    assertEquals(SqlExpressionApp(SqlOperator.bitNot, Seq(SqlExpressionColumn("set"))).toSQL,
      ("~(set)", Seq.empty))
    assertEquals(SqlExpressionApp(SqlOperator.between, Seq(SqlExpressionColumn("age"), SqlExpressionConst(Type.integer, 20), SqlExpressionConst(Type.integer, 40))).toSQL,
      ("age BETWEEN ? AND ?", Seq((Type.integer, 20), (Type.integer, 40))))
    assertEquals(SqlExpressionApp(SqlOperator.concat, Seq(SqlExpressionConst(Type.string, "xx"), SqlExpressionColumn("arg"))).toSQL,
      ("CONCAT(?,arg)", Seq((Type.string, "xx"))))
    assertEquals(SqlExpressionApp(SqlOperator.gt, Seq(
      SqlExpressionApp(SqlOperator.sum, Seq(SqlExpressionColumn("stueck"))),
      SqlExpressionConst(Type.integer, 10))).toSQL,
      ("(SUM(stueck) > ?)", Seq((Type.integer, 10))))
    // ToDo Test more, test other operators
  }


  test("Expression (others)") {
    assertEquals(SqlExpressionColumn("blub").toSQL, ("blub", Seq.empty))
    assertEquals(SqlExpressionConst(Type.integer, 5).toSQL, ("?", Seq((Type.integer, 5))))

    // Tuple
    assertEquals(SqlExpressionTuple(Seq(SqlExpressionConst(Type.integer, 5))).toSQL, ("(?)", Seq((Type.integer, 5))))
    assertEquals(SqlExpressionTuple(Seq(
        SqlExpressionConst(Type.integer, 5),
        SqlExpressionTuple(Seq(
          SqlExpressionApp(SqlOperator.eq, Seq(SqlExpressionColumn("age"), SqlExpressionConst(Type.integer, 95))),
          SqlExpressionConst(Type.string, "Alter"))),
        SqlExpressionColumn("name"))).toSQL,
      ("(?, ((age = ?), ?), name)", Seq((Type.integer, 5), (Type.integer, 95), (Type.string, "Alter"))))

    // Case
    assertEquals(SqlExpressionCase(None, Seq(
      (SqlExpressionApp(SqlOperator.eq, Seq(SqlExpressionColumn("age"), SqlExpressionConst(Type.integer, 30))),
        SqlExpressionColumn("gebjahr"))),
      None).toSQL,
      ("(CASE WHEN (age = ?) THEN gebjahr END)", Seq((Type.integer, 30))))
    assertEquals(SqlExpressionCase(Some(SqlExpressionColumn("flag")), Seq(
      (SqlExpressionConst(Type.integer, 1), SqlExpressionColumn("wert1")),
      (SqlExpressionConst(Type.integer, 2), SqlExpressionColumn("wert2")),
      (SqlExpressionConst(Type.integer, 3), SqlExpressionConst(Type.integer, -1))),
      Some(SqlExpressionConst(Type.integer, -2))).toSQL,
      ("(CASE flag WHEN ? THEN wert1 WHEN ? THEN wert2 WHEN ? THEN ? ELSE ? END)",
        Seq((Type.integer, 1), (Type.integer, 2), (Type.integer, 3), (Type.integer, -1), (Type.integer, -2))))
    try {
      val t1 = SqlExpressionCase(Some(SqlExpressionColumn("blub")), Seq.empty, Some(SqlExpressionColumn("foo"))).toSQL
      fail()
    } catch {
      case _ : AssertionError => // wanted
    }

    // Exists
    assertEquals(SqlExpressionExists(adr1).toSQL, ("EXISTS (SELECT * FROM addresses)", Seq.empty))
    assertEquals(SqlExpressionExists(tbl3).toSQL, ("EXISTS "+tbl3S._1, tbl3S._2))

    // subQuery
    assertEquals(SqlExpressionSubquery(adr1).toSQL, ("(SELECT * FROM addresses)", Seq.empty))
    assertEquals(SqlExpressionSubquery(tbl3).toSQL, tbl3S)

    // AND and OR
    assertEquals(longExpr.toSQL, longExpr1T)
  }





  test("SelectCombineOperations") {
    // Test all known CombineOpterators
    def withAll(sqlI1: SqlInterpretations, sqlI2: SqlInterpretations, strLeft: String, strRight: String,
                paramsLeft: Seq[(Type, String)], paramsRight: Seq[(Type, String)]) = {
      val check : Seq[(SqlCombineOperator, String)] = Seq(
        (SqlCombineOperator.Union, " UNION "),
        (SqlCombineOperator.Intersection, " INTERSECT "),
        (SqlCombineOperator.Difference, " EXCEPT "))
      for (elem <- check) {
        assertEquals(SqlSelectCombine(elem._1, sqlI1, sqlI2).toSQL,
          (strLeft+elem._2+strRight, paramsLeft++paramsRight))
        // should also work the other way round; is symetrisch
        // -> Result of the Query could be different but the way get the Query is the same
        assertEquals(SqlSelectCombine(elem._1, sqlI2, sqlI1).toSQL,
          (strRight+elem._2+strLeft, paramsRight++paramsLeft))
      }
    }

    withAll(adr1, firmAddr, "(SELECT * FROM addresses)", "(SELECT * FROM firm_address)", Seq.empty, Seq.empty)
    withAll(tbl2, tbl3, tbl2S._1, tbl3S._1, tbl2S._2, tbl3S._2)
  }



  test("toSQL / simple Querys") {
    assertEquals(tbl1.toSQL, ("SELECT * FROM personen", Seq.empty))
    // TODO Zwischenergebnisse Ergebnis vervollständigt sich noch
    assertEquals(select1.toSQL, ("SELECT * FROM personen", Seq.empty))
    assertEquals(SQL.makeSqlSelect(Seq.empty, Seq((Some("personen"), tbl1))).toSQL,
      ("SELECT * FROM personen AS personen", Seq.empty))
  }



  test("joins") {

  }


}
