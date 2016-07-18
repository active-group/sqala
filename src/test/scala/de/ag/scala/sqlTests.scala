package de.ag.scala

import de.ag.sqala._
import minitest._

object SqlTests extends SimpleTestSuite {

  val tbl1 = SqlSelectTable("personen", RelationalScheme.make(Seq(("id", Type.integer), ("first", Type.string), ("last", Type.string))))
  val adr1 = SqlSelectTable("addresses", RelationalScheme.make(Seq(("city", Type.string))))
  val firmAddr = SqlSelectTable("firm_address", RelationalScheme.make(Seq(("orte", Type.string))))
  val standorte = SqlSelectTable("standorte", RelationalScheme.make(Seq(("orte", Type.string))))

  val select1 = SQL.makeSqlSelect(Seq.empty, Seq((None, tbl1)))

  test("Expression - operators / simple Tests") {
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
    // ToDo Test more, test other operators
  }


  test("SelectCombineOperations") {
    assertEquals(SqlSelectCombine(SqlCombineOperator.Union, adr1, firmAddr).toSQL,
      ("(SELECT * FROM addresses) UNION (SELECT * FROM firm_address)", Seq.empty))
    assertEquals(SqlSelectCombine(
      SqlCombineOperator.Difference,
      SQL.makeSqlSelect(Seq(("city", SqlExpressionColumn("city")), ("xx", SqlExpressionConst(Type.string, "BlX"))), Seq((None, adr1))),
      SQL.makeSqlSelect(Seq(("city", SqlExpressionColumn("orte")), ("xx", SqlExpressionConst(Type.string, "Nn"))), Seq((None, standorte)))).toSQL,
      ("(SELECT city, ? AS xx FROM addresses) EXCEPT (SELECT orte AS city, ? AS xx FROM standorte)", Seq((Type.string, "BlX"), (Type.string, "Nn"))))
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
