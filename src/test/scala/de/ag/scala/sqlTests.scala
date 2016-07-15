package de.ag.scala

import de.ag.sqala._
import minitest._

object SqlTests extends SimpleTestSuite {

  // TODO test more over the small functions in PutSQL & SqlUtils
  val att1 = Seq(("first", SqlExpressionColumn("first")), ("abc", SqlExpressionColumn("second")))
  val att2 = Seq(("blub", SqlExpressionConst(Type.string, "")), ("blubStr", SqlExpressionConst(Type.string, "X")),
    ("blub2", SqlExpressionConst(Type.integer, 4)))

  test("PutSQL - attributes (& putLiteral)") {
    assertEquals(PutSQL.attributes(Seq.empty), (Some("*"), Seq.empty))
    assertEquals(PutSQL.attributes(att1), (Some("first, second AS abc"), Seq.empty))
    assertEquals(PutSQL.attributes(att2), (Some("? AS blub, ? AS blubStr, ? AS blub2"),
      Seq((Type.string, ""), (Type.string, "X"), (Type.integer, 4))))
    // TODO Test more
  }

  test("Expression - opertors / simple Tests") {
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




  val tbl1 = SqlTable("personen", RelationalScheme(
    Vector[String]("id", "first", "last"),
    Map("id" -> Type.integer, "first" -> Type.string, "last" -> Type.string),
    None))

  val select1 = SQL.makeSqlSelect(Seq.empty, Seq(("personen", tbl1)))

  test("toSQL / simple Querys") {
    assertEquals(tbl1.toSQL, ("SELECT * FROM personen", Seq.empty))
    // TODO Zwischenergebnisse Ergebnis vervollständigt sich noch
    assertEquals(select1.toSQL, ("SELECT *", Seq.empty))
  }


}
