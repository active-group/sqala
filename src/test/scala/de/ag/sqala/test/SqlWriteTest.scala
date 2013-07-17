package de.ag.sqala.test

import org.scalatest.FunSuite
import de.ag.sqala._
import de.ag.sqala.SqlQueryTable
import de.ag.sqala.SqlQuerySelectFrom

/**
 * Test writing sql
 */
class SqlWriteTest extends FunSuite {
  val tbl1 = SqlQueryTable("tbl1")

  val baseQuery = SqlQuery.makeSelect(from = Seq(SqlQuerySelectFrom(tbl1, None)))
  Seq(
    (Some("SqlQueryTable"), "SELECT * FROM tbl1", tbl1),
    (None, "SELECT * FROM tbl1", baseQuery),
    (None, "SELECT DISTINCT * FROM tbl1", baseQuery.copy(options = Seq("DISTINCT"))),
    (None, "SELECT id FROM tbl1",
      baseQuery.copy(attributes = Seq(SqlQuerySelectAttribute(SqlExprColumn("id"), None)))),
    (Some("infix operator"), "SELECT (id = 0) FROM tbl1",
      baseQuery.copy(attributes = Seq(SqlQuerySelectAttribute(
        SqlExprApp(SqlOperatorEq, Seq(SqlExprColumn("id"), SqlExprConst(SqlLiteralNumber(0)))), None)))),
    (Some("postfix operator"), "SELECT (id IS NULL) FROM tbl1",
      baseQuery.copy(attributes = Seq(SqlQuerySelectAttribute(
        SqlExprApp(SqlOperatorIsNull, Seq(SqlExprColumn("id"))), None)))),
    (Some("prefix operator"), "SELECT COUNT(id) FROM tbl1",
      baseQuery.copy(attributes = Seq(SqlQuerySelectAttribute(
        SqlExprApp(SqlOperatorCount, Seq(SqlExprColumn("id"))), None)))),
    (None, "SELECT id FROM tbl1 GROUP BY id",
      baseQuery.copy(attributes = Seq(SqlQuerySelectAttribute(SqlExprColumn("id"), None)),
        groupBy = Seq(SqlExprColumn("id")))),
    (None, "SELECT * FROM tbl1 ORDER BY id ASC",
      baseQuery.copy(orderBy = Seq(SqlQuerySelectOrderBy(SqlExprColumn("id"), Ascending)))),
    (None, "SELECT * FROM tbl1 HAVING (id = 0)",
      baseQuery.copy(having = Some(SqlExprApp(SqlOperatorEq,
        Seq(SqlExprColumn("id"), SqlExprConst(SqlLiteralNumber(0))))))),
    (None, "SELECT * FROM tbl1 WHERE (id IS NULL)",
      baseQuery.copy(where = Seq(SqlExprApp(SqlOperatorIsNull, Seq(SqlExprColumn("id")))))),
    (None, "SELECT * FROM tbl1 WHERE (id IS NULL) AND (company IS NOT NULL)",
      baseQuery.copy(where = Seq(SqlExprApp(SqlOperatorIsNull, Seq(SqlExprColumn("id"))),
        SqlExprApp(SqlOperatorIsNotNull, Seq(SqlExprColumn("company")))))),
    (Some("all fields"), "SELECT id, company, COUNT(employee) AS employees FROM tbl1 WHERE (id > 12) GROUP BY company HAVING (employees < 10.2) ORDER BY company ASC",
      baseQuery.copy(
        attributes = Seq(SqlQuerySelectAttribute(SqlExprColumn("id"), None),
          SqlQuerySelectAttribute(SqlExprColumn("company"), None),
          SqlQuerySelectAttribute(SqlExprApp(SqlOperatorCount, Seq(SqlExprColumn("employee"))), Some("employees"))),
        where = Seq(SqlExprApp(SqlOperatorGt, Seq(SqlExprColumn("id"), SqlExprConst(SqlLiteralNumber(12))))),
        groupBy = Seq(SqlExprColumn("company")),
        having = Some(SqlExprApp(SqlOperatorLt, Seq(SqlExprColumn("employees"), SqlExprConst(SqlLiteralNumber(10.2))))),
        orderBy = Seq(SqlQuerySelectOrderBy(SqlExprColumn("company"), Ascending))
      )),
    (None, "SELECT 'Guns N'' Roses' FROM tbl1",
      baseQuery.copy(
      attributes = Seq(SqlQuerySelectAttribute(SqlExprConst(SqlLiteralString("Guns N' Roses")), None))
      ))
  ).foreach(s => s._1 match {
    case None => testWriteSql(s._2, s._2, s._3)
    case Some(testLbl) => testWriteSql(testLbl + ": " + s._2, s._2, s._3)
  })

  def testWriteSql(name:String, expected:String, sqlQuery:SqlQuery) {
    test(name) {
      expectResult(expected) {sqlQuery.toString}
    }
  }
}
