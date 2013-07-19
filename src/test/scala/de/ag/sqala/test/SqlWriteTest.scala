package de.ag.sqala.test

import org.scalatest.FunSuite
import de.ag.sqala.sql._
import de.ag.sqala.Ascending

/**
 * Test writing sql
 */
class SqlWriteTest extends FunSuite {
  val tbl1 = QueryTable("tbl1")

  val baseQuery = Query.makeSelect(from = Seq(QuerySelectFrom(tbl1, None)))
  Seq(
    (Some("QueryTable"), "SELECT * FROM tbl1", tbl1),
    (None, "SELECT * FROM tbl1", baseQuery),
    (None, "SELECT DISTINCT * FROM tbl1", baseQuery.copy(options = Seq("DISTINCT"))),
    (None, "SELECT id FROM tbl1",
      baseQuery.copy(attributes = Seq(QuerySelectAttribute(ExprColumn("id"), None)))),
    (Some("infix operator"), "SELECT (id = 0) FROM tbl1",
      baseQuery.copy(attributes = Seq(QuerySelectAttribute(
        ExprApp(OperatorEq, Seq(ExprColumn("id"), ExprConst(LiteralNumber(0)))), None)))),
    (Some("postfix operator"), "SELECT (id IS NULL) FROM tbl1",
      baseQuery.copy(attributes = Seq(QuerySelectAttribute(
        ExprApp(OperatorIsNull, Seq(ExprColumn("id"))), None)))),
    (Some("prefix operator"), "SELECT COUNT(id) FROM tbl1",
      baseQuery.copy(attributes = Seq(QuerySelectAttribute(
        ExprApp(OperatorCount, Seq(ExprColumn("id"))), None)))),
    (None, "SELECT id FROM tbl1 GROUP BY id",
      baseQuery.copy(attributes = Seq(QuerySelectAttribute(ExprColumn("id"), None)),
        groupBy = Seq(ExprColumn("id")))),
    (None, "SELECT * FROM tbl1 ORDER BY id ASC",
      baseQuery.copy(orderBy = Seq(QuerySelectOrderBy(ExprColumn("id"), Ascending)))),
    (None, "SELECT * FROM tbl1 HAVING (id = 0)",
      baseQuery.copy(having = Some(ExprApp(OperatorEq,
        Seq(ExprColumn("id"), ExprConst(LiteralNumber(0))))))),
    (None, "SELECT * FROM tbl1 WHERE (id IS NULL)",
      baseQuery.copy(where = Seq(ExprApp(OperatorIsNull, Seq(ExprColumn("id")))))),
    (None, "SELECT * FROM tbl1 WHERE (id IS NULL) AND (company IS NOT NULL)",
      baseQuery.copy(where = Seq(ExprApp(OperatorIsNull, Seq(ExprColumn("id"))),
        ExprApp(OperatorIsNotNull, Seq(ExprColumn("company")))))),
    (Some("all fields"), "SELECT id, company, COUNT(employee) AS employees FROM tbl1 WHERE (id > 12) GROUP BY company HAVING (employees < 10.2) ORDER BY company ASC",
      baseQuery.copy(
        attributes = Seq(QuerySelectAttribute(ExprColumn("id"), None),
          QuerySelectAttribute(ExprColumn("company"), None),
          QuerySelectAttribute(ExprApp(OperatorCount, Seq(ExprColumn("employee"))), Some("employees"))),
        where = Seq(ExprApp(OperatorGt, Seq(ExprColumn("id"), ExprConst(LiteralNumber(12))))),
        groupBy = Seq(ExprColumn("company")),
        having = Some(ExprApp(OperatorLt, Seq(ExprColumn("employees"), ExprConst(LiteralNumber(10.2))))),
        orderBy = Seq(QuerySelectOrderBy(ExprColumn("company"), Ascending))
      )),
    (None, "SELECT 'Guns N'' Roses' FROM tbl1",
      baseQuery.copy(
      attributes = Seq(QuerySelectAttribute(ExprConst(LiteralString("Guns N' Roses")), None))
      )),
    (None, "SELECT id FROM tbl1 WHERE (company IN (SELECT id FROM companies WHERE (name LIKE '% Inc.')))",
      baseQuery.copy(
        attributes = Seq(QuerySelectAttribute(ExprColumn("id"), None)),
        where = Seq(ExprApp(OperatorIn, Seq(ExprColumn("company"), ExprSubQuery(
          Query.makeSelect(from = Seq(QuerySelectFrom(QueryTable("companies"), None)),
            attributes = Seq(QuerySelectAttribute(ExprColumn("id"), None)),
            where = Seq(ExprApp(OperatorLike, Seq(ExprColumn("name"), ExprConst(LiteralString("% Inc."))))))
        ))))
      )),
    (None, "SELECT id FROM tbl1 AS t1",
      Query.makeSelect(
      attributes=Seq(QuerySelectAttribute(ExprColumn("id"), None)),
      from=Seq(QuerySelectFrom(tbl1, Some("t1"))))
      ),
    (None, "SELECT id FROM tbl1, tbl2",
      Query.makeSelect(
        attributes = Seq(QuerySelectAttribute(ExprColumn("id"), None)),
        from = Seq(QuerySelectFrom(tbl1, None), QuerySelectFrom(QueryTable("tbl2"), None))
      ))
  ).foreach(s => s._1 match {
    case None => testWriteSql(s._2, s._2, s._3)
    case Some(testLbl) => testWriteSql(testLbl + ": " + s._2, s._2, s._3)
  })

  def testWriteSql(name:String, expected:String, Query:Query) {
    test(name) {
      expectResult(expected) {Query.toString}
    }
  }
}
