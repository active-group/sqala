package de.ag.sqala.test

import org.scalatest.FunSuite
import de.ag.sqala.sql._
import de.ag.sqala.{Domain, Ascending, Operator}
import de.ag.sqala.relational.Schema
import de.ag.sqala.relational.Query.Base

/**
 * Test writing sql
 */
class SqlWriteTest extends FunSuite {
  val tbl1Schema = new Schema(Seq(("id", Domain.Integer), ("company", Domain.Integer), ("employee", Domain.Integer)))
  val companiesSchema: Schema = new Schema(Seq("id" -> Domain.Integer, "name" -> Domain.String))
  val tbl2Schema: Schema = new Schema(Seq("id" -> Domain.Integer))
  val tbl1 = Query.Base(Base("tbl1", tbl1Schema))

  val baseQuery = Query.makeSelect(from = Seq(Query.SelectFromQuery(tbl1, None)))
  Seq(
    (Some("Query.Base"), "SELECT * FROM tbl1", tbl1),
    (None, "SELECT * FROM tbl1", baseQuery),
    (None, "SELECT DISTINCT * FROM tbl1", baseQuery.copy(options = Seq("DISTINCT"))),
    (None, "SELECT id FROM tbl1",
      baseQuery.copy(attributes = Seq(Query.SelectAttribute(Expr.Column("id"), None)))),
    (Some("infix operator"), "SELECT (id = 0) FROM tbl1",
      baseQuery.copy(attributes = Seq(Query.SelectAttribute(
        Expr.App(Operator.Eq, Seq(Expr.Column("id"), Expr.Const(Expr.Literal.Integer(0)))), None)))),
    (Some("postfix operator"), "SELECT (id IS NULL) FROM tbl1",
      baseQuery.copy(attributes = Seq(Query.SelectAttribute(
        Expr.App(Operator.IsNull, Seq(Expr.Column("id"))), None)))),
    (Some("prefix operator"), "SELECT COUNT(id) FROM tbl1",
      baseQuery.copy(attributes = Seq(Query.SelectAttribute(
        Expr.App(Operator.Count, Seq(Expr.Column("id"))), None)))),
    (None, "SELECT id FROM tbl1 GROUP BY id",
      baseQuery.copy(attributes = Seq(Query.SelectAttribute(Expr.Column("id"), None)),
        groupBy = Seq(Expr.Column("id")))),
    (None, "SELECT * FROM tbl1 ORDER BY id ASC",
      baseQuery.copy(orderBy = Seq(Query.SelectOrderBy(Expr.Column("id"), Ascending)))),
    (None, "SELECT * FROM tbl1 HAVING (id = 0)",
      baseQuery.copy(having = Some(Expr.App(Operator.Eq,
        Seq(Expr.Column("id"), Expr.Const(Expr.Literal.Integer(0))))))),
    (None, "SELECT * FROM tbl1 WHERE (id IS NULL)",
      baseQuery.copy(where = Seq(Expr.App(Operator.IsNull, Seq(Expr.Column("id")))))),
    (None, "SELECT * FROM tbl1 WHERE (id IS NULL) AND (company IS NOT NULL)",
      baseQuery.copy(where = Seq(Expr.App(Operator.IsNull, Seq(Expr.Column("id"))),
        Expr.App(Operator.IsNotNull, Seq(Expr.Column("company")))))),
    (Some("all fields"), "SELECT id, company, COUNT(employee) AS employees FROM tbl1 WHERE (id > 12) GROUP BY company HAVING (employees < 10.2) ORDER BY company ASC",
      baseQuery.copy(
        attributes = Seq(Query.SelectAttribute(Expr.Column("id"), None),
          Query.SelectAttribute(Expr.Column("company"), None),
          Query.SelectAttribute(Expr.App(Operator.Count, Seq(Expr.Column("employee"))), Some("employees"))),
        where = Seq(Expr.App(Operator.Gt, Seq(Expr.Column("id"), Expr.Const(Expr.Literal.Integer(12))))),
        groupBy = Seq(Expr.Column("company")),
        having = Some(Expr.App(Operator.Lt, Seq(Expr.Column("employees"), Expr.Const(Expr.Literal.Double(10.2))))),
        orderBy = Seq(Query.SelectOrderBy(Expr.Column("company"), Ascending))
      )),
    (None, "SELECT 'Guns N'' Roses' FROM tbl1",
      baseQuery.copy(
      attributes = Seq(Query.SelectAttribute(Expr.Const(Expr.Literal.String("Guns N' Roses")), None))
      )),
    (None, "SELECT id FROM tbl1 WHERE (company IN (SELECT id FROM companies WHERE (name LIKE '% Inc.')))",
      baseQuery.copy(
        attributes = Seq(Query.SelectAttribute(Expr.Column("id"), None)),
        where = Seq(Expr.App(Operator.In, Seq(Expr.Column("company"), Expr.SubQuery(
          Query.makeSelect(from = Seq(Query.SelectFromQuery(Query.Base(Base("companies", companiesSchema)), None)),
            attributes = Seq(Query.SelectAttribute(Expr.Column("id"), None)),
            where = Seq(Expr.App(Operator.Like, Seq(Expr.Column("name"), Expr.Const(Expr.Literal.String("% Inc."))))))
        ))))
      )),
    (None, "SELECT id FROM tbl1 AS t1",
      Query.makeSelect(
      attributes=Seq(Query.SelectAttribute(Expr.Column("id"), None)),
      from=Seq(Query.SelectFromQuery(tbl1, Some("t1"))))
      ),
    (None, "SELECT id FROM tbl1, tbl2",
      Query.makeSelect(
        attributes = Seq(Query.SelectAttribute(Expr.Column("id"), None)),
        from = Seq(Query.SelectFromQuery(tbl1, None), Query.SelectFromQuery(Query.Base(Base("tbl2", tbl2Schema)), None))
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
