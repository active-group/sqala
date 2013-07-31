package de.ag.sqala.test.adt

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
  val tbl1 = View.Table("tbl1", tbl1Schema)

  val baseSelect = View.makeSelect(from = Seq(View.SelectFromView(tbl1, None)))
  Seq(
    (Some("View.Table"), "SELECT * FROM tbl1", tbl1),
    (None, "SELECT * FROM tbl1", baseSelect),
    (None, "SELECT DISTINCT * FROM tbl1", baseSelect.copy(options = Seq("DISTINCT"))),
    (None, "SELECT id FROM tbl1",
      baseSelect.copy(attributes = Seq(View.SelectAttribute(Expr.Column("id"), None)))),
    (Some("infix operator"), "SELECT (id = 0) FROM tbl1",
      baseSelect.copy(attributes = Seq(View.SelectAttribute(
        Expr.App(Operator.Eq, Seq(Expr.Column("id"), Expr.Const(Expr.Literal.Integer(0)))), None)))),
    (Some("postfix operator"), "SELECT (id IS NULL) FROM tbl1",
      baseSelect.copy(attributes = Seq(View.SelectAttribute(
        Expr.App(Operator.IsNull, Seq(Expr.Column("id"))), None)))),
    (Some("prefix operator"), "SELECT COUNT(id) FROM tbl1",
      baseSelect.copy(attributes = Seq(View.SelectAttribute(
        Expr.App(Operator.Count, Seq(Expr.Column("id"))), None)))),
    (None, "SELECT id FROM tbl1 GROUP BY id",
      baseSelect.copy(attributes = Seq(View.SelectAttribute(Expr.Column("id"), None)),
        groupBy = Seq(Expr.Column("id")))),
    (None, "SELECT * FROM tbl1 ORDER BY id ASC",
      baseSelect.copy(orderBy = Seq(View.SelectOrderBy(Expr.Column("id"), Ascending)))),
    (None, "SELECT * FROM tbl1 HAVING (id = 0)",
      baseSelect.copy(having = Some(Expr.App(Operator.Eq,
        Seq(Expr.Column("id"), Expr.Const(Expr.Literal.Integer(0))))))),
    (None, "SELECT * FROM tbl1 WHERE (id IS NULL)",
      baseSelect.copy(where = Seq(Expr.App(Operator.IsNull, Seq(Expr.Column("id")))))),
    (None, "SELECT * FROM tbl1 WHERE (id IS NULL) AND (company IS NOT NULL)",
      baseSelect.copy(where = Seq(Expr.App(Operator.IsNull, Seq(Expr.Column("id"))),
        Expr.App(Operator.IsNotNull, Seq(Expr.Column("company")))))),
    (Some("all fields"), "SELECT id, company, COUNT(employee) AS employees FROM tbl1 WHERE (id > 12) GROUP BY company HAVING (employees < 10.2) ORDER BY company ASC",
      baseSelect.copy(
        attributes = Seq(View.SelectAttribute(Expr.Column("id"), None),
          View.SelectAttribute(Expr.Column("company"), None),
          View.SelectAttribute(Expr.App(Operator.Count, Seq(Expr.Column("employee"))), Some("employees"))),
        where = Seq(Expr.App(Operator.Gt, Seq(Expr.Column("id"), Expr.Const(Expr.Literal.Integer(12))))),
        groupBy = Seq(Expr.Column("company")),
        having = Some(Expr.App(Operator.Lt, Seq(Expr.Column("employees"), Expr.Const(Expr.Literal.Double(10.2))))),
        orderBy = Seq(View.SelectOrderBy(Expr.Column("company"), Ascending))
      )),
    (None, "SELECT 'Guns N'' Roses' FROM tbl1",
      baseSelect.copy(
      attributes = Seq(View.SelectAttribute(Expr.Const(Expr.Literal.String("Guns N' Roses")), None))
      )),
    (None, "SELECT id FROM tbl1 WHERE (company IN (SELECT id FROM companies WHERE (name LIKE '% Inc.')))",
      baseSelect.copy(
        attributes = Seq(View.SelectAttribute(Expr.Column("id"), None)),
        where = Seq(Expr.App(Operator.In, Seq(Expr.Column("company"), Expr.SubView(
          View.makeSelect(from = Seq(View.SelectFromView(View.Table("companies", companiesSchema), None)),
            attributes = Seq(View.SelectAttribute(Expr.Column("id"), None)),
            where = Seq(Expr.App(Operator.Like, Seq(Expr.Column("name"), Expr.Const(Expr.Literal.String("% Inc."))))))
        ))))
      )),
    (None, "SELECT id FROM tbl1 AS t1",
      View.makeSelect(
      attributes=Seq(View.SelectAttribute(Expr.Column("id"), None)),
      from=Seq(View.SelectFromView(tbl1, Some("t1"))))
      ),
    (None, "SELECT id FROM tbl1, tbl2",
      View.makeSelect(
        attributes = Seq(View.SelectAttribute(Expr.Column("id"), None)),
        from = Seq(View.SelectFromView(tbl1, None), View.SelectFromView(View.Table("tbl2", tbl2Schema), None))
      ))
  ).foreach(s => s._1 match {
    case None => testWriteSql(s._2, s._2, s._3)
    case Some(testLbl) => testWriteSql(testLbl + ": " + s._2, s._2, s._3)
  })

  def testWriteSql(name:String, expected:String, Query:View) {
    test(name) {
      expectResult(expected) {Query.toString}
    }
  }
}
