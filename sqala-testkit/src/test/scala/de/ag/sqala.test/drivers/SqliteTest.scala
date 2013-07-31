package de.ag.sqala.test.drivers

import org.scalatest.{BeforeAndAfter, FunSuite}
import de.ag.sqala.drivers.Sqlite3DbConnection
import de.ag.sqala.sql._
import de.ag.sqala.{Domain, DbConnection}
import de.ag.sqala.relational.Schema
import de.ag.sqala.Operator
import de.ag.sqala.relational.Query.Base
import java.sql.SQLException

/**
 *
 */
class SqliteTest extends FunSuite with BeforeAndAfter {
  var conn:DbConnection = _

  before {
    conn = Sqlite3DbConnection.openInMemory()
  }

  val tbl1Schema: Schema = Schema("one" -> Domain.BoundedString(10), "two"-> Domain.Integer)

  val data = Seq(
    ("test", 10),
    ("foo", 12),
    ("bar", -1),
    ("filderstadt", 70794)
  )

  def createTbl1() {
    conn.execute("CREATE TABLE tbl1(one VARCHAR(10), two SMALLINT)")
  }

  def createAndFillTbl1() {
    createTbl1()
    fillTbl1()
  }


  def fillTbl1() {
    data.foreach {
      case (s, i) =>
        conn.insert("tbl1", tbl1Schema, Seq(s, Integer.valueOf(i)))
    }
  }

  test("open/close") {}

  test("create tables") {
    createTbl1()
  }

  test("insert") {
    createTbl1()
    expectResult(1){conn.insert("tbl1", tbl1Schema, Seq("test", Integer.valueOf(10)))}
  }

  test("insert & query") {
    createTbl1()
    assert(1 == conn.insert("tbl1", tbl1Schema, Seq("test", Integer.valueOf(10))))

    val results = conn.read(View.makeSelect(
      attributes = Seq(View.SelectAttribute(Expr.Column("one"), None), View.SelectAttribute(Expr.Column("two"), None)),
      from = Seq(View.SelectFromView(View.Table("tbl1", tbl1Schema), None))
    ),
      new Schema(Seq(("one", Domain.String), ("two", Domain.Integer))))
      .toArray

    expectResult(1) {
      results.size
    }
    expectResult(Seq("test", 10)) {
      results(0)
    }
  }

  test("insert & query many") {
    createAndFillTbl1()
    expectResult(data.map{d => Seq(d._1, d._2)}.toSet){
      conn.read(View.Table("tbl1", tbl1Schema), tbl1Schema)
        .toSet
    }
  }

  test("delete") {
    createAndFillTbl1()

    expectResult(1){conn.delete("tbl1", Expr.App(Operator.Eq, Seq(Expr.Column("one"), Expr.Const(Expr.Literal.String("test")))))}
    expectResult(2){conn.delete("tbl1", Expr.App(Operator.Or, Seq(
        Expr.App(Operator.Eq, Seq(Expr.Column("one"), Expr.Const(Expr.Literal.String("foo")))),
        Expr.App(Operator.Eq, Seq(Expr.Column("two"), Expr.Const(Expr.Literal.Integer(-1)))))))}
    expectResult(0){conn.delete("tbl1", Expr.App(Operator.Eq, Seq(Expr.Column("one"), Expr.Const(Expr.Literal.String("test")))))}
  }

  test("update") {
    createAndFillTbl1()

    expectResult(1){conn.update("tbl1",
      Expr.App(Operator.Eq, Seq(Expr.Column("one"), Expr.Const(Expr.Literal.String("bar")))),
      Seq(("two", Expr.Const(Expr.Literal.Integer(12)))))}
    expectResult(0){conn.update("tbl1",
      Expr.App(Operator.Eq, Seq(Expr.Column("one"), Expr.Const(Expr.Literal.String("not there")))),
      Seq(("two", Expr.Const(Expr.Literal.Integer(12)))))}
    expectResult(2){conn.update("tbl1",
      Expr.App(Operator.Eq, Seq(Expr.Column("two"), Expr.Const(Expr.Literal.Integer(12)))),
      Seq(("two", Expr.Const(Expr.Literal.Null))))}
  }

  test("drop table") {
    createTbl1()
    conn.dropTable("tbl1")
    intercept[SQLException]{
      fillTbl1()
    }
  }

  test("drop table if exists") {
    conn.dropTableIfExists("foo")
    createTbl1()
    conn.dropTableIfExists("tbl1")
    intercept[SQLException]{
      fillTbl1()
    }
  }

  test("identity column") {
    val tbl2Schema = Schema("id" -> Domain.IdentityInteger)
    conn.createTable("tbl2", tbl2Schema)
    (1 to 10).foreach{_ => conn.insert("tbl2", tbl2Schema, Seq(null))}
    expectResult((1 to 10).map{Seq(_)}){conn.read(View.Table("tbl2", tbl2Schema), tbl2Schema).toSeq}
  }
}
