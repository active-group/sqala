package de.ag.sqala

import TestUtil.assertEquals

import org.scalatest.FunSuite

class SqlUtilsTest extends FunSuite {

  val att1 = Seq(("first", SqlExpressionColumn("first")), ("abc", SqlExpressionColumn("second")))
  val att2 = Seq(("blub", SqlExpressionConst(Type.string, "")), ("blubStr", SqlExpressionConst(Type.string, "X")),
    ("blub2", SqlExpressionConst(Type.integer, 4)))

  test("putPaddingIf...") {
    assertEquals(SqlUtils.putPaddingIfNonNull[Int](Seq.empty, {case x: Seq[Int] => (x.map(x => (x+1).toString).mkString(","), x.map(i => (Type.integer, i)))}, "BLA "),
      None)
    assertEquals(SqlUtils.putPaddingIfNonNull[Int](Seq(2),
      {case x: Seq[Int] => (x.map(x => (x+1).toString).mkString(","), x.map(i => (Type.integer, i)))}, "BLA "),
      Some(("BLA 3", Seq((Type.integer, 2)))))
    assertEquals(SqlUtils.putPaddingIfNonNull[Int](Seq(2, 5, 9),
      {case x: Seq[Int] => (x.map(x => (x+1).toString).mkString(","), x.map(i => (Type.integer, i)))}, "BLA "),
      Some(("BLA 3,6,10", Seq((Type.integer, 2), (Type.integer, 5), (Type.integer, 9)))))
    assertEquals(SqlUtils.putPaddingIfNonNull[Int](Seq(2, 5, 9),
      {case x: Seq[Int] => (x.map(x => (x+2).toString).mkString("-"), Seq.empty)}, "BAL "),
      Some(("BAL 4-7-11", Seq.empty)))
  }

  val ret1 = ("BlabaB", Seq.empty)
  val ret2 = ("This to Go", Seq((Type.integer, 4), (Type.string, "foo")))

  test("function with alias") {
    assertEquals(SqlUtils.defaultPutAlias(None), "")
    assertEquals(SqlUtils.defaultPutAlias(Some("Blub")), " AS Blub")
    assertEquals(SqlUtils.putAlias(None), None)
    assertEquals(SqlUtils.putAlias(Some("B")), Some(" AS B"))
    assertEquals(SqlUtils.putColumnAnAlias(ret1, None), ret1)
    assertEquals(SqlUtils.putColumnAnAlias(ret2, None), ret2)
    assertEquals(SqlUtils.putColumnAnAlias(ret1, Some("X")), ("BlabaB AS X", ret1._2))
    assertEquals(SqlUtils.putColumnAnAlias(ret2, Some("X")), ("This to Go AS X", ret2._2))
  }

  test("joiningInfix") {
    assertEquals(SqlUtils.putJoiningInfix[String](Seq.empty, ",", x => ("BX", Seq.empty)), ("", Seq.empty))
    assertEquals(SqlUtils.putJoiningInfixOption[String](Seq.empty, ",", x => ("BX", Seq.empty)), Some(("", Seq.empty)))
    assertEquals(SqlUtils.putJoiningInfix[String](Seq("a", "b"), "-", x => ("BX", Seq((Type.string, x)))),
      ("BX-BX", Seq((Type.string, "a"), (Type.string, "b"))))
  }




  test("attributes (& putLiteral)") {
    assertEquals(SQL.attributes(Seq.empty), Some(("*", Seq.empty)))
    assertEquals(SQL.attributes(att1), Some(("first, second AS abc", Seq.empty)))
    assertEquals(SQL.attributes(att2), Some(("? AS blub, ? AS blubStr, ? AS blub2",
      Seq((Type.string, ""), (Type.string, "X"), (Type.integer, 4)))))
    assertEquals(SqlUtils.putLiteral(Type.integer, 5), ("?", Seq((Type.integer, 5))))
  }



  test("surround and concat SQL") {
    assertEquals(SqlUtils.surroundSQL("", ("blublba", Seq((Type.string, "bla"))), ""), ("blublba", Seq((Type.string, "bla"))))
    assertEquals(SqlUtils.surroundSQL("y", ("blublba", Seq((Type.string, "bla"))), "<"), ("yblublba<", Seq((Type.string, "bla"))))
    assertEquals(SqlUtils.concatSQL(Seq.empty), ("", Seq.empty))
    assertEquals(SqlUtils.concatSQL(Seq(
      ("a", Seq((Type.boolean, true))),
      ("b", Seq((Type.integer, 3), (Type.string, "a"))),
      ("d", Seq.empty),
      ("c", Seq((Type.integer, -1))))),
      ("abdc", Seq((Type.boolean, true), (Type.integer, 3), (Type.string, "a"), (Type.integer, -1))))
  }




  val tbl1 = SqlSelectTable("tabelleA", RelationalScheme.make(Seq(("a", Type.string), ("b", Type.string))))
  val tbl2 = SqlSelectTable("tabelleB", RelationalScheme.make(Seq(("b", Type.string), ("c", Type.string))))
  val tbl3 = SQL.makeSqlSelect(Seq(("a", SqlExpressionColumn("b"))), Seq((Some("t1"), tbl1)))

  test("SQL - join") {
    assertEquals(SQL.join(Seq((None, tbl1)), Seq.empty, Seq.empty), Some(("FROM tabelleA", Seq.empty)))
    assertEquals(SQL.join(Seq((Some("A"), tbl1)), Seq.empty, Seq.empty), Some(("FROM tabelleA AS A", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1), (None, tbl2), (Some("tdx"), tbl3)), Seq.empty, Seq.empty),
      Some(("FROM tabelleA, tabelleB, (SELECT b AS a FROM tabelleA AS t1) AS tdx", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1)), Seq((Some("tx"), tbl2)),
      Seq(SqlExpressionApp(SqlOperator.eq, Seq(SqlExpressionColumn("a"), SqlExpressionColumn("b"))))),
      Some(("FROM tabelleA LEFT JOIN tabelleB AS tx ON (a = b)", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1), (Some("tdx"), tbl3)), Seq((None, tbl2)),
      Seq(SqlExpressionApp(SqlOperator.eq, Seq(SqlExpressionColumn("a"), SqlExpressionColumn("b"))))),
      Some(("FROM (SELECT * FROM tabelleA, (SELECT b AS a FROM tabelleA AS t1) AS tdx) LEFT JOIN tabelleB ON (a = b)", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1)), Seq((Some("t1"), tbl1), (Some("t2"), tbl2), (Some("tt"), tbl1)),
      Seq(SqlExpressionApp(SqlOperator.eq, Seq(SqlExpressionColumn("b"), SqlExpressionColumn("a"))))),
      Some(("FROM tabelleA LEFT JOIN tabelleA AS t1 ON (1=1) LEFT JOIN tabelleB AS t2 ON (1=1) LEFT JOIN tabelleA AS tt ON (b = a)", Seq.empty)))
  }

  /*

  Ein paar Gedankengänge ...

SELECT *
FROM (VALUES (1, 'b'), (2, 'a')) AS t1 (id,nam)
LEFT OUTER JOIN (VALUES (3, 'c'), (4,'b')) AS t2 (id,nam) ON (1=1)
LEFT OUTER JOIN (VALUES (5,'x'), (6,'x')) t3 (id,nam) ON t1.nam = t2.nam AND t1.nam = t3.nam

=>
  Cols t1 |  Cols t2 |  Cols t3
 id | nam | id | nam | id | nam
----+-----+----+-----+----+-----
 1  |  b  | 3  |  c  |    |
 1  |  b  | 4  |  b  |    |
 2  |  a  | 3  |  c  |    |
 2  |  a  | 4  |  b  |    |




SELECT *
FROM (VALUES (1, 'b'), (2, 'a')) AS t1 (id,nam)
LEFT OUTER JOIN (VALUES (3, 'c'), (4,'b')) AS t2 (id,nam) ON t1.nam = t2.nam
LEFT OUTER JOIN (VALUES (5,'x'), (6,'x')) t3 (id,nam) ON t1.nam = t3.nam

=>
  Cols t1 |  Cols t2 |  Cols t3
 id | nam | id | nam | id | nam
----+-----+----+-----+----+-----
 1  |  b  | 4  |  b  |    |
 2  |  a  |    |     |    |


 => (1=1) kann nicht pauschal bei LEFT JOIN verwendet werden.


In Sqloure:

  (sqlosure.sql-put/sql-select->string
          (query->sql
           (opt/optimize-query
            (get-query
             (monadic
              [t1 (embed (make-sql-table "t1"
                                         (alist->rel-scheme [["id" integer%]
                                                             ["nam" string%]])
                                         :universe test-universe))]
              [t2
               (outer (make-sql-table "t2"
                                         (alist->rel-scheme [["id" integer%]
                                                             ["nam" string%]])
                                         :universe test-universe))]
              (restrict-outer (=$ (! t1 "nam")
                                  (! t2 "nam"))) ;; muss hier gesetzt sein, sonst falsches Ergebnis!
              [t3
               (outer (make-sql-table "t3"
                                      (alist->rel-scheme [["id" integer%]
                                                          ["nam" string%]])
                                      :universe test-universe))]
              (restrict-outer (=$ (! t1 "nam")
                                  (! t3 "nam")))
              (project [["fo1" (! t1 "nam")]
                        ["fo2" (! t2 "nam")]
                        ["fo3" (! t3 "nam")]]))))))

   */




  test("having") {
    assertEquals(SQL.having(None), None)
    assertEquals(SQL.having(Some(Seq.empty)), None)
    assertEquals(SQL.having(Some(Seq(SqlTest.longExpr))), Some(("HAVING " + SqlTest.longExpr1T._1, SqlTest.longExpr1T._2)))
    assertEquals(SQL.having(Some(Seq(
      SqlExpressionApp(SqlOperator.between, Seq(SqlExpressionColumn("valueA"), SqlExpressionConst(Type.integer, 4), SqlExpressionConst(Type.integer, 10))),
      SqlExpressionExists(SqlTest.adr1)
    ))),
      Some(("HAVING ((valueA BETWEEN ? AND ?) AND EXISTS (SELECT * FROM addresses))", Seq((Type.integer, 4), (Type.integer, 10)))))
  }

  test("group by") {
    assertEquals(SQL.groupBy(None), None)
    assertEquals(SQL.groupBy(Some(Seq.empty)), None)
    assertEquals(SQL.groupBy(Some(Seq("blub"))), Some(("GROUP BY blub", Seq.empty)))
    assertEquals(SQL.groupBy(Some(Seq("one", "two"))), Some(("GROUP BY one, two", Seq.empty)))
  }

  test("order") {
    assertEquals(SQL.orderBy(None), None)
    assertEquals(SQL.orderBy(Some(Seq.empty)), None)
    assertEquals(SQL.orderBy(Some(Seq(("one", SqlOrderAscending)))), Some(("ORDER BY one ASC", Seq.empty)))
    assertEquals(SQL.orderBy(Some(Seq(("one", SqlOrderDescending), ("third", SqlOrderAscending)))),
      Some(("ORDER BY one DESC, third ASC", Seq.empty)))
  }

  test("extra") {
    assertEquals(SQL.extra(None), None)
    assertEquals(SQL.extra(Some(Seq.empty)), None)
    assertEquals(SQL.extra(Some(Seq("LIMIT 1", ",", "5"))), Some(("LIMIT 1 , 5", Seq.empty)))
  }
}
