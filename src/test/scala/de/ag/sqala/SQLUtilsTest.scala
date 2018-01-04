package de.ag.sqala

import TestUtil.assertEquals

import org.scalatest.FunSuite

class SQLUtilsTest extends FunSuite {

  val att1 = Seq(("first", SQLExpressionColumn("first")), ("abc", SQLExpressionColumn("second")))
  val att2 = Seq(("blub", SQLExpressionConst(Type.string, "")), ("blubStr", SQLExpressionConst(Type.string, "X")),
    ("blub2", SQLExpressionConst(Type.integer, 4)))

  test("putPaddingIf...") {
    assertEquals(SQLUtils.putPaddingIfNonNull[Int](Seq.empty, {case x: Seq[Int] => (x.map(x => (x+1).toString).mkString(","), x.map(i => (Type.integer, i)))}, "BLA "),
      None)
    assertEquals(SQLUtils.putPaddingIfNonNull[Int](Seq(2),
      {case x: Seq[Int] => (x.map(x => (x+1).toString).mkString(","), x.map(i => (Type.integer, i)))}, "BLA "),
      Some(("BLA 3", Seq((Type.integer, 2)))))
    assertEquals(SQLUtils.putPaddingIfNonNull[Int](Seq(2, 5, 9),
      {case x: Seq[Int] => (x.map(x => (x+1).toString).mkString(","), x.map(i => (Type.integer, i)))}, "BLA "),
      Some(("BLA 3,6,10", Seq((Type.integer, 2), (Type.integer, 5), (Type.integer, 9)))))
    assertEquals(SQLUtils.putPaddingIfNonNull[Int](Seq(2, 5, 9),
      {case x: Seq[Int] => (x.map(x => (x+2).toString).mkString("-"), Seq.empty)}, "BAL "),
      Some(("BAL 4-7-11", Seq.empty)))
  }

  test("function with alias") {
    val ret1 = ("BlabaB", Seq.empty)
    val ret2 = ("This to Go", Seq((Type.integer, 4), (Type.string, "foo")))

    assertEquals(SQLUtils.defaultPutAlias(None), "")
    assertEquals(SQLUtils.defaultPutAlias(Some("Blub")), " AS Blub")
    assertEquals(SQLUtils.putColumnAnAlias(ret1, None), (ret1._1, ret1._2))
    assertEquals(SQLUtils.putColumnAnAlias(ret2, None), (ret2._1, ret2._2))
    assertEquals(SQLUtils.putColumnAnAlias(ret1, Some("X")), ("BlabaB AS X", ret1._2))
    assertEquals(SQLUtils.putColumnAnAlias(ret2, Some("X")), ("This to Go AS X", ret2._2))
    // TODO:
    //assertEquals(SQLUtils.putTableAnAlias(ret2, None), (ret2._1, ret2._2))
    //assertEquals(SQLUtils.putTableAnAlias(ret1, Some("X")), ("BlabaB AS X", ret1._2))
  }

  test("joiningInfix") {
    assert(SQLUtils.putJoiningInfix(Seq.empty, ",") { x => ("BX", Seq.empty) } === ("", Seq.empty))
    assert(SQLUtils.putJoiningInfix(Seq("a", "b"), "-") { x => ("BX", Seq((Type.string, x))) } ===
      ("BX-BX", Seq((Type.string, "a"), (Type.string, "b"))))
  }




  test("attributes (& putLiteral)") {
    assertEquals(SQL.attributes(Seq.empty), Some(("*", Seq.empty)))
    assertEquals(SQL.attributes(att1), Some(("first, second AS abc", Seq.empty)))
    assertEquals(SQL.attributes(att2), Some(("? AS blub, ? AS blubStr, ? AS blub2",
      Seq((Type.string, ""), (Type.string, "X"), (Type.integer, 4)))))
    assertEquals(SQLUtils.putLiteral(Type.integer, 5), ("?", Seq((Type.integer, 5))))
  }



  test("surround and concat SQL") {
    assertEquals(SQLUtils.surroundSQL("", ("blublba", Seq((Type.string, "bla"))), ""), ("blublba", Seq((Type.string, "bla"))))
    assertEquals(SQLUtils.surroundSQL("y", ("blublba", Seq((Type.string, "bla"))), "<"), ("yblublba<", Seq((Type.string, "bla"))))
    assertEquals(SQLUtils.concatSQL(Seq.empty), ("", Seq.empty))
    assertEquals(SQLUtils.concatSQL(Seq(
      ("a", Seq((Type.boolean, true))),
      ("b", Seq((Type.integer, 3), (Type.string, "a"))),
      ("d", Seq.empty),
      ("c", Seq((Type.integer, -1))))),
      ("abdc", Seq((Type.boolean, true), (Type.integer, 3), (Type.string, "a"), (Type.integer, -1))))
  }




  val tbl1 = SQLSelectTable("tabelleA", RelationalScheme.make(Seq(("a", Type.string), ("b", Type.string))))
  val tbl2 = SQLSelectTable("tabelleB", RelationalScheme.make(Seq(("b", Type.string), ("c", Type.string))))
  val tbl3 = SQL.makeSQLSelect(Seq(("a", SQLExpressionColumn("b"))), Seq((Some("t1"), tbl1)))

  test("SQL - join") {
    assertEquals(SQL.join(Seq((None, tbl1)), Seq.empty, Seq.empty), Some(("FROM tabelleA AS __dummy", Seq.empty)))
    assertEquals(SQL.join(Seq((Some("A"), tbl1)), Seq.empty, Seq.empty), Some(("FROM tabelleA AS A", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1), (None, tbl2), (Some("tdx"), tbl3)), Seq.empty, Seq.empty),
      Some((s"FROM tabelleA AS __dummy, tabelleB AS __dummy, (SELECT b AS a FROM tabelleA AS t1) AS tdx", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1)), Seq((Some("tx"), tbl2)),
      Seq(SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("a"), SQLExpressionColumn("b"))))),
      Some(("FROM tabelleA AS __dummy LEFT JOIN tabelleB AS tx ON (a = b)", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1), (Some("tdx"), tbl3)), Seq((None, tbl2)),
      Seq(SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("a"), SQLExpressionColumn("b"))))),
      Some(("FROM (SELECT * FROM tabelleA AS __dummy, (SELECT b AS a FROM tabelleA AS t1) AS tdx) LEFT JOIN tabelleB AS __dummy ON (a = b)", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1)), Seq((Some("t1"), tbl1), (Some("t2"), tbl2), (Some("tt"), tbl1)),
      Seq(SQLExpressionApp(SQLOperator.eq, Seq(SQLExpressionColumn("b"), SQLExpressionColumn("a"))))),
      Some(("FROM tabelleA AS __dummy LEFT JOIN tabelleA AS t1 ON (1=1) LEFT JOIN tabelleB AS t2 ON (1=1) LEFT JOIN tabelleA AS tt ON (b = a)", Seq.empty)))
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


In SQLoure:

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
    assertEquals(SQL.having(Some(Seq(SQLTest.longExpr))), Some(("HAVING " + SQLTest.longExpr1T._1, SQLTest.longExpr1T._2)))
    assertEquals(SQL.having(Some(Seq(
      SQLExpressionApp(SQLOperator.between, Seq(SQLExpressionColumn("valueA"), SQLExpressionConst(Type.integer, 4), SQLExpressionConst(Type.integer, 10))),
      SQLExpressionExists(SQLTest.adr1)
    ))),
      Some(("HAVING ((valueA BETWEEN ? AND ?) AND EXISTS (SELECT * FROM addresses))", Seq((Type.integer, 4), (Type.integer, 10)))))
  }

  test("group by") {
    assertEquals(SQL.groupBy(None), None)
    assertEquals(SQL.groupBy(Some(Set.empty)), None)
    assertEquals(SQL.groupBy(Some(Set("blub"))), Some(("GROUP BY blub", Seq.empty)))
    assertEquals(SQL.groupBy(Some(Set("one", "two"))), Some(("GROUP BY one, two", Seq.empty)))
  }

  test("order") {
    assertEquals(SQL.orderBy(None), None)
    assertEquals(SQL.orderBy(Some(Seq.empty)), None)
    assertEquals(SQL.orderBy(Some(Seq(("one", SQLOrderAscending)))), Some(("ORDER BY one ASC", Seq.empty)))
    assertEquals(SQL.orderBy(Some(Seq(("one", SQLOrderDescending), ("third", SQLOrderAscending)))),
      Some(("ORDER BY one DESC, third ASC", Seq.empty)))
  }

  test("extra") {
    assertEquals(SQL.extra(None), None)
    assertEquals(SQL.extra(Some(Seq.empty)), None)
    assertEquals(SQL.extra(Some(Seq("LIMIT 1", ",", "5"))), Some(("LIMIT 1 , 5", Seq.empty)))
  }
}
