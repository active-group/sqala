package de.ag.sqala

import de.ag.sqala


object PutSQL { // TODO zusammenlegen mit SqlUtils

  /**
    * String : SQL-Statment
    * Seq[(Type, Any)] : (Datentyp, fester Wert) -> ersetzt später z.B. ein Fragezeichen
    */
  type SqlWithParams = Option[(String, Seq[(Type, Any)])]
  type SqlWithParamsFix = (String, Seq[(Type, Any)])


  /* Interpretationsfunktionen aus sql_put.clj
    */

  def select(sqlSels: SqlInterpretations): SqlWithParams = ??? // ToDo needed ?

  def attributes(atts: Seq[(String, SqlExpression)]) : SqlWithParams = {
    if(atts.isEmpty)
      Some(("*", Seq.empty))
    else {
      SqlUtils.putJoiningInfixWP[(String, SqlExpression)](atts, ", ", {
        case (col: String, sqlE: SqlExpressionColumn) if col == sqlE.name => (col, Seq.empty)
        case (col: String, sqlE: SqlExpression) => putColumnAnAlias(expression(sqlE), Some(col))
      })
    }
  }

  // FixMe : Zur Info: Weiche hier von dem orginal Sqlosure ab, da die restricts hier direkt dazu implementiert werden sollten!
  /*
  Überlegungen dies am sinvollsten zu implementieren...
  OuterRestrictions = Expressions  ->  Seq[Expressions]
  Expressions müssen aber auf die Spalten geprüft werden - da beim richtigen JOIN dazugefügt werden müssen -> Wissen über die aktuellen Spalten nötig!!
    => umgesetzt mit Variable allColumns (enthält nur aktuelle Columns!

   ...
   evt. andere Umsetzung:
   Tupel/Trippel aus OuterTable und dazu gehörigen Criteria -> die direkt als ON angefügt werden ?!
    */
  // add outer-restricts
  def join(tables: Seq[(Option[String], SqlInterpretations)], outerTables: Seq[(Option[String], SqlInterpretations)],
           outerCriteria: Seq[SqlExpression]) : SqlWithParams = {
    val tempTabs = putTables(tables, ", ")
    Some(PutSQL.surroundSqlString("FROM ",
      PutSQL.concatSQL(Seq(
        {if(outerTables.isEmpty || tables.size == 1)
          tempTabs
        else PutSQL.surroundSqlString("(SELECT * FROM ", tempTabs, ")")},
        {if(outerTables.isEmpty)
          ("", Seq.empty)
        else
          PutSQL.surroundSqlString(" LEFT JOIN ",
            PutSQL.concatSQL(Seq(
              putTables(outerTables, " ON (1=1) LEFT JOIN "), // TODO : führt zu Fehlern siehe Kommentar in SqlUtilsTests
              PutSQL.surroundSqlString(" ON ", // add last ON with all JOIN Criterias
                conditions(outerCriteria),
                ""))),
            "")}
      )),
      ""))
  }

  def conditions(exprs: Seq[SqlExpression]) : SqlWithParamsFix =
    SqlExpressionAnd(exprs).toSQL // concat via AND

  def expression(expr: SqlExpression) : SqlWithParamsFix = expr.toSQL

  def groupBy(grBy: Option[Seq[String]]) : SqlWithParams =
    grBy.flatMap(g => Some((SqlUtils.putJoiningInfix[String](g, ", ", { case x: String => x }), Seq.empty)))

  def having(hav: Option[Seq[SqlExpression]]) : SqlWithParams =
    hav.flatMap(e => Some(SqlExpressionAnd(e).toSQL))


  /** */
  def putColumnAnAlias(expr: SqlWithParamsFix, alias: Option[String]): SqlWithParamsFix = expr match {
    case (sql: String, seqTyps: Seq[(Type, Any)]) =>
      (sql+SqlUtils.defaultPutAlias(alias), seqTyps)
  }

  def putTables(tables: Seq[(Option[String], SqlInterpretations)], between: String) : SqlWithParamsFix =
    SqlUtils.putJoiningInfixWPFix[(Option[String], SqlInterpretations)](tables, between, {
      case (alias: Option[String], select: SqlSelectTable) => putColumnAnAlias((select.name, Seq.empty), alias)
      case (alias: Option[String], select: SqlInterpretations) => {
        val temp: SqlWithParamsFix = select.toSQL
        putColumnAnAlias(("("+temp._1+")", temp._2), alias)
      }
    })

  def surroundSqlString(before: String, sql : PutSQL.SqlWithParamsFix, after: String) : PutSQL.SqlWithParamsFix =
    (before+sql._1+after, sql._2)

  def concatSQL(sqls : Seq[PutSQL.SqlWithParamsFix]) : PutSQL.SqlWithParamsFix =
    (sqls.map(x => x._1).mkString(""), sqls.map(x => x._2).flatten)


}

trait SqlInterpretations {
  /**
    * wandelt die Strukturen in SQL-Statmentes um
    *
    * @return (SQL-Query, Seq[(Typ, Wert)]    : Als Seq wird der Datentyp und der Wert und die Variable übermittelt
    */
  def toSQL : PutSQL.SqlWithParamsFix
}
/*
  LIKE: Table, Select, Select-Combine, Select-Empty -- FIXME SelectEmpty ist als SQL nicht ausführbar/einbettbar !!
 */


final case class SqlSelectTable(
                                 name: String,
                                 schema: RelationalScheme
                               ) extends SqlInterpretations {
  override def toSQL : PutSQL.SqlWithParamsFix = ("SELECT * FROM "+name, Seq.empty)
}

final case class SqlSelect(
                            options: Option[Seq[String]], // like DISTINCT, ALL ...
                            attributes: Seq[(String, SqlExpression)], // column, expression
                            //nullary: Boolean, // FixME : was ist damit gemeint ?
                            tables: Seq[(Option[String], SqlInterpretations)],
                            outerTables: Seq[(Option[String], SqlInterpretations)],
                            criteria: Seq[SqlExpression], // where
                            outerCriteria: Seq[SqlExpression], // left join ... on
                            groupBy: Option[Seq[String]],
                            having: Option[Seq[SqlExpression]], // FixMe: evt Aggregation einschränken - bzw. diese nur hier und nicht in Criteria zulassen ?!
                            orderBy: Any
                            //extra: Any // ??? was ist da mit gemeint
                          ) extends SqlInterpretations {
  override def toSQL : PutSQL.SqlWithParamsFix = {
    val tempSeq : Seq[PutSQL.SqlWithParams] = Seq(
      Some(("SELECT", Seq.empty)),
      this.options.flatMap(x => Some((x.mkString(" "), Seq.empty))),
      PutSQL.attributes(attributes),
      PutSQL.join(tables, outerTables, outerCriteria),
      PutSQL.having(having),
      PutSQL.groupBy(groupBy)
      // ORder
      // extra (Top, Limit ..)
    )
    SqlUtils.putJoiningInfixWPFix[PutSQL.SqlWithParams](tempSeq.filter(_.isDefined), " ",
      {x => x.get})
    //PutSQL.concatSQL(.map(_.get))
    /*
    val validSeqMember = tempSeq.filter(_._1.isDefined)
    (validSeqMember.map(_._1.get).mkString(" "), validSeqMember.map(_._2).flatten)*/
  }

  /*
  def putSqlJoin : Option[(String, Seq[(Type, Any)])] = {
    if(outerTables.isEmpty || tables.size == 1) {
      Some(("FROM "+,
        Seq.empty))
    } else {

    }
  }*/
}


/**
  * CombineOperation like UNION, INTERSECT ...
  */

final case class SqlSelectCombine(
                                   operation: SqlCombineOperator,
                                   left: SqlInterpretations,
                                   right: SqlInterpretations
                                 ) extends SqlInterpretations {
  override def toSQL : PutSQL.SqlWithParamsFix =
    operation.toSQL(left.toSQL, right.toSQL)
}

sealed trait SqlCombineOperator {
  def toSQL(left: PutSQL.SqlWithParamsFix, right: PutSQL.SqlWithParamsFix) : PutSQL.SqlWithParamsFix =
    PutSQL.concatSQL(Seq(
      PutSQL.surroundSqlString("(", left, ") "+getOpName),
      PutSQL.surroundSqlString(" (", right, ")")))
  protected val getOpName : String
}

object SqlCombineOperator {
  case object Union extends SqlCombineOperator {
    protected val getOpName = "UNION"
  }
  case object Intersection extends SqlCombineOperator {
    protected val getOpName = "INTERSECT"
  }
  case object Difference extends SqlCombineOperator {
    protected val getOpName = "EXCEPT"
  }
}








object SqlSelectEmpty // TODO








trait SqlExpression { // kein SqlInterpretations, da nicht eigenständig ausführbar
  def toSQL : PutSQL.SqlWithParamsFix
}

case class SqlExpressionColumn(name: String) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix = (name, Seq.empty)
}
case class SqlExpressionApp(rator: SqlOperator,
                            rands: Seq[SqlExpression]) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix = SqlOperator.toSQL(rator, rands.map(x => x.toSQL))
}
case class SqlExpressionConst(typ: Type, value: Any) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix = SqlUtils.putLiteral(typ, value)
}
case class SqlExpressionCase(input: Option[SqlExpression],
                             branches: Seq[(SqlExpression, SqlExpression)],
                             default: Option[SqlExpression]) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix = {
    if(branches.isEmpty)
      throw new AssertionError("Invalid branche-size (=0) in SqlExpressionCase")
    else
      PutSQL.surroundSqlString("(CASE ",
        PutSQL.concatSQL(Seq(
          // optional value behinde CASE
          {if(input.isDefined)
            PutSQL.surroundSqlString("", input.get.toSQL, " ")
          else ("", Seq.empty)},
          // WHEN ... THEN ... - part
          SqlUtils.putJoiningInfixWPFix[(SqlExpression, SqlExpression)](branches, " ", {
            case (exWhen: SqlExpression, exThen: SqlExpression) => {
              val sqlWhen : PutSQL.SqlWithParamsFix = exWhen.toSQL
              val sqlThen : PutSQL.SqlWithParamsFix = exThen.toSQL
              ("WHEN "+sqlWhen._1+" THEN "+sqlThen._1, sqlWhen._2++sqlThen._2)
            }}),
          // optional default
          {if(default.isDefined)
            PutSQL.surroundSqlString(" ELSE ", default.get.toSQL, "")
          else ("", Seq.empty)}
        )),
      " END)")
  }
}
case class SqlExpressionExists(select: SqlInterpretations) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix =
    PutSQL.surroundSqlString("EXISTS (", select.toSQL, ")")
}
case class SqlExpressionTuple(expressions: Seq[SqlExpression]) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix =
    PutSQL.surroundSqlString("(", SqlUtils.putJoiningInfixWPFix[SqlExpression](expressions, ", ", {
      e: SqlExpression => e.toSQL
    }), ")")
}
case class SqlExpressionSubquery(query: SqlInterpretations) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix =
    PutSQL.surroundSqlString("(", query.toSQL, ")")
}

// AND & OR
case class SqlExpressionOr(exprs: Seq[SqlExpression]) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix = {
    if (exprs.size == 1)
      exprs(0).toSQL
    else
      PutSQL.surroundSqlString("(", SqlUtils.putJoiningInfixWPFix[SqlExpression](exprs, " OR ", { e => e.toSQL }), ")")
  }
}
case class SqlExpressionAnd(exprs: Seq[SqlExpression]) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix = {
    if (exprs.size == 1)
      exprs(0).toSQL
    else
      PutSQL.surroundSqlString("(", SqlUtils.putJoiningInfixWPFix[SqlExpression](exprs, " AND ", { e => e.toSQL }), ")")
  }
}


// SQL Aggregation
/*
case class SqlAggregationApp(rator: SqlAggOperator,
                             rands: Seq[SqlExpression]) extends SqlAggregation {
override def toSQL : PutSQL.SqlWithParamsFix = SqlOperator.toSQL(rator, rands.map(x => x.toSQL))
}*/






object SQL {
  // TODO add universe ?!
  /*def makeSqlTable(name: String, schema: RelationalScheme) : BaseRelation[SqlSelectTable] =
    BaseRelation(name, schema, SqlSelectTable(name, schema))*/

  def makeSqlSelect(attributes: Seq[(String, SqlExpression)], tables: Seq[(Option[String], SqlInterpretations)]) : SqlSelect =
    SqlSelect(None, attributes, tables, Seq.empty,
      Seq.empty, Seq.empty,
      None, None, None)

  def makeSqlSelect(options: Seq[String], attributes: Seq[(String, SqlExpression)], tables: Seq[(Option[String], SqlInterpretations)]) : SqlSelect =
    SqlSelect(Some(options), attributes, tables, Seq.empty,
      Seq.empty, Seq.empty,
      None, None, None)
}

sealed trait SqlOrder

object Ascending extends SqlOrder

object Descending extends SqlOrder





/**
  * A SqlOperator is e.g. a equality-check in the WHERE-clause
 *
  * @param name   the character/-s which used in sql
  * @param arity  the arity defined in SqlOperatorArity
  * @param extra  a optional extra String, which is needed in some Arities (like BETWEEN ... 'AND' ...)
  *               BETWEEN is the name an AND is defined in EXTRA, so the arity can be used for other operators like this
  */

case class SqlOperator(name: String,
                       arity: SqlOperatorArity,
                       extra: Option[String] = None)

object SqlOperator {
  val eq : SqlOperator = SqlOperator("=", SqlOperatorArity.Infix)
  val gt : SqlOperator = SqlOperator(">", SqlOperatorArity.Infix)
  val lt : SqlOperator = SqlOperator("<", SqlOperatorArity.Infix)
  val geq : SqlOperator = SqlOperator(">=", SqlOperatorArity.Infix)
  val leq : SqlOperator = SqlOperator("<=", SqlOperatorArity.Infix)
  val neq : SqlOperator = SqlOperator("<>", SqlOperatorArity.Infix)
  val neq2 : SqlOperator = SqlOperator("!=", SqlOperatorArity.Infix)
  val and : SqlOperator = SqlOperator("AND", SqlOperatorArity.Infix)
  val or : SqlOperator = SqlOperator("OR", SqlOperatorArity.Infix)
  val like : SqlOperator = SqlOperator("LIKE", SqlOperatorArity.Infix)
  val in : SqlOperator = SqlOperator("IN", SqlOperatorArity.Infix)
  val between : SqlOperator = SqlOperator("BETWEEN", SqlOperatorArity.Prefix3, Some("AND"))
  val cat : SqlOperator = SqlOperator("CAT", SqlOperatorArity.Infix)
  val plus : SqlOperator = SqlOperator("+", SqlOperatorArity.Infix)
  val minus : SqlOperator = SqlOperator("-", SqlOperatorArity.Infix)
  val mult : SqlOperator = SqlOperator("*", SqlOperatorArity.Infix)

  val div : SqlOperator = SqlOperator("/", SqlOperatorArity.Infix)
  val mod : SqlOperator = SqlOperator("/", SqlOperatorArity.Infix)
  val bitNot : SqlOperator = SqlOperator("~", SqlOperatorArity.Prefix)
  val bitAnd : SqlOperator = SqlOperator("&", SqlOperatorArity.Infix)
  val bitOr : SqlOperator = SqlOperator("|", SqlOperatorArity.Infix)
  val bitXor : SqlOperator = SqlOperator("^", SqlOperatorArity.Infix)
  val asg : SqlOperator = SqlOperator("=", SqlOperatorArity.Infix)

  val concat : SqlOperator = SqlOperator("CONCAT", SqlOperatorArity.Prefix2, Some(","))
  val lower : SqlOperator = SqlOperator("LOWER", SqlOperatorArity.Prefix)
  val upper : SqlOperator = SqlOperator("UPPER", SqlOperatorArity.Prefix)

  val not : SqlOperator = SqlOperator("NOT", SqlOperatorArity.Prefix)
  val isNull : SqlOperator = SqlOperator("IS NULL", SqlOperatorArity.Postfix)
  val isNotNull : SqlOperator = SqlOperator("IS NOT NULL", SqlOperatorArity.Postfix)
  val length : SqlOperator = SqlOperator("LENGTH", SqlOperatorArity.Prefix)

  // FIXME : Aggregations - separat definieren like SqlAggregation ??
  val count : SqlOperator = SqlOperator("COUNT", SqlOperatorArity.Prefix)
  val countAll : SqlOperator = SqlOperator("COUNT", SqlOperatorArity.Prefix)
  val sum : SqlOperator = SqlOperator("SUM", SqlOperatorArity.Prefix)
  val avg : SqlOperator = SqlOperator("AVG", SqlOperatorArity.Prefix)
  val min : SqlOperator = SqlOperator("MIN", SqlOperatorArity.Prefix)
  val max : SqlOperator = SqlOperator("MAX", SqlOperatorArity.Prefix)
  val stdDev : SqlOperator = SqlOperator("StdDev", SqlOperatorArity.Prefix)
  val stdDevP : SqlOperator = SqlOperator("StdDevP", SqlOperatorArity.Prefix)
  val vari : SqlOperator = SqlOperator("Var", SqlOperatorArity.Prefix)
  val varP : SqlOperator = SqlOperator("VarP", SqlOperatorArity.Prefix)
  // ...

  def toSQL(rator : SqlOperator, rands: Seq[PutSQL.SqlWithParamsFix]) : PutSQL.SqlWithParamsFix =
    rator.arity.toSql(rator, rands)
}


/**
  * Arity for the SqlOperators
  */

trait SqlOperatorArity {
  /**
    * übergebene Liste enthält geforderte Werte, z.B. was links und rechts vom Infix-Operator steht
    * Werte werden aufgrund der angegebenen randSize auf die korrekte Anzahl überprüft und dann korrekt angeordnet
    */
  protected val randsSize : Int
  protected val extraIsSet : Boolean = false
  def toSql(op: SqlOperator, rands: Seq[PutSQL.SqlWithParamsFix]) = {
    if(rands.size != randsSize)
      throw new AssertionError("Invalid arity. Expected for "+op+" arity of "+randsSize+" but getting "+rands.size)
    else if(extraIsSet && op.extra.isEmpty)
      throw new AssertionError("Extrainformation is missing. There is an extra Information/String in the SqlOperator ("+op+") missing für the choosen Arity.")
    else
      toSQLHelper(op, rands)
  }
  protected def toSQLHelper(op : SqlOperator, rands : Seq[PutSQL.SqlWithParamsFix]) : PutSQL.SqlWithParamsFix
}
object SqlOperatorArity {

  case object Postfix extends SqlOperatorArity { // sqlosure: -1
    override protected val randsSize = 1
    override protected def toSQLHelper(op : SqlOperator, rands : Seq[PutSQL.SqlWithParamsFix]) =
      ("("+rands(0)._1+") "+op.name, rands(0)._2)
  }

  case object Prefix extends SqlOperatorArity { // sqlosure: 1
    override protected val randsSize = 1
    override protected def toSQLHelper(op : SqlOperator, rands : Seq[PutSQL.SqlWithParamsFix]) =
      (op.name+"("+rands(0)._1+")", rands(0)._2)
  }

  case object Prefix2 extends SqlOperatorArity { // sqlosure: -2
    override protected val randsSize = 2
    override protected val extraIsSet = true
    override protected def toSQLHelper(op : SqlOperator, rands : Seq[PutSQL.SqlWithParamsFix]) =
      (op.name+"("+rands(0)._1+op.extra.get+rands(1)._1+")", rands(0)._2++rands(1)._2)
  }

  case object Infix extends SqlOperatorArity { // sqlosure: 2
    override protected val randsSize = 2
    override protected def toSQLHelper(op : SqlOperator, rands : Seq[PutSQL.SqlWithParamsFix]) =
      ("("+rands(0)._1+" "+op.name+" "+rands(1)._1+")", rands(0)._2++rands(1)._2)
  }

  case object Prefix3 extends SqlOperatorArity { // sqlosure: 3
    // FIXME : Fehlende Klammern beabsichtigt ?
  override protected val randsSize = 3
    override protected val extraIsSet = true
    override protected def toSQLHelper(op : SqlOperator, rands : Seq[PutSQL.SqlWithParamsFix]) =
      (rands(0)._1+" "+op.name+" "+rands(1)._1+" "+op.extra.get+" "+rands(2)._1, rands(0)._2++rands(1)._2++rands(2)._2)
  }
}