package de.ag.sqala


object SQL {
  /** Tupel aus (String, Seq[(Type, Any)])
    * String : SQL-Statement
    * Seq[(Type, Any)] : (Datentyp, fester Wert) -> ersetzt später z.B. ein Fragezeichen
    */
  type SqlReturnOption = Option[(String, Seq[(Type, Any)])]
  type SqlReturn = (String, Seq[(Type, Any)])



  // TODO add universe ?!
  /*def makeSqlTable(name: String, schema: RelationalScheme) : BaseRelation[SqlSelectTable] =
    BaseRelation(name, schema, SqlSelectTable(name, schema))*/

  def makeSqlSelect(attributes: Seq[(String, SqlExpression)], tables: Seq[(Option[String], SqlInterpretations)]) : SqlSelect =
    SqlSelect(None, attributes, tables, Seq.empty,
      Seq.empty, Seq.empty,
      None, None, None, None)

  def makeSqlSelect(options: Seq[String], attributes: Seq[(String, SqlExpression)], tables: Seq[(Option[String], SqlInterpretations)]) : SqlSelect =
    SqlSelect(Some(options), attributes, tables, Seq.empty,
      Seq.empty, Seq.empty,
      None, None, None, None)



  /**
    * Functions for translation into SQL
    */
  def attributes(atts: Seq[(String, SqlExpression)]) : SqlReturnOption = {
    if(atts.isEmpty)
      Some(("*", Seq.empty))
    else {
      SqlUtils.putJoiningInfixOption[(String, SqlExpression)](atts, ", ", {
        case (col: String, sqlE: SqlExpressionColumn) if col == sqlE.name => (col, Seq.empty)
        case (col: String, sqlE: SqlExpression) => SqlUtils.putColumnAnAlias(expression(sqlE), Some(col))
      })
    }
  }


  // FixMe : Zur Info: Weiche hier von dem orginal Sqlosure ab, da die restricts hier direkt dazu implementiert werden!
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
           outerCriteria: Seq[SqlExpression]) : SqlReturnOption = {
    val tempTabs = putTables(tables, ", ")
    Some(SqlUtils.surroundSQL("FROM ",
      SqlUtils.concatSQL(Seq(
        {if(outerTables.isEmpty || tables.size == 1)
          tempTabs
        else SqlUtils.surroundSQL("(SELECT * FROM ", tempTabs, ")")},
        {if(outerTables.isEmpty)
          ("", Seq.empty)
        else
          SqlUtils.surroundSQL(" LEFT JOIN ",
            SqlUtils.concatSQL(Seq(
              putTables(outerTables, " ON (1=1) LEFT JOIN "), // TODO : kann zu Fehlern führen, siehe Kommentar in SqlUtilsTests - wurde so von Sqlosure übernommen
              SqlUtils.surroundSQL(" ON ", // add last ON with all JOIN Criterias
                conditions(outerCriteria),
                ""))),
            "")}
      )),
      ""))
  }

  def putTables(tables: Seq[(Option[String], SqlInterpretations)], between: String) : SqlReturn =
    SqlUtils.putJoiningInfix[(Option[String], SqlInterpretations)](tables, between, {
      case (alias: Option[String], select: SqlSelectTable) => SqlUtils.putColumnAnAlias((select.name, Seq.empty), alias)
      case (alias: Option[String], select: SqlInterpretations) => {
        val temp: SqlReturn = select.toSQL
        SqlUtils.putColumnAnAlias(("("+temp._1+")", temp._2), alias)
      }
    })


  def criterias(crit: Seq[SqlExpression]) : SqlReturnOption =
    SqlUtils.putPaddingIfNonNull(crit, conditions, "WHERE ")

  def groupBy(grBy: Option[Seq[String]]) : SqlReturnOption =
    grBy.flatMap(g => SqlUtils.putPaddingIfNonNull[String](g,
      {case gg: Seq[String] => SqlUtils.putJoiningInfix[String](gg, ", ", { case x: String => (x, Seq.empty) })},
      "GROUP BY "))

  def having(hav: Option[Seq[SqlExpression]]) : SqlReturnOption =
    hav.flatMap({
      case e: Seq[SqlExpression] if !e.isEmpty => Some(SqlUtils.surroundSQL(
        "HAVING ", SqlExpressionAnd(e).toSQL, ""))
      case _ => None
    })

  def orderBy(ordBy: Option[Seq[(String, SqlOrder)]]) : SqlReturnOption =
    ordBy.flatMap({
      case o: Seq[(String, SqlOrder)] if !o.isEmpty => Some(SqlUtils.concatSQL(Seq(
        ("ORDER BY ", Seq.empty),
        SqlUtils.putJoiningInfix[(String, SqlOrder)](o, ", ",
          {case (s: String, ord: SqlOrder) => ord.toSQL(s)})
      )))
      case _ => None
    })

  def extra(v: Option[Seq[String]]) : SqlReturnOption =
    v.flatMap({
      case v: Seq[String] if !v.isEmpty => Some((v.mkString(" "), Seq.empty))
      case _ => None
    })

  // Collection of Conditions
  def conditions(exprs: Seq[SqlExpression]) : SqlReturn =
    SqlExpressionAnd(exprs).toSQL // default concat via AND

  // Translate a Expression
  def expression(expr: SqlExpression) : SqlReturn =
    expr.toSQL // can translate itself to SQL
}







trait SqlInterpretations {
  /**
    * wandelt die Strukturen in SQL-Statmentes um
    *
    * @return (SQL-Query, Seq[(Typ, Wert)]    : Als Seq wird der Datentyp und der Wert und die Variable übermittelt
    */
  def toSQL : SQL.SqlReturn
}
/*
  LIKE: Table, Select, Select-Combine, Select-Empty -- FIXME SelectEmpty ist als SQL nicht ausführbar/einbettbar !!
 */


final case class SqlSelectTable(
                                 name: String,
                                 schema: RelationalScheme
                               ) extends SqlInterpretations {
  override def toSQL : SQL.SqlReturn = ("SELECT * FROM "+name, Seq.empty)
}

final case class SqlSelect(
                            options: Option[Seq[String]], // like DISTINCT, ALL ...
                            attributes: Seq[(String, SqlExpression)], // column, expression
                            //nullary: Boolean, // FixME : was ist damit gemeint ?
                            tables: Seq[(Option[String], SqlInterpretations)],
                            outerTables: Seq[(Option[String], SqlInterpretations)],
                            criteria: Seq[SqlExpression], // WHERE ...
                            outerCriteria: Seq[SqlExpression], // left join ... on ...
                            groupBy: Option[Seq[String]],
                          // FixMe - groupBy : statt SqlExpression nur String - da nur Column zulässig wäre
                            having: Option[Seq[SqlExpression]],
                            // FixMe - having : evt Aggregation einschränken - bzw. diese nur hier und nicht in Criteria zulassen ?!
                            orderBy: Option[Seq[(String, SqlOrder)]],
                          // FixMe - orderBy : statt SqlExpression nur String - da nur die Column zulässig wäre (und nicht andere Expressions)!
                          // FixMe groupBy, having, orderBy werden bei leeren Seq falsch ausgewertet -> soll abgefangen werden?
                            extra: Option[Seq[String]] // TODO: -> LIMIT, TOP   ...
                          ) extends SqlInterpretations {
  override def toSQL : SQL.SqlReturn = {
    val tempSeq : Seq[SQL.SqlReturnOption] = Seq(
      Some(("SELECT", Seq.empty)),
      this.options.flatMap(x => Some((x.mkString(" "), Seq.empty))),
      SQL.attributes(attributes),
      SQL.join(tables, outerTables, outerCriteria),
      SQL.criterias(criteria),
      SQL.groupBy(groupBy),
      SQL.having(having),
      SQL.orderBy(orderBy),
      SQL.extra(extra)
      // extra (Top, Limit ..)
    )
    SqlUtils.putJoiningInfix[SQL.SqlReturnOption](tempSeq.filter(_.isDefined), " ",
      {x => x.get})
  }
}


/**
  * CombineOperation like UNION, INTERSECT ...
  */

final case class SqlSelectCombine(
                                   operation: SqlCombineOperator,
                                   left: SqlInterpretations,
                                   right: SqlInterpretations
                                 ) extends SqlInterpretations {
  override def toSQL : SQL.SqlReturn =
    operation.toSQL(left.toSQL, right.toSQL)
}

sealed trait SqlCombineOperator {
  def toSQL(left: SQL.SqlReturn, right: SQL.SqlReturn) : SQL.SqlReturn =
    SqlUtils.concatSQL(Seq(
      SqlUtils.surroundSQL("(", left, ") "+getOpName),
      SqlUtils.surroundSQL(" (", right, ")")))
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








object SqlSelectEmpty extends SqlInterpretations {
  override def toSQL : SQL.SqlReturn = ("", Seq.empty)
}








trait SqlExpression { // kein SqlInterpretations, da nicht eigenständig ausführbar
  def toSQL : SQL.SqlReturn
}

case class SqlExpressionColumn(name: String) extends SqlExpression {
  override def toSQL : SQL.SqlReturn = (name, Seq.empty)
}
case class SqlExpressionApp(rator: SqlOperator,
                            rands: Seq[SqlExpression]) extends SqlExpression {
  override def toSQL : SQL.SqlReturn = SqlOperator.toSQL(rator, rands.map(x => x.toSQL))
}
case class SqlExpressionConst(typ: Type, value: Any) extends SqlExpression {
  override def toSQL : SQL.SqlReturn = SqlUtils.putLiteral(typ, value)
}
case class SqlExpressionCase(input: Option[SqlExpression],
                             branches: Seq[(SqlExpression, SqlExpression)],
                             default: Option[SqlExpression]) extends SqlExpression {
  override def toSQL : SQL.SqlReturn = {
    if(branches.isEmpty)
      throw new AssertionError("Invalid branche-size (=0) in SqlExpressionCase")
    else
      SqlUtils.surroundSQL("(CASE ",
        SqlUtils.concatSQL(Seq(
          // optional value behinde CASE
          {if(input.isDefined)
            SqlUtils.surroundSQL("", input.get.toSQL, " ")
          else ("", Seq.empty)},
          // WHEN ... THEN ... - part
          SqlUtils.putJoiningInfix[(SqlExpression, SqlExpression)](branches, " ", {
            case (exWhen: SqlExpression, exThen: SqlExpression) => {
              val sqlWhen : SQL.SqlReturn = exWhen.toSQL
              val sqlThen : SQL.SqlReturn = exThen.toSQL
              ("WHEN "+sqlWhen._1+" THEN "+sqlThen._1, sqlWhen._2++sqlThen._2)
            }}),
          // optional default
          {if(default.isDefined)
            SqlUtils.surroundSQL(" ELSE ", default.get.toSQL, "")
          else ("", Seq.empty)}
        )),
      " END)")
  }
}
case class SqlExpressionExists(select: SqlInterpretations) extends SqlExpression {
  override def toSQL : SQL.SqlReturn =
    SqlUtils.surroundSQL("EXISTS (", select.toSQL, ")")
}
case class SqlExpressionTuple(expressions: Seq[SqlExpression]) extends SqlExpression {
  override def toSQL : SQL.SqlReturn =
    SqlUtils.surroundSQL("(", SqlUtils.putJoiningInfix[SqlExpression](expressions, ", ", {
      e: SqlExpression => e.toSQL
    }), ")")
}
case class SqlExpressionSubquery(query: SqlInterpretations) extends SqlExpression {
  override def toSQL : SQL.SqlReturn =
    SqlUtils.surroundSQL("(", query.toSQL, ")")
}

// AND & OR
case class SqlExpressionOr(exprs: Seq[SqlExpression]) extends SqlExpression {
  override def toSQL : SQL.SqlReturn = {
    if (exprs.size == 1)
      exprs(0).toSQL
    else
      SqlUtils.surroundSQL("(", SqlUtils.putJoiningInfix[SqlExpression](exprs, " OR ", { e => e.toSQL }), ")")
  }
}
case class SqlExpressionAnd(exprs: Seq[SqlExpression]) extends SqlExpression {
  override def toSQL : SQL.SqlReturn = {
    if (exprs.size == 1)
      exprs(0).toSQL
    else
      SqlUtils.surroundSQL("(", SqlUtils.putJoiningInfix[SqlExpression](exprs, " AND ", { e => e.toSQL }), ")")
  }
}






/**
  * Sql OrderBy Objects
  */
sealed trait SqlOrder {
  def toSQL(col: String) : SQL.SqlReturn =
    (col+" "+toSQLHelper, Seq.empty)

  val toSQLHelper : String
}

object SqlOrderAscending extends SqlOrder {
  override val toSQLHelper : String = "ASC"
}

object SqlOrderDescending extends SqlOrder {
  override val toSQLHelper : String = "DESC"
}





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

  // FIXME : Aggregations - separat definieren ??
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

  def toSQL(rator : SqlOperator, rands: Seq[SQL.SqlReturn]) : SQL.SqlReturn =
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
  def toSql(op: SqlOperator, rands: Seq[SQL.SqlReturn]) = {
    if(rands.size != randsSize)
      throw new AssertionError("Invalid arity. Expected for "+op+" arity of "+randsSize+" but getting "+rands.size)
    else if(extraIsSet && op.extra.isEmpty)
      throw new AssertionError("Extrainformation is missing. There is an extra Information/String in the SqlOperator ("+op+") missing für the choosen Arity.")
    else
      toSQLHelper(op, rands)
  }
  protected def toSQLHelper(op : SqlOperator, rands : Seq[SQL.SqlReturn]) : SQL.SqlReturn
}
object SqlOperatorArity {

  case object Postfix extends SqlOperatorArity { // sqlosure: -1
    override protected val randsSize = 1
    override protected def toSQLHelper(op : SqlOperator, rands : Seq[SQL.SqlReturn]) =
      ("("+rands(0)._1+") "+op.name, rands(0)._2)
  }

  case object Prefix extends SqlOperatorArity { // sqlosure: 1
    override protected val randsSize = 1
    override protected def toSQLHelper(op : SqlOperator, rands : Seq[SQL.SqlReturn]) =
      (op.name+"("+rands(0)._1+")", rands(0)._2)
  }

  case object Prefix2 extends SqlOperatorArity { // sqlosure: -2
    override protected val randsSize = 2
    override protected val extraIsSet = true
    override protected def toSQLHelper(op : SqlOperator, rands : Seq[SQL.SqlReturn]) =
      (op.name+"("+rands(0)._1+op.extra.get+rands(1)._1+")", rands(0)._2++rands(1)._2)
  }

  case object Infix extends SqlOperatorArity { // sqlosure: 2
    override protected val randsSize = 2
    override protected def toSQLHelper(op : SqlOperator, rands : Seq[SQL.SqlReturn]) =
      ("("+rands(0)._1+" "+op.name+" "+rands(1)._1+")", rands(0)._2++rands(1)._2)
  }

  case object Prefix3 extends SqlOperatorArity { // sqlosure: 3
    // FIXME : Fehlende Klammern beabsichtigt ? - habe diese hinzugefügt
  override protected val randsSize = 3
    override protected val extraIsSet = true
    override protected def toSQLHelper(op : SqlOperator, rands : Seq[SQL.SqlReturn]) =
      ("("+rands(0)._1+" "+op.name+" "+rands(1)._1+" "+op.extra.get+" "+rands(2)._1+")", rands(0)._2++rands(1)._2++rands(2)._2)
  }
}