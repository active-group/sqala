package de.ag.sqala


object SQL {
  /** Tupel aus (String, Seq[(Type, Any)])
    * String : SQL-Statement
    * Seq[(Type, Any)] : (Datentyp, fester Wert) -> ersetzt später z.B. ein Fragezeichen
    */
  type ReturnOption = Option[(String, Seq[(Type, Any)])]
  type Return = (String, Seq[(Type, Any)])



  // TODO add universe ?!
  /*def makeSQLTable(name: String, schema: RelationalScheme) : BaseRelation[SQLSelectTable] =
    BaseRelation(name, schema, SQLSelectTable(name, schema))*/

  def makeSQLSelect(attributes: Seq[(String, SQLExpression)], tables: Seq[(Option[String], SQL)]) : SQLSelect =
    SQLSelect(None, attributes, tables, Seq.empty,
      Seq.empty, Seq.empty,
      None, None, None, None)

  def makeSQLSelect(options: Seq[String], attributes: Seq[(String, SQLExpression)], tables: Seq[(Option[String], SQL)]) : SQLSelect =
    SQLSelect(Some(options), attributes, tables, Seq.empty,
      Seq.empty, Seq.empty,
      None, None, None, None)



  /**
    * Functions for translation into SQL
    */
  def attributes(atts: Seq[(String, SQLExpression)]) : ReturnOption = {
    if(atts.isEmpty)
      Some(("*", Seq.empty))
    else {
      SQLUtils.putJoiningInfixOption[(String, SQLExpression)](atts, ", ", {
        case (col: String, sqlE: SQLExpressionColumn) if col == sqlE.name => (col, Seq.empty)
        case (col: String, sqlE: SQLExpression) => SQLUtils.putColumnAnAlias(expression(sqlE), Some(col))
      })
    }
  }


  // FixMe : Zur Info: Weiche hier von dem orginal SQLosure ab, da die restricts hier direkt dazu implementiert werden!
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
  def join(tables: Seq[(Option[String], SQL)], outerTables: Seq[(Option[String], SQL)],
           outerCriteria: Seq[SQLExpression]) : ReturnOption = {
    val tempTabs = putTables(tables, ", ")
    Some(SQLUtils.surroundSQL("FROM ",
      SQLUtils.concatSQL(Seq(
        {if(outerTables.isEmpty || tables.size == 1)
          tempTabs
        else SQLUtils.surroundSQL("(SELECT * FROM ", tempTabs, ")")},
        {if(outerTables.isEmpty)
          ("", Seq.empty)
        else
          SQLUtils.surroundSQL(" LEFT JOIN ",
            SQLUtils.concatSQL(Seq(
              putTables(outerTables, " ON (1=1) LEFT JOIN "), // TODO : kann zu Fehlern führen, siehe Kommentar in SQLUtilsTests - wurde so von SQLosure übernommen
              SQLUtils.surroundSQL(" ON ", // add last ON with all JOIN Criterias
                conditions(outerCriteria),
                ""))),
            "")}
      )),
      ""))
  }

  def putTables(tables: Seq[(Option[String], SQL)], between: String) : Return =
    SQLUtils.putJoiningInfix[(Option[String], SQL)](tables, between, {
      case (alias: Option[String], select: SQLSelectTable) => SQLUtils.putColumnAnAlias((select.name, Seq.empty), alias)
      case (alias: Option[String], select: SQL) => {
        val temp: Return = select.toSQLText
        SQLUtils.putColumnAnAlias(("("+temp._1+")", temp._2), alias)
      }
    })


  def criterias(crit: Seq[SQLExpression]) : ReturnOption =
    SQLUtils.putPaddingIfNonNull(crit, conditions, "WHERE ")

  def groupBy(grBy: Option[Seq[String]]) : ReturnOption =
    grBy.flatMap(g => SQLUtils.putPaddingIfNonNull[String](g,
      {case gg: Seq[String] => SQLUtils.putJoiningInfix[String](gg, ", ", { case x: String => (x, Seq.empty) })},
      "GROUP BY "))

  def having(hav: Option[Seq[SQLExpression]]) : ReturnOption =
    hav.flatMap(h => SQLUtils.putPaddingIfNonNull[SQLExpression](h, e => SQLExpressionAnd(e).toSQL,
      "HAVING "))

  def orderBy(ordBy: Option[Seq[(String, SQLOrder)]]) : ReturnOption =
    ordBy.flatMap(o => SQLUtils.putPaddingIfNonNull[(String, SQLOrder)](o,
      tup => SQLUtils.putJoiningInfix[(String, SQLOrder)](tup, ", ",
          {case (s: String, ord: SQLOrder) => ord.toSQL(s)}),
      "ORDER BY "))

  def extra(v: Option[Seq[String]]) : ReturnOption =
    v.flatMap(sq => SQLUtils.putPaddingIfNonNull[String](sq, s => (s.mkString(" "), Seq.empty), ""))

  // Collection of Conditions
  def conditions(exprs: Seq[SQLExpression]) : Return =
    SQLExpressionAnd(exprs).toSQL // default concat via AND

  // Translate a Expression
  def expression(expr: SQLExpression) : Return =
    expr.toSQL // can translate itself to SQL

  def fromQuery(q: Query): SQL = {
    q match {
      case BaseRelation(name, scheme, handle) => SQLSelectTable(name, scheme)
      case EmptyQuery => SQLSelect.empty
      case Projection(alist, query)  => {
        val qSQL = fromQuery(query)
        val projAlist : Seq[(String, SQLExpression)] = alist.map {case (s, e) => (s, SQLExpression.fromExpression(e)) }
        SQL.makeSQLSelect(projAlist, Seq((None, fromQuery(query)))) // FixMe : None not the best Implementation!!
      }
      case Restriction(exp, query) => {
        val select = toSQLSelect(fromQuery(query))
        if (exp.isAggregate)
          // note that having is empty as a result of toSQLSelect
          select.copy(having = Some(Seq(SQLExpression.fromExpression(exp))))
        else
          // ditto for criteria
          select.copy(criteria = Seq(SQLExpression.fromExpression(exp)))
      }
      case Product(query1, query2) => {
        val sql1 = fromQuery(query1)
        val sql2 = fromQuery(query2)
        sql1 match {
          case sel1: SQLSelect if (sel1.attributes.isEmpty) => sel1.addTable(sql2)
          case _ =>
            sql2 match {
              case sel2: SQLSelect if (sel2.attributes.isEmpty) => sel2.addTable(sql1)
              case _ =>
                SQLSelect.make(tables = Seq((None, sql1), (None, sql2)))
            }
        }
      }
    }
  }

  def toSQLSelect(thing: SQL): SQLSelect = {
    thing match {
      case SQLSelectEmpty => SQLSelect.empty
      case SQLSelectTable(name, schema) =>
        SQLSelect.make(tables = Seq((None, thing)))
      case _: SQLSelectCombine =>
        SQLSelect.make(tables = Seq((None, thing)))
      case sel: SQLSelect => { // FIXME: nicer way?
        if (sel.attributes.isEmpty)
          sel
        else if (sel.groupBy.isDefined)
          SQLSelect.make(tables = Seq((None, thing)))
        else
          SQLSelect.make(tables = Seq((None, sel.copy(groupBy = None))),
            groupBy = sel.groupBy)
      }
    }
  }
}


sealed trait SQL {
  /**
    * wandelt die Strukturen in SQL-Statmentes um
    *
    * @return (SQL-Query, Seq[(Typ, Wert)]    : Als Seq wird der Datentyp und der Wert und die Variable übermittelt
    */
  def toSQLText: SQL.Return
}


final case class SQLSelectTable(
                                 name: String,
                                 schema: RelationalScheme
                               ) extends SQL {
  override def toSQLText: SQL.Return = ("SELECT * FROM "+name, Seq.empty)
}

final case class SQLSelect(
                            options: Option[Seq[String]], // like DISTINCT, ALL ...
                            attributes: Seq[(String, SQLExpression)], // column, expression
                            //nullary: Boolean, // TODO : was ist damit gemeint ?
                            tables: Seq[(Option[String], SQL)],
                            outerTables: Seq[(Option[String], SQL)],
                            criteria: Seq[SQLExpression], // WHERE ...
                            outerCriteria: Seq[SQLExpression], // left join ... on ...
                            groupBy: Option[Seq[String]],
                          // FixMe - groupBy : statt SQLExpression nur String - da nur Column zulässig wäre
                            having: Option[Seq[SQLExpression]],
                            // FixMe - having : evt Aggregation einschränken - bzw. diese nur hier und nicht in Criteria zulassen ?!
                            orderBy: Option[Seq[(String, SQLOrder)]],
                          // FixMe - orderBy : statt SQLExpression nur String - da nur die Column zulässig wäre (und nicht andere Expressions)!
                          // FixMe groupBy, having, orderBy werden bei leeren Seq falsch ausgewertet -> soll abgefangen werden?
                            extra: Option[Seq[String]] // TODO: -> LIMIT, TOP   ...
                          ) extends SQL {
  override def toSQLText: SQL.Return = {
    val tempSeq : Seq[SQL.ReturnOption] = Seq(
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
    SQLUtils.putJoiningInfix[SQL.ReturnOption](tempSeq.filter(_.isDefined), " ",
      {x => x.get})
  }

  def addTable(sql: SQL): SQLSelect = {
    val p = (None, sql) // avoid compiler warning
    this.copy(tables = tables :+ p)
  }
}

object SQLSelect {
  def make(options: Option[Seq[String]] = None, // like DISTINCT, ALL ...
    attributes: Seq[(String, SQLExpression)] = Seq.empty, // column, expression
                                              // nullary: Boolean, // TODO : was ist damit gemeint ?
    tables: Seq[(Option[String], SQL)] = Seq.empty,
    outerTables: Seq[(Option[String], SQL)] = Seq.empty,
    criteria: Seq[SQLExpression] = Seq.empty, // WHERE ...
    outerCriteria: Seq[SQLExpression] = Seq.empty, // left join ... on ...
    groupBy: Option[Seq[String]] = None,
    // FixMe - groupBy : statt SQLExpression nur String - da nur Column zulässig wäre
    having: Option[Seq[SQLExpression]] = None,
    // FixMe - having : evt Aggregation einschränken - bzw. diese nur hier und nicht in Criteria zulassen ?!
    orderBy: Option[Seq[(String, SQLOrder)]] = None,
    // FixMe - orderBy : statt SQLExpression nur String - da nur die Column zulässig wäre (und nicht andere Expressions)!
    // FixMe groupBy, having, orderBy werden bei leeren Seq falsch ausgewertet -> soll abgefangen werden?
    extra: Option[Seq[String]] = None) = // TODO: -> LIMIT, TOP   ...
    SQLSelect(options, attributes, tables, outerTables, criteria, outerCriteria, groupBy, having, orderBy, extra)


  val empty = make()
}

/**
  * CombineOperation like UNION, INTERSECT ...
  */

final case class SQLSelectCombine(
                                   operation: SQLCombineOperator,
                                   left: SQL,
                                   right: SQL
                                 ) extends SQL {
  override def toSQLText: SQL.Return =
    operation.toSQLText(left.toSQLText, right.toSQLText)
}

sealed trait SQLCombineOperator {
  def toSQLText(left: SQL.Return, right: SQL.Return) : SQL.Return =
    SQLUtils.concatSQL(Seq(
      SQLUtils.surroundSQL("(", left, ") "+getOpName),
      SQLUtils.surroundSQL(" (", right, ")")))
  protected val getOpName : String
}

object SQLCombineOperator {
  case object Union extends SQLCombineOperator {
    protected val getOpName = "UNION"
  }
  case object Intersection extends SQLCombineOperator {
    protected val getOpName = "INTERSECT"
  }
  case object Difference extends SQLCombineOperator {
    protected val getOpName = "EXCEPT"
  }
}








object SQLSelectEmpty extends SQL {
  override def toSQLText: SQL.Return = ("", Seq.empty)
}




object SQLExpression {
  def fromExpression(exp: Expression): SQLExpression = {
    exp match {
      case AttributeRef(name) => SQLExpressionColumn(name)
      case c: Const => SQLExpressionConst(c.ty, c.value)
      case Null(ty) => SQLExpressionNull
      case Application(rator, rands) =>
        rator match {
          case has: HasSQLOperator =>
            SQLExpressionApp(has.sqlOperator, rands.map(fromExpression(_)))
        }
      case Tuple(exprs) => SQLExpressionTuple(exprs.map(fromExpression(_)))
      case Aggregation(op, exp) =>
        SQLExpressionApp(op match {
          case AggregationOp.Count   => SQLOperator.count
          case AggregationOp.Sum     => SQLOperator.sum
          case AggregationOp.Avg     => SQLOperator.avg
          case AggregationOp.Min     => SQLOperator.min
          case AggregationOp.Max     => SQLOperator.max
          case AggregationOp.StdDev  => SQLOperator.stdDev
          case AggregationOp.StdDevP => SQLOperator.stdDevP
          case AggregationOp.Var     => SQLOperator.vari
          case AggregationOp.VarP    => SQLOperator.varP
        }, Seq(fromExpression(exp)))
      case AggregationAll(op) =>
        SQLExpressionApp(op match {
          case AggregationAllOp.CountAll => SQLOperator.countAll
        }, Seq())
      case Case(alist, default) =>
        SQLExpressionCase(None,
          alist.map { case (e1, e2) => (fromExpression(e1), fromExpression(e2)) }, 
          Some(fromExpression(default)))
      case ScalarSubquery(query) => SQLExpressionSubquery(SQL.fromQuery(query))
      case SetSubquery(query) => SQLExpressionSubquery(SQL.fromQuery(query))
    }
  }
}


trait SQLExpression { // kein SQL, da nicht eigenständig ausführbar
  def toSQL : SQL.Return
}

case class SQLExpressionColumn(name: String) extends SQLExpression {
  override def toSQL : SQL.Return = (name, Seq.empty)
}
case class SQLExpressionApp(rator: SQLOperator,
                            rands: Seq[SQLExpression]) extends SQLExpression {
  override def toSQL : SQL.Return = SQLOperator.toSQL(rator, rands.map(x => x.toSQL))
}
case class SQLExpressionConst(typ: Type, value: Any) extends SQLExpression {
  override def toSQL : SQL.Return = SQLUtils.putLiteral(typ, value)
}
object SQLExpressionNull extends SQLExpression { // FixMe : ok?
  override def toSQL : SQL.Return = ("null", Seq.empty)
}
case class SQLExpressionCase(input: Option[SQLExpression],
                             branches: Seq[(SQLExpression, SQLExpression)],
                             default: Option[SQLExpression]) extends SQLExpression {
  override def toSQL : SQL.Return = {
    if(branches.isEmpty)
      throw new AssertionError("Invalid branche-size (=0) in SQLExpressionCase")
    else
      SQLUtils.surroundSQL("(CASE ",
        SQLUtils.concatSQL(Seq(
          // optional value behinde CASE
          {if(input.isDefined)
            SQLUtils.surroundSQL("", input.get.toSQL, " ")
          else ("", Seq.empty)},
          // WHEN ... THEN ... - part
          SQLUtils.putJoiningInfix[(SQLExpression, SQLExpression)](branches, " ", {
            case (exWhen: SQLExpression, exThen: SQLExpression) => {
              val sqlWhen : SQL.Return = exWhen.toSQL
              val sqlThen : SQL.Return = exThen.toSQL
              ("WHEN "+sqlWhen._1+" THEN "+sqlThen._1, sqlWhen._2++sqlThen._2)
            }}),
          // optional default
          {if(default.isDefined)
            SQLUtils.surroundSQL(" ELSE ", default.get.toSQL, "")
          else ("", Seq.empty)}
        )),
      " END)")
  }
}
case class SQLExpressionExists(select: SQL) extends SQLExpression {
  override def toSQL : SQL.Return =
    SQLUtils.surroundSQL("EXISTS (", select.toSQLText, ")")
}
case class SQLExpressionTuple(expressions: Seq[SQLExpression]) extends SQLExpression {
  override def toSQL : SQL.Return =
    SQLUtils.surroundSQL("(", SQLUtils.putJoiningInfix[SQLExpression](expressions, ", ", {
      e: SQLExpression => e.toSQL
    }), ")")
}
case class SQLExpressionSubquery(query: SQL) extends SQLExpression {
  override def toSQL : SQL.Return =
    SQLUtils.surroundSQL("(", query.toSQLText, ")")
}

// AND & OR
// also implemented in SQLExpressionApp - but there can only be
case class SQLExpressionOr(exprs: Seq[SQLExpression]) extends SQLExpression {
  override def toSQL : SQL.Return = {
    if (exprs.size == 1)
      exprs(0).toSQL
    else
      SQLUtils.surroundSQL("(", SQLUtils.putJoiningInfix[SQLExpression](exprs, " OR ", { e => e.toSQL }), ")")
  }
}
case class SQLExpressionAnd(exprs: Seq[SQLExpression]) extends SQLExpression {
  override def toSQL : SQL.Return = {
    if (exprs.size == 1)
      exprs(0).toSQL
    else
      SQLUtils.surroundSQL("(", SQLUtils.putJoiningInfix[SQLExpression](exprs, " AND ", { e => e.toSQL }), ")")
  }
}






/**
  * SQL OrderBy Objects
  */
sealed trait SQLOrder {
  def toSQL(col: String) : SQL.Return =
    (col+" "+toSQLHelper, Seq.empty)

  val toSQLHelper : String
}

object SQLOrderAscending extends SQLOrder {
  override val toSQLHelper : String = "ASC"
}

object SQLOrderDescending extends SQLOrder {
  override val toSQLHelper : String = "DESC"
}





/**
  * A SQLOperator is e.g. a equality-check in the WHERE-clause
 *
  * @param name   the character/-s which used in sql
  * @param arity  the arity defined in SQLOperatorArity
  * @param extra  a optional extra String, which is needed in some Arities (like BETWEEN ... 'AND' ...)
  *               BETWEEN is the name an AND is defined in EXTRA, so the arity can be used for other operators like this
  */

case class SQLOperator(name: String,
                       arity: SQLOperatorArity,
                       extra: Option[String] = None)

object SQLOperator {
  val eq : SQLOperator = SQLOperator("=", SQLOperatorArity.Infix)
  val gt : SQLOperator = SQLOperator(">", SQLOperatorArity.Infix)
  val lt : SQLOperator = SQLOperator("<", SQLOperatorArity.Infix)
  val geq : SQLOperator = SQLOperator(">=", SQLOperatorArity.Infix)
  val leq : SQLOperator = SQLOperator("<=", SQLOperatorArity.Infix)
  val neq : SQLOperator = SQLOperator("<>", SQLOperatorArity.Infix)
  val neq2 : SQLOperator = SQLOperator("!=", SQLOperatorArity.Infix)
  val and : SQLOperator = SQLOperator("AND", SQLOperatorArity.Infix)
  val or : SQLOperator = SQLOperator("OR", SQLOperatorArity.Infix)
  val like : SQLOperator = SQLOperator("LIKE", SQLOperatorArity.Infix)
  val in : SQLOperator = SQLOperator("IN", SQLOperatorArity.Infix)
  val between : SQLOperator = SQLOperator("BETWEEN", SQLOperatorArity.Prefix3, Some("AND"))
  val cat : SQLOperator = SQLOperator("CAT", SQLOperatorArity.Infix)
  val plus : SQLOperator = SQLOperator("+", SQLOperatorArity.Infix)
  val minus : SQLOperator = SQLOperator("-", SQLOperatorArity.Infix)
  val mult : SQLOperator = SQLOperator("*", SQLOperatorArity.Infix)

  val div : SQLOperator = SQLOperator("/", SQLOperatorArity.Infix)
  val mod : SQLOperator = SQLOperator("/", SQLOperatorArity.Infix)
  val bitNot : SQLOperator = SQLOperator("~", SQLOperatorArity.Prefix)
  val bitAnd : SQLOperator = SQLOperator("&", SQLOperatorArity.Infix)
  val bitOr : SQLOperator = SQLOperator("|", SQLOperatorArity.Infix)
  val bitXor : SQLOperator = SQLOperator("^", SQLOperatorArity.Infix)
  val asg : SQLOperator = SQLOperator("=", SQLOperatorArity.Infix)

  val concat : SQLOperator = SQLOperator("CONCAT", SQLOperatorArity.Prefix2, Some(","))
  val lower : SQLOperator = SQLOperator("LOWER", SQLOperatorArity.Prefix)
  val upper : SQLOperator = SQLOperator("UPPER", SQLOperatorArity.Prefix)

  val not : SQLOperator = SQLOperator("NOT", SQLOperatorArity.Prefix)
  val isNull : SQLOperator = SQLOperator("IS NULL", SQLOperatorArity.Postfix)
  val isNotNull : SQLOperator = SQLOperator("IS NOT NULL", SQLOperatorArity.Postfix)
  val length : SQLOperator = SQLOperator("LENGTH", SQLOperatorArity.Prefix)

  // FIXME : Aggregations - separat definieren ??
  val count : SQLOperator = SQLOperator("COUNT", SQLOperatorArity.Prefix)
  val countAll : SQLOperator = SQLOperator("COUNT", SQLOperatorArity.Prefix)
  val sum : SQLOperator = SQLOperator("SUM", SQLOperatorArity.Prefix)
  val avg : SQLOperator = SQLOperator("AVG", SQLOperatorArity.Prefix)
  val min : SQLOperator = SQLOperator("MIN", SQLOperatorArity.Prefix)
  val max : SQLOperator = SQLOperator("MAX", SQLOperatorArity.Prefix)
  val stdDev : SQLOperator = SQLOperator("StdDev", SQLOperatorArity.Prefix)
  val stdDevP : SQLOperator = SQLOperator("StdDevP", SQLOperatorArity.Prefix)
  val vari : SQLOperator = SQLOperator("Var", SQLOperatorArity.Prefix)
  val varP : SQLOperator = SQLOperator("VarP", SQLOperatorArity.Prefix)
  // ...

  def toSQL(rator : SQLOperator, rands: Seq[SQL.Return]) : SQL.Return =
    rator.arity.toSQL(rator, rands)
}

trait HasSQLOperator {
  val sqlOperator: SQLOperator
}

/**
  * Arity for the SQLOperators
  */

trait SQLOperatorArity {
  /**
    * übergebene Liste enthält geforderte Werte, z.B. was links und rechts vom Infix-Operator steht
    * Werte werden aufgrund der angegebenen randSize auf die korrekte Anzahl überprüft und dann korrekt angeordnet
    */
  protected val randsSize : Int
  protected val extraIsSet : Boolean = false
  def toSQL(op: SQLOperator, rands: Seq[SQL.Return]) = {
    if(rands.size != randsSize)
      throw new AssertionError("Invalid arity. Expected for "+op+" arity of "+randsSize+" but getting "+rands.size)
    else if(extraIsSet && op.extra.isEmpty)
      throw new AssertionError("Extrainformation is missing. There is an extra Information/String in the SQLOperator ("+op+") missing für the choosen Arity.")
    else
      toSQLHelper(op, rands)
  }
  protected def toSQLHelper(op : SQLOperator, rands : Seq[SQL.Return]) : SQL.Return
}
object SQLOperatorArity {

  case object Postfix extends SQLOperatorArity { // sqlosure: -1
    override protected val randsSize = 1
    override protected def toSQLHelper(op : SQLOperator, rands : Seq[SQL.Return]) =
      ("("+rands(0)._1+") "+op.name, rands(0)._2)
  }

  case object Prefix extends SQLOperatorArity { // sqlosure: 1
    override protected val randsSize = 1
    override protected def toSQLHelper(op : SQLOperator, rands : Seq[SQL.Return]) =
      (op.name+"("+rands(0)._1+")", rands(0)._2)
  }

  case object Prefix2 extends SQLOperatorArity { // sqlosure: -2
    override protected val randsSize = 2
    override protected val extraIsSet = true
    override protected def toSQLHelper(op : SQLOperator, rands : Seq[SQL.Return]) =
      (op.name+"("+rands(0)._1+op.extra.get+rands(1)._1+")", rands(0)._2++rands(1)._2)
  }

  case object Infix extends SQLOperatorArity { // sqlosure: 2
    override protected val randsSize = 2
    override protected def toSQLHelper(op : SQLOperator, rands : Seq[SQL.Return]) =
      ("("+rands(0)._1+" "+op.name+" "+rands(1)._1+")", rands(0)._2++rands(1)._2)
  }

  case object Prefix3 extends SQLOperatorArity { // sqlosure: 3
    // FIXME : Fehlende Klammern beabsichtigt ? - habe diese hinzugefügt
  override protected val randsSize = 3
    override protected val extraIsSet = true
    override protected def toSQLHelper(op : SQLOperator, rands : Seq[SQL.Return]) =
      ("("+rands(0)._1+" "+op.name+" "+rands(1)._1+" "+op.extra.get+" "+rands(2)._1+")", rands(0)._2++rands(1)._2++rands(2)._2)
  }
}
