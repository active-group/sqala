package de.ag.sqala



object PutSQL { // TODO zusammenlegen mit SqlUtils

  /**
    * String : SQL-Statment
    * Seq[(Type, Any)] : (Datentyp, fester Wert) -> ersetzt später z.B. ein Fragezeichen
    */
  type SqlWithParams = (Option[String], Seq[(Type, Any)])
  type SqlWithParamsFix = (String, Seq[(Type, Any)])


  /* Interpretationsfunktionen aus sql_put.clj
    */

  def select(sqlSels: SqlInterpretations): SqlWithParams = ??? // ToDo needed ?

  def attributes(atts: Seq[(String, SqlExpression)]) : SqlWithParams = {
    if(atts.isEmpty)
      (Some("*"), Seq.empty)
    else {
      SqlUtils.putJoiningInfixWP[(String, SqlExpression)](atts, ", ", {
        case (col: String, sqlE: SqlExpressionColumn) if col == sqlE.name => (col, Seq.empty)
        case (col: String, sqlE: SqlExpression) => putColumnAnAlias(expression(sqlE), col)
      })
    }
  }

  def join(tables: Seq[(String, SqlInterpretations)], outerTables: Seq[(String, SqlInterpretations)]) : SqlWithParams = {
    val tempTabs = putTables(tables, ", ")
    val outer = putTables(outerTables, " ON (1=1) LEFT JOIN ")
    if(outerTables.isEmpty || tables.size == 1) {
      if(outerTables.isEmpty)
        (Some("FROM "+tempTabs._1), tempTabs._2)
      else
        (Some("FROM "+tempTabs._1+" LEFT JOIN "+outer._1), tempTabs._2++outer._2)
    } else {
      (Some(" FROM (SELECT * FROM "+tempTabs._1+") LEFT JOIN "+outer._1), tempTabs._2++outer._2)
    }
  }

  def expression(expr: SqlExpression) : SqlWithParamsFix = expr.toSQL

  def groupBy(grBy: Seq[String]) : SqlWithParams =
    (Some(SqlUtils.putJoiningInfix[String](grBy, ", ", {case x:String => x})), Seq.empty)


  /** */
  def putColumnAnAlias(expr: SqlWithParamsFix, alias: String): SqlWithParamsFix = expr match {
    case (sql: String, seqTyps: Seq[(Type, Any)]) =>
      (sql+SqlUtils.defaultPutAlias(Some(alias)), seqTyps)
  }

  def putTables(tables: Seq[(String, SqlInterpretations)], between: String) : SqlWithParamsFix =
    SqlUtils.putJoiningInfixWPFix[(String, SqlInterpretations)](tables, between, {
      case (alias: String, select: SqlTable) => putColumnAnAlias((select.name, Seq.empty), alias)
      case (alias: String, select: SqlInterpretations) => {
        val temp: SqlWithParamsFix = select.toSQL
        putColumnAnAlias(("("+temp._1+")", temp._2), alias)
      }
    })


}

trait SqlInterpretations {
  /**
    * wandelt die Strukturen in SQL-Statmentes um
    *
    * @return (SQL-Query, Seq[(Typ, Wert)]    : Als Seq wird der Datentyp und der Wert und die Variable übermittelt
    */
  def toSQL : PutSQL.SqlWithParamsFix
  def isExpression : Boolean
}
/*
  LIKE: Table, Select, Select-Combine, Select-Empty
 */

final case class SqlTable(
                         name: String,
                         schema: RelationalScheme
                         ) extends SqlInterpretations {
  override def toSQL : (String, Seq[(Type, Any)]) = ("SELECT * FROM "+name, Seq.empty)
  override def isExpression = false
}

final case class SqlSelect(
                            options: Option[Seq[String]], // like DISTINCT, ALL ...
                            attributes: Seq[(String, SqlExpression)], // column, expression
                            //nullary: Boolean, // FixME : was ist damit gemeint
                            tables: Seq[(String, SqlInterpretations)],
                            outerTables: Seq[(String, SqlInterpretations)],
                            criteria: Any, // where
                            outerCriteria: Any, // left join ... on
                            groupBy: Seq[String],
                            having: Any,
                            orderBy: Any
                            //extra: Any // ??? was ist da mit gemeint
                          ) extends SqlInterpretations {
  override def toSQL : PutSQL.SqlWithParamsFix = {
    val tempSeq : Seq[(Option[String], Seq[(Type, Any)])] = Seq(
      (Some("SELECT"), Seq.empty),
      (this.options.flatMap(x => Some(x.mkString(" "))), Seq.empty),
      PutSQL.attributes(attributes)
      // TODO (next) add join ...
    )
    val validSeqMember = tempSeq.filter(_._1.isDefined)
    (validSeqMember.map(_._1.get).mkString(" "), validSeqMember.map(_._2).flatten)
  }

  override def isExpression = false

  /*
  def putSqlJoin : Option[(String, Seq[(Type, Any)])] = {
    if(outerTables.isEmpty || tables.size == 1) {
      Some(("FROM "+,
        Seq.empty))
    } else {

    }
  }*/
}

sealed trait CombineOperation
object CombineOperation {
  case object Union extends CombineOperation
  case object Intersection extends CombineOperation
  case object Difference extends CombineOperation
}

final case class SqlSelectCombine(
                                 operation: CombineOperation,
                                 left: SqlSelect,
                                 right: SqlSelect
                                 )

final case class SqlSelectTable(
                               name: String
                               )

object SqlSelectEmpty

trait SqlExpression extends SqlInterpretations {
  override def toSQL : PutSQL.SqlWithParamsFix = ("", Seq.empty) // TODO : default-Wert entfernen, wenn untenstehende Expressions implementiert
  override def isExpression = true
}

case class SqlExpressionColumn(name: String) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix = (name, Seq.empty)
}
case class SqlExpressionApp(rator: SqlOperator, rands: Seq[SqlExpression]) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix = SqlOperator.toSQL(rator, rands.map(x => x.toSQL))
}
case class SqlExpressionConst(typ: Type, value: Any) extends SqlExpression {
  override def toSQL : PutSQL.SqlWithParamsFix = SqlUtils.putLiteral(typ, value)
}
case class SqlExpressionCase() extends SqlExpression
case class SqlExpressionExists(select: SqlSelect) extends SqlExpression
case class SqlExpressionTuple() extends SqlExpression
case class SqlExpressionSubquery(query: Query) extends SqlExpression

object SQL {
  // TODO add universe ?!
  def makeSqlTable(name: String, schema: RelationalScheme) : BaseRelation[SqlTable] =
    BaseRelation(name, schema, SqlTable(name, schema))

  def makeSqlSelect(attributes: Seq[(String, SqlExpression)], tables: Seq[(String, SqlInterpretations)]) : SqlSelect =
    SqlSelect(None, attributes, tables, Seq.empty,
      None, None,
      Seq.empty, None, None)

  def makeSqlSelect(options: Seq[String], attributes: Seq[(String, SqlExpression)], tables: Seq[(String, SqlInterpretations)]) : SqlSelect =
    SqlSelect(Some(options), attributes, tables, Seq.empty,
      None, None,
      Seq.empty, None, None)

  //def newSqlSelect: SqlSelect = SqlSelect(???)

  //def isSqlOrder
  // def isSqlCombineOp
}

sealed trait SqlOrder

object Ascending extends SqlOrder

object Descending extends SqlOrder



case class SqlOperator(name: String, arity: SqlOperatorArity, extra: Option[String] = None)

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