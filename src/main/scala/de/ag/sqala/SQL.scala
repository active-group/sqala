package de.ag.sqala

final case class SqlTable(
                         name: String,
                         schema: RelationalScheme
                         )

final case class SqlSelect(
                          options: Option[List[String]], // like DISTINCT, ALL ...
                          attributes: List[(String, SqlExpression)], // column, expression
                          nullary: Boolean,
                          tables: List[(String, SqlTable)],
                          outerTables: Any,
                          criteria: Any,
                          outerCriteria: Any,
                          groupBy: Seq[String],
                          having: Any,
                          orderBy: Any,
                          extra: Any
                          ) {
  def toSQL : (String, Seq[(Type, Any)]) = {
    val tempSeq : Seq[(Type, Any)] = Seq.empty
    val tempSql = List[Option[String]](
      Some("SELECT"),
      // options
      this.options.flatMap(x => Some(x.mkString(" "))),
      // attributes
      {if(this.attributes.isEmpty) Some("*")
      else
        { Some(SqlUtils.putJoiningInfix[(String,SqlExpression)](this.attributes, ", ",
          {case ((col: String, expr: SqlExpression)) => {
            if(expr.isInstanceOf[SqlExpressionColumn] && col == expr.asInstanceOf[SqlExpressionColumn].name) col
            else {
              val erg = expr.toSql
              tempSeq ++ erg._2
              SqlUtils.defaultPutAlias(Some(erg._1)).getOrElse(throw new AssertionError("not parsable"))
            }}})) // ToDo not that good way - I'm working on it

        }
      }, // -> simples toSql in Expressions !
      // ...
      Some("")
      // TODO weitermachen
    ).filter(_.isDefined).mkString(" ")
    (tempSql, tempSeq)
  }
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

trait SqlExpression {
  def toSql : (String, Seq[(Type, Any)]) = ("", Seq.empty)
}

case class SqlExpressionColumn(name: String) extends SqlExpression {
  override def toSql : (String, Seq[(Type, Any)]) = (name, Seq.empty)
}
case class SqlExpressionApp(rator: Any, rands: Any) extends SqlExpression
case class SqlExpressionConst(typ: Type, value: Any) extends SqlExpression {

}
case class SqlExpressionCase() extends SqlExpression
case class SqlExpressionExists(select: SqlSelect) extends SqlExpression
case class SqlExpressionTuple() extends SqlExpression
case class SqlExpressionSubquery(query: Query) extends SqlExpression

object SQL {
  // TODO add universe ?!
  def makeSqlTable(name: String, schema: RelationalScheme) : BaseRelation[SqlTable] =
    BaseRelation(name, schema, SqlTable(name, schema))

  //def newSqlSelect: SqlSelect = SqlSelect(???)

  //def isSqlOrder
  // def isSqlCombineOp
}

sealed trait SqlOrder

object Ascending extends SqlOrder

object Descending extends SqlOrder



case class SqlOperator(name: String, arity: Any)

object SqlOperator {
  def gleich : SqlOperator = SqlOperator("=", 2)
  // ...
}