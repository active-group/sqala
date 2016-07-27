package de.ag.sqala

object SqlUtils {

  def putSpace : String = " "

  // TODO write tests + look at types + return print or String ?!

  // TODO types - until now not needed
  def putPaddingIfNonNull[A](lis: Seq[A], proc: Seq[A] => SQL.SqlReturn, group: String) : SQL.SqlReturnOption = {
    if(lis.isEmpty) None
    else Some(surroundSQL(group, proc(lis), ""))
  }

  /**
    * Work with Alias
    *   like column AS alias
    */
  // Statt nil use Option
  def defaultPutAlias(alias: Option[String]) : String = alias match {
    case Some(al) => " AS "+al
    case None => ""
  }

  def putAlias(alias: Option[String]) : Option[String] =
    alias.flatMap(x => Some(defaultPutAlias(Some(x))))

  def putDummyAlias(alias: Option[String]) : String =
    defaultPutAlias(alias) // gensym TODO

  def putColumnAnAlias(expr: SQL.SqlReturn, alias: Option[String]): SQL.SqlReturn = expr match {
    case (sql: String, seqTyps: Seq[(Type, Any)]) =>
      (sql+SqlUtils.defaultPutAlias(alias), seqTyps)
  }


  /**
    * Set Literal
    *   -> Values which are replaced '?' in SQL
    */
  // TODO type value; check type correctness of value ??
  // z.B. check like typ.contains(value) - evt. done before
  def putLiteral(typ: Type, value: Any) : (String, Seq[(Type, Any)]) =
    ("?", Seq((typ, value)))


  /**
    * execute the procedure on every element of the sequence, then concat them and set the 'between'-String between them
    */
  def putJoiningInfixOption[A](lis: Seq[A], between: String, proc: (A) => (String, Seq[(Type, Any)])) : SQL.SqlReturnOption =
    Some(putJoiningInfix(lis, between, proc))

  def putJoiningInfix[A](lis: Seq[A], between: String, proc: (A) => (String, Seq[(Type, Any)])) : SQL.SqlReturn = {
    val tempSeq = lis.map(proc)
    (tempSeq.map(_._1).mkString(between), tempSeq.map(_._2).flatten)
  }

  /**
    * Helper-functions like:
    *   surround: set given Strings before and after the SQL-Statement
    *   concat: concat the Sql-Statements and Literals
    */
  def surroundSQL(before: String, sql : SQL.SqlReturn, after: String) : SQL.SqlReturn =
    (before+sql._1+after, sql._2)

  def concatSQL(sqls : Seq[SQL.SqlReturn]) : SQL.SqlReturn =
    (sqls.map(x => x._1).mkString(""), sqls.map(x => x._2).flatten)

}
