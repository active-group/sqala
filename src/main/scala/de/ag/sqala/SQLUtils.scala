package de.ag.sqala

object SQLUtils {

  def putSpace : String = " "

  // TODO write tests + look at types + return print or String ?!

  // TODO types - until now not needed
  def putPaddingIfNonNull[A](lis: Seq[A], proc: Seq[A] => SQL.Return, group: String) : SQL.ReturnOption = {
    if(lis.isEmpty) None
    else Some(surroundSQL(group, proc(lis), ""))
  }

  /**
    * Work with Alias
    *   like column AS alias
    */
  def defaultPutAlias(alias: Option[String]): String = alias match {
    case Some(al) => " AS "+al
    case None => ""
  }

  def indexedPutAlias(alias: Option[String], idx: Int) : String = alias match {
    case Some(al) => " AS "+al
    case None => s" AS __dummy$idx"
  }

  // Note: also used for table aliases.
  def putColumnAnAlias(expr: SQL.Return, alias: Option[String]): SQL.Return = {
    val (sql: String, seqTyps: Seq[(Type, Any)]) = expr
    val al = SQLUtils.defaultPutAlias(alias)
    (sql+al, seqTyps)
  }

  def putTableAnAlias(expr: SQL.Return, alias: Option[String], idx: Int): SQL.Return = {
    val (sql: String, seqTyps: Seq[(Type, Any)]) = expr
    val al = SQLUtils.indexedPutAlias(alias, idx)
    (sql+al, seqTyps)
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
  def putJoiningInfix[A, B](lis: Seq[A], between: String)(proc: (A) => (String, Seq[B])) : (String, Seq[B]) = {
    val tempSeq = lis.map(proc)
    (tempSeq.map(_._1).mkString(between), tempSeq.map(_._2).flatten)
  }

  /**
    * Helper-functions like:
    *   surround: set given Strings before and after the SQL-Statement
    *   concat: concat the SQL-Statements and Literals
    */
  def surroundSQL[B](before: String, sql : (String, B), after: String) : (String, B) =
    (before+sql._1+after, sql._2)

  def concatSQL[B](sqls : Seq[(String, Seq[B])]) : (String, Seq[B]) =
    (sqls.map(x => x._1).mkString(""), sqls.map(x => x._2).flatten)

}
