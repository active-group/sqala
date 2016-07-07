package de.ag.sqala

object SqlUtils {

  def putSpace : String = " "

  // TODO write tests + look at types + return print or String ?!

  // TODO types
  def putPaddingIfNonNull(lis: List[Any], proc: List[Any] => Any) : Option[Any] = {
    if(lis.isEmpty) None
    else Some(proc(lis))
  }

  // Statt nil use Option
  def defaultPutAlias(alias: Option[String]) : Option[String] = {
    if(alias.isDefined) Some(" AS "+alias.get)
    else None
  }

  def putDummyAlias(alias: Option[String]) : String =
    defaultPutAlias(alias).getOrElse("AS "+"1") // gensym TODO

  // TODO type value; check type correctness of value
  def putLiteral(typ: Type, value: Any) : (String, (Type, Any)) =
    ("?", (typ, value))

  def putAlias(alias: Option[String]) : Option[String] = defaultPutAlias(alias)

  def putSqlSelect(sel: SqlSelect) : String =  List().mkString(" ")

  def putJoiningInfix[A](lis: List[A], between: String, proc: (A) => String) : String =
    lis.map(proc).mkString(between)
}
