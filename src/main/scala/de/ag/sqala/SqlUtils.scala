package de.ag.sqala

object SqlUtils {

  def putSpace : String = " "

  // TODO write tests + look at types + return print or String ?!

  // TODO types - until now not needed
  def putPaddingIfNonNull(lis: List[Any], proc: List[Any] => Any) : Option[Any] = {
    if(lis.isEmpty) None
    else Some(proc(lis))
  }

  // Statt nil use Option
  def defaultPutAlias(alias: Option[String]) : String = alias match {
    case Some(al) => " AS "+al
    case None => ""
    /*if(alias.isDefined) " AS "+alias.get
    else
      throw new AssertionError("Alias is not defined!")*/
  }

  def putDummyAlias(alias: Option[String]) : String =
    defaultPutAlias(alias) // gensym TODO

  // TODO type value; check type correctness of value
  // z.B. check like typ.contains(value) - evt. done before
  def putLiteral(typ: Type, value: Any) : (String, Seq[(Type, Any)]) =
    ("?", Seq((typ, value)))

  def putAlias(alias: Option[String]) : Option[String] =
    alias.flatMap(x => Some(defaultPutAlias(Some(x))))

  def putSqlSelect(sel: SqlSelect) : String =  List().mkString(" ")

  def putJoiningInfix[A](lis: Seq[A], between: String, proc: (A) => String) : String =
    lis.map(proc).mkString(between)

  def putJoiningInfixWP[A](lis: Seq[A], between: String, proc: (A) => (String, Seq[(Type, Any)])) : PutSQL.SqlWithParams = {
    val tempSeq = lis.map(proc)
    Some((tempSeq.map(_._1).mkString(between), tempSeq.map(_._2).flatten))
  }

  def putJoiningInfixWPFix[A](lis: Seq[A], between: String, proc: (A) => (String, Seq[(Type, Any)])) : PutSQL.SqlWithParamsFix = {
    val tempSeq = lis.map(proc)
    (tempSeq.map(_._1).mkString(between), tempSeq.map(_._2).flatten)
  }

}
