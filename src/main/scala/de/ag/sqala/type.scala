package de.ag.sqala

trait Type {
  def name: String
  def contains(value: Any): Boolean
  def isNullable: Boolean
  def toNullable(): Type
  def toNonNullable(): Type
  def isNumeric: Boolean
  def isOrdered: Boolean
  def toSqlForm(value: Any) : String
}

case class AtomicType(val name: String,
                      predicate: (Any) => Boolean,
                      val isNumeric: Boolean = false,
                      val isOrdered: Boolean = false,
                      val isNullable: Boolean = false,
                      val sqlForm: Any => String = {x => x.toString})
    extends Type {
  def contains(value: Any) = predicate(value)
  def toNullable() = this.copy(isNullable=true)
  def toNonNullable() = this.copy(isNullable=false)
  def toSqlForm(value: Any) = sqlForm(value)
  // FIXME: equality?
}

object Type {
  val string = new AtomicType("string", _.isInstanceOf[String], isNumeric = false, isOrdered = true,
    sqlForm = {v => "'"+v.toString+"'"})
  val boolean = new AtomicType("boolean", _.isInstanceOf[Boolean], isNumeric = false, isOrdered = false)
  def isInteger(x: Any): Boolean =
    x.isInstanceOf[Integer] || x.isInstanceOf[Long]
  val integer = new AtomicType("integer", isInteger, isNumeric = true, isOrdered = true)
}
