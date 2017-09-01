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

case class AtomicType(name: String,
                      predicate: (Any) => Boolean,
                      isNumeric: Boolean = false,
                      isOrdered: Boolean = false,
                      isNullable: Boolean = false,
                      sqlForm: Any => String = {x => x.toString})
    extends Type {
  def contains(value: Any) = predicate(value)
  def toNullable() = this.copy(isNullable=true)
  def toNonNullable() = this.copy(isNullable=false)
  def toSqlForm(value: Any) = sqlForm(value)
  // FIXME: equality?
}

// FIXME: flatten component types?

case class ProductType(componentTypes: Seq[Type]) extends Type {
  override def name: String =
    "tuple" // FIXME

  override def toNonNullable(): Type = this

  override def toNullable(): Type =
    ProductType(componentTypes.map(_.toNullable()))

  override def isNumeric: Boolean = false

  override def isOrdered: Boolean = false

  override def isNullable: Boolean = false

  override def contains(value: Any): Boolean = ??? // FIXME

  override def toSqlForm(value: Any): String = ??? // FIXME
}

case class SetType(member: Type) extends Type {
  override def name: String =
    "set" // FIXME

  override def toNonNullable(): Type = this

  override def toNullable(): Type = this

  override def isNumeric: Boolean = false

  override def isOrdered: Boolean = false

  override def isNullable: Boolean = false

  override def contains(value: Any): Boolean = ??? // FIXME

  override def toSqlForm(value: Any): String = ??? // FIXME
}

object Type {
  val string = AtomicType("string", _.isInstanceOf[String], isNumeric = false, isOrdered = true,
    sqlForm = {v => "'"+v.toString+"'"})
  val boolean = AtomicType("boolean", _.isInstanceOf[Boolean], isNumeric = false, isOrdered = false)
  def isInteger(x: Any): Boolean =
    x.isInstanceOf[Integer] || x.isInstanceOf[Long]
  val integer = AtomicType("integer", isInteger, isNumeric = true, isOrdered = true)
  def product(componentTypes: Seq[Type]): Type = ProductType(componentTypes)
  def set(member: Type): Type = SetType(member)
}
