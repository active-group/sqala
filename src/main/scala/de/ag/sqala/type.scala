package de.ag.sqala

trait Type {
  def name: String
  def contains(value: Any): Boolean
  def isNullable: Boolean
  def toNullable(): Type
  def toNonNullable(): Type
  def isNumeric: Boolean
  def isOrdered: Boolean
}

case class AtomicType(name: String,
                      predicate: (Any) => Boolean,
                      isNumeric: Boolean = false,
                      isOrdered: Boolean = false,
                      isNullable: Boolean = false)
    extends Type {
  def contains(value: Any) = predicate(value)
  def toNullable() = this.copy(isNullable=true)
  def toNonNullable() = this.copy(isNullable=false)
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
}

object Type {
  val string = new AtomicType("string", _.isInstanceOf[String], isNumeric = false, isOrdered = true)
  val boolean = new AtomicType("boolean", _.isInstanceOf[Boolean], isNumeric = false, isOrdered = false)
  def isInteger(x: Any): Boolean =
    x.isInstanceOf[Integer] || x.isInstanceOf[Long]
  val integer = new AtomicType("integer", isInteger, isNumeric = true, isOrdered = true)
  def product(componentTypes: Seq[Type]): Type = ProductType(componentTypes)
  def set(member: Type): Type = SetType(member)
}
