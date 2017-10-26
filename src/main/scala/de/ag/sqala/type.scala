package de.ag.sqala

trait Type {
  def name: String
  def coerce(value: Any): Option[Any]
  def isNullable: Boolean
  def toNullable(): Type
  def toNonNullable(): Type
  def isNumeric: Boolean
  def isOrdered: Boolean
  def toSqlForm(value: Any) : String
}

class AtomicType(val name: String,
  val coercer: (Any) => Option[Any],
  val isNumeric: Boolean = false,
  val isOrdered: Boolean = false,
  val isNullable: Boolean = false,
  val sqlForm: Any => String = {x => x.toString})
    extends Type {

  def copy(name: String = name,
    coercer: (Any) => Option[Any] = coercer,
    isNumeric: Boolean = isNumeric,
    isOrdered: Boolean = isOrdered,
    isNullable: Boolean = isNullable,
    sqlForm: Any => String = sqlForm) =
    new AtomicType(name, coerce, isNumeric, isOrdered, isNullable, sqlForm)

  def coerce(value: Any) = this.coercer(value)
  def toNullable() = this.copy(isNullable=true)
  def toNonNullable() = this.copy(isNullable=false)
  def toSqlForm(value: Any) = sqlForm(value)

  // FIXME: stopgap measure
  override def equals(that: Any): Boolean =
    that match {
      case thatT: AtomicType => {
        (name == thatT.name) && (isNullable == thatT.isNullable)
      }
      case _ => false
    }
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

  override def coerce(value: Any): Option[Any] = ??? // FIXME

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

  override def coerce(value: Any): Option[Any] = ??? // FIXME

  override def toSqlForm(value: Any): String = ??? // FIXME
}

object Type {
  case object string extends AtomicType("string",
    { case s: String => Some(s)
      case _ => None },
    isNumeric = false, isOrdered = true,
    sqlForm = {v => "'"+v.toString+"'"})
  case object boolean extends AtomicType("boolean",
    { case b : Boolean => Some(b)
      case _ => None },
    isNumeric = false, isOrdered = false)
  case object integer extends AtomicType("integer",
    { case i: Int => Some(i.longValue)
      case l: Long => Some(l)
      case _ => None },
    isNumeric = true, isOrdered = true)
  def isDouble(x: Any): Boolean =
    x.isInstanceOf[Double]
  case object double extends AtomicType("double",
    { case d: Double => Some(d)
      case d: Float => Some(d.doubleValue)
      case _ => None },
    isNumeric = true, isOrdered = true)
  def product(componentTypes: Seq[Type]): Type = ProductType(componentTypes)
  def set(member: Type): Type = SetType(member)
}
