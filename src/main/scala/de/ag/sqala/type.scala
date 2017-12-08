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
  val getNullable: () => Type,
  val coercer: (Any) => Option[Any],
  val isNumeric: Boolean = false,
  val isOrdered: Boolean = false,
  val sqlForm: Any => String = {x => x.toString})
    extends Type {

  def isNullable = false
  lazy val nullable = getNullable()
  def toNullable() = nullable
  def toNonNullable() = this
  def coerce(value: Any) = this.coercer(value)
  def toSqlForm(value: Any) = sqlForm(value)
}

class NullableAtomicType(nonNullable: AtomicType) extends Type {
  val _name = nonNullable.name + "/nullable"
  def name: String = _name
  def coerce(value: Any): Option[Any] = nonNullable.coerce(value)
  def isNullable: Boolean = true
  def toNullable(): Type = this
  def toNonNullable(): Type = nonNullable
  def isNumeric: Boolean = nonNullable.isNumeric
  def isOrdered: Boolean = nonNullable.isOrdered
  def toSqlForm(value: Any): String = nonNullable.toSqlForm(value)
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
    { () => stringNullable },
    { case s: String => Some(s)
      case _ => None },
    isNumeric = false, isOrdered = true,
    sqlForm = {v => "'"+v.toString+"'"})
  case object stringNullable extends NullableAtomicType(string)

  case object boolean extends AtomicType("boolean",
    { () => booleanNullable },
    { case b : Boolean => Some(b)
      case _ => None },
    isNumeric = false, isOrdered = false)
  case object booleanNullable extends NullableAtomicType(boolean)

  case object integer extends AtomicType("integer",
    { () => integerNullable },
    { case i: Int => Some(i.longValue)
      case l: Long => Some(l)
      case _ => None },
    isNumeric = true, isOrdered = true)
  case object integerNullable extends NullableAtomicType(integer)

  def isDouble(x: Any): Boolean =
    x.isInstanceOf[Double]

  case object double extends AtomicType("double",
    { () => doubleNullable },
    { case d: Double => Some(d)
      case d: Float => Some(d.doubleValue)
      case _ => None },
    isNumeric = true, isOrdered = true)
  case object doubleNullable extends NullableAtomicType(double)

  def product(componentTypes: Seq[Type]): Type = ProductType(componentTypes)
  def set(member: Type): Type = SetType(member)

  def valueLessThan(ty: Type, val1: Any, val2: Any) = {
    val1 match {
      case _: String => val1.asInstanceOf[String] < val2.asInstanceOf[String]
      case _: Long => val1.asInstanceOf[Long] < val2.asInstanceOf[Long]
      case _: Int => val1.asInstanceOf[Int] < val2.asInstanceOf[Int]
    }
  }
}
