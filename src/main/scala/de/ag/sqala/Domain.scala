package de.ag.sqala

import de.ag.sqala.sql.Expr.Literal

/**
 * Abstract base for all domains
 *
 * @param name human-readable description of the domain
 */
abstract class Domain(val name: String) {
  def isNumeric: Boolean

  def isStringLike: Boolean

  def isOrdered: Boolean

  def sqlLiteralValueOf(value:Any): sql.Expr.Literal

  override def equals(other: Any) = other match {
    case that: Domain => that.domainEquals(this)
    case _ => false
  }

  def domainEquals(that: Domain): Boolean

  override def hashCode(): Int = name.hashCode()
}

/**
 * Domains of values in relational algebra, aka 'type'.
 */
object Domain {

  case object String extends Domain("string") {
    def isNumeric: Boolean = false

    def isStringLike: Boolean = true

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Literal = value match {
      case s:String => sql.Expr.Literal.String(s)
      case _ => throw new IllegalArgumentException("not a string: " + value)
    }
  }

  case object Integer extends Domain("integer") {
    def isNumeric: Boolean = true

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Literal = value match {
      case i:scala.Int => sql.Expr.Literal.Integer(i)
      case _ => throw new IllegalArgumentException("not an int: " + value)
    }

  }

  case object Double extends Domain("double") {
    def isNumeric: Boolean = true

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Literal = value match {
      case d:scala.Double => sql.Expr.Literal.Double(d)
      case _ => throw new IllegalArgumentException("not a double: " + value)
    }

  }

  case object Boolean extends Domain("boolean") {
    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Literal = value match {
      case b:scala.Boolean => sql.Expr.Literal.Boolean(b)
      case _ => throw new IllegalArgumentException("not a boolean: " + value)
    }

  }

  case object CalendarTime extends Domain("calendar time") {
    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Literal = value match {
      case t:java.util.Date => throw new RuntimeException("not implemented")
      case _ => throw new IllegalArgumentException("not a string: " + value)
    }

  }

  case object Blob extends Domain("blob") {
    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = false

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Literal = value match {
      case s:java.io.InputStream => throw new RuntimeException("not implemented")
      case _ => throw new IllegalArgumentException("not a blob: " + value)
    }

  }

  case class BoundedString(maxSize: Int) extends Domain("bounded string") {
    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that match {
      case BoundedString(thatMaxSize) => thatMaxSize == this.maxSize
      case _ => false
    }

    def sqlLiteralValueOf(value: Any): Literal = value match {
      case s:String if s.size <= maxSize => sql.Expr.Literal.String(s)
      case _ => throw new IllegalArgumentException("not a string of size <= %d: %s".format(maxSize, value))
    }

  }

  case class Nullable(underlying: Domain) extends Domain("nullable '" + underlying.name + "'") {
    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = underlying.isOrdered

    def domainEquals(that: Domain): Boolean = that match {
      case Nullable(thatUnderlying) => thatUnderlying.domainEquals(this.underlying)
      case _ => false
    }

    def sqlLiteralValueOf(value: Any): Literal = value match {
      case null => sql.Expr.Literal.Null
      case _ => throw new IllegalArgumentException("not null: " + value)
    }

  }

  case class Product(components: Seq[Domain]) extends Domain("product of '" + components.map {
    _.name
  } + "'") {
    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = components.forall(_.isOrdered) // then can order component-wise

    def domainEquals(that: Domain): Boolean = that match {
      case Product(thatComponents) =>
        thatComponents.length == this.components.length &&
          thatComponents.zip(this.components).forall {
            case (d1, d2) => d1.domainEquals(d2)
          }
      case _ => false
    }

    def sqlLiteralValueOf(value: Any): Literal = value match {
      case s:Seq[Any] => throw new RuntimeException("not implemented")
//        sql.Expr.Literal.Tuple(components.zip(s).map{ case (c,v) => c.sqlLiteralValueOf(v) })
      case _ => throw new IllegalArgumentException("not a product: " + value)
    }

  }

  case class Set(member: Domain) extends Domain("set of '" + member.name + "'") {
    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = false // per definition

    def domainEquals(that: Domain): Boolean = that match {
      case Set(thatMember) => thatMember.equals(this.member)
      case _ => false
    }

    def sqlLiteralValueOf(value: Any): Literal = value match {
      case _  => throw new RuntimeException("not implemented")
//      case _ => throw new IllegalArgumentException("not a set: " + value)
    }

  }

}