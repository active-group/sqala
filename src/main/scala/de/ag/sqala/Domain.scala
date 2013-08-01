package de.ag.sqala

import de.ag.sqala.sql.Expr.Literal

/**
 * Abstract base for all domains
 *
 * @param name human-readable description of the domain
 */
abstract class Domain(val name: String) {
  type ScalaType
  /** Does this domain only cover values that can be considered 'numeric'? */
  def isNumeric: Boolean

  /** Does this domain only cover values that can be considered 'string'? */
  def isStringLike: Boolean

  /** Does this domain only cover values that can be ordered? */
  def isOrdered: Boolean

  /** Convert value to SQL Literal, if possible */
  def sqlLiteralValueOf(value:Any): Option[sql.Expr.Literal]

  /** Equality: is the same domain */
  override def equals(other: Any) = other match {
    case that: Domain => that.domainEquals(this)
    case _ => false
  }

  /** Equality: is the same domain */
  def domainEquals(that: Domain): Boolean

  override def hashCode(): Int = name.hashCode()
}

/**
 * Domains of values in relational algebra, aka 'type'.
 */
object Domain {

  case object String extends Domain("string") {
    type ScalaType = java.lang.String
    def isNumeric: Boolean = false

    def isStringLike: Boolean = true

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case s:String => Some(sql.Expr.Literal.String(s))
      case _ => None
    }
  }

  case object Integer extends Domain("integer") {
    type ScalaType = java.lang.Integer
    def isNumeric: Boolean = true

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case i:scala.Int => Some(sql.Expr.Literal.Integer(i))
      case _ => None
    }

  }

  case object IdentityInteger extends Domain("identity integer") {
    type ScalaType = java.lang.Integer
    def isNumeric: Boolean = true

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case i:scala.Int => Some(sql.Expr.Literal.Integer(i))
      case _ => None
    }
  }

  case object Double extends Domain("double") {
    type ScalaType = java.lang.Double
    def isNumeric: Boolean = true

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case d:scala.Double => Some(sql.Expr.Literal.Double(d))
      case _ => None
    }

  }

  case object Boolean extends Domain("boolean") {
    type ScalaType = java.lang.Boolean
    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case b:scala.Boolean => Some(sql.Expr.Literal.Boolean(b))
      case _ => None
    }

  }

  case object CalendarTime extends Domain("calendar time") {
    type ScalaType = java.sql.Timestamp

    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case t:java.util.Date => throw new RuntimeException("not implemented")
      case _ => None
    }

  }

  case object Blob extends Domain("blob") {
    type ScalaType = scala.Null // FIXME streaming not implemented

    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = false

    def domainEquals(that: Domain): Boolean = that.eq(this)

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case s:java.io.InputStream => throw new RuntimeException("not implemented")
      case _ => None
    }

  }

  case class BoundedString(maxSize: Int) extends Domain("bounded string") {
    type ScalaType = java.lang.String

    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that match {
      case BoundedString(thatMaxSize) => thatMaxSize == this.maxSize
      case _ => false
    }

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case s:String if s.size <= maxSize => Some(sql.Expr.Literal.String(s))
      case _ => None
    }

  }

  case class Nullable(underlying: Domain) extends Domain("nullable '" + underlying.name + "'") {
    type ScalaType = underlying.ScalaType
    
    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = underlying.isOrdered

    def domainEquals(that: Domain): Boolean = that match {
      case Nullable(thatUnderlying) => thatUnderlying.domainEquals(this.underlying)
      case _ => false
    }

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case null => Some(sql.Expr.Literal.Null)
      case _ => None
    }

  }

  case class Product(components: Seq[Domain]) extends Domain("product of '" + components.map {
    _.name
  } + "'") {
    type ScalaType = Seq[AnyRef]

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

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case s:Seq[Any] => throw new RuntimeException("not implemented")
//        sql.Expr.Literal.Tuple(components.zip(s).map{ case (c,v) => c.sqlLiteralValueOf(v) })
      case _ => None
    }

  }

  case class Set(member: Domain) extends Domain("set of '" + member.name + "'") {
    type ScalaType = scala.collection.immutable.Set[member.ScalaType]

    def isNumeric: Boolean = false

    def isStringLike: Boolean = false

    def isOrdered: Boolean = false // per definition

    def domainEquals(that: Domain): Boolean = that match {
      case Set(thatMember) => thatMember.equals(this.member)
      case _ => false
    }

    def sqlLiteralValueOf(value: Any): Option[Literal] = value match {
      case _  => None
//      case _ => throw new IllegalArgumentException("not a set: " + value)
    }

  }

}