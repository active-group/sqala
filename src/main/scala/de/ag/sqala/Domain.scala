package de.ag.sqala

/**
 * Abstract base for all domains
 *
 * @param name human-readable description of the domain
 */
abstract class Domain(val name: String) {
  def isNumeric: Boolean

  def isOrdered: Boolean

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

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)
  }

  case object Integer extends Domain("integer") {
    def isNumeric: Boolean = true

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)
  }

  case object Double extends Domain("double") {
    def isNumeric: Boolean = true

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)
  }

  case object Boolean extends Domain("boolean") {
    def isNumeric: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)
  }

  case object CalendarTime extends Domain("calendar time") {
    def isNumeric: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that.eq(this)
  }

  case object Blob extends Domain("blob") {
    def isNumeric: Boolean = false

    def isOrdered: Boolean = false

    def domainEquals(that: Domain): Boolean = that.eq(this)
  }

  case class BoundedString(maxSize: Int) extends Domain("bounded string") {
    def isNumeric: Boolean = false

    def isOrdered: Boolean = true

    def domainEquals(that: Domain): Boolean = that match {
      case BoundedString(thatMaxSize) => thatMaxSize == this.maxSize
      case _ => false
    }
  }

  case class Nullable(underlying: Domain) extends Domain("nullable '" + underlying.name + "'") {
    def isNumeric: Boolean = false

    def isOrdered: Boolean = underlying.isOrdered

    def domainEquals(that: Domain): Boolean = that match {
      case Nullable(thatUnderlying) => thatUnderlying.domainEquals(this.underlying)
      case _ => false
    }
  }

  case class Product(components: Seq[Domain]) extends Domain("product of '" + components.map {
    _.name
  } + "'") {
    def isNumeric: Boolean = false

    def isOrdered: Boolean = components.forall(_.isOrdered) // then can order component-wise

    def domainEquals(that: Domain): Boolean = that match {
      case Product(thatComponents) =>
        thatComponents.length == this.components.length &&
          thatComponents.zip(this.components).forall {
            case (d1, d2) => d1.domainEquals(d2)
          }
      case _ => false
    }
  }

  case class Set(member: Domain) extends Domain("set of '" + member.name + "'") {
    def isNumeric: Boolean = false

    def isOrdered: Boolean = false // per definition

    def domainEquals(that: Domain): Boolean = that match {
      case Set(thatMember) => thatMember.equals(this.member)
      case _ => false
    }
  }

}