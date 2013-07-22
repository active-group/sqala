package de.ag.sqala

/**
 * Domains of values in relational algebra, aka 'type'.
 *
 * The abstract base class is not sealed to allow extensions.
 *
 * @param name human-readable description of the domain
 */
abstract class Domain(val name: String) {
  def isNumeric: Boolean

  def isOrdered: Boolean

  override def equals(other:Any) = other match {
    case that:Domain => that.domainEquals(this)
    case _ => false
  }

  def domainEquals(that: Domain): Boolean

  override def hashCode():Int = name.hashCode()
}

case object DBString extends Domain("string") {
  def isNumeric: Boolean = false

  def isOrdered: Boolean = true

  def domainEquals(that: Domain): Boolean = that match {
    case DBString => true
    case _ => false
  }
}

case object DBInteger extends Domain("integer") {
  def isNumeric: Boolean = true

  def isOrdered: Boolean = true

  def domainEquals(that: Domain): Boolean = that match {
    case DBInteger => true
    case _ => false
  }
}

case object DBDouble extends Domain("double") {
  def isNumeric: Boolean = true

  def isOrdered: Boolean = true

  def domainEquals(that: Domain): Boolean = that match {
    case DBDouble => true
    case _ => false
  }
}

case object DBBoolean extends Domain("boolean") {
  def isNumeric: Boolean = false

  def isOrdered: Boolean = true

  def domainEquals(that: Domain): Boolean = that match {
    case DBBoolean => true
    case _ => false
  }
}

case object DBCalendarTime extends Domain("calendar time") {
  def isNumeric: Boolean = false

  def isOrdered: Boolean = true

  def domainEquals(that: Domain): Boolean = that match {
    case DBCalendarTime => true
    case _ => false
  }
}

case object DBBlob extends Domain("blob") {
  def isNumeric: Boolean = false

  def isOrdered: Boolean = false

  def domainEquals(that: Domain): Boolean = that match {
    case DBBlob => true
    case _ => false
  }
}

case class DBBoundedString(maxSize: Int) extends Domain("bounded string") {
  def isNumeric: Boolean = false

  def isOrdered: Boolean = true

  def domainEquals(that: Domain): Boolean = that match {
    case DBBoundedString(thatMaxSize) => thatMaxSize == this.maxSize
    case _ => false
  }
}

case class DBNullable(underlying: Domain) extends Domain("nullable '" + underlying.name + "'") {
  def isNumeric: Boolean = false

  def isOrdered: Boolean = underlying.isOrdered

  def domainEquals(that: Domain): Boolean = that match {
    case DBNullable(thatUnderlying) => thatUnderlying.domainEquals(this.underlying)
    case _ => false
  }
}

case class DBProduct(components: Seq[Domain]) extends Domain("product of '" + components.map {
  _.name
} + "'") {
  def isNumeric: Boolean = false

  def isOrdered: Boolean = components.forall(_.isOrdered) // then can order component-wise

  def domainEquals(that: Domain): Boolean = that match {
    case DBProduct(thatComponents) =>
      thatComponents.length == this.components.length &&
        thatComponents.zip(this.components).forall {
          case (d1, d2) => d1.domainEquals(d2)
        }
    case _ => false
  }
}

case class DBSet(member: Domain) extends Domain("set of '" + member.name + "'") {
  def isNumeric: Boolean = false

  def isOrdered: Boolean = false // per definition

  def domainEquals(that: Domain): Boolean = that match {
    case DBSet(thatMember) => thatMember.equals(this.member)
    case _ => false
  }
}

