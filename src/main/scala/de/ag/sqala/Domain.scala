package de.ag.sqala

/**
 *
 */
abstract class Domain(val name: String) {
  def isNumeric: Boolean = this match {
    case DBInteger | DBDouble => true
    case _ => false
  }

  def isOrdered: Boolean = this match { // what about product, null, bounded string, blob?
    case DBInteger | DBDouble | DBString | DBCalendarTime => true
    case _ => false
  }
}
case object DBString extends Domain("string")
case object DBInteger extends Domain("integer")
case object DBDouble extends Domain("double")
case object DBBoolean extends Domain("boolean")
case object DBCalendarTime extends Domain("calendar time")
case object DBBlob extends Domain("blob")
case class DBBoundedString(maxSize:Int) extends Domain("bounded string")
case class DBNullable(underlying:Domain) extends Domain("nullable '" + underlying.name + "'")
case class DBProduct(components:Seq[Domain]) extends Domain("product of '" + components.map{_.name} + "'")
case class DBSet(member:Domain) extends Domain("set of '" + member.name + "'")

