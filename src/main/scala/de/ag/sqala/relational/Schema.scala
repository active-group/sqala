package de.ag.sqala.relational

import de.ag.sqala.sql._
import de.ag.sqala.Domain

/**
 *
 */
class Schema(val schema:Seq[(Schema.Attribute, Domain)]) {
  private lazy val environment = schema.toMap
  def dom(attribute:Schema.Attribute):Domain = environment(attribute)
  def degree = environment.size
  def difference(that:Schema):Schema = {
    val thatKeys = that.environment.keySet
    // keep order of attributes
    Schema(schema.filter(tuple => !thatKeys.contains(tuple._1)))
  }
  def isUnary = !schema.isEmpty && schema.tail.isEmpty
  def attributes:Seq[Schema.Attribute] = schema.map(_._1)
  def domains:Seq[Domain] = schema.map(_._2)
  // have own equals, etc. instead of case class to avoid construction of schemaMap
  override def equals(it:Any) = it match {
    case that:Schema => isComparable(that) && that.schema == this.schema
    case _ => false
  }
  override def hashCode() = schema.hashCode()
  def isComparable(that:Any) = that.isInstanceOf[Schema]
  def toEnvironment:Environment = new Environment(environment)
  override def toString = "Schema(%s)".format(
    schema.map{case (attr, domain) => "%s -> %s".format(attr, domain)}.mkString(", ")
  )
}

object Schema {
  type Attribute = String

  val empty: Schema = Schema(Seq())

  // can have only one or the other apply, not both (same type after erasure);
  // settled for the one not requiring (...:*) for Seq arguments
  // def apply(schema:(Attribute, Domain)*) =
  //   new Schema(schema)
  def apply(schema:Seq[(Attribute, Domain)]) =
    new Schema(schema)
  def unapply(schema:Schema) = Some(schema.schema)
}

