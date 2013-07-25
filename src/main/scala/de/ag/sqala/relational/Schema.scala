package de.ag.sqala.relational

import de.ag.sqala.sql._
import de.ag.sqala.Domain

/**
 * Schema of a relational algebra
 */
class Schema(val schema:Seq[(Schema.Attribute, Domain)]) {
  private lazy val environment = schema.toMap
  /** Domain of an attribute */
  def dom(attribute:Schema.Attribute):Domain = environment(attribute)
  /** Number of (unique) attributes */
  def degree = environment.size

  /** This schema's attributes that are not in the other schema
    *
    * @param that Schema to subtract
    * @return     Schema that contains all attributes of this schema, that are not in that schema
    */
  def difference(that:Schema):Schema = {
    val thatKeys = that.environment.keySet
    // keep order of attributes
    Schema(schema.filter(tuple => !thatKeys.contains(tuple._1)))
  }
  /** True iff schema has only one attribute */
  def isUnary = !schema.isEmpty && schema.tail.isEmpty
  /** All attributes, in order */
  def attributes:Seq[Schema.Attribute] = schema.map(_._1)
  /** All domains, in order */
  def domains:Seq[Domain] = schema.map(_._2)
  /** Equality */
  // have own equals, etc. instead of case class to avoid construction of schemaMap
  override def equals(it:Any) = it match {
    case that:Schema => isComparable(that) && that.schema == this.schema
    case _ => false
  }
  override def hashCode() = schema.hashCode()
  protected def isComparable(that:Any) = that.isInstanceOf[Schema]

  /** Turn this schema with ordered list of attributes to environment with unordered list of attributes.
    * Note that duplicate attributes will be overriden by later attributes.
    * @return environment corresponding to this schema
    */
  def toEnvironment:Environment = new Environment(environment)
  override def toString = "Schema(%s)".format(
    schema.map{case (attr, domain) => "%s -> %s".format(attr, domain)}.mkString(", ")
  )
}

object Schema {
  type Attribute = String

  /** The empty schema */
  val empty: Schema = Schema(Seq())

  // can have only one or the other apply, not both (same type after erasure);
  // settled for the one not requiring (...:*) for Seq arguments
  // def apply(schema:(Attribute, Domain)*) =
  //   new Schema(schema)
  def apply(schema:Seq[(Attribute, Domain)]) =
    new Schema(schema)
  def unapply(schema:Schema) = Some(schema.schema)
}

