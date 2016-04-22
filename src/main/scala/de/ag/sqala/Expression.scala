package de.ag.sqala

import Aliases._

sealed trait Expression {
  def getType(env: Environment): Type
  def isAggregate: Boolean
  /** Check whether all attribute refs in an expression
    * that are not inside an application of an aggregate occur in `grouped`.
  */
  def checkGrouped(grouped: Option[Set[String]]): Boolean
}

object Expression {
  def makeAttributeRef(name: String): Expression = AttributeRef(name)
  def makeConst(ty: Type, value: Any): Expression = Const(ty, value)
}

case class AttributeRef(val name: String) extends Expression {
  def getType(env: Environment): Type = env(name)
  def isAggregate = false
  def checkGrouped(grouped: Option[Set[String]]): Boolean = {
    grouped match {
      case None => throw new AssertionError("non-aggregate expression")
      case Some(s) => assert(s.contains(name))
    }
    true
  }
}

case class Const(val ty: Type, val value: Any) extends Expression {
  def getType(env: Environment): Type = ty
  def isAggregate = false
  def checkGrouped(grouped: Option[Set[String]]): Boolean = true
}

