package de.ag.sqala

object Environment {

  val empty = Map[String, Type]()

  def make(bindings: (String, Type)*): Environment =
    bindings.toMap

  def compose(env1: Environment, env2: Environment) =
    env1 ++ env2
}
