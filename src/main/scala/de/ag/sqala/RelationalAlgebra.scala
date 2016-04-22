package de.ag.sqala

object Aliases {
  type Environment = Map[String, Type]

  val emptyEnvironment = Map[String, Type]()

  def composeEnvironments(env1: Environment, env2: Environment) =
    env1 ++ env2
}

import Aliases._

case class RelationalScheme(columns: Vector[String], map: Environment, grouped: Option[Set[String]]) {
  def environment(): Environment = map
}

object RelationalScheme {
  def make(alist: Seq[(String, Type)]): RelationalScheme =
    RelationalScheme(alist.map(_._1).toVector, alist.toMap, None)

  val empty = RelationalScheme(Vector[String](), emptyEnvironment, None)
}

sealed abstract class Query {
  var relSchemeCache = scala.collection.mutable.Map[Environment, RelationalScheme]()

  def computeScheme(env: Environment): RelationalScheme

  def getScheme(env: Environment): RelationalScheme =
    this.synchronized {
      relSchemeCache.getOrElseUpdate(env, computeScheme(env))
    }

  def getScheme(): RelationalScheme = computeScheme(emptyEnvironment)

  private def reallyProject(alist: Seq[(String, Expression)]): Query = {
    val baseScheme = this.getScheme()
    val grouped = baseScheme.grouped
    if (alist.exists({ case (name, exp) => exp.isAggregate })
        || grouped.isDefined) {
      // we're doing aggregation
      for ((_, e) <- alist)
        e.checkGrouped(grouped)
    }
    Projection(alist, this)
  }

  def project(alist: Seq[(String, Expression)]): Query = {
    // FIXME: grouping - see really-make-project
    if (alist.isEmpty)
      this match {
        case Projection(_, query) => query.project(alist)
        case _ => Projection(alist, this)
      }
    else
      reallyProject(alist)
  }

  def restrict(exp: Expression): Query = Restriction(exp, this)

  def extend(alist: Seq[(String, Expression)]): Query = {
    val scheme = this.getScheme()
    val base = scheme.grouped match {
      case Some(grouped) => scheme.columns.filter(grouped.contains(_))
      case None =>
        if (alist.exists({ case (name, exp) => exp.isAggregate }))
          Vector[String]()
        else
          scheme.columns
    }
    this.project(base.map(k => (k, Expression.makeAttributeRef(k))) ++ alist)
  }
}

object Query {
  def makeBaseRelation[H](name: String, scheme: RelationalScheme, handle: H): Query =
    BaseRelation(name, scheme, handle)
}

case class BaseRelation[H](name: String, scheme: RelationalScheme, handle: H) extends Query {
  def computeScheme(env: Environment): RelationalScheme = scheme
}

case object EmptyQuery extends Query {
  def computeScheme(env: Environment): RelationalScheme = RelationalScheme.empty
}

case class Projection(alist: Seq[(String, Expression)], query: Query) extends Query {
  def computeScheme(env: Environment): RelationalScheme = {
    val baseScheme = query.getScheme(env)
    val baseEnv = baseScheme.environment()
    val tyAlist = alist.map({ case (name, exp) => {
      val typ = exp.getType(composeEnvironments(baseEnv, env))
      // FIXME: check for non-product type
      (name, typ) }})
    RelationalScheme.make(tyAlist)
  }
}

case class Restriction(exp: Expression, query: Query) extends Query {
  def computeScheme(env: Environment): RelationalScheme = {
    assert(exp.getType(composeEnvironments(query.getScheme(env).environment(), env)) == Type.boolean,
           "not a boolean expression")
    query.computeScheme(env)
  }
}

case class OuterRestriction(exp: Expression, query: Query) extends Query {
  def computeScheme(env: Environment): RelationalScheme = {
    assert(exp.getType(composeEnvironments(query.getScheme(env).environment(), env)) == Type.boolean,
           "not a boolean expression")
    query.computeScheme(env)
  }
}


