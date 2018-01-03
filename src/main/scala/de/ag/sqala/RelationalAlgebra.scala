package de.ag.sqala

case class RelationalScheme(columns: Vector[String], map: Map[String, Type], grouped: Option[Set[String]]) {
  // FIXME: why do we have environment() and toEnvironment() ... cache the environment!
  def environment(): Environment = map

  def ++(other: RelationalScheme): RelationalScheme =
    RelationalScheme(this.columns ++ other.columns,
        this.map ++ other.map,
        (this.grouped, other.grouped) match {
          case (None, g2) => g2
          case (g1, None) => g1
          case (Some(g1), Some(g2)) => Some(g1 ++ g2)
        })

  def toNullable() : RelationalScheme =
    RelationalScheme(this.columns,
      this.map map { case (name, ty: Type) => (name, ty.toNullable()) },
      this.grouped)

  // In postgreSQL: EXCEPT
  def difference(other: RelationalScheme): RelationalScheme = {
    val cols2 = other.columns.toSet
    val cols = this.columns.filter(!cols2.contains(_))
    require(!cols.isEmpty)
    RelationalScheme(cols,
      this.map.filterKeys(k => cols.contains(k)),
      this.grouped.map(cs => cs -- cols))
  }

  def toEnvironment(): Environment = this.map

  def isUnary(): Boolean = columns.length == 1

  /** find the position of a name within the scheme */
  def pos(name: String): Int = columns.indexOf(name)

  /** check if the scheme has any grouping */
  def isGrouped(): Boolean = grouped.isDefined

  lazy val groupedSet = grouped match {
      case None => Set.empty[String]
      case Some(s) => s
    }

  /** positions in either grouped or ungrouped, see `MemoryQuery.Group`*/
  lazy val groupedUngroupedPosition: Map[String, Either[Int, Int]] =
    grouped match {
      case None => columns.map { c => (c, Right(pos(c))) }.toMap
      case Some(groupedSet) =>
        columns.foldLeft((0, 0, Map.empty[String, Either[Int, Int]])) { case ((groupedIndex, nonGroupedIndex, map), name) =>
          if (groupedSet.contains(name))
            (groupedIndex + 1, nonGroupedIndex, map + (name -> Left(groupedIndex)))
          else
            (groupedIndex, nonGroupedIndex + 1, map + (name -> Right(nonGroupedIndex)))
        }._3
    }

  /** returns pair of (positions of ungrouped, positions of grouped) */
  def groupedUngroupedPositions(cols: Seq[String]): (Seq[Int], Seq[Int]) = {
    val poses = cols.map(groupedUngroupedPosition(_))
    val groupedPos = poses.flatMap(_.left.toOption)
    val ungroupedPos = poses.flatMap(_.right.toOption)
    (groupedPos, ungroupedPos)
  }

  /*** First, figure out what columns are in this that are also in `s2`.
   
   Then, return a function that will accept a row of the same schema
   as this, and will return a row with exactly those columns.

   Used for implementing quotient.
   */
  def makeExtractor(s2: RelationalScheme): Row => Row = {
    val characteristics = columns.map(s2.map.contains(_))
    (row: Row) => characteristics.zip(row).filter(_._1).map(_._2)
  }


}

object RelationalScheme {
  def make(alist: Seq[(String, Type)]): RelationalScheme =
    RelationalScheme(alist.map(_._1).toVector, alist.toMap, None)

  def make(name: String, ty: Type): RelationalScheme = make(Seq(name -> ty))

  val empty = RelationalScheme(Vector[String](), Environment.empty, None)
}

sealed abstract class Query {
  var relSchemeCache = scala.collection.mutable.Map[Environment, RelationalScheme]()

  def computeScheme(env: Environment): RelationalScheme

  def getScheme(env: Environment): RelationalScheme =
    this.synchronized {
      relSchemeCache.getOrElseUpdate(env, computeScheme(env))
    }

  def getScheme(): RelationalScheme = computeScheme(Environment.empty)

  // helper method for subqueries of expressions
  /** assuming this is a subquery of an expression, determine what
    * attribute names of the underlying query it uses */
  def attributeNames(): Set[String]

  private def reallyProject(alist: Seq[(String, Expression)]): Query =
    Projection(alist, this)

  def project(alist: Seq[(String, Expression)]): Query = {
    // FIXME: grouping - see really-make-project
    if (alist.isEmpty)
      this match {
        case Projection(_, query) => query.project(alist)
        case _ => Projection(alist, this) // DF: not EmptyQuery?
      }
    else
      reallyProject(alist)
  }

  def restrict(exp: Expression): Query = Restriction(exp, this)

  def extend(alist: Seq[(String, Expression)], env: Environment = Environment.empty): Query = {
    val scheme = this.getScheme(env)
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

  def restrictOuter(exp: Expression): Query = OuterRestriction(exp, this)

  def *(other: Query): Query = if (this == EmptyQuery) other else if (other == EmptyQuery) this else Product(this, other)
  def leftOuterProduct(other: Query): Query = LeftOuterProduct(this, other)
  def /(other: Query): Query = Quotient(this, other)
  def union(other: Query): Query = Union(this, other)
  def intersection(other: Query): Query = Intersection(this, other)
  def difference(other: Query): Query = Difference(this, other)

  def order(alist: Seq[(String, Direction)]): Query =
    Order(alist, this)

  def group(columns: Set[String]): Query =
    Group(columns, this)

  def top(offset: Int, count: Int): Query =
    Top(offset, count, this)
}

object Query {
  /**
    * handle could be a SQLTable or ...
    */
  def makeBaseRelation[H](name: String, scheme: RelationalScheme, handle: H): Query =
    BaseRelation(name, scheme, handle)

  val empty = EmptyQuery
}

case class BaseRelation[H](name: String, scheme: RelationalScheme, handle: H) extends Query {
  override def computeScheme(env: Environment): RelationalScheme = scheme

  override def attributeNames(): Set[String] = Set()

}

case object EmptyQuery extends Query {
  override def computeScheme(env: Environment): RelationalScheme = RelationalScheme.empty

  override def attributeNames(): Set[String] = Set()
}

case class Projection(alist: Seq[(String, Expression)], query: Query) extends Query {
  override def computeScheme(env: Environment): RelationalScheme = {
    val baseScheme = query.getScheme(env)
    val baseEnv = baseScheme.environment()
    val grouped = baseScheme.grouped
    if (alist.exists({ case (name, exp) => exp.isAggregate })
        || grouped.isDefined) {
      // we're doing aggregation
      for ((_, e) <- alist)
        e.checkGrouped(grouped)
    }
    val tyAlist = alist.map({ case (name, exp) => {
      val typ = exp.getType(Environment.compose(baseEnv, env))
      // FIXME: check for non-product type
      (name, typ) }})
    RelationalScheme.make(tyAlist)
  }

  override def attributeNames(): Set[String] = {
    val expNames = alist.flatMap { case (_, exp) => exp.attributeNames() }.toSet
    // FIXME: shouldn't we pass an environment to getScheme?
    val schemeNames = query.getScheme().environment().keySet
    (expNames -- schemeNames) ++ query.attributeNames()
  }
}

case class Restriction(exp: Expression, query: Query) extends Query {
  override def computeScheme(env: Environment): RelationalScheme = {
    require(exp.getType(Environment.compose(query.getScheme(env).environment(), env)) == Type.boolean,
           "not a boolean expression")
    query.computeScheme(env)
  }

  override def attributeNames(): Set[String] =
    (exp.attributeNames() -- query.getScheme().environment().keySet) ++ query.attributeNames()
}

case class OuterRestriction(exp: Expression, query: Query) extends Query {
  override def computeScheme(env: Environment): RelationalScheme = {
    require(exp.getType(Environment.compose(query.getScheme(env).environment(), env)) == Type.boolean,
      "not a boolean expression")
    query.computeScheme(env)
  }

  override def attributeNames(): Set[String] =
    (exp.attributeNames() -- query.getScheme().environment().keySet) ++ query.attributeNames()

}

trait Combination {
  val query1: Query
  val query2: Query
}

case class Product(query1: Query, query2: Query) extends Query with Combination {
  override def computeScheme(env: Environment): RelationalScheme = {
    val r1 = query1.getScheme(env)
    val r2 = query2.getScheme(env)
    val a1 = r1.map
    val a2 = r2.map
    for ((k, _) <- a1)
      require(!a2.contains(k))
    r1 ++ r2
  }

  override def attributeNames(): Set[String] =
    query1.attributeNames() ++ query2.attributeNames()
}

case class LeftOuterProduct(query1: Query, query2: Query) extends Query with Combination {
  override def computeScheme(env: Environment): RelationalScheme = {
    val r1 = query1.getScheme(env)
    val r2 = query2.getScheme(env).toNullable()
    val a1 = r1.map
    val a2 = r2.map
    for ((k, _) <- a1)
      require(!a2.contains(k))
    r1 ++ r2
  }

  override def attributeNames(): Set[String] =
    query1.attributeNames() ++ query2.attributeNames()
}

case class Quotient(query1: Query, query2: Query) extends Query with Combination {
  override def computeScheme(env: Environment): RelationalScheme = {
    val s1 = query1.getScheme(env)
    val s2 = query2.getScheme(env)
    val a1 = s1.map
    val a2 = s2.map
    val grouped = s1.groupedSet
    for ((k, v) <- a2) {
      // ensure that query2 has a subscheme of the scheme of query1
      a1.get(k) match {
        case Some(p2) => require(v == p2)
        case _ => ()
      }
      // no column in query2 can be grouped in query1, as we're doing
      // all-quantification of the values
      require(!grouped.contains(k))
    }

    s1.difference(s2)
  }

  override def attributeNames(): Set[String] =
    query1.attributeNames() ++ query2.attributeNames()
}

sealed abstract class SetCombination extends Query with Combination {
  override def computeScheme(env: Environment): RelationalScheme = {
    val s1 = query1.getScheme(env)
    val s2 = query2.getScheme(env)
    // FIXED : require(s1 == s2) ist falsch ... die Typen + die Reihenfolge müssen passen, die Spaltennamen nicht!
    require(s1.columns.map(c => s1.map.get(c)) == s2.columns.map(c => s2.map.get(c)))
    s1
  }

  override def attributeNames(): Set[String] =
    query1.attributeNames() ++ query2.attributeNames()
}

case class Union(val query1: Query, val query2: Query) extends SetCombination
case class Intersection(val query1: Query, val query2: Query) extends SetCombination
case class Difference(val query1: Query, val query2: Query) extends SetCombination

sealed trait Direction

object Direction {
  case object Ascending extends Direction
  case object Descending extends Direction
}

case class Order(alist: Seq[(String, Direction)], query: Query) extends Query {
  override def computeScheme(env: Environment): RelationalScheme = {
    val s = query.getScheme(env)
    require(!s.isGrouped())
    val env2 = Environment.compose(s.toEnvironment(), env)
    for ((col, _) <- alist) {
      val t = env2(col)
      require(t.isOrdered)
    }
    s
  }

  override def attributeNames(): Set[String] =
    (alist.map(_._1).toSet -- query.getScheme().environment().keySet) ++ query.attributeNames()
}

case class Top(offset: Int, count: Int, query: Query) extends Query {
  override def computeScheme(env: Environment): RelationalScheme =
    query.getScheme(env)

  override def attributeNames(): Set[String] =
    query.attributeNames()
}

case class Group(columns: Set[String], query: Query) extends Query {
  override def computeScheme(env: Environment): RelationalScheme = {
    val s = query.getScheme(env)
    val g = s.grouped match {
      case None => columns
      case Some(grouped) => grouped.union(columns)
    }
    s.copy(grouped = Some(g))
  }

  override def attributeNames(): Set[String] =
    (columns -- query.getScheme().environment().keySet) ++ query.attributeNames()
}
