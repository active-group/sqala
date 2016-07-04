package de.ag.sqala

import QueryMonad._

// is used as the state of the query monad. This is used to
// track the current state and later rebuild the resulting, correct references when
// running the query monad
case class Relation(alias: Alias, scheme: RelationalScheme) {
  def !(n: String) = {
    assert(this.scheme.map.contains(n))
    Expression.makeAttributeRef(freshName(n, this.alias))
  }
}

case class QueryMonad[A](transform: State => (A, State)) {
  def map[B](f: A => B): QueryMonad[B] =
    QueryMonad { st0: State =>
                  val (a, st1) = transform(st0)
                  val b = f(a)
                  (b, st1) }

  def flatMap[B](f: A => QueryMonad[B]): QueryMonad[B] =
    QueryMonad { st0: State =>
                 val (a, st1) = transform(st0)
                 val qm = f(a)
                 qm.transform(st1) }
}

object QueryMonad {
  type Alias = Int

  case class State(alias: Alias, query: Query)

  val emptyState = State(0, Query.empty)

  def freshName(name: String, alias: Alias): String =
    name + "_" + alias

  def setAlias(a: Alias): QueryMonad[Unit] =
    QueryMonad { st0: State => ((), st0.copy(alias = a)) }

  val currentAlias: QueryMonad[Alias] =
    QueryMonad { st0: State => (st0.alias, st0) }

  val newAlias: QueryMonad[Alias] =
    for {
      a <- currentAlias
      _ <- setAlias(a + 1)
    } yield a


  val currentQuery: QueryMonad[Query] =
    QueryMonad { st0: State => (st0.query, st0) }

  def setQuery(q: Query): QueryMonad[Unit] =
    QueryMonad { st0: State => ((), st0.copy(query = q)) }

  def addToProduct(makeProduct: (Query, Query) => Query, transformScheme: RelationalScheme => RelationalScheme,
                   q: Query): QueryMonad[Relation] =
    for {
      alias <- newAlias
      query <- currentQuery
      scheme = transformScheme(q.getScheme())
      columns = scheme.columns
      fresh = columns.map { k => freshName(k,alias) }
      projectAlist = (columns, fresh).zipped.map { (k, fresh) => (fresh, Expression.makeAttributeRef(k)) }
      qq = q.project(projectAlist)
      _ <- setQuery(makeProduct(query, qq))
    } yield Relation(alias, scheme)

  def embed(q: Query): QueryMonad[Relation] =
    addToProduct(_ * _, identity, q)

  def outer(q: Query): QueryMonad[Relation] =
    addToProduct(_.leftOuterProduct(_), _.toNullable, q)

  def project(alist: Seq[(String, Expression)]): QueryMonad[Relation] =
    for {
      alias <- newAlias
      query <- currentQuery
      query1 = query.extend(alist.map { case (k, v) => (freshName(k, alias), v) })
      _ <- setQuery(query1)
    } yield Relation(alias,
              { val scheme = query.getScheme()
                val env = scheme.toEnvironment()
                RelationalScheme.make(alist.map { case (k, v) => (k, v.getType(env)) }) })

  def restrict(expr: Expression): QueryMonad[Unit] =
    for {
      old <- currentQuery
      _ <- setQuery(old.restrict(expr))
    } yield ()

  def restrictOutr(expr: Expression): QueryMonad[Unit] =
    for {
      old <- currentQuery
      _ <- setQuery(old.restrictOuter(expr))
    } yield ()

  def group(colrefs: (Relation, String)*): QueryMonad[Unit] = {
    assert(colrefs.forall { case (r, n) => r.scheme.map.contains(n) })
    for {
      old <- currentQuery
      _ <- setQuery(old.group(colrefs.map { case (r, n) => freshName(n, r.alias) } toSet))
    } yield ()
  }
}
