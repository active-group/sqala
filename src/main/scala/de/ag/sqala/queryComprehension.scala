package de.ag.sqala

import QueryMonad._

// is used as the state of the query monad. This is used to
// track the current state and later rebuild the resulting, correct references when
// running the query monad
case class Relation(alias: Alias, scheme: RelationalScheme) {
  def !(n: String): Expression = {
    assert(this.scheme.map.contains(n))
    Expression.makeAttributeRef(freshName(n, this.alias))
  }

  def buildQueryNScheme(): QueryMonad[(Query, RelationalScheme)] =
    for {
      state <- getState
      alist = scheme.columns.map { k => (k, this!k) }
    } yield (state.query.project(alist), scheme)

  def buildQuery(): QueryMonad[Query] =
    buildQueryNScheme().map(_._1)
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

  def run(state: QueryMonad.State = emptyState) =
    transform(state)
}

object QueryMonad {
  type Alias = Int
  
  type Comprehension = QueryMonad[Relation]

  case class State(alias: Alias, query: Query)

  val emptyState = State(0, Query.empty)

  val getState: QueryMonad[State] = QueryMonad { st => (st, st) }

  def putState(st: State): QueryMonad[Unit] =
    QueryMonad { _ => ((), st) }

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
                   q: Query): Comprehension =
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

  def embed(q: Query): Comprehension =
    addToProduct(_ * _, identity, q)

  def outer(q: Query): Comprehension =
    addToProduct(_.leftOuterProduct(_), _.toNullable, q)

  def project(alist: Seq[(String, Expression)]): Comprehension =
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

  def combination1(op: (Query, Query) => Query,
                   computeScheme: (RelationalScheme, RelationalScheme) => RelationalScheme,
                   oldQuery: Query,
                   rel1: Relation, q1: Query, rel2: Relation, q2: Query,
                   alias: Alias): Comprehension = {
    val p1 = q1.project(rel1.scheme.columns.map { k => (freshName(k, alias),
      Expression.makeAttributeRef(freshName(k, rel1.alias)))
    })
    val p2 = q2.project(rel2.scheme.columns.map { k => (freshName(k, alias),
      Expression.makeAttributeRef(freshName(k, rel2.alias)))
    })
    for {
      _ <- setAlias(alias + 1)
      _ <- setQuery(op(p1, p2) * oldQuery)
    } yield Relation(alias, computeScheme(rel1.scheme, rel2.scheme))
  }

  def combination(op: (Query, Query) => Query,
                  computeScheme: (RelationalScheme, RelationalScheme) => RelationalScheme,
                  prod1: Comprehension, prod2: Comprehension): Comprehension =
    for {
      query0 <- currentQuery
      alias0 <- currentAlias
      (rel1, state1) = prod1.run(State(alias0, query0))
      (rel2, state2) = prod2.run(State(state1.alias, query0))
      res <- combination1(op, computeScheme, query0, rel1, state1.query, rel2, state2.query, alias0)
    } yield res

  // FIXME: these can be methods in QueryMonad, with the right type restriction
  // But Mike needs to lookup how that works, and is sitting on a traint
  def union(p1: Comprehension, p2: Comprehension): Comprehension =
    combination(Union, (s1, s2) => s1, p1, p2)

  def intersect(p1: Comprehension, p2: Comprehension): Comprehension =
    combination(Intersection, (s1, s2) => s1, p1, p2)

  def divide(p1: Comprehension, p2: Comprehension): Comprehension =
    combination(Quotient, (s1, s2) => s1.difference(s2), p1, p2)

  def subtract(p1: Comprehension, p2: Comprehension): Comprehension =
    combination(Difference, (s1, s2) => s1, p1, p2)

  def order(alist: Seq[(Expression, Direction)]): QueryMonad[Unit] =
    for {
      old <- currentQuery
      _ <- setQuery(old.order(alist))
    } yield ()

  def top(offset: Int, count: Int): QueryMonad[Unit] =
    for {
      old <- currentQuery
      _ <- setQuery(old.top(offset, count))
    } yield ()

}
