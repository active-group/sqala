package de.ag.sqala

import QueryMonad._
import scala.language.postfixOps

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
  import QueryMonad._

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

  def union(p2: Comprehension)(implicit ev:  QueryMonad[A] =:= Comprehension): Comprehension =
    combination(_.union(_), (s1, s2) => s1, this, p2)

  def intersect(p2: Comprehension)(implicit ev: QueryMonad[A] =:= Comprehension): Comprehension =
    combination(_.intersection(_), (s1, s2) => s1, this, p2)

  def divide(p2: Comprehension)(implicit ev: QueryMonad[A] =:= Comprehension): Comprehension =
    combination(_ / _, (s1, s2) => s1.difference(s2), this, p2)

  def subtract(p2: Comprehension)(implicit ev: QueryMonad[A] =:= Comprehension): Comprehension =
    combination(_.difference(_), (s1, s2) => s1, this, p2)

  /**
    * 
    * See [[buildQuery]] for a convenient way to get the monad result.
    */
  def run(state: QueryMonad.State = emptyState) =
    transform(state)
  
  def buildQuery(state: QueryMonad.State = emptyState): Query =
    run(state) match { case (r: Relation, s) =>
      s.query.project(
        r.scheme.columns.map { case s =>
          (s, AttributeRef(freshName(s, r.alias)))}
      )
    }
}

object QueryMonad {
  type Alias = Int
  
  type Comprehension = QueryMonad[Relation]

  // we need the environment for subqueries, where the query providing
  // the bindings is elsewhere
  case class State(alias: Alias, query: Query, env: Environment)

  val emptyState = State(0, Query.empty, Environment.empty)

  val getState: QueryMonad[State] = QueryMonad { st => (st, st) }

  def apply[A](x: A): QueryMonad[A] = QueryMonad { s: State => (x, s) }

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
      env <- currentEnvironment
      qscheme = q.getScheme()
      qenv = qscheme.environment
      scheme = transformScheme(qscheme)
      columns = scheme.columns
      fresh = columns.map { k => freshName(k,alias) }
      ps = (columns, fresh).zipped
      projectAlist = ps.map { (k, fresh) => (fresh, Expression.makeAttributeRef(k)) }
      qenvFresh = Environment.make(ps.map { (k, fresh) => (fresh, qenv(k)) } :_*)
      qq = q.project(projectAlist)
      _ <- setQuery(makeProduct(query, qq))
      _ <- setEnvironment(Environment.compose(env, qenvFresh))
    } yield Relation(alias, scheme)


  val currentEnvironment: QueryMonad[Environment] =
    QueryMonad { st0: State => (st0.env, st0) }

  def setEnvironment(env: Environment): QueryMonad[Unit] =
    QueryMonad { st0: State => ((), st0.copy(env = env)) }

  def embed(q: Query): Comprehension =
    addToProduct(_ * _, identity, q)

  def outer(q: Query): Comprehension =
    addToProduct(_.leftOuterProduct(_), _.toNullable, q)

  def project(alist: Seq[(String, Expression)]): Comprehension =
    for {
      alias <- newAlias
      query <- currentQuery
      env <- currentEnvironment
      query1 = query.extend(alist.map { case (k, v) => (freshName(k, alias), v) }, env)
      _ <- setQuery(query1)
      env1 = Environment.compose(env,
        Environment.make(alist.map { case (n, e) => (n, e.getType(env)) }:_*))
      _ <- setEnvironment(env1)
    } yield Relation(alias,
      RelationalScheme.make(alist.map { case (k, v) => (k, env1(k)) }))

  def restrict(expr: Expression): QueryMonad[Unit] =
    for {
      old <- currentQuery
      _ <- setQuery(old.restrict(expr))
    } yield ()

  def restrictOuter(expr: Expression): QueryMonad[Unit] =
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
      scheme = computeScheme(rel1.scheme, rel2.scheme)
      env0 <- currentEnvironment
      _ <- setEnvironment(Environment.compose(env0, scheme.environment))
    } yield Relation(alias, scheme)
  }

    def combination(op: (Query, Query) => Query,

                  computeScheme: (RelationalScheme, RelationalScheme) => RelationalScheme,
                  prod1: Comprehension, prod2: Comprehension): Comprehension =
    for {
      env0 <- currentEnvironment
      query0 <- currentQuery
      alias0 <- currentAlias
      (rel1, state1) = prod1.run(State(alias0, query0, env0))
      (rel2, state2) = prod2.run(State(state1.alias, query0, env0))
      res <- combination1(op, computeScheme, query0, rel1, state1.query, rel2, state2.query, alias0)
    } yield res

  def order(alist: Seq[(String, Direction)]): QueryMonad[Unit] =
    for {
      old <- currentQuery
      _ <- setQuery(old.order(alist))
    } yield ()

  def top(offset: Int, count: Int): QueryMonad[Unit] =
    for {
      old <- currentQuery
      _ <- setQuery(old.top(offset, count))
    } yield ()

  def subquery(c: Comprehension): QueryMonad[Expression] =
    for {
      st <- getState
      q = c.buildQuery(st.copy(query = Query.empty))
    } yield ScalarSubquery(q)
}
