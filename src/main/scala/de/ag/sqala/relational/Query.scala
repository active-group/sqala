package de.ag.sqala.relational

import de.ag.sqala._
import de.ag.sqala.relational.Schema.Attribute

/**
 * Query in a relational algebra.
 */
sealed abstract class Query {

  /**
   * Schema of query
   * @return Schema of query.  No checks are performed, but may throw NoSuchElementException
   *         if attribute reference refers to an unknown attribute.
   */
  def schema(): Schema = {
    schema(Environment.empty, IgnoringDomainChecker)
  }

  /**
   * Check domains of underlying schema. Throws DomainCheckException if there are domain
   * mismatches and NoSuchElementException if an attribute reference refers to an unknown attribute.
   */
  def checkDomains() {
    schema(Environment.empty, ExceptionThrowingDomainChecker)
  }

  /**
   * Schema of query, after checkDomains() is called.
   *
   * This is faster than calling checkDomains() and schema() in succession.
   *
   * @return Schema of query. See checkDomains() for exceptions that may be thrown.
   */
  def checkedSchema(): Schema = {
    schema(Environment.empty, ExceptionThrowingDomainChecker)
  }

  /**
   * Schema of the query in a certain environment.
   * @param env           environment
   * @param domainCheck   DomainChecker; use DomainChecker.IgnoringDomainChecker to not perform domain checking
   * @return              Schema of the query in the given environment.
   *                      Throws DomainCheckException if a domain check fails,
   *                      or other exceptions if no domain check is performed but the query is not sound.
   */
  def schema(env:Environment, domainCheck:DomainChecker): Schema = {
    def toEnv(schema:Schema) =
      schema.toEnvironment.compose(env)

    /** common for Query.{Union,Intersection,Difference} */
    def uiq(query: Query, query1: Query, query2: Query): Schema = {
      val schema1 = rec(query1)
      domainCheck {fail =>
        if (!schema1.equals(rec(query2))) fail(schema1.toString, query)
      }
      schema1
    }

    def rec(q:Query): Schema = q match {
      case Query.Empty => Schema.empty
      case Query.Base(_, schema) => schema
      case Query.Project(subset, query) =>
        val baseSchema = rec(query)
        domainCheck { fail =>
          subset.foreach {
            case (attr, expr) => if (expr.isAggregate) fail("non-aggregate", expr)}
        }
        Schema(
          subset.map{case (attr, expr) =>
            val domain = toEnv(baseSchema).expressionDomain(expr, domainCheck)
            domainCheck { fail => if (domain.isInstanceOf[Domain.Product]) fail("non-product domain", domain) }
            (attr, domain)
          }.toSeq
        )
      case Query.Restrict(expr, query) =>
        val schema = rec(query)
        domainCheck { fail =>
          val domain = toEnv(schema).expressionDomain(expr, domainCheck)
          if (!domain.isInstanceOf[Domain.Boolean.type]) fail("boolean", expr)
        }
        schema
      case Query.Product(query1, query2) =>
        val schema1 = rec(query1)
        val schema2 = rec(query2)
        domainCheck { fail =>
          val env2 = schema2.toEnvironment
          // no two attributes with same name; avoid creating both environments
          schema1.schema.foreach {
            case (attr, domain) =>
              if (env2.contains(attr)) fail("non-duplicate " + attr, attr)
          }
        }
        Schema(schema1.schema ++ schema2.schema)
      case Query.Quotient(query1, query2) =>
        val schema1 = rec(query1)
        val schema2 = rec(query2)
        domainCheck { fail =>
          val env1 = schema1.toEnvironment
          schema2.schema.foreach{ case (attr2, domain2) =>
            env1.get(attr2) match {
              case None => fail("schema with attribute " + attr2, schema1)
              case Some(domain1) => if (!domain1.domainEquals(domain2))
                fail("environment where attribute %s is of domain %s".format(attr2, domain2),
                  "environment where attribute %s is of domain %s". format(attr2, domain1))
            }
          }
        }
        schema1.difference(schema2)
      case Query.Union(q1, q2) => uiq(q, q1, q2)
      case Query.Intersection(q1, q2) => uiq(q, q1, q2)
      case Query.Difference(q1, q2) => uiq(q, q1, q2)
      case Query.GroupingProject(alist, query) =>
        val schema = rec(query)
        val environment = toEnv(schema)
        Schema(
          alist.map{case (attr, expr) =>
            val domain = environment.expressionDomain(expr, domainCheck)
            domainCheck { fail => if (domain.isInstanceOf[Domain.Product]) fail("non-product domain", domain) }
            (attr, domain)
          }
        )
      case Query.Order(by, query) =>
        val schema = rec(query)
        val env = toEnv(schema)
        domainCheck { fail =>
          by.foreach { case (expr, order) =>
            val domain = env.expressionDomain(expr, domainCheck)
            if (!domain.isOrdered) fail("ordered domain", domain)}
        }
        schema
      case Query.Top(n, query) => rec(query)
    }
    rec(this)
  }

  /**
   * Convert this query to an SQL Table (query)
   * @return SQL table (query) equivalent to this relational query
   */
  def toSqlTable:sql.Table = {
    /** helper method for product case */
    def product(query1: relational.Query, query2: relational.Query): sql.Table = {
      val table1 = query1.toSqlTable
      val table2 = query2.toSqlTable
      table1 match { // micro-optimizing 'SELECT *' queries
        case select1:sql.Table.Select if select1.attributes.isEmpty => addTable(select1, table2)
        case _ => table2 match {
          case select2:sql.Table.Select if select2.attributes.isEmpty => addTable(select2, table1)
          case _ => sql.Table.makeSelect(from=Seq(sql.Table.SelectFromTable(table1, None),
            sql.Table.SelectFromTable(table2, None)))
        }
      }
    }

    /** Append table2 to FROM-clause of sql-Select table1 */
    def addTable(table1:sql.Table.Select, table2:sql.Table):sql.Table =
      table1.copy(from = table1.from ++ Seq(sql.Table.SelectFromTable(table2, None)))

    /** helper method for quotient case */
    def quotient(query1: relational.Query, query2: relational.Query): sql.Table = {
      val schema1 = query1.schema()
      val schema2 = query2.schema()
      val diffSchema = schema1.difference(schema2)
      if (schema2.isUnary) {
        /*
        ;; from Matos, Grasser: A Simpler (and Better) SQL Approach to Relational Division
        ;; SELECT A
        ;; FROM T1
        ;; WHERE B IN ( SELECT B FROM T2 )
        ;; GROUP BY A
        ;; HAVING COUNT(*) =
        ;; ( SELECT COUNT (*) FROM T2 );
        */
        val table1 = query1.toSqlTable
        val table2 = query2.toSqlTable
        val name2 = schema2.schema.head._1
        sql.Table.makeSelect(
          from = Seq(sql.Table.SelectFromTable(table1, None)),
          attributes = diffSchema.schema.map{case (attr, domain) => sql.Table.SelectAttribute(sql.Expr.Column(attr), Some(attr))},
          where = Seq(sql.Expr.App(Operator.In, Seq(sql.Expr.Column(name2), sql.Expr.SubTable(table2)))),
          groupBy = diffSchema.schema.map{case (attr, domain) => sql.Expr.Column(attr)},
          having = Some(sql.Expr.App(Operator.Eq, Seq(
            sql.Expr.App(Operator.Count, Seq(sql.Expr.Column(diffSchema.schema.head._1))),
            sql.Expr.SubTable(
              sql.Table.makeSelect(from = Seq(sql.Table.SelectFromTable(table2, None)),
                attributes = Seq(sql.Table.SelectAttribute(sql.Expr.App(Operator.Count, Seq(sql.Expr.Column(name2))), None)))
            )
          )))
        )
      } else {
        val diffProjectAlist = diffSchema.schema.map{case (attr, domain) => (attr, relational.Expr.AttributeRef(attr))}
        val q1ProjectAlist = schema1.schema.map{case (attr, domain) => (attr, relational.Expr.AttributeRef(attr))}
        val pruned = relational.Query.Project(diffProjectAlist, query1)
        relational.Query.Difference(pruned,
          relational.Query.Project(diffProjectAlist,
            relational.Query.Difference(
              relational.Query.Project(q1ProjectAlist,
                relational.Query.Product(query2, pruned)),
              query1
            ))).toSqlTable
      }
    }

    /** Convert sequence of (Attribute, Expr) to sequence of attributes of a sql table-select */
    def alistToSql(tuples: Seq[(Schema.Attribute, Expr)]): Seq[sql.Table.SelectAttribute] = {
      tuples.map{case (attr, expr) => sql.Table.SelectAttribute(alias=Some(attr), expr=expr.toSqlExpr)}
    }

    /** helper method converting relational query to sql table-select */
    def queryToSelect(query:Query): sql.Table.Select =
      query.toSqlTable.toSelect

    this match {
      case Query.Empty => sql.Table.Empty
      case Query.Base(name, schema) => // TODO what about handle?
        /* schemeql2 has this:
          (if (not (sql-table? (base-relation-handle q)))
              (assertion-violation 'query->sql
                                    "base relation not an SQL table"
                                    q))
         */
        sql.Table.Base(name, schema)
      case Query.Project(subset, query) =>
        val select = queryToSelect(query)
        val attributes: Seq[sql.Table.SelectAttribute] = if (subset.isEmpty) {
          Seq(sql.Table.SelectAttribute(sql.Expr.Const(sql.Expr.Literal.String("dummy")), None))
        } else {
          alistToSql(subset)
        }
        // TODO what about `(set-sql-nullary? sql #t)` ?
        select.copy(attributes = attributes)
      case Query.Restrict(expr, query) =>
        val select = queryToSelect(query)
        select.copy(where = expr.toSqlExpr +: select.where)
      case Query.Product(query1, query2) => product(query1, query2)
      case Query.Quotient(query1, query2) => quotient(query1, query2)
      case Query.Union(q1, q2) => sql.Table.Combine(sql.Expr.CombineOp.Union, q1.toSqlTable, q2.toSqlTable)
      case Query.Intersection(q1, q2) => sql.Table.Combine(sql.Expr.CombineOp.Intersect, q1.toSqlTable, q2.toSqlTable)
      case Query.Difference(q1, q2) => sql.Table.Combine(sql.Expr.CombineOp.Except, q1.toSqlTable, q2.toSqlTable)
      case Query.GroupingProject(alist, query) => // TODO consider merging GroupingProject with Project (keeping isAggregate filter)
        val select = queryToSelect(query)
        val groupByClauses = alist
          .map(_._2)
          .filterNot(_.isAggregate)
          .map(_.toSqlExpr)
        select.copy(attributes = alistToSql(alist),
          groupBy = groupByClauses ++ select.groupBy)
      case Query.Order(by, query) =>
        val select = queryToSelect(query)
        val newOrder = by.map {
          case (expr, direction) => sql.Table.SelectOrderBy(expr.toSqlExpr, direction)
        }
        select.copy(orderBy = newOrder ++ select.orderBy)
      case Query.Top(n, query) =>
        val select = queryToSelect(query)
        select.copy(extra = " LIMIT %d ".format(n) +: select.extra)
    }
  }

}

object Query {
  type Name = String
  /** an empty query */
  case object Empty extends Query
  /** named schema */
  case class Base(name:Query.Name,
                  baseSchema:Schema
                  /* TODO handle? */) extends Query
  /** Projection (select and alias columns) */
  case class Project(subset:Seq[(Attribute, Expr)], query:Query) extends Query // map newly bound attributes to expressions
  /** Restriction (select rows) */
  case class Restrict(expr:Expr /*returning boolean*/, query:Query) extends Query
  /** (Cross-) Product */
  case class Product(query1:Query, query2:Query) extends Query
  /** Union (contents of both queries, "vertical cat") */
  case class Union(query1:Query, query2:Query) extends Query
  /** Intersection (rows common to both queries) */
  case class Intersection(query1:Query, query2:Query) extends Query
  /**
   * Quotient (aka 'division')
   *
   * keep columns of query1 not in query2, and rows of query1 that contain all rows of query2).
   * If Y = Z - X (ie. Z = X `union` Y), then
   * R(Z) / S(X) = T1 - T2 where
   * T1 = Project(Y, R) and
   * T2 = Project(Y, Product(S, T1) - R)
   */
  case class Quotient(query1:Query, query2:Query) extends Query
  /* Difference (rows in query1 not in query2) */
  case class Difference(query1:Query, query2:Query) extends Query
  /**
   * Like Project, but the underlying query is grouped by the non-aggregate expressions in
   * the grouping (hu? FIXME)
   */
  case class GroupingProject(grouping:Seq[(Attribute, Expr)], query:Query) extends Query
  /** Order rows */
  case class Order(by:Seq[(Expr, OrderDirection)], query:Query) extends Query
  /** Top n entries */
  case class Top(n:Int, query:Query) extends Query

  // TODO add make-extend
}