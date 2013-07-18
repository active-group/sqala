package de.ag.sqala

class Cursor //FIXME

sealed abstract class Handle
case class JDBCHandle(connection: java.sql.Connection) extends Handle

/**
 *
 */
trait DbConnection {
  val kind: Symbol       // what kind of database we're attached to
  val name: String       // same, for humans
  val handle: Handle     // DB-specific connection handle (?)
  val sqlWriteParameterization: SqlWriteParameterization

  def close():Unit
  def query(sqlQuery:SqlQuery, schema:Schema): Cursor
  def insert(s:String /*FIXME*/, schema:Schema, values:Seq[Any/*FIXME*/]): Any /*FIXME*/ // FIXME structured?
  def delete(s:String /*FIXME*/, expr:SqlExpr): Any /*FIXME*/ // FIXME structured?
  def update(s:String /*FIXME*/, schema:Schema, expr:SqlExpr, a:Seq[(String, SqlExpr)]): Int // FIXME structured?
  def execute(sql:String):Any /*FIXME*/ // run raw SQL, for emergencies
}
