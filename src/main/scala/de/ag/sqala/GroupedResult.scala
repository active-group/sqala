package de.ag.sqala

/**
  * A "grouped" is an encoding of a sequence of rows, where `groupedRow`
  * contains the grouped columns, i.e. those whose values are the same across all rows.
  * `ungroupedRows` contains the other columns.
  */
case class GroupedResult(scheme: RelationalScheme, groupedRow: Row, ungroupedRows: Seq[Row]) {

  require(!ungroupedRows.isEmpty)
  require(scheme.columns.size == groupedRow.size + ungroupedRows.head.size)
  require(if (!scheme.isGrouped()) ungroupedRows.size == 1 else true)

  def lookup(name: String): Either[Any, Seq[Any]] =
    scheme.groupedUngroupedPosition(name) match {
      case Left(groupedIndex) => Left(groupedRow(groupedIndex))
      case Right(ungroupedIndex) => Right(ungroupedRows.map(_(ungroupedIndex)))
    }

  /** essentially do a cartasian product of two Grouped */
  def compose(other: GroupedResult): GroupedResult = {
    val newUngroupedRows =
      ungroupedRows.flatMap { row =>
        other.ungroupedRows.map(row ++ _)
      }
    GroupedResult(scheme ++ other.scheme,
      groupedRow ++ other.groupedRow,
      newUngroupedRows)
  }

  /** first column, no matter if grouped or ungrouped */
  lazy val col0 =
    scheme.groupedUngroupedPosition(scheme.columns(0)) match {
      case Left(i) => groupedRow(i)
      case Right(i) => ungroupedRows(0)(i)
    }

  /** split at a certain index, which becomes the first element of the second return value */
  def splitAt(n: Int): (Option[GroupedResult], Option[GroupedResult]) = {
    val (ungrouped1, ungrouped2) = ungroupedRows.splitAt(n)
    (GroupedResult.maybeMake(scheme, groupedRow, ungrouped1),
      GroupedResult.maybeMake(scheme, groupedRow, ungrouped2))
  }

  /** split grouped according to function on ungrouped rows
    * 
    * support function for quotient
    */
  def splitBy[A](f: Row => A): Map[A, GroupedResult] = {
    ungroupedRows.groupBy(f).mapValues { ungroupedRows =>
      GroupedResult(scheme, groupedRow, ungroupedRows)
    }
  }

  /** undo a `splitBy` */
  def merge(other: GroupedResult) = {
    require(scheme == other.scheme)
    require(groupedRow == other.groupedRow)
    GroupedResult(scheme, groupedRow, ungroupedRows ++ other.ungroupedRows)
  }

  lazy val positions = scheme.columns.map(scheme.groupedUngroupedPosition(_))

  /** give us a set of rows */
  def flatten(): Seq[Row] = {
    ungroupedRows.map { ungroupedRow =>
      positions.map {
        case Left(pos) => groupedRow(pos)
        case Right(pos) => ungroupedRow(pos)
      }
    }
  }

  def oneByOne(): Seq[GroupedResult] =
    ungroupedRows.map { row => GroupedResult(scheme, groupedRow, Seq(row)) }

}

object GroupedResult {
  def make(scheme: RelationalScheme, row: Row): GroupedResult = {
    assert(scheme.grouped.isEmpty)
    GroupedResult(scheme, Row.empty, Seq(row))
  }

  def maybeMake(scheme: RelationalScheme, groupedRow: Row, ungroupedRows: Seq[Row]): Option[GroupedResult] =
    if (ungroupedRows.isEmpty)
      None
    else
      Some(GroupedResult(scheme, groupedRow, ungroupedRows))

  def makeSeq(scheme: RelationalScheme, rows: Seq[Row]): Seq[GroupedResult] =
    rows.map(GroupedResult.make(scheme, _))

  def make(name: String, ty: Type, value: Any): GroupedResult =
    GroupedResult.make(RelationalScheme.make(name, ty), Row.make(value))

  val empty = GroupedResult.make(RelationalScheme.empty, Row.empty)

}

