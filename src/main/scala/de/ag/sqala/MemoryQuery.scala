package de.ag.sqala

import scala.annotation.tailrec

/*** evaluate a query in memory **/
object MemoryQuery {
  import Aliases._

  def computeQueryResults(q: Query): Seq[GroupedResult] =
    computeQueryResults(GroupedResult.empty, q)

  def computeQueryResults(grouped: GroupedResult, q0: Query): Seq[GroupedResult] = {
    val outputScheme = q0.getScheme(grouped.scheme.environment())
    q0 match {
      case EmptyQuery => Seq(GroupedResult.empty)
      case BaseRelation(name, scheme, handle) => {
        handle match {
          case g: Galaxy => g.enumerate().map { row => GroupedResult.make(scheme, row) }
        }
      }
      case Projection(alist, sub) => {
        val scheme = sub.getScheme(grouped.scheme.environment)
        computeQueryResults(grouped, sub).flatMap { grouped1 =>
          val gr = grouped1.compose(grouped)
          val rows =
            alist.map { case (name, exp) =>
              exp.evalAll(gr)
            }.transpose.map(Row.fromSeq _)
          rows.map { row =>
            GroupedResult(outputScheme,
              Row.empty, Seq(row))
          }
        }
      }
      case Restriction(exp, sub) => {
        val scheme = sub.getScheme(grouped.scheme.environment)
        computeQueryResults(grouped, sub)
          .flatMap { grouped1 =>
            val tests = exp.evalAll(grouped1).map(_.asInstanceOf[Boolean])
            val rows = grouped1.ungroupedRows.zip(tests).filter(_._2).map(_._1)
            if (rows.isEmpty)
              None
            else
              Some(GroupedResult(grouped1.scheme, grouped1.groupedRow, rows))
          }
      }
      case Product(q1, q2) => {
        val grs1 = computeQueryResults(grouped, q1)
        val grs2 = computeQueryResults(grouped, q2)
        grs1.flatMap { gr1 =>
          grs2.map(gr1.compose(_))
        }
      }
      case Union(q1, q2) => {
        val scheme = q1.getScheme(grouped.scheme.environment)
        val grs1 = computeQueryResults(grouped, q1)
        val grs2 = computeQueryResults(grouped, q2)
        scheme.grouped match {
          case None =>
            grs1.toSet.union(grs2.toSet).toSeq
          case Some(_) =>
            grs1.groupBy(_.groupedRow).map { case (groupedRow, ungroupeds) =>
              GroupedResult(outputScheme,
                groupedRow,
                ungroupeds.map(_.ungroupedRows.toSet).reduce(_.union(_)).toSeq)
            }.toSeq
        }
      }
      case Intersection(q1, q2) => {
        val scheme = q1.getScheme(grouped.scheme.environment)
        val grs1 = computeQueryResults(grouped, q1)
        val grs2 = computeQueryResults(grouped, q2)
        scheme.grouped match {
          case None =>
            grs1.toSet.intersect(grs2.toSet).toSeq
          case Some(_) =>
            grs1.groupBy(_.groupedRow).flatMap { case (groupedRow, ungroupeds) =>
              val ug =
                ungroupeds.map(_.ungroupedRows.toSet).reduce(_.intersect(_))
              if (ug.isEmpty)
                None
              else
                Some(GroupedResult(outputScheme, groupedRow, ug.toSeq))
            }.toSeq
        }
     }

      case Group(cols, q) => {
        val scheme = q.getScheme(grouped.scheme.environment())
        val en = computeQueryResults(grouped, q)
        val groupedSet = scheme.groupedSet
        val ungroupedColIndices = cols.toSeq.map { c =>
          scheme.groupedUngroupedPosition(c) match {
            case Left(_) => None // already grouped
            case Right(i) => Some(i)
          }
        }.flatten
        val groupedColIndices = {
          val s = ungroupedColIndices.toSet
          val groupedCount = scheme.columns.size - s.size
          (0 until groupedCount).filter(!s.contains(_))
        }

        en.map { group =>
          val g = 
            group.ungroupedRows.groupBy { row =>
              ungroupedColIndices.map(row(_))
            }
          g.map { case (grouped, ungrouped) =>
            GroupedResult(outputScheme, group.groupedRow ++ grouped, ungrouped)
          }
        }.flatten
      }

      case Quotient(q1, q2) => {
        val env = grouped.scheme.environment()
        val s1 = q1.getScheme(env)
        val s2 = q2.getScheme(env)
        val sd = s1.difference(s2)
        val r1 = computeQueryResults(grouped, q1)
        val r2 = computeQueryResults(grouped, q2)
        val rows2 = r2.flatMap(_.flatten())
        // Makes my brain hurt.

        // We need to group r1 by the cols of s1 - s2.  To that end,
        // we need to split each grouped in r1 along those lines
        // first, so that the cols of s1 - s2 are the same in each
        // grouped.

        // Now if we have splinters from two different groupeds in r1,
        // can they have the same columns in s1 - s2?  No, because
        // s1 - s2 is a superset of the grouped columns in s1.

        // So that means that we can use the grouping to solve the
        // problem: Just look for all the rows of r2 in each r1.

        // All this is for the case where there's grouping going on in
        // r2.

        // What if nothing's grouped in s1? Then we need to do
        // grouping here - different case.
        s1.grouped match {
          case None => {
            val diffPosesIn1 = sd.columns.map(s1.groupedUngroupedPosition(_).right.get)
            val s2PosesIn1 = s2.columns.map(s1.groupedUngroupedPosition(_).right.get)
            // extract columns of sd from a grouped
            def extractDiff(grouped1: GroupedResult): Row =
              // note there's only one row
              diffPosesIn1.map(grouped1.ungroupedRows.head(_))

            def extract2(grouped1: GroupedResult): Row =
              // ditto
              s2PosesIn1.map(grouped1.ungroupedRows.head(_))

            val groups = r1.groupBy(extractDiff _)
            val rows =
              groups.filter { case (key, groupeds) =>
                rows2.forall { row2 =>
                  groupeds.exists { grouped1 => row2 == extract2(grouped1) }
                }
              }.map(_._1)

            rows.map(GroupedResult.make(sd, _)).toSeq
          }
          case Some(_) => {
            // sequence of maps, maps sd columns to grouped
            val grmps = r1.map { grouped =>
              grouped.splitBy { ungroupedRow =>
                sd.columns.map { col =>
                  s1.groupedUngroupedPosition(col) match {
                    case Left(pos) => grouped.groupedRow(pos)
                    case Right(pos) => ungroupedRow(pos)
                  }
                }
              }
            }
            val poses2in1 = s2.columns.map(s1.groupedUngroupedPosition(_).right.get)
            grmps.flatMap { mp =>
              // after filtering, we can re-merge
              val groupeds =
                mp.values.filter { case grouped =>
                  rows2.forall { row2 =>
                    grouped.ungroupedRows.exists { ungroupedRow =>
                      poses2in1.map(ungroupedRow(_)) == row2
                    }
                  }
                }
              if (groupeds.isEmpty)
                None
              else
                Some(groupeds.reduce(_.merge(_)))
            }
          }
        }
      }

      case Order(alist, q) => {
        val en = computeQueryResults(grouped, q).map(_.ungroupedRows.head)
        val scheme = q.getScheme(grouped.scheme.environment())
        val env = grouped.scheme.environment()
        val typesDirs = alist.map { case (col, dir) =>
          (env(col), dir)
        }
        val poses = alist.map { case (col, _) => scheme.pos(col) }.toIndexedSeq
        val lt = rowLessThan(typesDirs)_
        en.sortWith { (row1, row2) =>
          val cs1 = poses.map(row1(_))
          val cs2 = poses.map(row2(_))
          lt(cs1, cs2)
        }.map { row => GroupedResult(outputScheme, Row.empty, Seq(row)) }
      }

      case Top(offset, length, q) => {
        val en = computeQueryResults(grouped, q)
        // hack off the front
        val ps = en
          .scanLeft(offset) { (offsetLeft, next) => offsetLeft - next.ungroupedRows.size }
          .zip(en)
          .dropWhile { case (offsetLeft, _) => offsetLeft > 0 }
        if (ps.isEmpty)
          Seq.empty[GroupedResult]
        else {
          val (offsetLeft, grouped) = ps.head
          val (_, rest0) = grouped.splitAt(offsetLeft - offset)
          // this now starts at the right element:
          val rest = rest0 ++ ps.tail.map(_._2)
          // now need to hack off the tail
          val (front, rear) = rest
            .scanLeft(length) { (lengthLeft, next) => lengthLeft - next.ungroupedRows.size }
            .zip(rest)
            .span { case (lengthLeft, _) => lengthLeft > 0 }
          if (rear.isEmpty)
            front.map(_._2).toSeq
          else {
            val (lengthLeft, grouped) = rear.head
            val (rest0, _) = grouped.splitAt(lengthLeft)
            (front.map(_._2) ++ rest0).toSeq
          }
        }
      }
    }
  }

  // FIXME: put this in Type
  def valueLessThan(ty: Type, val1: Any, val2: Any) = {
    val1 match {
      case _: String => val1.asInstanceOf[String] < val2.asInstanceOf[String]
      case _: Long => val1.asInstanceOf[Long] < val2.asInstanceOf[Long]
      case _: Int => val1.asInstanceOf[Int] < val2.asInstanceOf[Int]
    }
  }

  /// check if one row is less than another
  def rowLessThan(typesDirs: Seq[(Type, Direction)])(row1: Row, row2: Row): Boolean = {
    for (((ty, dir), (v1, v2)) <- typesDirs.zip(row1.zip(row2))) {
      if (v1 != v2) {
        val comp = valueLessThan(ty, v1, v2)
        dir match {
          case Direction.Ascending => return comp
          case Direction.Descending => return !comp
        }
      }
    }
    return false
  }
}

/**
  * This is a "primitive relation" within a universe.
  */
trait Galaxy {
  /** enumerate all the tuples in the relation */
  def enumerate(): Seq[Row]
}

object Galaxy {
  def fromSequence(sq: Seq[Row]): Galaxy =
    new Galaxy {
      def enumerate() = sq
    }

  def makeBaseRelation(name: String, scheme: RelationalScheme, g: Galaxy) =
    Query.makeBaseRelation(name, scheme, g)

  def makeBaseRelation(name: String, scheme: RelationalScheme, sq: Seq[Row]) =
    Query.makeBaseRelation(name, scheme, Galaxy.fromSequence(sq))

}
