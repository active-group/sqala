package de.ag.sqala

/*** evaluate a query in memory **/
object MemoryQuery {
  import Aliases._
  import Expression.GroupEnvironment

  def computeQueryResults(q: Query): Seq[IndexedSeq[Any]] =
    computeQueryResults(GroupEnvironment.empty, q)

  // FIXME Mike: This needs to be refactored to return a grouped result always. Noticed too late ...

  def computeQueryResults(groupEnv: GroupEnvironment, q: Query): Seq[IndexedSeq[Any]] = {

    def addValueEnv(scheme: RelationalScheme, row: IndexedSeq[Any]): GroupEnvironment =
      GroupEnvironment.make(scheme, row).compose(groupEnv)

    q match {
      case EmptyQuery => Seq(IndexedSeq.empty) // one empty row
      case BaseRelation(name, scheme, handle) => {
        handle match {
          case g: Galaxy => g.enumerate()
        }
      }
      case Projection(alist, sub) => {
        val scheme = sub.getScheme(groupEnv.scheme.environment)
        computeQueryResults(groupEnv, sub).map { row =>
          val env = addValueEnv(scheme, row)
          alist.map { case (name, exp) =>
            exp.eval(env)
          }.toIndexedSeq
        }
      }
      case Restriction(exp, sub) => {
        val scheme = sub.getScheme(groupEnv.scheme.environment)
        computeQueryResults(groupEnv, sub)
          .filter { row => exp.eval(GroupEnvironment.make(scheme, row)).asInstanceOf[Boolean] }
      }
      case Product(q1, q2) => {
        val en1 = computeQueryResults(groupEnv, q1)
        val en2 = computeQueryResults(groupEnv, q2)
        en1.flatMap { row1 =>
          en2.map(row1 ++ _)
        }
      }
      case Union(q1, q2) => {
        val en1 = computeQueryResults(groupEnv, q1)
        val en2 = computeQueryResults(groupEnv, q2)
        (en1.toSet ++ en2.toSet).toSeq
      }
      case Intersection(q1, q2) => {
        val en1 = computeQueryResults(groupEnv, q1)
        val en2 = computeQueryResults(groupEnv, q2)
          (en1.toSet.intersect(en2.toSet)).toSeq
      }
      // dividing q1[a,b] by q2[b]
      case Quotient(q1, q2) => {
        val env = groupEnv.scheme.environment()
        val s1 = q1.getScheme(env)
        val s2 = q2.getScheme(env)
        val r1 = computeQueryResults(groupEnv, q1)
        val r2 = computeQueryResults(groupEnv, q2)
        val extractA = s1.makeExtractor(s1.difference(s2))
        val extractB = s1.makeExtractor(s2)
        val groups = r1.groupBy(extractA)
        groups.filter { case (key, rows) =>
          r2.forall {  row2 =>
            rows.exists { row1 => row2 == extractB(row1) }
          }
        }.map(_._1).toSeq
      }
        /*
      case Group(cols, q) => {
        val scheme = q.getScheme(groupEnv.scheme.environment)
        val en = computeQueryResults(groupEnv, q)
        val colSq = cols.toSeq
        val grouped = en.groupBy { row =>
          colSq.map { col =>
            GroupEnvironment.make(scheme, row).lookup(col)
          }
        }
        grouped.map { case (vals, rows) =>

          ???

        }
        ???
      }
         */
      case Order(alist, q) => {
        val en = computeQueryResults(groupEnv, q)
        val scheme = q.getScheme(groupEnv.scheme.environment())
        val typesDirs = alist.map { case (col, dir) =>
          (groupEnv.scheme.environment()(col), dir)
        }
        val poses = alist.map { case (col, _) => scheme.pos(col) }
        val lt = rowLessThan(typesDirs)_
        en.sortWith { (row1, row2) =>
          val cs1 = poses.map(row1(_))
          val cs2 = poses.map(row2(_))
          lt(cs1, cs2)
        }
      }
      case Top(offset, length, q) => {
        val en = computeQueryResults(groupEnv, q)
        en.drop(offset).take(length)
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
  def rowLessThan(typesDirs: Seq[(Type, Direction)])(row1: Seq[Any], row2: Seq[Any]): Boolean = {
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
  def enumerate(): Seq[IndexedSeq[Any]]
}

object Galaxy {
  def fromSequence(sq: Seq[IndexedSeq[Any]]): Galaxy =
    new Galaxy {
      def enumerate() = sq
    }

  def makeBaseRelation(name: String, scheme: RelationalScheme, g: Galaxy) =
    Query.makeBaseRelation(name, scheme, g)

  def makeBaseRelation(name: String, scheme: RelationalScheme, sq: Seq[IndexedSeq[Any]]) =
    Query.makeBaseRelation(name, scheme, Galaxy.fromSequence(sq))

}
