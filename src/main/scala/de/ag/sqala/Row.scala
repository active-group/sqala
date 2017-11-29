package de.ag.sqala

object Row {
  def make(vals: Any*): Row = vals.toArray[Any]

  def fromSeq(vals: Seq[Any]): Row = vals.toArray[Any]

  val empty = make()
}
