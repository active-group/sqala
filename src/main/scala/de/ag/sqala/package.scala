package de.ag

package object sqala {
  type Environment = Map[String, Type]

  type Row = IndexedSeq[Any]
}
