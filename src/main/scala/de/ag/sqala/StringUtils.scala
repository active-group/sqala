package de.ag.sqala

import java.io.Writer

/**
 *
 */
object StringUtils {
  /**
   * Write all elements of a sequence of things, separated by a string
   * @param out     output sink
   * @param strings sequence of things to write
   * @param sep     separator to write between two consecutive things
   */
  def writeJoined(out: Writer, strings:Seq[String], sep:String) {
    if (!strings.isEmpty) {
      out.write(strings.head)
      strings.tail.foreach({s => out.write(sep); out.write(s)})
    }
  }

  /**
   * Call write method for all elements of a sequence of things, separating the writes by a string
   * @param out     output sink
   * @param things sequence of things to call write function for
   * @param writeProc write method to be called for each string in turn
   * @param sep     separator to write between two consecutive things
   */
  def writeJoined[T](out: Writer, things:Seq[T], sep:String, writeProc:(Writer, T) => Unit) {
    if (!things.isEmpty) {
      writeProc(out, things.head)
      things.tail.foreach({s => out.write(sep); writeProc(out, s)})
    }
  }

  def writeWithSpaceIfNotEmpty[T](out:Writer, things:Seq[T])(proc:(Seq[T]) => Unit) {
    if (!things.isEmpty) {
      out.write(" ")
      proc(things)
    }
  }

  def writeSpace(out:Writer) {
    out.write(' ')
  }
}
