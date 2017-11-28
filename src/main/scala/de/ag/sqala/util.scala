package de.ag.sqala

// FIXME: can be replaced by require
object Assertions {
  // like assert, not always on

  def ensure(t: Boolean, msg: String): Unit = {
    if (!t)
      throw new AssertionError(msg)
  }

  def ensure(t: Boolean): Unit =
    ensure(t, "assertion violation")
}
