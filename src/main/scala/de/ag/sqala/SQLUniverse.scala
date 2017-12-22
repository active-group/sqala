package de.ag.sqala

object SQLUniverse {
  // woefully incomplete
  val eq = new Rator("=",
    // FIXME: coercibility
    { _ => Type.boolean },
    // FIXME: coercion
    { args => args(0) == args(1) })
    with HasSQLOperator {
      val sqlOperator = SQLOperator.eq
    }
}
