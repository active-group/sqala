package de.ag.sqala

object SQLUniverse {
  // woefully incomplete
  val eq = new Rator("=",
    // FIXME: coercibility
    { _ => Type.boolean },
    // FIXME: coercion
    { args => args(0) == args(1) }) with HasSQLOperator {
    val sqlOperator = SQLOperator.eq
  }

  val in = new Rator("IN",
    // FIXME: coercibility
    { _ => Type.boolean },
    // FIXME: coercion
    { args => args(1).asInstanceOf[Set[Any]].contains(args(0)) }) with HasSQLOperator {
    val sqlOperator = SQLOperator.in
  }

  val oneOf = new Rator("ONE_OF",
    { _ => Type.boolean },
    { args => args.tail.toSet.contains(args.head) }) with HasSQLOperator {
    val sqlOperator = SQLOperator.oneOf
  }
}
