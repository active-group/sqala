package de.ag.sqala

/**
 * Operators in relational algebra and SQL
 */
sealed abstract
class Operator(val name: String, val arity: Int) {
  def rangeDomain(domainCheck:DomainChecker, operands:Seq[Domain]): Domain
}

abstract case class InfixOperator(override val name:String) extends Operator(name, 2) {
  def rangeDomain(domainCheck:DomainChecker, operand1:Domain, operand2:Domain):Domain
  
  override def rangeDomain(domainCheck:DomainChecker, operands:Seq[Domain]) =
    rangeDomain(domainCheck, operands(0), operands(1))
}

abstract case class PrefixOperator(override val name:String) extends Operator(name, 1) {
  def rangeDomain(domainCheck:DomainChecker, operand1:Domain):Domain

  override def rangeDomain(domainCheck:DomainChecker, operands:Seq[Domain]) =
    rangeDomain(domainCheck, operands(0))
}

abstract case class PostfixOperator(override val name:String) extends Operator(name, 1) {
  def rangeDomain(domainCheck:DomainChecker, operand1:Domain):Domain

  override def rangeDomain(domainCheck:DomainChecker, operands:Seq[Domain]) =
    rangeDomain(domainCheck, operands(0))
}

object Operator {
  abstract class Notation
  case object Prefix extends Notation
  case object Infix extends Notation
  case object Postfix extends Notation

  object Eq extends InfixOperator("=") with RangeType.Equality
  object Ne extends InfixOperator("<>") with RangeType.Equality
  object Lt extends InfixOperator("<") with RangeType.ComparatorRangeType
  object Gt extends InfixOperator(">") with RangeType.ComparatorRangeType
  object Le extends InfixOperator("<=") with RangeType.ComparatorRangeType
  object Ge extends InfixOperator(">=") with RangeType.ComparatorRangeType
  object NLt extends InfixOperator("!<") with RangeType.ComparatorRangeType
  object NGt extends InfixOperator("!>") with RangeType.ComparatorRangeType

  object And extends InfixOperator("AND") with RangeType.InfixEndomorphic { val domain = Domain.Boolean }
  object Or extends InfixOperator("OR") with RangeType.InfixEndomorphic{ val domain = Domain.Boolean }
  object Like extends InfixOperator("LIKE") with RangeType.InfixEndomorphic{ val domain = Domain.String }
  object In extends InfixOperator("IN") {
    def rangeDomain(domainCheck: DomainChecker, operand1:Domain, operand2:Domain): Domain = {
      domainCheck{ fail =>
        val operand1Set = Domain.Set(operand1)
        if (!operand2.domainEquals(operand1Set)) fail(operand1Set, operand2)
      }
      Domain.Boolean
    }
  }
  object Cat extends InfixOperator("+"){
    def rangeDomain(domainCheck: DomainChecker, operand1:Domain, operand2:Domain): Domain = {
      domainCheck{ fail =>
        RangeType.checkIsStringLike(fail, operand1)
        RangeType.checkIsStringLike(fail, operand2)
        RangeType.checkIsSameDomain(fail, operand1, operand2)
      }
      operand1
    }
  }
  object Plus extends InfixOperator("+") with RangeType.InfixNumeric
  object Minus extends InfixOperator("-") with RangeType.InfixNumeric
  object Mul extends InfixOperator("*") with RangeType.InfixNumeric
  object Div extends InfixOperator("/") with RangeType.InfixNumeric
  object Mod extends InfixOperator("MOD") with RangeType.InfixEndomorphic { val domain = Domain.Integer }
  object BitNot extends PrefixOperator("~") with RangeType.PrefixEndomorphic { val domain = Domain.Integer }
  object BitAnd extends InfixOperator("&") with RangeType.InfixEndomorphic { val domain = Domain.Integer }
  object BitOr extends InfixOperator("|") with RangeType.InfixEndomorphic { val domain = Domain.Integer }
  object BitXor extends InfixOperator("^") with RangeType.InfixEndomorphic { val domain = Domain.Integer }

  object Not extends PrefixOperator("NOT") with RangeType.PrefixEndomorphic { val domain = Domain.Boolean }
  object IsNull extends PostfixOperator("IS NULL") with RangeType.Nullable
  object IsNotNull extends PostfixOperator("IS NOT NULL") with RangeType.Nullable
  object Length extends PrefixOperator("LENGTH") {
    /* A => Int where A is string-like */
    def rangeDomain(domainCheck:DomainChecker, operand1:Domain): Domain = {
      domainCheck{ fail => RangeType.checkIsStringLike(fail, operand1)}
      Domain.Integer
    }
  }

  object Count extends PrefixOperator("COUNT") {
    /* A => Int */
    def rangeDomain(domainCheck:DomainChecker, operand1:Domain): Domain = {
      Domain.Integer
    }
  }
  object Sum extends PrefixOperator("SUM") with RangeType.PrefixNumeric
  object Avg extends PrefixOperator("AVG") with RangeType.PrefixNumeric
  object Min extends PrefixOperator("MIN") with RangeType.PrefixNumeric
  object Max extends PrefixOperator("MAX") with RangeType.PrefixNumeric
  object StdDev extends PrefixOperator("StdDev") with RangeType.Statistics
  object StdDevP extends PrefixOperator("StdDevP") with RangeType.Statistics
  object Var extends PrefixOperator("Var") with RangeType.Statistics
  object VarP extends PrefixOperator("VarP") with RangeType.Statistics
}

object RangeType {
  def checkIsOrdered(fail: DomainChecker.FailProc, d:Domain) {
    if(!d.isOrdered) fail("ordered", d)
  }

  def checkIsSameDomain(fail: DomainChecker.FailProc, d1:Domain, d2:Domain) {
    if (!d1.domainEquals(d2)) fail("same domain", (d1, d2))
  }

  def checkIsNullable(fail: DomainChecker.FailProc, d: Domain) {
    if (!d.isInstanceOf[Domain.Nullable]) fail("nullable", d)
  }

  def checkIsStringLike(fail: DomainChecker.FailProc, d: Domain) {
    if (!d.isStringLike) fail("string-like", d)
  }

  def checkIsNumeric(fail: DomainChecker.FailProc, d: Domain) {
    if (!d.isNumeric) fail("numeric", d)
  }

  /** Range type A, A => A  */
  trait InfixEndomorphic {
    val domain:Domain
    def rangeDomain(domainCheck: DomainChecker, operand1:Domain, operand2:Domain):Domain = {
      domainCheck { fail =>
        checkIsSameDomain(fail, domain, operand1)
        checkIsSameDomain(fail, operand1, operand2)
      }
      domain
    }
  }
  /** A => A  */
  trait PrefixEndomorphic {
    val domain:Domain
    def rangeDomain(domainCheck: DomainChecker, operand1:Domain):Domain = {
      domainCheck { fail => checkIsSameDomain(fail, domain, operand1)}
      domain
    }
  }

  /** Range type: A, A => Boolean where A is ordered*/
  trait ComparatorRangeType {
    def rangeDomain(domainCheck: DomainChecker, operand1:Domain, operand2:Domain):Domain = {
      domainCheck { fail =>
        checkIsSameDomain(fail, operand1, operand2)
        checkIsOrdered(fail, operand1)
        checkIsOrdered(fail, operand2)
      }
      Domain.Boolean
    }
  }

  /** Range type: A, A => Boolean */
  trait Equality {
    def rangeDomain(domainCheck: DomainChecker, operand1:Domain, operand2:Domain):Domain = {
      domainCheck{ fail => checkIsSameDomain(fail, operand1, operand2) }
      Domain.Boolean
    }
  }

  /** Range type: Nullable => Boolean */
  trait Nullable {
    def rangeDomain(domainCheck: DomainChecker, operand:Domain): Domain = {
      domainCheck{ fail => checkIsNullable(fail, operand) }
      Domain.Boolean
    }
  }

  /** Range type: A, A => A where A is numeric */
  trait InfixNumeric {
    def rangeDomain(domainCheck: DomainChecker, operand1:Domain, operand2:Domain):Domain = {
      domainCheck{ fail =>
        checkIsNumeric(fail, operand1)
        checkIsNumeric(fail, operand2)
        checkIsSameDomain(fail, operand1, operand2)
      }
      operand1
    }
  }

  /** Range type: A => A where A is numeric */
  trait PrefixNumeric {
    def rangeDomain(domainCheck: DomainChecker, operand1:Domain):Domain = {
      domainCheck{ fail => checkIsNumeric(fail, operand1) }
      operand1
    }
  }

  /** Range type: A => Double where A is numeric */
  trait Statistics {
    def rangeDomain(domainCheck: DomainChecker, operand1:Domain):Domain = {
      domainCheck{ fail => checkIsNumeric(fail, operand1) }
      Domain.Double
    }
  }


}