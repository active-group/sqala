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

  object Eq extends InfixOperator("=") with RangeDomain.Equality
  object Ne extends InfixOperator("<>") with RangeDomain.Equality
  object Lt extends InfixOperator("<") with RangeDomain.ComparatorRangeDomain
  object Gt extends InfixOperator(">") with RangeDomain.ComparatorRangeDomain
  object Le extends InfixOperator("<=") with RangeDomain.ComparatorRangeDomain
  object Ge extends InfixOperator(">=") with RangeDomain.ComparatorRangeDomain
  object NLt extends InfixOperator("!<") with RangeDomain.ComparatorRangeDomain
  object NGt extends InfixOperator("!>") with RangeDomain.ComparatorRangeDomain

  object And extends InfixOperator("AND") with RangeDomain.InfixEndomorphic { val domain = Domain.Boolean }
  object Or extends InfixOperator("OR") with RangeDomain.InfixEndomorphic{ val domain = Domain.Boolean }
  object Like extends InfixOperator("LIKE") with RangeDomain.InfixEndomorphic{ val domain = Domain.String }
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
        RangeDomain.checkIsStringLike(fail, operand1)
        RangeDomain.checkIsStringLike(fail, operand2)
        RangeDomain.checkIsSameDomain(fail, operand1, operand2)
      }
      operand1
    }
  }
  object Plus extends InfixOperator("+") with RangeDomain.InfixNumeric
  object Minus extends InfixOperator("-") with RangeDomain.InfixNumeric
  object Mul extends InfixOperator("*") with RangeDomain.InfixNumeric
  object Div extends InfixOperator("/") with RangeDomain.InfixNumeric
  object Mod extends InfixOperator("MOD") with RangeDomain.InfixEndomorphic { val domain = Domain.Integer }
  object BitNot extends PrefixOperator("~") with RangeDomain.PrefixEndomorphic { val domain = Domain.Integer }
  object BitAnd extends InfixOperator("&") with RangeDomain.InfixEndomorphic { val domain = Domain.Integer }
  object BitOr extends InfixOperator("|") with RangeDomain.InfixEndomorphic { val domain = Domain.Integer }
  object BitXor extends InfixOperator("^") with RangeDomain.InfixEndomorphic { val domain = Domain.Integer }

  object Not extends PrefixOperator("NOT") with RangeDomain.PrefixEndomorphic { val domain = Domain.Boolean }
  object IsNull extends PostfixOperator("IS NULL") with RangeDomain.Nullable
  object IsNotNull extends PostfixOperator("IS NOT NULL") with RangeDomain.Nullable
  object Length extends PrefixOperator("LENGTH") {
    /* A => Int where A is string-like */
    def rangeDomain(domainCheck:DomainChecker, operand1:Domain): Domain = {
      domainCheck{ fail => RangeDomain.checkIsStringLike(fail, operand1)}
      Domain.Integer
    }
  }

  object Count extends PrefixOperator("COUNT") {
    /* A => Int */
    def rangeDomain(domainCheck:DomainChecker, operand1:Domain): Domain = {
      Domain.Integer
    }
  }
  object Sum extends PrefixOperator("SUM") with RangeDomain.PrefixNumeric
  object Avg extends PrefixOperator("AVG") with RangeDomain.PrefixNumeric
  object Min extends PrefixOperator("MIN") with RangeDomain.PrefixNumeric
  object Max extends PrefixOperator("MAX") with RangeDomain.PrefixNumeric
  object StdDev extends PrefixOperator("StdDev") with RangeDomain.Statistics
  object StdDevP extends PrefixOperator("StdDevP") with RangeDomain.Statistics
  object Var extends PrefixOperator("Var") with RangeDomain.Statistics
  object VarP extends PrefixOperator("VarP") with RangeDomain.Statistics
}
