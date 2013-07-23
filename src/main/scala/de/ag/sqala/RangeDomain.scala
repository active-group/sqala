package de.ag.sqala

/**
 *
 */
object RangeDomain {
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
  trait ComparatorRangeDomain {
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