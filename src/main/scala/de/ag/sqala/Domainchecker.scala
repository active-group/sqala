package de.ag.sqala

/** Base class for domain checkers
  *
  * Call apply method to perform domain check.
  */
// Why is this a class? Because we cannot have types at package level
// and I did not want to create a package object.
abstract class DomainChecker {
  /**
   * (maybe) call domainChecker
   *
   * @param domainChecker domainChecker to call
   *
   */
  def apply(domainChecker: DomainChecker.DomainCheckProc):Unit
}

/**
 * DomainChecker that throws DomainCheckException if check fails
 */
object ExceptionThrowingDomainChecker extends DomainChecker {
  def apply(actualDomainChecker: DomainChecker.DomainCheckProc) =
    actualDomainChecker(DomainChecker.failWithException)
}

/**
 * DomainChecker that does not check anything
 */
object IgnoringDomainChecker extends DomainChecker {
  /**
   * Never calls domainChecker
   *
   * @param domainChecker ignored
   */
  def apply(domainChecker: DomainChecker.DomainCheckProc) {}
}

/**
 *
 */
object DomainChecker {
  /**
   * signals failure (throws exception, stderr output, etc.)
   * args: expected, actual => Unit;
   */
  type FailProc = (Any, Any) => Unit
  /**
   * proc that (maybe) checks domains and calls FailProc if check failed
   */
  type DomainCheckProc = FailProc => Unit


  /**
   * Base class for domain-check exceptions
   */
  class DomainCheckException extends Exception

  /**
   * Signals that an unexpected domain was encountered
   * @param expected  what was expected (may be object or human-readable string)
   * @param actual    what was encountered (usually an object)
   */
  class UnexpectedDomainException(expected:Any, actual:Any) extends DomainCheckException {
    override def toString = "expected: %s, actual: %s".format(expected.toString, actual.toString)
  }

  /**
   * FailProc that throws UnexpectedDomainException
   * @param expected  what was expected (may be object or human-readable string)
   * @param actual    what was encountered (usually an object)
   */
  def failWithException(expected: Any, actual: Any) {
    throw new UnexpectedDomainException(expected, actual)
  }
}