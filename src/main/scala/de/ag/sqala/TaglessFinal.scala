package de.ag.sqala

import scala.language.higherKinds

trait F[A[_]] {
  def foo[B](x: A[B]): Int
}

trait Symantics {
  type Repr[_]

  def int(n: Int): Repr[Int]
  def lam[A, B](b: Repr[A] => Repr[B]): Repr[A => B]
}


object R extends Symantics {
  type Repr[A] = A

  def int(n: Int): Repr[Int] = n

  def lam[A, B](b: Repr[A] => Repr[B]): Repr[A => B] = b
}

object TaglessFinal {
  def q1(S: Symantics)(implicit f: F[S.Repr]) = {
    f.foo(S.lam { ra: S.Repr[Boolean] => S.int(5) })
  }


  // val Q1R = Q1(R)

  // val Q1Rres = Q1R.res
}
