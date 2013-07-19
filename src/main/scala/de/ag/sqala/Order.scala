package de.ag.sqala

/**
 * Indicating order direction
 */
sealed abstract class Order
case object Ascending extends Order
case object Descending extends Order

