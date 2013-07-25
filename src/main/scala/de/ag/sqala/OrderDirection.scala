package de.ag.sqala

/**
 * Indicating order direction
 */
sealed abstract class OrderDirection
case object Ascending extends OrderDirection
case object Descending extends OrderDirection

