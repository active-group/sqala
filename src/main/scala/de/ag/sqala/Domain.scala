package de.ag.sqala

/**
 *
 */
abstract class Domain(val name: String)
case object DBString extends Domain("string")
case object DBInteger extends Domain("integer")
case object DBDouble extends Domain("double")
case object DBBoolean extends Domain("boolean")
case object DBCalendarTime extends Domain("calendar time")
case object DBBlob extends Domain("blob")
