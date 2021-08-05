package com.lannister.core.domain

object Severity extends Enumeration {
  type Severity = Value
  val NONE = Value(0, "None")
  val LOW = Value(1, "Low")
  val MODERATE = Value(2, "Moderate")
  val SEVERE = Value(3, "Severe")
  val CRITICAL = Value(4, "Critical")

  def bigger(a: Severity, b: Severity): Boolean = {
    if (a.id > b.id) {
      true
    } else {
      false
    }
  }

  def max(a: Severity, b: Severity): Severity = {
    if (a.id > b.id) {
      a
    } else {
      b
    }
  }

  def max(severities: Severity*): Severity = {
    severities.fold(NONE)(max)
  }

}
