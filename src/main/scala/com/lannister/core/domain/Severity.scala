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


  val SEVERITY_ASC = Array(NONE, LOW, MODERATE, SEVERE, CRITICAL)

  /*
  * When asc is true, The higher the value, the more critical, the thresholds value from low to high
  * When asc is false, The lower the value, the more critical, the thresholds value from high to low
  * */
  def getSeverity(value: Number, asc: Boolean, thresholds: Number*): Severity = {
    val check = if (asc) {
      (a: Number, b: Number) => a.doubleValue() < b.doubleValue()
    } else {
      (a: Number, b: Number) => a.doubleValue() > b.doubleValue()
    }

    thresholds.zipWithIndex
      .foreach { case (v, i) => if (check(value, v)) {
        return SEVERITY_ASC(i)
      }
      }

    CRITICAL
  }

}
