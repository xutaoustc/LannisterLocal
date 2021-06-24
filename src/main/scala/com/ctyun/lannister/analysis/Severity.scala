package com.ctyun.lannister.analysis

object Severity extends Enumeration {
  type Severity = Value
  val NONE = Value(0, "None")
  val LOW = Value(1, "Low")
  val MODERATE = Value(2, "Moderate")
  val SEVERE = Value(3, "Severe")
  val CRITICAL = Value(4, "Critical")

  def max(a: Severity, b: Severity): Severity = {
    if (a.id > b.id) {
      a
    } else {
      b
    }
  }

  def max(severities: Severity*): Severity = {
    var currentSeverity = NONE
    for (severity <- severities) {
      currentSeverity = max(currentSeverity, severity)
    }
    currentSeverity
  }

  def getSeverityAscending(value: Number, low: Number, moderate: Number, severe: Number,
                           critical: Number): Severity = {
    if (value.doubleValue() >= critical.doubleValue()) {
      return CRITICAL;
    }
    if (value.doubleValue() >= severe.doubleValue()) {
      return SEVERE;
    }
    if (value.doubleValue() >= moderate.doubleValue()) {
      return MODERATE;
    }
    if (value.doubleValue() >= low.doubleValue()) {
      return LOW;
    }
    return NONE;
  }

  def  getSeverityDescending(value: Number, low: Number, moderate: Number, severe: Number,
                            critical: Number): Severity = {
    if (value.doubleValue() <= critical.doubleValue()) {
      return CRITICAL;
    }
    if (value.doubleValue() <= severe.doubleValue()) {
      return SEVERE;
    }
    if (value.doubleValue() <= moderate.doubleValue()) {
      return MODERATE;
    }
    if (value.doubleValue() <= low.doubleValue()) {
      return LOW;
    }
    return NONE;
  }
}
