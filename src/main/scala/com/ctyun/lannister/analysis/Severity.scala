package com.ctyun.lannister.analysis

object Severity extends Enumeration {
  type Severity = Value
  val NONE = Value(0, "None")
  val LOW = Value(1, "Low")
  val MODERATE = Value(2, "Moderate")
  val SEVERE = Value(3, "Severe")
  val CRITICAL = Value(4, "Critical")

  def max(a:Severity,b:Severity)={
    if(a.id> b.id)
      a
    else
      b
  }
}
