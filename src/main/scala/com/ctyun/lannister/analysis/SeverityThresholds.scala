package com.ctyun.lannister.analysis

import com.ctyun.lannister.analysis.Severity.Severity

case class SeverityThresholds(low: Number, moderate: Number, severe: Number, critical: Number, ascending: Boolean) {
  def severityOf(value: Number): Severity = if (ascending) {
    Severity.getSeverityAscending(value, low, moderate, severe, critical)
  } else {
    Severity.getSeverityDescending(value, low, moderate, severe, critical)
  }
}

object SeverityThresholds {
  def parse(
             rawString: String,
             ascending: Boolean,
             transformer: String=>Double = _.toDouble
           ): SeverityThresholds = {
    val nums = rawString.split(",").map(str => transformer( str.trim))
    SeverityThresholds(low = nums(0), moderate = nums(1), severe = nums(2), critical = nums(3), ascending)
  }
}