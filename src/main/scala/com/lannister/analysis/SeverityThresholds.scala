package com.lannister.analysis

import com.lannister.core.domain.Severity
import com.lannister.core.domain.Severity.Severity

case class SeverityThresholds(low: Number, moderate: Number, severe: Number,
                              critical: Number, ascending: Boolean) {
  def of(value: Number): Severity =
    Severity.getSeverity(value, ascending, low, moderate, severe, critical)
}

object SeverityThresholds {
  def parse(
             rawString: String,
             ascending: Boolean,
             transformer: String => Double = _.toDouble
           ): SeverityThresholds = {
    val nums = rawString.split(",").map(str => transformer( str.trim))
    SeverityThresholds(low = nums(0), moderate = nums(1), severe = nums(2),
      critical = nums(3), ascending)
  }
}
