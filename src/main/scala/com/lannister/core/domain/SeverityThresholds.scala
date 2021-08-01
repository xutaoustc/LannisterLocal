package com.lannister.core.domain

import java.lang.{Boolean => JBool, Number => JNum}

import com.lannister.core.domain.Severity.Severity


case class SeverityThresholds(asc: JBool, low: JNum, moderate: JNum, severe: JNum, critical: JNum) {
  def of(value: JNum): Severity = Severity.getSeverity(value, asc, low, moderate, severe, critical)
  def of(value: Long): Severity = of(value.asInstanceOf[JNum])
}

object SeverityThresholds {
  def parse(s: String, asc: JBool = true, fm: String => Double = _.toDouble): SeverityThresholds = {
    val nums = s.split(",").map(_.trim).map(fm)
    SeverityThresholds(asc, low = nums(0), moderate = nums(1), severe = nums(2), critical = nums(3))
  }
}
