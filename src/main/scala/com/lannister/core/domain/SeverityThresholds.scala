package com.lannister.core.domain

import java.lang.{Boolean => JBool, Number => JNum}

import com.lannister.core.domain.Severity.Severity


case class SeverityThresholds(asc: JBool, low: JNum, moderate: JNum, severe: JNum, critical: JNum) {
  def of(value: JNum): Severity = Severity.getSeverity(value, asc, low, moderate, severe, critical)
}

object SeverityThresholds {
  def parse(asc: JBool, rawStr: String, tran: String => Double = _.toDouble): SeverityThresholds = {
    val nums = rawStr.split(",").map(_.trim).map(tran)
    SeverityThresholds(asc, low = nums(0), moderate = nums(1), severe = nums(2), critical = nums(3))
  }
}
