package com.lannister.core.domain

import java.lang.{Boolean => JBool, Number => JNum}

import com.lannister.core.domain.Severity.{CRITICAL, LOW, MODERATE, NONE, SEVERE, Severity}


case class SeverityThresholds(asc: JBool, low: JNum, moderate: JNum, severe: JNum, critical: JNum) {
  def of(value: JNum): Severity = getSeverity(value, asc, low, moderate, severe, critical)
  def of(value: Long): Severity = of(value.asInstanceOf[JNum])


  val SEVERITY_ASC = Array(NONE, LOW, MODERATE, SEVERE, CRITICAL)
  /*
  * When asc is true, The higher the value, the more critical, the thresholds value from low to high
  * When asc is false, The lower the value, the more critical, the thresholds value from high to low
  * */
  private def getSeverity(value: Number, asc: Boolean, thresholds: Number*): Severity = {
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

object SeverityThresholds {
  def parse(s: String, asc: JBool = true, fm: String => Double = _.toDouble): SeverityThresholds = {
    val nums = s.split(",").map(_.trim).map(fm)
    SeverityThresholds(asc, low = nums(0), moderate = nums(1), severe = nums(2), critical = nums(3))
  }
}
