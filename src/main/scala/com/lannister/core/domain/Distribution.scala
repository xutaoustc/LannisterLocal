package com.lannister.core.domain

import com.lannister.core.domain.Severity.Severity
import com.lannister.core.math.Statistics

case class Distribution(min: Long, p25: Long, median: Long, p75: Long, max: Long) {
  def text(formatter: Long => String, separator: String = ", "): String = {
    val labels = Seq(
      s"min: ${formatter(min)}",
      s"p25: ${formatter(p25)}",
      s"median: ${formatter(median)}",
      s"p75: ${formatter(p75)}",
      s"max: ${formatter(max)}"
    )
    labels.mkString(separator)
  }


  def severityOfDistribution(implicit severityThresholds: SeverityThresholds,
                             ignoreMaxLessThanThreshold: Long): Severity = {
    if (max < ignoreMaxLessThanThreshold) {
      Severity.NONE
    } else if (median == 0L) {
      severityThresholds.of(Long.MaxValue)
    } else {
      severityThresholds.of(BigDecimal(max) / BigDecimal(median))
    }
  }
}

object Distribution {
  def apply(values: Seq[Long]): Distribution = {
    val sortedValues = values.sorted.toList
    Distribution(
      sortedValues.min,
      p25 = Statistics.percentile(sortedValues, 25),
      Statistics.median(sortedValues),
      p75 = Statistics.percentile(sortedValues, 75),
      sortedValues.max
    )
  }
}
