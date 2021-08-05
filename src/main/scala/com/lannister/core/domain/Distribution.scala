package com.lannister.core.domain

import com.lannister.core.domain.Severity.Severity

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
      p25 = percentile(sortedValues, 25),
      median(sortedValues),
      p75 = percentile(sortedValues, 75),
      sortedValues.max
    )
  }

  private def median(values: List[Long]): Long = {
    val sorted = values.sorted
    val middle = sorted.size / 2
    if (sorted.size % 2 == 0) {
      (sorted(middle - 1) + sorted(middle)) / 2
    } else {
      sorted(middle)
    }
  }

  private def percentile(values: List[Long], percentile: Int): Long = {
    if (percentile == 0) {
      return 0
    }

    val sorted = values.sorted

    val position = Math.ceil(sorted.size * percentile / 100.0).toInt

    if (position == 0) {
      return sorted(position)
    }

    sorted(position-1)
  }
}
