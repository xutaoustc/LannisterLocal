package com.ctyun.lannister.analysis

import com.ctyun.lannister.core.domain.ApplicationData

trait MetricsAggregator {
  def aggregate(data: ApplicationData): MetricsAggregator
  def getResult: AggregatedData
}
