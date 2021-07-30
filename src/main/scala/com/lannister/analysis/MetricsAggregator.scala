package com.lannister.analysis

import com.lannister.core.domain.ApplicationData

trait MetricsAggregator {
  def aggregate(data: ApplicationData): MetricsAggregator
  def getResult: AggregatedData
}
