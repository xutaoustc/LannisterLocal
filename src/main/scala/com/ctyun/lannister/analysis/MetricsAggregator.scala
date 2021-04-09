package com.ctyun.lannister.analysis

trait MetricsAggregator {
  def aggregate(data: ApplicationData)
  def getResult: AggregatedData
}
