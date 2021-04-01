package com.ctyun.lannister.analysis

trait MetricsAggregator {
  def aggregate(data: AggregatedData)
  def getResult(): AggregatedData
}
