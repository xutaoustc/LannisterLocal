package com.ctyun.lannister.analysis

trait MetricsAggregator {
  def aggregate(data: ApplicationData): MetricsAggregator
  def getResult: AggregatedData
}
