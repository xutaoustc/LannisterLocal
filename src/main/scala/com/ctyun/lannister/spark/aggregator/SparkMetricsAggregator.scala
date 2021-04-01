package com.ctyun.lannister.spark.aggregator

import com.ctyun.lannister.analysis.{AggregatedData, MetricsAggregator}
import com.ctyun.lannister.conf.aggregator.AggregatorConfigurationData

class SparkMetricsAggregator(private val aggregatorConfigurationData: AggregatorConfigurationData) extends MetricsAggregator{
  override def aggregate(data: AggregatedData): Unit = ???

  override def getResult(): AggregatedData = ???
}
