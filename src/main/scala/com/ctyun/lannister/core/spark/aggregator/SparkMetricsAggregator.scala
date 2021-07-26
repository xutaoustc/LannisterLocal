package com.ctyun.lannister.core.spark.aggregator

import com.ctyun.lannister.analysis.{AggregatedData, ApplicationData, MetricsAggregator}
import com.ctyun.lannister.core.conf.aggregator.AggregatorConfiguration
import com.ctyun.lannister.util.Logging

class SparkMetricsAggregator(private val aggregatorConfigurationData: AggregatorConfiguration)
  extends MetricsAggregator with Logging{

  private val aggregatedData: AggregatedData = AggregatedData()

  override def getResult: AggregatedData = { null }

  override def aggregate(data: ApplicationData): MetricsAggregator = this

}