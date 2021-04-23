package com.ctyun.lannister.spark.aggregator

import com.ctyun.lannister.analysis.{AggregatedData, ApplicationData, MetricsAggregator}
import com.ctyun.lannister.conf.aggregator.AggregatorConfigurationData
import com.ctyun.lannister.util.Logging

class SparkMetricsAggregator(private val aggregatorConfigurationData: AggregatorConfigurationData) extends MetricsAggregator with Logging{

  private val aggregatedData:AggregatedData = AggregatedData()

  override def getResult: AggregatedData = { null }

  override def aggregate(data: ApplicationData): MetricsAggregator = {this}

}
