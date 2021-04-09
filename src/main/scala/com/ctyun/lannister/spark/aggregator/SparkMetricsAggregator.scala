package com.ctyun.lannister.spark.aggregator

import com.ctyun.lannister.analysis.{AggregatedData, ApplicationData, MetricsAggregator}
import com.ctyun.lannister.conf.aggregator.AggregatorConfigurationData

class SparkMetricsAggregator(private val aggregatorConfigurationData: AggregatorConfigurationData) extends MetricsAggregator{

  override def getResult: AggregatedData = ???

  override def aggregate(data: ApplicationData): Unit = ???
}
