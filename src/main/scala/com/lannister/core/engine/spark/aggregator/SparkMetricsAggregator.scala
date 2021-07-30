package com.lannister.core.engine.spark.aggregator

import com.lannister.analysis.{AggregatedData, MetricsAggregator}
import com.lannister.core.conf.aggregator.AggregatorConfiguration
import com.lannister.core.domain.ApplicationData
import com.lannister.core.util.Logging

class SparkMetricsAggregator(private val aggregatorConfigurationData: AggregatorConfiguration)
  extends MetricsAggregator with Logging{

  private val aggregatedData: AggregatedData = AggregatedData()

  override def getResult: AggregatedData = { null }

  override def aggregate(data: ApplicationData): MetricsAggregator = this

}
