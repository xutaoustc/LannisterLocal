package com.lannister.core.engine.spark.aggregator

import com.lannister.analysis.{AggregatedData, Aggregator}
import com.lannister.core.conf.AggregatorConfiguration
import com.lannister.core.domain.ApplicationData
import com.lannister.core.util.Logging

class SparkAggregator(private val aggregatorConfigurationData: AggregatorConfiguration)
  extends Aggregator with Logging{

  private val aggregatedData: AggregatedData = AggregatedData()

  override def getResult: AggregatedData = { null }

  override def aggregate(data: ApplicationData): Aggregator = this

}
