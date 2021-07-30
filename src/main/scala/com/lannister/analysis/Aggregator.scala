package com.lannister.analysis

import com.lannister.core.domain.ApplicationData

trait Aggregator {
  def aggregate(data: ApplicationData): Aggregator
  def getResult: AggregatedData
}
