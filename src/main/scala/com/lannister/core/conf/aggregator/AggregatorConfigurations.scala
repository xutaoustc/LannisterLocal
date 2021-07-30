package com.lannister.core.conf.aggregator

import java.util

import scala.beans.BeanProperty

class AggregatorConfigurations {
  @BeanProperty var aggregators: util.ArrayList[AggregatorConfiguration] = _

  def iterator: Iterator[AggregatorConfiguration] = {
    import collection.JavaConverters._
    aggregators.iterator().asScala
  }
}
