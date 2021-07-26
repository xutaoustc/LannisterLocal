package com.ctyun.lannister.core.conf.aggregator

import java.util

import scala.beans.BeanProperty

class AggregatorConfigurations extends Iterable[AggregatorConfiguration]{
  @BeanProperty var aggregators: util.ArrayList[AggregatorConfiguration] = _

  override def iterator: Iterator[AggregatorConfiguration] = {
    import collection.JavaConverters._
    aggregators.iterator().asScala
  }
}
