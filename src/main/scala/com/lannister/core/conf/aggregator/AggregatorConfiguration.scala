package com.lannister.core.conf.aggregator

import java.util

import scala.beans.BeanProperty

class AggregatorConfiguration{
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var params: util.HashMap[String, String] = _
}


class AggregatorConfigurations {
  @BeanProperty var aggregators: util.ArrayList[AggregatorConfiguration] = _

  def iterator: Iterator[AggregatorConfiguration] = {
    import collection.JavaConverters._
    aggregators.iterator().asScala
  }
}
