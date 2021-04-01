package com.ctyun.lannister.conf.aggregator

import java.util
import scala.beans.BeanProperty

class AggregatorConfiguration{
  @BeanProperty var aggregators: util.ArrayList[AggregatorConfigurationData] = _
}
