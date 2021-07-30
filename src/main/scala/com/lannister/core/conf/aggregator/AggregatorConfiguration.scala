package com.lannister.core.conf.aggregator

import java.util

import scala.beans.BeanProperty

class AggregatorConfiguration{
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var params: util.HashMap[String, String] = _
}
