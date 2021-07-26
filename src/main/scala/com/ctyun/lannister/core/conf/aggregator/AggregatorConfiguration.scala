package com.ctyun.lannister.core.conf.aggregator

import com.ctyun.lannister.analysis.ApplicationType
import java.util
import scala.beans.BeanProperty

class AggregatorConfiguration{
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var params: util.HashMap[String, String] = _

  def getAppType: ApplicationType = ApplicationType(applicationType)
}
