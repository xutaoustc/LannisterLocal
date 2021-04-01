package com.ctyun.lannister.conf.aggregator

import com.ctyun.lannister.analysis.ApplicationType
import java.util
import scala.beans.BeanProperty

class AggregatorConfigurationData{
  @BeanProperty var classname:String = _
  @BeanProperty var applicationtype:String = _
  @BeanProperty var params: util.HashMap[String,String] = _

  def getAppType = ApplicationType(applicationtype)
}
