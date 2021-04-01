package com.ctyun.lannister.conf.fetcher

import com.ctyun.lannister.analysis.ApplicationType

import java.util
import scala.beans.BeanProperty

class FetcherConfigurationData{
  @BeanProperty var classname:String = _
  @BeanProperty var applicationtype:String = _
  @BeanProperty var params: util.HashMap[String,String] = _

  def getAppType = ApplicationType(applicationtype)
}
