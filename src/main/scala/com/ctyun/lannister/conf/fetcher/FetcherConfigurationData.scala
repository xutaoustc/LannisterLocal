package com.ctyun.lannister.conf.fetcher

import java.util

import scala.beans.BeanProperty

import com.ctyun.lannister.analysis.ApplicationType

class FetcherConfigurationData{
  @BeanProperty var classname: String = _
  @BeanProperty var applicationtype: String = _
  @BeanProperty var params: util.HashMap[String, String] = _

  def getAppType: ApplicationType = ApplicationType(applicationtype)
}
