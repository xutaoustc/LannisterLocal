package com.ctyun.lannister.core.conf.fetcher

import java.util

import scala.beans.BeanProperty

import com.ctyun.lannister.analysis.ApplicationType

class FetcherConfiguration{
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var params: util.HashMap[String, String] = _

  def getAppType: ApplicationType = ApplicationType(applicationType)
}
