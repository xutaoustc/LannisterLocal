package com.ctyun.lannister.core.conf.heuristic

import java.util

import scala.beans.BeanProperty

import com.ctyun.lannister.analysis.ApplicationType

class HeuristicConfiguration {
  @BeanProperty var name: String = _
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var viewname: String = _
  @BeanProperty var params: util.HashMap[String, String] = _

  def getAppType: ApplicationType = ApplicationType(applicationType)
}
