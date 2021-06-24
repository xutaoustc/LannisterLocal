package com.ctyun.lannister.conf.heuristic

import java.util

import scala.beans.BeanProperty

import com.ctyun.lannister.analysis.ApplicationType

class HeuristicConfigData {
  @BeanProperty var name: String = _
  @BeanProperty var classname: String = _
  @BeanProperty var applicationtype: String = _
  @BeanProperty var viewname: String = _
  @BeanProperty var params: util.HashMap[String, String] = _

  def getAppType: ApplicationType = ApplicationType(applicationtype)
}
