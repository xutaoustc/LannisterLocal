package com.ctyun.lannister.analysis

import scala.beans.BeanProperty

class JobType {
  @BeanProperty var name:String = _
  @BeanProperty var applicationtype:String = _
  @BeanProperty var conf: String = _
  @BeanProperty var value: String = _

  def getAppType = ApplicationType(applicationtype)
}
