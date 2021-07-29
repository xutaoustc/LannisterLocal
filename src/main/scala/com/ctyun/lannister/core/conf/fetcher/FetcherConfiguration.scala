package com.ctyun.lannister.core.conf.fetcher

import java.util

import scala.beans.BeanProperty

class FetcherConfiguration{
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var params: util.HashMap[String, String] = _
}
