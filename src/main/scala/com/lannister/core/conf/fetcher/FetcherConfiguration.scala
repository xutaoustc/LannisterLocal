package com.lannister.core.conf.fetcher

import java.util

import scala.beans.BeanProperty

class FetcherConfiguration{
  @BeanProperty var classname: String = _
  @BeanProperty var applicationType: String = _
  @BeanProperty var params: util.HashMap[String, String] = _
}


class FetcherConfigurations extends Iterable[FetcherConfiguration] {
  @BeanProperty var fetchers: util.ArrayList[FetcherConfiguration] = _

  def iterator: Iterator[FetcherConfiguration] = {
    import collection.JavaConverters._
    fetchers.iterator().asScala
  }
}
