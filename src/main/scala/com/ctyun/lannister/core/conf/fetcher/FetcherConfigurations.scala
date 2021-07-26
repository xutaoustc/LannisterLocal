package com.ctyun.lannister.core.conf.fetcher

import java.util

import scala.beans.BeanProperty

class FetcherConfigurations extends Iterable[FetcherConfiguration] {
  @BeanProperty var fetchers: util.ArrayList[FetcherConfiguration] = _

  override def iterator: Iterator[FetcherConfiguration] = {
    import collection.JavaConverters._
    fetchers.iterator().asScala
  }
}
