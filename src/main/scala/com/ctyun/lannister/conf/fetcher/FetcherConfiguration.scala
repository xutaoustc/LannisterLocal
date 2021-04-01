package com.ctyun.lannister.conf.fetcher

import java.util
import scala.beans.BeanProperty

class FetcherConfiguration {
  @BeanProperty var fetchers: util.ArrayList[FetcherConfigurationData] = _
}
