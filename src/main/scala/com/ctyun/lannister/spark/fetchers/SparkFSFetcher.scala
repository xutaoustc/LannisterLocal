package com.ctyun.lannister.spark.fetchers

import com.ctyun.lannister.analysis.{AnalyticJob, Fetcher}
import com.ctyun.lannister.conf.fetcher.FetcherConfigurationData

class SparkFSFetcher(fetcherConfigurationData: FetcherConfigurationData) extends Fetcher{
  override def fetchData(job: AnalyticJob): Unit = ???
}
