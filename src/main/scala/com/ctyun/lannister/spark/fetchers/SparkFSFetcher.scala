package com.ctyun.lannister.spark.fetchers

import com.ctyun.lannister.analysis.{AnalyticJob, Fetcher}
import com.ctyun.lannister.conf.fetcher.FetcherConfigurationData
import com.ctyun.lannister.spark.data.SparkApplicationData

class SparkFSFetcher(fetcherConfigurationData: FetcherConfigurationData) extends Fetcher[SparkApplicationData]{
  override def fetchData(job: AnalyticJob): SparkApplicationData = ???
}
