package com.ctyun.lannister.analysis

trait Fetcher[T <: ApplicationData] {
  def fetchData(job: AnalyticJob):T
}
