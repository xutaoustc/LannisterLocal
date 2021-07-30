package com.lannister.core.domain

trait Fetcher[T <: ApplicationData] {
  def fetchData(job: AnalyticJob): Option[T]
}
