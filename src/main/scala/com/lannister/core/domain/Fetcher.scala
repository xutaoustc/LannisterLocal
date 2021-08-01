package com.lannister.core.domain

trait Fetcher[T <: ApplicationData] {
  def fetchAndParse(job: AnalyticJob): Option[T]
}
