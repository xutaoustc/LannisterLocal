package com.lannister.core.domain

import com.lannister.analysis.AnalyticJob

trait Fetcher[T <: ApplicationData] {
  def fetchData(job: AnalyticJob): Option[T]
}
