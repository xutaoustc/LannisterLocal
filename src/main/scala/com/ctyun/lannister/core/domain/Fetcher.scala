package com.ctyun.lannister.core.domain

import com.ctyun.lannister.analysis.AnalyticJob

trait Fetcher[T <: ApplicationData] {
  def fetchData(job: AnalyticJob): Option[T]
}
