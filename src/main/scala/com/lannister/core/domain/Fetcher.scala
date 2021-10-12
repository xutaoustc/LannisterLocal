package com.lannister.core.domain

trait Fetcher[T <: ApplicationData] {
  def fetch(job: AnalyticJob): Option[T]
}
