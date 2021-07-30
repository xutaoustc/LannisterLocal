package com.lannister.core.jobGenerator

import com.lannister.core.domain.AnalyticJob

trait AnalyticJobGenerator {
  def configure: Unit

  def fetchAnalyticJobs: List[AnalyticJob]

  def addIntoRetries(job: AnalyticJob): Unit

  def addIntoSecondRetryQueue(job: AnalyticJob): Unit
}
