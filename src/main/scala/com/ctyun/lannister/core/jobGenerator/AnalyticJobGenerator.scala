package com.ctyun.lannister.core.jobGenerator

import com.ctyun.lannister.analysis.AnalyticJob

trait AnalyticJobGenerator {
  def configure: Unit

  def fetchAnalyticJobs: List[AnalyticJob]

  def addIntoRetries(job: AnalyticJob): Unit

  def addIntoSecondRetryQueue(job: AnalyticJob): Unit
}
