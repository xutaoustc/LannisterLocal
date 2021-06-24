package com.ctyun.lannister.analysis

trait AnalyticJobGenerator {
  def configure : Unit

  def fetchAnalyticJobs : List[AnalyticJob]

  def addIntoRetries(job: AnalyticJob): Unit

  def addIntoSecondRetryQueue(job: AnalyticJob): Unit
}
