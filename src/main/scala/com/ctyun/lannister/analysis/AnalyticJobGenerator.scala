package com.ctyun.lannister.analysis

trait AnalyticJobGenerator {
  def updateResourceManagerAddresses

  def fetchAnalyticJobs:List[AnalyticJob]
}
