package com.ctyun.lannister.analysis

import com.ctyun.lannister.LannisterContext
import com.ctyun.lannister.conf.Configs
import com.ctyun.lannister.model.{AppHeuristicResult, AppHeuristicResultDetails, AppResult}
import com.ctyun.lannister.util.Logging
import java.util.concurrent.Future
import scala.collection.mutable

case class AnalyticJob(appId:String, applicationType:ApplicationType, user:String, name:String, queueName:String,
                       trackingUrl:String, startTime:Long, finishTime:Long) extends Logging{
  private var _jobFuture:Future[_] = null
  private var _retries = 0
  private var _secondRetries = 0
  private var _timeLeftToRetry = 0


  def setJobFuture(future:Future[_])={
    this._jobFuture = future
    this
  }

  def getAnalysis(context:LannisterContext): AppResult = {
    // Fetch
    val fetcher = context.getFetcherForApplicationType(applicationType)
    val dataOption = fetcher.fetchData(this)

    // Heuristic
    val heuristicResults = mutable.ListBuffer[HeuristicResult]()
    dataOption match {
      case Some(data) => {
        val heuristics = context.getHeuristicsForApplicationType(applicationType)
        heuristicResults ++= heuristics.map(h=> h.apply(data))
      }
      case None => {
        heuristicResults += HeuristicResult.NO_DATA
        warn(s"No Data Received for analytic job: $appId")
      }
    }


    // Aggregator
    val metricsAggregator = context.getAggregatorForApplicationType(applicationType)
    dataOption match {
      case Some(data) => {
        metricsAggregator.aggregate(data)
      }
      case None => {
      }
    }
    val aggregatedData = metricsAggregator.getResult

    val result = new AppResult()
    result.appId = appId
    result.trackingUrl = trackingUrl
    result.queueName = queueName
    result.username = user
    result.startTime = startTime
    result.finishTime = finishTime
    result.name = name
    result.jobType = applicationType.upperName
    result.resourceUsed = 0 //TODO
    result.totalDelay = 0 //TODO
    result.resourceWasted = 0  //TODO

    var jobScore = 0
    var worstSeverity = Severity.NONE
    heuristicResults.foreach(heu=>{
      val heuForSave = new AppHeuristicResult
      heuForSave.heuristicClass = heu.heuristicClass
      heuForSave.heuristicName = heu.heuristicName
      heuForSave.severity = heu.severity
      heuForSave.severityId = heu.severity.id
      heuForSave.score = heu.score
      result.heuristicResults += heuForSave

      heu.heuristicResultDetails.foreach(heuDtl=>{
        val heuDetailForSave = new AppHeuristicResultDetails
        heuDetailForSave.name = heuDtl.name
        heuDetailForSave.value = heuDtl.value
        heuDetailForSave.details = heuDtl.details
        heuForSave.heuristicResultDetails += heuDetailForSave
      })
      worstSeverity = Severity.max(worstSeverity, heuForSave.severity)
      jobScore = jobScore + heuForSave.score
    })

    result.severityId = worstSeverity.id
    result.score = jobScore

    result
  }

  def readyForSecondRetry={
    this._timeLeftToRetry = this._timeLeftToRetry - 1
    this._timeLeftToRetry <= 0
  }

  def setTimeToSecondRetry: AnalyticJob ={
    this._timeLeftToRetry = this._secondRetries * 5
    this
  }

  def retry()={
    val b =  _retries < Configs.RETRY_LIMIT.getValue
    _retries = _retries + 1
    b
  }

  def isSecondPhaseRetry = {
    val b = _secondRetries < Configs.SECOND_RETRY_LIMIT.getValue
    _secondRetries = _secondRetries + 1
    b
  }
}
