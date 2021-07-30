package com.lannister.core.domain

import com.lannister.LannisterContext
import com.lannister.analysis.Aggregator
import com.lannister.core.conf.Configs
import com.lannister.core.util.Logging
import com.lannister.model.{AppHeuristicResult, AppHeuristicResultDetail, AppResult}
import com.lannister.service.PersistService

case class AnalyticJob(
    appId: String,
    applicationType: String,
    user: String,
    name: String,
    queueName: String,
    trackingUrl: String,
    startTime: Long,
    finishTime: Long) extends Logging {

  private var successfulJob = false

  private var _fetcher: Fetcher[_ <: ApplicationData] = _
  private var _heuristics: List[Heuristic] = _
  private var _aggregator: Aggregator = _
  private var _persistService: PersistService = _

  private var _retries = 0
  private var _secondRetries = 0
  private var _secondRetriesDequeueGap = 0


  def setSuccessfulJob: AnalyticJob = {
    this.successfulJob = true
    this
  }

  def setLannisterComponent(context: LannisterContext): AnalyticJob = {
    _fetcher = context.getFetcherForApplicationType(applicationType)
    _heuristics = context.getHeuristicsForApplicationType(applicationType)
    _aggregator = context.getAggregatorForApplicationType(applicationType)
    this
  }

  def setPersistService(persistService: PersistService): AnalyticJob = {
    this._persistService = persistService
    this
  }

  def applicationTypeNameAndAppId(): String = s"$applicationType $appId"


  def analysis: AppResult = {
    val (heuristicResults, aggregatedData) = _fetcher.fetchData(this) match {
      case Some(data) => (_heuristics.map(_.apply(data)), _aggregator.aggregate(data).getResult)
      case None =>
        warn(s"No Data Received for analytic job: $appId")
        (HeuristicResult.NO_DATA :: Nil, None)
    }

    save(heuristicResults)
  }

  private def save(heuristicResults: List[HeuristicResult]): AppResult = {
    val result = new AppResult()
    result.appId = appId
    result.trackingUrl = trackingUrl
    result.queueName = queueName
    result.username = user
    result.startTime = startTime
    result.finishTime = finishTime
    result.name = name
    result.successfulJob = successfulJob
    result.jobType = applicationType
    result.resourceUsed = 0 // TODO
    result.totalDelay = 0 // TODO
    result.resourceWasted = 0 // TODO
    heuristicResults.foreach(x => {
      if (x == HeuristicResult.NO_DATA) {
        result.isNoData = true
      }
    })

    heuristicResults.foreach(x => {
      result.heuristicResults +=
        AppHeuristicResult(x.heuristicClass, x.heuristicName, x.severity, x.score)

      result.heuristicResults.last.heuristicResultDetails =
        x.heuristicResultDetails.map(hd => AppHeuristicResultDetail(hd.name, hd.value))
    })

    result.score = heuristicResults.foldLeft(0)((s, v) => s + v.score)
    result.severityId =
      heuristicResults.foldLeft(Severity.NONE)((s, v) => Severity.max(s, v.severity)).id

    _persistService.save(result)
    result
  }


  def tryAdd2RetryQueue(): Boolean = {
    val b = _retries < Configs.RETRY_LIMIT.getValue
    _retries = _retries + 1
    b
  }

  def tryAdd2SecondRetryQueue(): Boolean = {
    val b = _secondRetries < Configs.SECOND_RETRY_LIMIT.getValue
    _secondRetries = _secondRetries + 1
    b
  }

  def setInitialSecondRetryGap: AnalyticJob = {
    this._secondRetriesDequeueGap = this._secondRetries * 5
    this
  }

  def tryFetchOutFromSecondRetryQueue: Boolean = {
    this._secondRetriesDequeueGap = this._secondRetriesDequeueGap - 1
    this._secondRetriesDequeueGap <= 0
  }
}
