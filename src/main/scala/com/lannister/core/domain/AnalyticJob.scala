package com.lannister.core.domain

import com.lannister.LannisterContext
import com.lannister.core.conf.Configs
import com.lannister.core.domain.{HeuristicResult => HR}
import com.lannister.core.util.Logging
import com.lannister.model.AppResult
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

  private var _fetcher: Fetcher[_ <: ApplicationData] = _
  private var _heuristics: List[Heuristic] = _

  private var _retries = 0
  private var _secondRetries = 0
  private var _secondRetriesDequeueGap = 0

  private var successfulJob = false
  private var _persistService: PersistService = _

  def setLannisterComponent(context: LannisterContext): AnalyticJob = {
    _fetcher = context.getFetcherForApplicationType(applicationType)
    _heuristics = context.getHeuristicsForApplicationType(applicationType)
    this
  }

  def setSuccessfulJob: AnalyticJob = {
    this.successfulJob = true
    this
  }

  def setPersistService(persistService: PersistService): AnalyticJob = {
    this._persistService = persistService
    this
  }

  def timeForRetryQueue(): Boolean = {
    val b = _retries < Configs.RETRY_LIMIT.getValue
    _retries = _retries + 1
    b
  }

  def timeForSecondRetryQueue(): Boolean = {
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

  def typeAndAppId(): String = s"$applicationType $appId"

  def analysisAndPersist: AppResult = {
    val heuristicResults = _fetcher.fetchAndParse(this) match {
      case Some(data) => _heuristics.map(_.apply(data))
      case None =>
        warn(s"No Data Received for analytic job: $appId")
        HeuristicResult.NO_DATA :: Nil
    }

    persist(heuristicResults)
  }

  private def persist(hrs: List[HR]): AppResult = {
    val result = new AppResult()
    result.appId = appId
    result.trackingUrl = trackingUrl
    result.queueName = queueName
    result.username = user
    result.startTime = startTime
    result.finishTime = finishTime
    result.name = name
    result.jobType = applicationType
    result.successfulJob = successfulJob
    result.resourceUsed = 0 // TODO
    result.totalDelay = 0 // TODO
    result.resourceWasted = 0 // TODO
    hrs.foreach { hr =>
      Option(hr).map(_ == HeuristicResult.NO_DATA).foreach { _ => result.isNoData = true}
      result.appHRs += hr
      result.score = result.score + hr.score
      result.severity = Severity.max(result.severity, hr.severity)
      result.severityId = result.severity.id
    }

    _persistService.save(result)
    result
  }
}
