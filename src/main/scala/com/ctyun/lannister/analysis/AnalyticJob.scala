package com.ctyun.lannister.analysis

import java.util.concurrent.Future

case class AnalyticJob(appId:String, applicationType:ApplicationType, user:String, name:String, queueName:String,
                       trackingUrl:String, startTime:Long, finishTime:Long) {
  private var _jobFuture:Future[_] = null

  def setJobFuture(future:Future[_])={
    this._jobFuture = future
    this
  }
}
