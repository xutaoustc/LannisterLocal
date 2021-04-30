package com.ctyun.lannister.metric

import com.codahale.metrics.MetricRegistry.name
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{Gauge, Histogram, Meter, MetricRegistry}
import com.ctyun.lannister.analysis.AnalyticJob
import org.springframework.stereotype.Component

@Component
class MetricsController {
  private var _metricRegistry:MetricRegistry = _

  private var _droppedJobs: Meter = _
  private var _skippedJobs:Meter = _
  private var _processedJobs:Meter = _
  private var _jobProcessingTime:Histogram = _

  private var _activeProcessingThread:Int = 0
  private var _queueSize:Int = 0
  private var _retryQueueSize:Int = 0
  private var _secondRetryQueueSize:Int = 0


  def init(): Unit ={
    _metricRegistry = new MetricRegistry

    val className = classOf[AnalyticJob].getSimpleName
    _droppedJobs = _metricRegistry.meter(name(className,"droppedJobs","count"))
    _skippedJobs = _metricRegistry.meter(name(className, "skippedJobs", "count"))
    _processedJobs = _metricRegistry.meter(name(className, "processedJobs", "count"))
    _jobProcessingTime = _metricRegistry.histogram(name(className, "jobProcessingTime", "ms"))

    _metricRegistry.register(name(className, "activeProcessingThread", "size"), new Gauge[Int] {
      override def getValue: Int = _activeProcessingThread
    })
    _metricRegistry.register(name(className, "jobQueue", "size"), new Gauge[Int] {
      override def getValue: Int = _queueSize
    })
    _metricRegistry.register(name(className, "retryQueue", "size"), new Gauge[Int] {
      override def getValue: Int = _retryQueueSize
    })
    _metricRegistry.register(name(className, "secondRetryQueue", "size"), new Gauge[Int] {
      override def getValue: Int = _secondRetryQueueSize
    })
//    _metricRegistry.register(name(className, "lastDayJobs", "count"), new Gauge[Int] {
//      override def getValue: Int = ???
//    })

    JmxReporter.forRegistry(_metricRegistry).build.start
  }

  def setActiveProcessingThread(size:Int): Unit ={
    _activeProcessingThread = size
  }

  def setQueueSize(size:Int): Unit ={
    _queueSize = size
  }

  def setRetryQueueSize(retryQueueSize:Int):Unit= {
    _retryQueueSize = retryQueueSize
  }

  def setSecondRetryQueueSize(secondRetryQueueSize:Int):Unit={
    _secondRetryQueueSize = secondRetryQueueSize
  }

  def markSkippedJobs(): Unit = {
    _skippedJobs.mark()
  }

  def markDroppedJobs(): Unit = {
    _droppedJobs.mark()
  }

  def markProcessedJobs(): Unit = {
    _processedJobs.mark
  }

  def setJobProcessingTime(processingTimeTaken: Long): Unit = {
    _jobProcessingTime.update(processingTimeTaken)
  }
}
