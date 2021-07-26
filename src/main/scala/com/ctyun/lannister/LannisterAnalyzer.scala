package com.ctyun.lannister

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeoutException, TimeUnit}

import com.ctyun.lannister.analysis.{AnalyticJob, AnalyticJobGeneratorHadoop3}
import com.ctyun.lannister.core.conf.Configs
import com.ctyun.lannister.core.hadoop.HadoopSecurity
import com.ctyun.lannister.metric.MetricsController
import com.ctyun.lannister.util.{Logging, Utils}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component


@Component
class LannisterAnalyzer extends Runnable with Logging{
  @Autowired
  private var _analyticJobGenerator: AnalyticJobGeneratorHadoop3 = _
  @Autowired
  private var _metricsController: MetricsController = _

  private var threadPoolExecutor: ThreadPoolExecutor = _
  private var eachRoundStartTs = 0L


  override def run(): Unit = {
    HadoopSecurity().doAs(core)
  }

  /*
  * Core logic of LannisterAnalysis:
  *   * Global configure
  *   * Loop each round
  *     * Fetch AnalyticJob ready to be analyzed
  *     * Encapsulate AnalyticJob to ExecutorJob, and submit ExecutorJob to ThreadPool
  *       * AnalyticJob.getAnalysis, Persist
  *       * Retry, add AnalyticJob back to AnalyticJobGenerator
  * */
  private def core(): Unit = {
    info("LannisterLogic has started")

    globalConfigure
    while(true) {
      fetchAndRunEachRound()
    }

    error("LannisterLogic stopped")
  }

  private def globalConfigure(): Unit = {
    threadPoolExecutor = new ThreadPoolExecutor(
                            Configs.EXECUTOR_NUM.getValue,
                            Configs.EXECUTOR_NUM.getValue,
                            0L, TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue[Runnable](),
                            new ThreadFactoryBuilder().setNameFormat("executor-thread-%d").build())
    info(s"executor num is ${Configs.EXECUTOR_NUM.getValue}")

    _analyticJobGenerator.configure
    _metricsController.init()
  }

  private def fetchAndRunEachRound(): Unit = {
    eachRoundStartTs = System.currentTimeMillis()

    try{
      _analyticJobGenerator.fetchAnalyticJobs
        .foreach(job => {
          threadPoolExecutor.submit( new ExecutorJob(job) )
        })
    } catch {
      case e: Exception => error("Error fetching job list. Try again later ...", e)
        waitInterval(Configs.RETRY_INTERVAL.getValue)
        return
    }

    _metricsController.setActiveProcessingThread(threadPoolExecutor.getActiveCount)
    _metricsController.setQueueSize(threadPoolExecutor.getQueue.size)
    waitInterval(Configs.FETCH_INTERVAL.getValue)
  }

  private def waitInterval(interval: Long) {
    val nextRun = eachRoundStartTs + interval
    val waitTime = nextRun - System.currentTimeMillis()

    if(waitTime > 0) {
      Thread.sleep(waitTime)
    }
  }



  /*
  * Runtime form of AnalyticJob:
  *   1. Execute AnalyticJob in thread
  *   2. log and metric
  *   3. retry
  * */
  class ExecutorJob(analyticJob: AnalyticJob) extends Runnable with Logging {
    override def run(): Unit = {
      val appTypeNameAndAppId = analyticJob.appTypeNameAndAppId
      info(s"Analyzing $appTypeNameAndAppId")

      try{
        val (time, isNoData) = Utils.executeWithRetTime(() => analyticJob.analysis.isNoData)

        _metricsController.markProcessedJobs()
        _metricsController.setJobProcessingTime(time)
        if(isNoData) {
          _metricsController.markSkippedJobs()
        }
        info(s"^o^ TOOK $time ms to analyze $appTypeNameAndAppId")
      } catch {
        case _: InterruptedException => // TODO
        case e: TimeoutException =>
          warn(s"Time out while fetching data. Exception is ${e.getMessage}")
          jobFate()
        case e: Exception =>
          error(s"Failed to analyze $appTypeNameAndAppId", e)
          jobFate()
      }
    }


    private def jobFate(): Unit = {
      if (analyticJob.retry()) {
        warn(s"Add job id [${analyticJob.appId}] into the retry list.")
        _analyticJobGenerator.addIntoRetries(analyticJob)
      } else if (analyticJob.isSecondPhaseRetry) {
        warn(s"Add job id [${analyticJob.appId}] into the second retry list}")
        _analyticJobGenerator.addIntoSecondRetryQueue(analyticJob)
      } else {
        _metricsController.markDroppedJobs()
        error(s"Drop the analytic job")
      }
    }
  }

}


