package com.lannister

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeoutException, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.lannister.core.conf.Configs
import com.lannister.core.domain.AnalyticJob
import com.lannister.core.hadoop.HadoopSecurity
import com.lannister.core.jobGenerator.AnalyticJobGeneratorHadoop3
import com.lannister.core.metric.MetricsController
import com.lannister.core.util.{Logging, Utils}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component


@Component
class LannisterAnalyzer extends Runnable with Logging{
  @Autowired
  private var _analyticJobGenerator: AnalyticJobGeneratorHadoop3 = _
  @Autowired
  private var _metricsController: MetricsController = _
  private var executor: ThreadPoolExecutor = _
  private var eachRoundStartTs: Long = _


  /*
  *   Core logic of LannisterAnalysis:
  *     * Global configure
  *     * Loop
  *       * Fetch AnalyticJob
  *       * Encapsulate AnalyticJob to ExecutorJob, and submit ExecutorJob to ThreadPool
  *         * AnalyticJob.getAnalysis, Persist
  *         * Retry, add AnalyticJob back to AnalyticJobGenerator
  *         * Log and metric
  * */
  override def run(): Unit = {
    HadoopSecurity().doAs {
      info("LannisterLogic has started")

      globalConfigure

      while(true) {
        fetchAndRunEachRound()
      }

      error("LannisterLogic stopped")
    }
  }

  private def globalConfigure(): Unit = {
    executor = new ThreadPoolExecutor(
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
    try {
      eachRoundStartTs = System.currentTimeMillis()
      _analyticJobGenerator.fetchAnalyticJobs.foreach(job => {
          executor.submit( new ExecutorJob(job) )
        })
    } catch {
      case e: Exception => error("Error fetching job list. Try again later ...", e)
        waitInterval(Configs.RETRY_INTERVAL.getValue)
        return
    }

    _metricsController.setActiveProcessingThread(executor.getActiveCount)
    _metricsController.setQueueSize(executor.getQueue.size)
    waitInterval(Configs.FETCH_INTERVAL.getValue)
  }

  private def waitInterval(interval: Long) {
    val nextRun = eachRoundStartTs + interval
    val waitTime = nextRun - System.currentTimeMillis()

    if(waitTime > 0) {
      Thread.sleep(waitTime)
    }
  }

  class ExecutorJob(job: AnalyticJob) extends Runnable with Logging {
    override def run(): Unit = {
      info(s"* * Analyzing ${job.applicationTypeNameAndAppId}")

      try{
        val (time, isNoData) = Utils.executeWithRetTime {
          job.doAnalysis.isNoData
        }

        _metricsController.markProcessedJobs()
        _metricsController.setJobProcessingTime(time)
        if(isNoData) {
          _metricsController.markSkippedJobs()
        }
        info(s"^o^ TOOK $time ms to analyze ${job.applicationTypeNameAndAppId}")
      } catch {
        case _: InterruptedException => // TODO
        case e: TimeoutException =>
          warn(s"Time out while fetching data. Exception is ${e.getMessage}")
          jobFate()
        case e: Exception =>
          error(s"Failed to analyze ${job.applicationTypeNameAndAppId}", e)
          jobFate()
      }
    }


    private def jobFate(): Unit = {
      if (job.tryAdd2RetryQueue()) {
        warn(s"Add job id [${job.appId}] into the retry list.")
        _analyticJobGenerator.addIntoRetries(job)
      } else if (job.tryAdd2SecondRetryQueue()) {
        warn(s"Add job id [${job.appId}] into the second retry list}")
        _analyticJobGenerator.addIntoSecondRetryQueue(job)
      } else {
        _metricsController.markDroppedJobs()
        error(s"Drop the analytic job")
      }
    }
  }

}


