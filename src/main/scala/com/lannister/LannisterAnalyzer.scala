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
  private var executor: ThreadPoolExecutor = _
  @Autowired private var _analyticJobGenerator: AnalyticJobGeneratorHadoop3 = _
  @Autowired private var _metricsController: MetricsController = _


  override def run(): Unit = {
    HadoopSecurity().doAs {
      globalConfigure

      while(true) {
        fetchAndRun()
      }
    }
  }

  private def globalConfigure(): Unit = {
    executor = new ThreadPoolExecutor(
                      Configs.EXECUTOR_NUM.getValue,
                      Configs.EXECUTOR_NUM.getValue,
                      0L, TimeUnit.MILLISECONDS,
                      new LinkedBlockingQueue[Runnable](),
                      new ThreadFactoryBuilder().setNameFormat("executor-thread-%d").build())
    _analyticJobGenerator.configure
    _metricsController.init()
  }

  private def fetchAndRun(): Unit = {
    try {
      _analyticJobGenerator.fetchAnalyticJobs.foreach { job =>
        executor.submit( ExecutorJob(job) )
      }
    } catch {
      case e: Exception => error("Error fetching job list. Try again later ...", e)
        Thread.sleep(Configs.RETRY_INTERVAL.getValue)
        return
    }

    _metricsController.setActiveProcessingThread(executor.getActiveCount)
    _metricsController.setQueueSize(executor.getQueue.size)
    Thread.sleep(Configs.FETCH_INTERVAL.getValue)
  }


  case class ExecutorJob(job: AnalyticJob) extends Runnable with Logging {
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


