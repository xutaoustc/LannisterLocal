package com.lannister

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeoutException, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.lannister.core.conf.Configs
import com.lannister.core.domain.AnalyticJob
import com.lannister.core.hadoop.HadoopSecurity
import com.lannister.core.jobGenerator.AnalyticJobGeneratorHadoop3
import com.lannister.core.metric.MetricsController
import com.lannister.core.util.{Logging, Utils}
import com.lannister.service.PersistService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component


@Component
class LannisterAnalyzer extends Runnable with Logging{
  private var executor: ThreadPoolExecutor = _
  @Autowired private var _analyticJobGenerator: AnalyticJobGeneratorHadoop3 = _
  @Autowired private var _metricsController: MetricsController = _
  @Autowired private var _context: LannisterContext = _
  @Autowired private var _persistService: PersistService = _

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
      _analyticJobGenerator.fetchAnalyticJobs
        .map(_.setLannisterComponent(_context))
        .map(_.setPersistService(_persistService))
        .map(ExecutorJob(_))
        .foreach { executor.submit }
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
      try {
        info(s"Start analyzing ${job.typeAndAppId}")
        val (time, _) = Utils.executeWithRetTime {
          job.analysisAndPersist
        }

        if(job.noData) {
          _metricsController.markSkippedJobs()
        }
        _metricsController.markProcessedJobs()
        _metricsController.setJobProcessingTime(time)
        info(s"^o^ TOOK $time ms to analyze ${job.typeAndAppId}")
      } catch {
        case _: InterruptedException => // TODO
        case e: TimeoutException =>
          warn(s"Time out while fetching data. Exception is ${e.getMessage}")
          jobFate()
        case e: Exception =>
          error(s"Failed to analyze ${job.typeAndAppId}", e)
          jobFate()
      }
    }


    private def jobFate(): Unit = {
      if (job.timeForRetryQueue()) {
        warn(s"Add job id [${job.appId}] into the retry list.")
        _analyticJobGenerator.addIntoRetries(job)
      } else if (job.timeForSecondRetryQueue()) {
        warn(s"Add job id [${job.appId}] into the second retry list}")
        _analyticJobGenerator.addIntoSecondRetryQueue(job)
      } else {
        _metricsController.markDroppedJobs()
        error(s"Drop the analytic job")
      }
    }
  }

}


