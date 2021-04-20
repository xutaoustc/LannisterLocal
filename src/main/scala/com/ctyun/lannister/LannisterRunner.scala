package com.ctyun.lannister

import com.ctyun.lannister.analysis.{AnalyticJob, AnalyticJobGeneratorHadoop3}
import com.ctyun.lannister.conf.Configs
import com.ctyun.lannister.security.HadoopSecurity
import com.ctyun.lannister.service.SaveService
import com.ctyun.lannister.util.Logging
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.springframework.beans.factory.annotation.{Autowired, Lookup}
import org.springframework.stereotype.Component

import java.security.PrivilegedAction
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit, TimeoutException}

@Component
class LannisterRunner extends Runnable with Logging{
  @Autowired
  var context: LannisterContext = _
  @Autowired
  var _analyticJobGenerator:AnalyticJobGeneratorHadoop3 = _
  @Autowired
  var saveService:SaveService = _

  private val running = new AtomicBoolean(true)
  private var thisRoundTs = 0L

  private val factory = new ThreadFactoryBuilder().setNameFormat("executor-thread-%d").build()
  private val threadPoolExecutor = new ThreadPoolExecutor(Configs.EXECUTOR_NUM.getValue, Configs.EXECUTOR_NUM.getValue,
    0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable](), factory)



  override def run(): Unit = {
    info("LannisterRunner has started")
    runWithSecurity(core)
  }


  private def core()={
    info(s"executor num is ${Configs.EXECUTOR_NUM.getValue}")
    while(running.get()) {
      fetchAndRun
    }
    error("LannisterRunner stopped")


    @Lookup
    def fetchAndRun(): Unit ={
      thisRoundTs = System.currentTimeMillis()

      // 1. Fetch
      var todos:List[AnalyticJob] = Nil
      try{
        todos = _analyticJobGenerator.fetchAnalyticJobs
      } catch{
        case e:Exception=> error("Error fetching job list. Try again later ...",e)
          waitInterval(Configs.RETRY_INTERVAL.getValue)
          return
      }

      // 2. Submit
      todos.foreach(job=>{
        val future = threadPoolExecutor.submit( new ExecutorJob(job) )
        job.setJobFuture(future)
      })

      info(s"After submitting fetching jobs, Job queue size is ${threadPoolExecutor.getQueue.size}");
      waitInterval(Configs.FETCH_INTERVAL.getValue)
    }
  }


  private def waitInterval(interval:Long){
    val nextRun = thisRoundTs + interval
    val waitTime = nextRun - System.currentTimeMillis()

    if(waitTime <= 0)
      return

    Thread.sleep(waitTime)
  }



  private def runWithSecurity(f:()=>Unit)={
    HadoopSecurity().getUGI.doAs(
      new PrivilegedAction[Unit](){
        override def run():Unit={
          f()
        }
      }
    )
  }



  class ExecutorJob(analyticJob: AnalyticJob) extends Runnable with Logging {
    override def run(): Unit = {
      info(s"[Analyzing] Analyzing ${analyticJob.applicationType.upperName} ${analyticJob.appId}")

      try{
        val analysisStartTimeMillis = System.currentTimeMillis
        val result = analyticJob.getAnalysis(context)
        saveService.save(result)
        val processingTime = System.currentTimeMillis() - analysisStartTimeMillis
        info(s"[Analyzing] ^o^ TOOK ${processingTime}ms to analyze ${analyticJob.applicationType.upperName} ${analyticJob.appId} ")
      }catch{
        case e: InterruptedException=> //TODO
        case e: TimeoutException=> warn(s"[Analyzing][Fate] Time out while fetching data. Exception message is ${e.getMessage}")
          jobFate()
        case e: Exception=>
          error(s"[Analyzing][Fate] Failed to analyze ${analyticJob.applicationType.upperName} ${analyticJob.appId}", e)
          jobFate()
      }
    }

    def jobFate(): Unit ={
      if(analyticJob.retry){
        warn(s"[Analyzing][Fate] Add analytic job id [${analyticJob.appId}] into the retry list.")
        _analyticJobGenerator.addIntoRetries(analyticJob)
      }else if(analyticJob.isSecondPhaseRetry){
        warn(s"[Analyzing][Fate] Add analytic job id [${analyticJob.appId}] into the second retry list}")
        _analyticJobGenerator.addIntoSecondRetryQueue(analyticJob)
      }else{
        error(s"[Analyzing][Fate] Drop the analytic job")
      }
    }
  }

}


