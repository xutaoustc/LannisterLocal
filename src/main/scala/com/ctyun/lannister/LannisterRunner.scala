package com.ctyun.lannister

import com.ctyun.lannister.analysis.AnalyticJobGeneratorHadoop3
import com.ctyun.lannister.conf.Configs
import com.ctyun.lannister.security.HadoopSecurity
import com.ctyun.lannister.util.Logging
import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.security.PrivilegedAction
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.Executors
import scala.util.control.Breaks.{break, breakable}

class LannisterRunner  extends Runnable with Logging{
  // initialize context first
  LannisterContext()

  private val _analyticJobGenerator = new AnalyticJobGeneratorHadoop3
  private val running = new AtomicBoolean(true)
  private var lastRun = 0L

  private val factory = new ThreadFactoryBuilder().setNameFormat("executor-thread-%d").build()
  private val threadPoolExecutor = Executors.newFixedThreadPool(Configs.EXECUTOR_NUM.getValue,factory)


  override def run(): Unit = {
    info("LannisterRunner has started")

    runWithSecurity(mainLogic)
  }


  private def mainLogic()={
    while(running.get()) {
      loopBody
    }
    logger.error("LannisterRunner stopped")


    def loopBody(): Unit ={
        _analyticJobGenerator.updateResourceManagerAddresses
        lastRun = System.currentTimeMillis()

        try{
          val todos = _analyticJobGenerator.fetchAnalyticJobs
        } catch{
          case e:Exception=> error("Error fetching job list. Try again later ...",e)
            waitInterval(Configs.RETRY_INTERVAL.value)
            return
        }

        waitInterval(Configs.FETCH_INTERVAL.value)
    }
  }


  private def waitInterval(interval:Long)={

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
}

