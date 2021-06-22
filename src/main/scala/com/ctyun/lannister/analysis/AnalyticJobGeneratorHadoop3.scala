package com.ctyun.lannister.analysis
import com.ctyun.lannister.LannisterContext
import com.ctyun.lannister.conf.Configs
import com.ctyun.lannister.hadoop.HadoopConf
import com.ctyun.lannister.metric.MetricsController
import com.ctyun.lannister.util.Logging
import org.apache.hadoop.conf.Configuration
import org.codehaus.jackson.map.ObjectMapper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import java.net.URL
import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ListBuffer


/*
* Generate Job waiting for analyze
* */
@Component
class AnalyticJobGeneratorHadoop3 extends AnalyticJobGenerator with Logging {
  @Autowired
  var context: LannisterContext = _
  @Autowired
  var _metricsController:MetricsController = _

  private val _configuration:Configuration = HadoopConf.conf

  private var _resourceManagerAddress:String = null
  private val _objectMapper = new ObjectMapper
  private var _lastTime:Long = 0L
  private var _currentTime = 0L
  private val FETCH_DELAY = 60000
  private val _firstRetryQueue:util.Queue[AnalyticJob] = new ConcurrentLinkedQueue[AnalyticJob]()
  private val _secondRetryQueue = new util.LinkedList[AnalyticJob]()


  private val IS_RM_HA_ENABLED = "yarn.resourcemanager.ha.enabled"
  private val RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.webapp.address"
  private val RESOURCE_MANAGER_IDS = "yarn.resourcemanager.ha.rm-ids"
  private val RM_NODE_STATE_URL = "http://%s/ws/v1/cluster/info"


  override def configure: Unit = {
    _lastTime = Configs.INITIAL_FETCH_START_TIME.getValue
  }

  override def fetchAnalyticJobs: List[AnalyticJob] = {
    updateResourceManagerAddresses

    val appList = ListBuffer[AnalyticJob]()
    _currentTime = System.currentTimeMillis - FETCH_DELAY
    val start = _lastTime + 1
    val end = _currentTime

    info(s"[Fetching] Fetching recent finished application runs between last time: ${start}, and current time: ${end}")
    val succeededAppsURL = new URL(new URL("http://" + _resourceManagerAddress), s"/ws/v1/cluster/apps?finalStatus=SUCCEEDED&finishedTimeBegin=${start}&finishedTimeEnd=${end}")
    info(s"[Fetching] The succeeded apps URL is ${succeededAppsURL}")
    val succeededApps = readApps(succeededAppsURL).map(_.setSuccessfulJob)
    appList ++= succeededApps

    val failedAppsURL = new URL(new URL("http://" + _resourceManagerAddress), s"/ws/v1/cluster/apps?finalStatus=FAILED&state=FINISHED&finishedTimeBegin=${start}&finishedTimeEnd=${end}")
    info(s"[Fetching] The falled apps URL is ${failedAppsURL}")
    val failedApps = readApps(failedAppsURL)
    appList ++= failedApps

    var firstRetryQueueFetchCount = 0
    while (!_firstRetryQueue.isEmpty()) {
      val job = _firstRetryQueue.poll()
      info(s"[Fetching] ${job.appId} polled from first retry queue")
      firstRetryQueueFetchCount = firstRetryQueueFetchCount + 1
      appList += job
    }

    var secondRetryQueueFetchCount = 0
    _secondRetryQueue.synchronized{
      val iteratorSecondRetry = _secondRetryQueue.iterator
      while(iteratorSecondRetry.hasNext){
        val job = iteratorSecondRetry.next()
        if (job.readyForSecondRetry) {
          info(s"[Fetching] ${job.appId} polled from second retry queue")
          secondRetryQueueFetchCount = secondRetryQueueFetchCount + 1
          appList += job
          iteratorSecondRetry.remove();
        }
      }
    }

    _lastTime = _currentTime
    info(s"[Fetching] Total ${appList.size} items fetched --- ${succeededApps.size} succeed, ${failedApps.size} falled, ${firstRetryQueueFetchCount} first retry, $secondRetryQueueFetchCount second retry")
    appList.toList
  }

  override def addIntoRetries(job: AnalyticJob): Unit = {
    _firstRetryQueue.add(job)
    _metricsController.setRetryQueueSize(_firstRetryQueue.size())
  }

  override def addIntoSecondRetryQueue(job: AnalyticJob) = {
    _secondRetryQueue.synchronized {
      _secondRetryQueue.add(job.setTimeToSecondRetry)
    }
    _metricsController.setSecondRetryQueueSize(_secondRetryQueue.size)
  }

  private def updateResourceManagerAddresses: Unit = {
    if(_configuration.get(IS_RM_HA_ENABLED).toBoolean){
      _configuration.get(RESOURCE_MANAGER_IDS).split(",").foreach(id=>{
        val resourceManager = _configuration.get(RESOURCE_MANAGER_ADDRESS + "." + id)
        val resourceManagerURL = String.format(RM_NODE_STATE_URL, resourceManager)
        val rootNode = readJsonNode(new URL(resourceManagerURL))
        val status = rootNode.path("clusterInfo").path("haState").asText()
        if("ACTIVE" == status){
          _resourceManagerAddress = resourceManager
        }
      })
    }else{
      _resourceManagerAddress = _configuration.get(RESOURCE_MANAGER_ADDRESS)
    }
  }

  private def readJsonNode(url: URL) = _objectMapper.readTree(url.openStream)

  private def readApps(url: URL)={
    val appList = ListBuffer[AnalyticJob]()
    val apps = readJsonNode(url).path("apps").path("app")

    apps.forEach(app=> {
        val appId = app.get("id").asText()
        val user = app.get("user").asText()
        val name = app.get("name").asText()
        val queueName = app.get("queue").asText()
        val trackingUrl = if( app.get("trackingUrl") != null)  app.get("trackingUrl").asText() else null
        val startTime = app.get("startedTime").asLong()
        val finishTime = app.get("finishedTime").asLong()

        val applicationType = context.getApplicationTypeForName(app.get("applicationType").asText())
        if(applicationType != None){
          appList += AnalyticJob(appId, applicationType.get, user, name, queueName, trackingUrl, startTime, finishTime)
        }
    })

    appList
  }

}

