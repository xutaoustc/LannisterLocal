package com.ctyun.lannister.analysis

import java.net.URL
import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.ctyun.lannister.LannisterContext
import com.ctyun.lannister.core.conf.Configs
import com.ctyun.lannister.core.hadoop.HadoopConf
import com.ctyun.lannister.core.metric.MetricsController
import com.ctyun.lannister.core.util.{Logging, Utils}
import com.ctyun.lannister.service.PersistService
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component


@Component
class AnalyticJobGeneratorHadoop3 extends AnalyticJobGenerator with Logging {
  @Autowired
  private var _context: LannisterContext = _
  @Autowired
  private var _metricsController : MetricsController = _
  @Autowired
  private var _persistService: PersistService = _

  // If we do not have a lag, we may have apps queried here, but the log file are not ready for read
  private val QUERY_DELAY = 60000
  private var _lastTime : Long = _
  private val _firstRetryQueue = new ConcurrentLinkedQueue[AnalyticJob]()
  private val _secondRetryQueue = new util.LinkedList[AnalyticJob]()

  private var _rmAddress : String = _
  private val IS_RM_HA_ENABLED = "yarn.resourcemanager.ha.enabled"
  private val RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.webapp.address"
  private val RESOURCE_MANAGER_IDS = "yarn.resourcemanager.ha.rm-ids"
  private val RM_NODE_STATE_URL = "http://%s/ws/v1/cluster/info"

  private val _objectMapper = new ObjectMapper


  override def configure: Unit = {
    _lastTime = Configs.INITIAL_FETCH_START_TIME.getValue
  }

  override def fetchAnalyticJobs: List[AnalyticJob] = {
    updateResourceManagerAddresses()

    val fetchStartTime = _lastTime + 1
    val fetchEndTime = System.currentTimeMillis - QUERY_DELAY
    info(s"Fetch recent finished application between last: $fetchStartTime, current: $fetchEndTime")
    def getURL(condition : String) = {
      val timeSpan = s"finishedTimeBegin=$fetchStartTime&finishedTimeEnd=$fetchEndTime"
      new URL(new URL(s"http://${_rmAddress}"), s"/ws/v1/cluster/apps?$condition&$timeSpan")
    }

    val appList = ListBuffer[AnalyticJob]()
    val succeededAppsURL = getURL("finalStatus=SUCCEEDED")
    info(s"The succeeded apps URL is $succeededAppsURL")
    val succeededApps = readApps(succeededAppsURL).map(_.setSuccessfulJob)
    appList ++= succeededApps

    val failedAppsURL = getURL("finalStatus=FAILED&state=FINISHED")
    info(s"The failed apps URL is $failedAppsURL")
    val failedApps = readApps(failedAppsURL)
    appList ++= failedApps

    var firstRetryQueueFetchCount = 0
    while (!_firstRetryQueue.isEmpty()) {
      val job = _firstRetryQueue.poll()
      info(s"First retry queue: ${job.appId} polled")
      firstRetryQueueFetchCount = firstRetryQueueFetchCount + 1
      appList += job
    }

    var secondRetryQueueFetchCount = 0
    _secondRetryQueue.synchronized{
      val iteratorSecondRetry = _secondRetryQueue.iterator
      while(iteratorSecondRetry.hasNext) {
        val job = iteratorSecondRetry.next()
        if (job.tryFetchOutFromSecondRetryQueue) {
          info(s"Second retry queue: ${job.appId} polled")
          secondRetryQueueFetchCount = secondRetryQueueFetchCount + 1
          appList += job
          iteratorSecondRetry.remove()
        }
      }
    }

    _lastTime = fetchEndTime
    info(s"Total ${appList.size} items fetched --- " +
      s"${succeededApps.size} succeed, ${failedApps.size} failed, " +
      s"$firstRetryQueueFetchCount first retry, $secondRetryQueueFetchCount second retry")
    appList.toList
      .map(_.setLannisterComponent(_context))
      .map(_.setPersistService(_persistService))
  }

  override def addIntoRetries(job: AnalyticJob): Unit = {
    _firstRetryQueue.add(job)
    _metricsController.setRetryQueueSize(_firstRetryQueue.size())
  }

  override def addIntoSecondRetryQueue(job: AnalyticJob): Unit = {
    _secondRetryQueue.synchronized {
      _secondRetryQueue.add(job.setInitialSecondRetryGap)
    }
    _metricsController.setSecondRetryQueueSize(_secondRetryQueue.size)
  }

  private def updateResourceManagerAddresses(): Unit = {
    val configuration = HadoopConf.conf
    if (configuration.get(IS_RM_HA_ENABLED).toBoolean) {
      configuration.get(RESOURCE_MANAGER_IDS).split(",").foreach(id => {
        val resourceManager = configuration.get(RESOURCE_MANAGER_ADDRESS + "." + id)
        val resourceManagerURL = String.format(RM_NODE_STATE_URL, resourceManager)
        val rootNode = readJsonNode(new URL(resourceManagerURL))
        val status = rootNode.path("clusterInfo").path("haState").asText()
        if ("ACTIVE" == status) {
          _rmAddress = resourceManager
        }
      })
    } else {
      _rmAddress = configuration.get(RESOURCE_MANAGER_ADDRESS)
    }
  }

  private def readJsonNode(url: URL) = _objectMapper.readTree(url.openStream)

  private def readApps(url: URL) = {
    readJsonNode(url).path("apps").path("app").iterator().asScala
      .filter(app => {
        _context.getAppTypeForName(app.get("applicationType").asText()).isDefined
      })
      .map(app => {
        val appId = app.get("id").asText()
        val user = app.get("user").asText()
        val name = app.get("name").asText()
        val queue = app.get("queue").asText()
        val url = Utils.getOrElse(app.get("trackingUrl"), (x: JsonNode) => x.asText(), null )
        val startTime = app.get("startedTime").asLong()
        val finishTime = app.get("finishedTime").asLong()
        val applicationType = _context.getAppTypeForName(app.get("applicationType").asText())

        AnalyticJob(appId, applicationType.get, user, name, queue, url, startTime, finishTime)
    })
  }
}

