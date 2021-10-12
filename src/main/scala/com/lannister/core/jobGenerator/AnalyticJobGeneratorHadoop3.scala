package com.lannister.core.jobGenerator

import java.net.URL
import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.lannister.LannisterContext
import com.lannister.core.conf.Configs
import com.lannister.core.domain.AnalyticJob
import com.lannister.core.hadoop.HadoopConf
import com.lannister.core.metric.MetricsController
import com.lannister.core.util.{Logging, Utils}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component


@Component
class AnalyticJobGeneratorHadoop3 extends AnalyticJobGenerator with Logging {
  @Autowired private var _context: LannisterContext = _
  @Autowired private var _metricsController : MetricsController = _
  private val _firstRetryQueue = new ConcurrentLinkedQueue[AnalyticJob]()
  private val _secondRetryQueue = new util.LinkedList[AnalyticJob]()
  // If we do not have a lag, we may have apps queried here, but the log file are not ready for read
  private val QUERY_DELAY = 60000
  private var _lastTime : Long = _


  override def configure: Unit = {
    _lastTime = Configs.INITIAL_FETCH_START_TIME.getValue
  }

  override def fetchAnalyticJobs: List[AnalyticJob] = {
    YarnUtil.updateResourceManagerAddresses()

    val fetchStartTime = _lastTime + 1
    val fetchEndTime = System.currentTimeMillis - QUERY_DELAY
    val timeSpan = s"finishedTimeBegin=$fetchStartTime&finishedTimeEnd=$fetchEndTime"
    val appList = ListBuffer[AnalyticJob]()

    info(s"Fetch finished application between last: $fetchStartTime, current: $fetchEndTime")
    val succeededApps = YarnUtil.readSuccApps(timeSpan).map(_.setSuccessfulJob)
    appList ++= succeededApps
    val failedApps = YarnUtil.readFailApps(timeSpan)
    appList ++= failedApps

    var firstRetryQueueFetchCount = 0
    while (!_firstRetryQueue.isEmpty()) {
      val job = _firstRetryQueue.poll()
      firstRetryQueueFetchCount = firstRetryQueueFetchCount + 1
      appList += job
      info(s"First retry queue: ${job.typeAndAppId} polled")
    }

    var secondRetryQueueFetchCount = 0
    _secondRetryQueue.synchronized{
      val iteratorSecondRetry = _secondRetryQueue.iterator
      while(iteratorSecondRetry.hasNext) {
        val job = iteratorSecondRetry.next()
        if (job.tryFetchOutFromSecondRetryQueue) {
          secondRetryQueueFetchCount = secondRetryQueueFetchCount + 1
          appList += job
          iteratorSecondRetry.remove()
          info(s"Second retry queue: ${job.typeAndAppId} polled")
        }
      }
    }

    _lastTime = fetchEndTime
    info(s"Total ${appList.size} items fetched --- " +
      s"${succeededApps.size} succeed, ${failedApps.size} failed, " +
      s"$firstRetryQueueFetchCount first retry, $secondRetryQueueFetchCount second retry")
    appList.toList
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



  object YarnUtil {
    private val _objectMapper = new ObjectMapper
    private val IS_RM_HA_ENABLED = "yarn.resourcemanager.ha.enabled"
    private val RESOURCE_MANAGER_IDS = "yarn.resourcemanager.ha.rm-ids"
    private val RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.webapp.address"
    private val RM_NODE_STATE_URL = "http://%s/ws/v1/cluster/info"
    private val RM_APPS_URL = "http://%s/ws/v1/cluster/apps?%s"
    private var _rmAddress : String = _

    def updateResourceManagerAddresses(): Unit = {
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

    def readSuccApps(condition : String): Iterator[AnalyticJob] = {
      val url = String.format(RM_APPS_URL, _rmAddress, s"$condition&finalStatus=SUCCEEDED")
      info(s"The succeeded apps URL is $url")
      readApps(new URL(url))
    }

    def readFailApps(condition : String): Iterator[AnalyticJob] = {
      val url = String.format(RM_APPS_URL, _rmAddress,
                              s"$condition&finalStatus=FAILED&state=FINISHED")
      info(s"The failed apps URL is $url")
      readApps(new URL(url))
    }

    private def readApps(url: URL): Iterator[AnalyticJob] = {
      readJsonNode(url).path("apps").path("app").iterator().asScala
        .filter { app =>
          _context.applicationTypeSupported(app.get("applicationType").asText())
        }
        .map { app =>
          val appId = app.get("id").asText()
          val user = app.get("user").asText()
          val name = app.get("name").asText()
          val queue = app.get("queue").asText()
          val url = Utils.getOrElse(app.get("trackingUrl"), (x: JsonNode) => x.asText(), null )
          val startTime = app.get("startedTime").asLong()
          val finishTime = app.get("finishedTime").asLong()
          val applicationType = app.get("applicationType").asText()

          AnalyticJob(appId, applicationType, user, name, queue, url, startTime, finishTime)
        }
    }

    private def readJsonNode(url: URL) = _objectMapper.readTree(url.openStream)
  }
}

