package com.ctyun.lannister.analysis
import com.ctyun.lannister.LannisterContext
import com.ctyun.lannister.util.Logging
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.codehaus.jackson.map.ObjectMapper

import java.net.URL
import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ListBuffer

class AnalyticJobGeneratorHadoop3 extends AnalyticJobGenerator with Logging {
  private var _configuration:Configuration = LannisterContext().getConfiguration
  private var _resourceManagerAddress:String = null
  private val _objectMapper = new ObjectMapper
  private var _lastTime = 0L
  private var _fetchStartTime = 0L
  private var _currentTime = 0L
  private val FETCH_DELAY = 60000
  private val _firstRetryQueue:util.Queue[AnalyticJob] = new ConcurrentLinkedQueue[AnalyticJob]()

  private val IS_RM_HA_ENABLED = "yarn.resourcemanager.ha.enabled"
  private val RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.webapp.address"
  private val RESOURCE_MANAGER_IDS = "yarn.resourcemanager.ha.rm-ids"
  private val RM_NODE_STATE_URL = "http://%s/ws/v1/cluster/info"


  override def updateResourceManagerAddresses: Unit = {
    if(_configuration.get(IS_RM_HA_ENABLED).toBoolean){
      val resourceManagers =  _configuration.get(RESOURCE_MANAGER_IDS)
      if(StringUtils.isBlank(resourceManagers))
        throw new IllegalArgumentException("yarn.resourcemanager.ha.rm-ids is empty while ha enabled")

      resourceManagers.split(",").foreach(id=>{
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


  override def fetchAnalyticJobs: List[AnalyticJob] = {
    val appList = ListBuffer[AnalyticJob]()

    _currentTime = System.currentTimeMillis - FETCH_DELAY

    info("Fetching recent finished application runs between last time: " + (_lastTime + 1) + ", and current time: " + _currentTime)
    val succeededAppsURL = new URL(new URL("http://" + _resourceManagerAddress), String.format("/ws/v1/cluster/apps?finalStatus=SUCCEEDED&finishedTimeBegin=%s&finishedTimeEnd=%s", String.valueOf(_lastTime + 1), String.valueOf(_currentTime)))
    info("The succeeded apps URL is " + succeededAppsURL)
    val succeededApps = readApps(succeededAppsURL)
    appList ++= succeededApps

    val failedAppsURL = new URL(new URL("http://" + _resourceManagerAddress), String.format("/ws/v1/cluster/apps?finalStatus=FAILED&state=FINISHED&finishedTimeBegin=%s&finishedTimeEnd=%s", String.valueOf(_lastTime + 1), String.valueOf(_currentTime)))
    val failedApps = readApps(failedAppsURL)
    appList ++= failedApps

    while (!_firstRetryQueue.isEmpty()) {
      appList += _firstRetryQueue.poll()
    }

    _lastTime = _currentTime
    appList.toList
  }


  private def readJsonNode(url: URL) = _objectMapper.readTree(url.openStream)

  private def readApps(url: URL)={
    val appList = ListBuffer[AnalyticJob]()

    val rootNode = readJsonNode(url)
    val apps = rootNode.path("apps").path("app")

    apps.forEach(
      app=> {
        val appId = app.get("id").asText()

        if (_lastTime > _fetchStartTime || (_lastTime == _fetchStartTime)) { //TODO  && AppResult.find.byId(appId) == null)
          val user = app.get("user").asText()
          val name = app.get("name").asText()
          val queueName = app.get("queue").asText()
          val trackingUrl = if( app.get("trackingUrl") != null)  app.get("trackingUrl").asText() else null
          val startTime = app.get("startedTime").asLong()
          val finishTime = app.get("finishedTime").asLong()


        }
      })

    appList
  }
}

