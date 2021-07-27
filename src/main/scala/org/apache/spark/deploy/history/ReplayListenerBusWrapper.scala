package org.apache.spark.deploy.history
// scalastyle:off
import com.ctyun.lannister.core.util.Logging
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.scheduler.ReplayListenerBus.{ReplayEventsFilter, SELECT_ALL_FILTER}
import org.apache.spark.status.{AppHistoryServerPlugin, AppStatusListener, ElementTrackingStore}
import org.apache.spark.util.kvstore.InMemoryStore
import org.apache.spark.util.{SystemClock, Utils}

import java.util.ServiceLoader
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class ReplayListenerBusWrapper(fs:FileSystem, finalAttempt:FileStatus) extends Logging{
  private val APPL_START_EVENT_PREFIX = "{\"Event\":\"SparkListenerApplicationStart\""
  private val APPL_END_EVENT_PREFIX = "{\"Event\":\"SparkListenerApplicationEnd\""
  private val LOG_START_EVENT_PREFIX = "{\"Event\":\"SparkListenerLogStart\""
  private val ENV_UPDATE_EVENT_PREFIX = "{\"Event\":\"SparkListenerEnvironmentUpdate\","
  private val eventsFilter: ReplayEventsFilter = { eventString =>
    eventString.startsWith(APPL_START_EVENT_PREFIX) ||
      eventString.startsWith(APPL_END_EVENT_PREFIX) ||
      eventString.startsWith(LOG_START_EVENT_PREFIX) ||
      eventString.startsWith(ENV_UPDATE_EVENT_PREFIX)
  }


  def parse() ={
    val replayConf = new SparkConf().set(ASYNC_TRACKING_ENABLED, false)
    val kvStore = new InMemoryStore
    val trackingStore = new ElementTrackingStore(kvStore, replayConf)

    try {
      // first parse to get the basic info
      val applicationInfo = basicParse()

      val reader = EventLogFileReader(fs, finalAttempt).get
      val replayBus = new ReplayListenerBus()
      val listener = new AppStatusListener(trackingStore, replayConf, false, lastUpdateTime = Some(applicationInfo.attempts.head.info.lastUpdated.getTime))
      replayBus.addListener(listener)

      for {
        plugin <- loadPlugins()
        listener <- plugin.createListeners(replayConf, trackingStore)
      } replayBus.addListener(listener)

      val eventLogFiles = reader.listEventLogFiles
      info(s"Parsing ${reader.rootPath} ...")
      parseAppEventLogs(eventLogFiles, replayBus, !reader.completed)
      trackingStore.close(false)
      info(s"Finished parsing ${reader.rootPath}")
      new HistoryAppStatusStore(new SparkConf(),kvStore)
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          trackingStore.close()
        }
        throw e
    }
  }

  private def loadPlugins(): Iterable[AppHistoryServerPlugin] = {
    ServiceLoader.load(classOf[AppHistoryServerPlugin], Utils.getContextOrSparkClassLoader).asScala
  }

  private def basicParse() ={
    val reader = EventLogFileReader(fs, finalAttempt).get
    val listener = new AppListingListener(reader, new SystemClock(), true)
    val bus = new ReplayListenerBus()
    bus.addListener(listener)

    val logFiles = reader.listEventLogFiles
    val appCompleted = reader.completed
    parseAppEventLogs(logFiles, bus, !appCompleted, eventsFilter)

    val lastFile = logFiles.last
    Utils.tryWithResource(EventLogFileReader.openEventLog(lastFile.getPath, fs)) { in =>
      bus.replay(in, lastFile.getPath.toString, !appCompleted, eventsFilter)
    }

    listener.applicationInfo.get
  }


  private def parseAppEventLogs(
                                 logFiles: Seq[FileStatus],
                                 replayBus: ReplayListenerBus,
                                 maybeTruncated: Boolean,
                                 eventsFilter: ReplayEventsFilter = SELECT_ALL_FILTER): Unit = {
    // stop replaying next log files if ReplayListenerBus indicates some error or halt
    var continueReplay = true
    logFiles.foreach { file =>
      if (continueReplay) {
        Utils.tryWithResource(EventLogFileReader.openEventLog(file.getPath, fs)) { in =>
          continueReplay = replayBus.replay(in, file.getPath.toString,
            maybeTruncated = maybeTruncated, eventsFilter = eventsFilter)
        }
      }
    }
  }

}
