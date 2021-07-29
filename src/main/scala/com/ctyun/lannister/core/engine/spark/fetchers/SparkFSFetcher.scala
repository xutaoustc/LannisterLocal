package com.ctyun.lannister.core.engine.spark.fetchers

import scala.collection.JavaConverters._

import com.ctyun.lannister.analysis.AnalyticJob
import com.ctyun.lannister.core.conf.fetcher.FetcherConfiguration
import com.ctyun.lannister.core.domain.Fetcher
import com.ctyun.lannister.core.engine.spark.data.SparkApplicationData
import com.ctyun.lannister.core.hadoop.{HadoopConf, HadoopSecurity}
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

import org.apache.spark.deploy.history.{HistoryAppStatusStoreWrapper, ReplayListenerBusWrapper}


class SparkFSFetcher(fetcherConfig: FetcherConfiguration) extends Fetcher[SparkApplicationData]{
  private val rootPath: String = fetcherConfig.params.asScala("rootPath")

  override def fetchData(job: AnalyticJob): Option[SparkApplicationData] = {
    HadoopSecurity().doAs {
      val fs = FileSystem.get(HadoopConf.conf)
      val attemptsList = fs.listStatus(new Path(rootPath), new PathFilter {
        override def accept(path: Path): Boolean = path.getName.contains(job.appId)
      })

      if(attemptsList.isEmpty) {
        fs.close()
        None
      } else {
        val finalAttempt = attemptsList
                            .sortWith((x, y) => x.getPath.getName > y.getPath.getName).head
        val replayBus = new ReplayListenerBusWrapper(fs, finalAttempt)
        val data = new SparkApplicationData( HistoryAppStatusStoreWrapper(replayBus.parse()) )
        fs.close()
        Option(data)
      }
    }
  }
}
