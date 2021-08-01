package com.lannister.core.engine.spark.fetchers

import scala.collection.JavaConverters._

import com.lannister.core.conf.fetcher.FetcherConfiguration
import com.lannister.core.domain.{AnalyticJob, Fetcher}
import com.lannister.core.hadoop.{HadoopConf, HadoopSecurity}
import com.lannister.core.util.Utils
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

import org.apache.spark.deploy.history.{HistoryAppStatusStoreWrapper, ReplayListenerBusWrapper}


class SparkFSFetcher(config: FetcherConfiguration) extends Fetcher[SparkApplicationData]{
  private val rootPath: String = config.params.asScala("rootPath")

  override def fetchData(job: AnalyticJob): Option[SparkApplicationData] = {
    HadoopSecurity().doAs {
      val fs = FileSystem.get(HadoopConf.conf)

      Utils.tryFinally {
        val jobAttemptPaths = fs.listStatus(new Path(rootPath), new PathFilter {
          override def accept(path: Path): Boolean = path.getName.contains(job.appId)
        })

        jobAttemptPaths match {
          case Array() => None
          case arr => val finalAttempt = arr.sortBy(_.getPath.getName).reverse.head
            val replayBus = new ReplayListenerBusWrapper(fs, finalAttempt)
            Option(SparkApplicationData(replayBus.parse()))
        }
      } {
        fs.close()
      }
    }
  }
}
