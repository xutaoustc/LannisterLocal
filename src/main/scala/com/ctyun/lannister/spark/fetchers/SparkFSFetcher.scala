package com.ctyun.lannister.spark.fetchers

import com.ctyun.lannister.analysis.{AnalyticJob, Fetcher}
import com.ctyun.lannister.conf.Configs
import com.ctyun.lannister.conf.fetcher.FetcherConfigurationData
import com.ctyun.lannister.spark.data.SparkApplicationData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.deploy.history.{EventLogFileReader, HistoryAppStatusStoreWrapper, ReplayListenerBusWrapper}
import org.apache.spark.scheduler.ReplayListenerBus

import java.nio.file.Paths
import scala.collection.JavaConverters._

class SparkFSFetcher(fetcherConfigurationData: FetcherConfigurationData) extends Fetcher[SparkApplicationData]{
  private val hadoopConf = new Configuration()
  hadoopConf.addResource(new Path(Paths.get(s"${Configs.hadoopConfDir.getValue}", "core-site.xml").toAbsolutePath.toFile.getAbsolutePath))
  hadoopConf.addResource(new Path(Paths.get(s"${Configs.hadoopConfDir.getValue}", "hdfs-site.xml").toAbsolutePath.toFile.getAbsolutePath))
  hadoopConf.addResource(new Path(Paths.get(s"${Configs.hadoopConfDir.getValue}", "yarn-site.xml").toAbsolutePath.toFile.getAbsolutePath))
  private val fs = FileSystem.get(hadoopConf)

  private val rootPath:String= fetcherConfigurationData.params.asScala("rootPath")

  override def fetchData(job: AnalyticJob): SparkApplicationData = {
    val attemptsList = fs.listStatus(new Path(rootPath), new PathFilter {
      override def accept(path: Path): Boolean = path.getName.contains(job.appId)
    })
    val finalAttempt = attemptsList.sortWith((x,y)=>x.getPath.getName > y.getPath.getName).head


    val replayBus = new ReplayListenerBusWrapper(fs, finalAttempt)
    new SparkApplicationData( HistoryAppStatusStoreWrapper(replayBus.parse()) )
  }
}
