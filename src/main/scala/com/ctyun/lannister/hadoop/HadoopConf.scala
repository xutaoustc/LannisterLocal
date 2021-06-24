package com.ctyun.lannister.hadoop

import java.nio.file.Paths

import com.ctyun.lannister.conf.Configs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


object HadoopConf {
  val conf = new Configuration()
  conf.addResource(new Path(
    Paths.get(s"${Configs.hadoopConfDir.getValue}", "core-site.xml")
      .toAbsolutePath.toFile.getAbsolutePath))
  conf.addResource(new Path(
    Paths.get(s"${Configs.hadoopConfDir.getValue}", "hdfs-site.xml")
      .toAbsolutePath.toFile.getAbsolutePath))
  conf.addResource(new Path(
    Paths.get(s"${Configs.hadoopConfDir.getValue}", "yarn-site.xml")
      .toAbsolutePath.toFile.getAbsolutePath))
}
