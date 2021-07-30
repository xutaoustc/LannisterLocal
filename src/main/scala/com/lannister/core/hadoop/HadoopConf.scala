package com.lannister.core.hadoop

import java.nio.file.Paths

import com.lannister.core.conf.Configs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object HadoopConf {
  val conf = new Configuration()
  val root = Configs.hadoopConfDir.getValue
  conf.addResource(new Path(Paths.get(root, "core-site.xml").toAbsolutePath.toFile.getAbsolutePath))
  conf.addResource(new Path(Paths.get(root, "hdfs-site.xml").toAbsolutePath.toFile.getAbsolutePath))
  conf.addResource(new Path(Paths.get(root, "yarn-site.xml").toAbsolutePath.toFile.getAbsolutePath))
}
