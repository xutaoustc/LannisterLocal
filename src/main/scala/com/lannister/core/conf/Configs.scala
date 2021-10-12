package com.lannister.core.conf

import org.apache.commons.lang3.StringUtils

object Configs {
  require(
      StringUtils.isNotBlank(CommonVars("hadoop.config.dir", "").getValue) ||
      StringUtils.isNotBlank(CommonVars("HADOOP_CONF_DIR", "").getValue),
      "Neither hadoop.config.dir or HADOOP_CONF_DIR should not be empty"
  )

  // HADOOP
  val hadoopConfDir = CommonVars("hadoop.config.dir", CommonVars("HADOOP_CONF_DIR", "").getValue)

  // Global fetcher, heuristic configuration
  val FETCHERS_CONF = CommonVars("lannister.fetchers.conf", "FetcherConf.yml")
  val HEURISTICS_CONF = CommonVars("lannister.heuristic.conf", "HeuristicConf.yml")

  // Security
  val KEYTAB_USER = CommonVars("lannister.keytab.user", "")
  val KEYTAB_LOCATION = CommonVars("lannister.keytab.location", "")

  // Execute params
  val INITIAL_FETCH_START_TIME = CommonVars("lannister.initialFetchStartTime", 0L)
  val EXECUTOR_NUM = CommonVars("lannister.executor.num", 5)
  val RETRY_INTERVAL = CommonVars("lannister.analysis.retry.interval", 60 * 1000)
  val FETCH_INTERVAL = CommonVars("lannister.analysis.fetch.interval", 1 * 1000)
  val RETRY_LIMIT = CommonVars("lannister.retry.limit", 3)
  val SECOND_RETRY_LIMIT = CommonVars("lannister.secondretry.limit", 5)


  val AUTO_TUNING_ENABLED = CommonVars("lannister.autotuning.enabled", "true")
}
