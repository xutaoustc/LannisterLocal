package com.ctyun.lannister.conf

object Configs {

  val AUTO_TUNING_ENABLED = CommonVars("lannister.autotuning.enabled", "true")

  val AGGREGATORS_CONF  = CommonVars("lannister.aggregators.conf", "AggregatorConf.yml")
  val FETCHERS_CONF  = CommonVars("lannister.fetchers.conf", "FetcherConf.yml")
  val HEURISTICS_CONF  = CommonVars("lannister.heuristic.conf", "HeuristicConf.yml")
  val JOBTYPES_CONF  = CommonVars("lannister.jobtype.conf", "JobTypeConf.yml")

  val EXECUTOR_NUM = CommonVars("lannister.executor.num", 5)
  val RETRY_INTERVAL = CommonVars("lannister.analysis.retry.interval", 60 * 1000)
  val FETCH_INTERVAL = CommonVars("lannister.analysis.fetch.interval", 10 * 1000)



  // Security
  val KEYTAB_USER = CommonVars("lannister.keytab.user", "")
  val KEYTAB_LOCATION = CommonVars("lannister.keytab.location", "")

}
