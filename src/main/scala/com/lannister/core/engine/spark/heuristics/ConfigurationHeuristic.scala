package com.lannister.core.engine.spark.heuristics

import scala.collection.mutable.ListBuffer

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain._
import com.lannister.core.domain.{HeuristicResult => HR}
import com.lannister.core.domain.{HeuristicResultDetail => HD}
import com.lannister.core.domain.Severity.Severity
import com.lannister.core.engine.spark.fetchers.SparkApplicationData
import com.lannister.core.util.MemoryFormatUtils._
import com.lannister.core.util.TimeUtils._


class ConfigurationHeuristic (val config: HeuristicConfiguration) extends Heuristic{

  /*
  * Heu configuration parse part
  * */
  import SeverityThresholds._
  val params = config.params
  val SPARK_OVERHEAD_MEMORY_THRESHOLD_KEY = "spark_overheadMemory_thresholds_key"
  val SPARK_MEMORY_THRESHOLD_KEY = "spark_memory_thresholds_key"
  val SPARK_CORE_THRESHOLD_KEY = "spark_core_thresholds_key"
  val SERIALIZER_IF_NON_NULL_RECOMMENDATION_KEY = "serializer_if_non_null_recommendation"
  val thresOverheadMem = parse(params.get(SPARK_OVERHEAD_MEMORY_THRESHOLD_KEY))
  val thresholdMemory = parse(params.get(SPARK_MEMORY_THRESHOLD_KEY), fm = str2Bytes)
  val thresholdCore = parse(params.get(SPARK_CORE_THRESHOLD_KEY))
  val serializerIfNonNullRecommendation = params.get(SERIALIZER_IF_NON_NULL_RECOMMENDATION_KEY)


  override def apply(data: ApplicationData): HeuristicResult = {
    import ConfigurationHeuristic._

    def fmt(property: Option[String]): String =
      property.getOrElse("Not presented. Using default.")

    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])
    var hds = ListBuffer(
      HD(SPARK_DRIVER_MEMORY, fmt(evaluator.driverMemBytes.map(bytes2Str))),
      HD(SPARK_EXECUTOR_MEMORY, fmt(evaluator.executorMemBytes.map(bytes2Str))),
      HD(SPARK_EXECUTOR_CORES, fmt(evaluator.executorCores.map(_.toString))),
      HD(SPARK_DRIVER_CORES, fmt(evaluator.driverCores.map(_.toString))),
      HD(SPARK_EXECUTOR_INSTANCES, fmt(evaluator.executorInstances.map(_.toString))),
      HD(SPARK_EXECUTOR_MEMORY_OVERHEAD, evaluator.executorMemOverhead),
      HD(SPARK_DRIVER_MEMORY_OVERHEAD, evaluator.driverMemOverhead),
      HD(SPARK_DYNAMIC_ALLOCATION_ENABLED, fmt(evaluator.dyAllocEnable.map(_.toString))),
      HD(SPARK_APPLICATION_DURATION, evaluator.appDuration.toString + " Seconds")
    )

    if (evaluator.severityMinExecutors == Severity.CRITICAL) {
      hds += HD("Minimum Executors", "The minimum executors for Dynamic Allocation should be <=1")
    }
    if (evaluator.severityMaxExecutors == Severity.CRITICAL) {
      hds += HD("Maximum Executors", "The maximum executors for Dynamic Allocation should be <=900")
    }
    if (evaluator.jarsSeverity == Severity.CRITICAL) {
      hds += HD("Jars notation", "Not use * while specifying jars in the field " + SPARK_YARN_JARS)
    }
    if(evaluator.severityDriverMemOverhead.id >= Severity.SEVERE.id) {
      hds += HD("Driver Overhead Memory", "Do not specify excessive overhead memory for Driver")
    }
    if(evaluator.severityExecutorMemOverhead.id >= Severity.SEVERE.id) {
      hds += HD("Executor Overhead Memory", "Do not specify excessive overhead memory for Executor")
    }
    if (evaluator.serializerSeverity != Severity.NONE) {
      hds += HD(SPARK_SERIALIZER_KEY, fmt(evaluator.serializer))
    }
    if (evaluator.shuffleAndDynamicAllocSeverity != Severity.NONE) {
      hds += HD(SPARK_SHUFFLE_SERVICE_ENABLED, fmt(evaluator.shuffleSerEnable.map(_.toString)))
    }


    HR(config.getClassname, config.getName, evaluator.severity, 0, hds.toList)
  }
}

object ConfigurationHeuristic {
  val SPARK_DRIVER_MEMORY = "spark.driver.memory"
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"
  val SPARK_DRIVER_CORES = "spark.driver.cores"
  val SPARK_EXECUTOR_INSTANCES = "spark.executor.instances"
  val SPARK_EXECUTOR_MEMORY_OVERHEAD = "spark.executor.memoryOverhead"
  val SPARK_DRIVER_MEMORY_OVERHEAD = "spark.driver.memoryOverhead"
  val SPARK_DYNAMIC_ALLOCATION_ENABLED = "spark.dynamicAllocation.enabled"
  val SPARK_SHUFFLE_SERVICE_ENABLED = "spark.shuffle.service.enabled"
  val SPARK_SERIALIZER_KEY = "spark.serializer"
  val SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = "spark.dynamicAllocation.minExecutors"
  val SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = "spark.dynamicAllocation.maxExecutors"
  val SPARK_YARN_JARS = "spark.yarn.secondary.jars"
  val SPARK_APPLICATION_DURATION = "spark.application.duration"
  val THRESHOLD_MIN_EXECUTORS = 1
  val THRESHOLD_MAX_EXECUTORS = 900

  class Evaluator(heuristic: ConfigurationHeuristic, data: SparkApplicationData) {
    /*
    * Common data retrieve part
    * */
    lazy val appConf = data.store.store.environmentInfo().sparkProperties.toMap
    lazy val appInfo = data.store.store.applicationInfo.attempts.last

    lazy val driverMemBytes = appConf.get(SPARK_DRIVER_MEMORY).map(str2Bytes)
    lazy val executorMemBytes = appConf.get(SPARK_EXECUTOR_MEMORY).map(str2Bytes)
    lazy val executorCores = appConf.get(SPARK_EXECUTOR_CORES).map(_.toInt)
    lazy val driverCores = appConf.get(SPARK_DRIVER_CORES).map(_.toInt)
    lazy val executorInstances = appConf.get(SPARK_EXECUTOR_INSTANCES).map(_.toInt)
    lazy val executorMemOverhead = appConf.getOrElse(SPARK_EXECUTOR_MEMORY_OVERHEAD, "0")
    lazy val driverMemOverhead = appConf.getOrElse(SPARK_DRIVER_MEMORY_OVERHEAD, "0")
    lazy val dyAllocEnable = Some(appConf.get(SPARK_DYNAMIC_ALLOCATION_ENABLED).exists(_.toBoolean))
    lazy val shuffleSerEnable = Some(appConf.get(SPARK_SHUFFLE_SERVICE_ENABLED).exists(_.toBoolean))
    lazy val serializer = appConf.get(SPARK_SERIALIZER_KEY)
    lazy val dynamicMinExecutors = appConf.get(SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS).map(_.toInt)
    lazy val dynamicMaxExecutors = appConf.get(SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS).map(_.toInt)
    lazy val yarnJars = appConf.getOrElse(SPARK_YARN_JARS, "")
    lazy val appDuration = (appInfo.endTime.getTime - appInfo.startTime.getTime)/ SECOND_IN_MS


    /*
    * Severity compute part
    *  */
    val severityDriverCores = heuristic.thresholdCore.of(driverCores.getOrElse(0))
    val severityDriverMemory = heuristic.thresholdMemory.of(driverMemBytes.getOrElse(0L))
    val severityExecutorCores = heuristic.thresholdCore.of(executorCores.getOrElse(0))
    val severityExecutorMemory = heuristic.thresholdMemory.of(executorMemBytes.getOrElse(0L))
    val severityMinExecutors = if (dynamicMinExecutors.getOrElse(0) > THRESHOLD_MIN_EXECUTORS) {
      Severity.CRITICAL
    } else {
      Severity.NONE
    }
    val severityMaxExecutors = if (dynamicMaxExecutors.getOrElse(0) > THRESHOLD_MAX_EXECUTORS) {
      Severity.CRITICAL
    } else {
      Severity.NONE
    }
    lazy val jarsSeverity = if (yarnJars.contains("*")) { Severity.CRITICAL } else { Severity.NONE }
    val severityExecutorMemOverhead = heuristic.thresOverheadMem.of(str2Bytes(executorMemOverhead))
    val severityDriverMemOverhead = heuristic.thresOverheadMem.of(str2Bytes(driverMemOverhead))
    lazy val serializerSeverity = serializer match {
      case None => Severity.SEVERE
      case Some(heuristic.serializerIfNonNullRecommendation) => Severity.NONE
      case Some(_) => Severity.MODERATE
    }
    lazy val shuffleAndDynamicAllocSeverity = (dyAllocEnable, shuffleSerEnable) match {
      case (_, Some(true)) => Severity.NONE
      case (Some(false), Some(false)) => Severity.MODERATE
      case (Some(true), Some(false)) => Severity.SEVERE
    }

    val severity: Severity = Severity.max(
      severityDriverCores,
      severityDriverMemory,
      severityExecutorCores,
      severityExecutorMemory,
      severityMinExecutors,
      severityMaxExecutors,
      jarsSeverity,
      severityExecutorMemOverhead,
      severityDriverMemOverhead,
      serializerSeverity,
      shuffleAndDynamicAllocSeverity)
  }

}
