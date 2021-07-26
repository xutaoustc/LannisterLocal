package com.ctyun.lannister.core.spark.heuristics

import com.ctyun.lannister.analysis._
import com.ctyun.lannister.analysis.Severity.Severity
import com.ctyun.lannister.core.conf.heuristic.HeuristicConfiguration
import com.ctyun.lannister.core.spark.data.SparkApplicationData
import com.ctyun.lannister.math.Statistics
import com.ctyun.lannister.util.MemoryFormatUtils

class ConfigurationHeuristic (val heuristicConfig: HeuristicConfiguration) extends Heuristic{

  import ConfigurationHeuristic._

  val sparkOverheadMemoryThreshold: SeverityThresholds = SeverityThresholds.parse(
    heuristicConfig.getParams.get(SPARK_OVERHEAD_MEMORY_THRESHOLD_KEY), ascending = true)
  val sparkMemoryThreshold: SeverityThresholds = SeverityThresholds.parse(
    heuristicConfig.getParams.get(SPARK_MEMORY_THRESHOLD_KEY),
    ascending = true, MemoryFormatUtils.stringToBytes)
  val sparkCoreThreshold: SeverityThresholds = SeverityThresholds.parse(
    heuristicConfig.getParams.get(SPARK_CORE_THRESHOLD_KEY), ascending = true)
  val serializerIfNonNullRecommendation: String =
    heuristicConfig.getParams.get(SERIALIZER_IF_NON_NULL_RECOMMENDATION_KEY)

  override def apply(data: ApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    def formatProperty(property: Option[String]): String =
      property.getOrElse("Not presented. Using default.")

    var resultDetails = Seq(
      new HeuristicResultDetails(SPARK_DRIVER_MEMORY_KEY,
        formatProperty(evaluator.driverMemoryBytes.map(MemoryFormatUtils.bytesToString))),
      new HeuristicResultDetails(SPARK_EXECUTOR_MEMORY_KEY,
        formatProperty(evaluator.executorMemoryBytes.map(MemoryFormatUtils.bytesToString))),
      new HeuristicResultDetails(SPARK_EXECUTOR_CORES_KEY,
        formatProperty(evaluator.executorCores.map(_.toString))),
      new HeuristicResultDetails(SPARK_DRIVER_CORES_KEY,
        formatProperty(evaluator.driverCores.map(_.toString))),
      new HeuristicResultDetails(SPARK_EXECUTOR_INSTANCES_KEY,
        formatProperty(evaluator.executorInstances.map(_.toString))),
      new HeuristicResultDetails(SPARK_APPLICATION_DURATION,
        evaluator.applicationDuration.toString + " Seconds"),
      new HeuristicResultDetails(SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD,
        evaluator.sparkYarnExecutorMemoryOverhead),
      new HeuristicResultDetails(SPARK_YARN_DRIVER_MEMORY_OVERHEAD,
        evaluator.sparkYarnDriverMemoryOverhead),
      new HeuristicResultDetails(SPARK_DYNAMIC_ALLOCATION_ENABLED,
        formatProperty(evaluator.isDynamicAllocEnabled.map(_.toString)))
    )

    if (evaluator.serializerSeverity != Severity.NONE) {
      resultDetails = resultDetails :+ HeuristicResultDetails(SPARK_SERIALIZER_KEY,
        formatProperty(evaluator.serializer))
    }
    if (evaluator.shuffleAndDynamicAllocSeverity != Severity.NONE) {
      resultDetails = resultDetails :+ HeuristicResultDetails(SPARK_SHUFFLE_SERVICE_ENABLED,
        formatProperty(evaluator.isShuffleServiceEnabled.map(_.toString)))
    }
    if (evaluator.severityMinExecutors == Severity.CRITICAL) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Minimum Executors",
        "The minimum executors for Dynamic Allocation should be <=1. " +
          "Please change it in the " + SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS + " field.")
    }
    if (evaluator.severityMaxExecutors == Severity.CRITICAL) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Maximum Executors",
        "The maximum executors for Dynamic Allocation should be <=900. " +
          "Please change it in the " + SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS + " field.")
    }
    if (evaluator.jarsSeverity == Severity.CRITICAL) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Jars notation",
"It is recommended to not use * notation while specifying jars in the field " + SPARK_YARN_JARS)
    }
    if(evaluator.severityDriverMemoryOverhead.id >= Severity.SEVERE.id) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Driver Overhead Memory",
        "Please do not specify excessive amount of overhead memory for Driver." +
          " Change it in the field " + SPARK_YARN_DRIVER_MEMORY_OVERHEAD)
    }
    if(evaluator.severityExecutorMemoryOverhead.id >= Severity.SEVERE.id) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Executor Overhead Memory",
        "Please do not specify excessive amount of overhead memory for Executors." +
          " Change it in the field " + SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD)
    }

    new HeuristicResult(
      heuristicConfig.getClassname,
      heuristicConfig.getName,
      evaluator.severity,
      0,
      resultDetails.toList
    )
  }
}

object ConfigurationHeuristic {
  val SPARK_OVERHEAD_MEMORY_THRESHOLD_KEY = "spark_overheadMemory_thresholds_key"
  val SPARK_MEMORY_THRESHOLD_KEY = "spark_memory_thresholds_key"
  val SPARK_CORE_THRESHOLD_KEY = "spark_core_thresholds_key"
  val SERIALIZER_IF_NON_NULL_RECOMMENDATION_KEY = "serializer_if_non_null_recommendation"

  val SPARK_DRIVER_MEMORY_KEY = "spark.driver.memory"
  val SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES_KEY = "spark.executor.cores"
  val SPARK_DRIVER_CORES_KEY = "spark.driver.cores"
  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"
  val SPARK_APPLICATION_DURATION = "spark.application.duration"
  val SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD = "spark.yarn.executor.memoryOverhead"
  val SPARK_YARN_DRIVER_MEMORY_OVERHEAD = "spark.yarn.driver.memoryOverhead"
  val SPARK_SHUFFLE_SERVICE_ENABLED = "spark.shuffle.service.enabled"
  val SPARK_DYNAMIC_ALLOCATION_ENABLED = "spark.dynamicAllocation.enabled"
  val SPARK_SERIALIZER_KEY = "spark.serializer"
  val SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = "spark.dynamicAllocation.minExecutors"
  val SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = "spark.dynamicAllocation.maxExecutors"
  val SPARK_YARN_JARS = "spark.yarn.secondary.jars"



  class Evaluator(configurationHeuristic: ConfigurationHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.store.store.environmentInfo().sparkProperties.toMap

    lazy val driverMemoryBytes: Option[Long] =
      appConfigurationProperties.get(SPARK_DRIVER_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)
    lazy val executorMemoryBytes: Option[Long] =
      appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)
    lazy val executorCores: Option[Int] =
      appConfigurationProperties.get(SPARK_EXECUTOR_CORES_KEY).map(_.toInt)
    lazy val driverCores: Option[Int] =
      appConfigurationProperties.get(SPARK_DRIVER_CORES_KEY).map(_.toInt)
    val severityExecutorMemory: Severity = configurationHeuristic.sparkMemoryThreshold
      .of(executorMemoryBytes.getOrElse(0).asInstanceOf[Number].longValue)
    val severityDriverMemory: Severity = configurationHeuristic.sparkMemoryThreshold
      .of(driverMemoryBytes.getOrElse(0).asInstanceOf[Number].longValue)
    val severityDriverCores: Severity = configurationHeuristic.sparkCoreThreshold
      .of(driverCores.getOrElse(0).asInstanceOf[Number].intValue)
    val severityExecutorCores: Severity = configurationHeuristic.sparkCoreThreshold
      .of(executorCores.getOrElse(0).asInstanceOf[Number].intValue)

    lazy val executorInstances: Option[Int] =
      appConfigurationProperties.get(SPARK_EXECUTOR_INSTANCES_KEY).map(_.toInt)
    lazy val applicationDuration: Long = {
      require(data.store.store.applicationInfo.attempts.nonEmpty)
      val lastApplicationAttemptInfo = data.store.store.applicationInfo.attempts.last
      (lastApplicationAttemptInfo.endTime.getTime - lastApplicationAttemptInfo.startTime.getTime)/
        Statistics.SECOND_IN_MS
    }

    lazy val sparkYarnExecutorMemoryOverhead: String =
      appConfigurationProperties.getOrElse(SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD, "0")
    lazy val sparkYarnDriverMemoryOverhead: String =
      appConfigurationProperties.getOrElse(SPARK_YARN_DRIVER_MEMORY_OVERHEAD, "0")
    val severityExecutorMemoryOverhead: Severity =
      configurationHeuristic.sparkOverheadMemoryThreshold.of(
        MemoryFormatUtils.stringToBytes(sparkYarnExecutorMemoryOverhead))
    val severityDriverMemoryOverhead: Severity =
      configurationHeuristic.sparkOverheadMemoryThreshold.of(
        MemoryFormatUtils.stringToBytes(sparkYarnDriverMemoryOverhead))

    lazy val isDynamicAllocEnabled: Option[Boolean] = Some(
      appConfigurationProperties.get(SPARK_DYNAMIC_ALLOCATION_ENABLED).exists(_.toBoolean == true))
    lazy val isShuffleServiceEnabled: Option[Boolean] = Some(
      appConfigurationProperties.get(SPARK_SHUFFLE_SERVICE_ENABLED).exists(_.toBoolean == true))
    lazy val shuffleAndDynamicAllocSeverity: Severity =
      (isDynamicAllocEnabled, isShuffleServiceEnabled) match {
        case (_, Some(true)) => Severity.NONE
        case (Some(false), Some(false)) => Severity.MODERATE
        case (Some(true), Some(false)) => Severity.SEVERE
      }

    private val serializerIfNonNullRecommendation =
      configurationHeuristic.serializerIfNonNullRecommendation
    private val DEFAULT_SERIALIZER_IF_NON_NULL_SEVERITY_IF_RECOMMENDATION_UNMET = Severity.MODERATE
    lazy val serializer: Option[String] = appConfigurationProperties.get(SPARK_SERIALIZER_KEY)
    lazy val serializerSeverity: Severity = serializer match {
      case None => Severity.MODERATE
      case Some(`serializerIfNonNullRecommendation`) => Severity.NONE
      case Some(_) => DEFAULT_SERIALIZER_IF_NON_NULL_SEVERITY_IF_RECOMMENDATION_UNMET
    }


    val THRESHOLD_MIN_EXECUTORS: Int = 1
    val THRESHOLD_MAX_EXECUTORS: Int = 900
    lazy val dynamicMinExecutors: Option[Int] =
      appConfigurationProperties.get(SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS).map(_.toInt)
    lazy val dynamicMaxExecutors: Option[Int] =
      appConfigurationProperties.get(SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS).map(_.toInt)
    val severityMinExecutors: Severity =
      if (dynamicMinExecutors.getOrElse(0).intValue > THRESHOLD_MIN_EXECUTORS) {
        Severity.CRITICAL
      } else {
        Severity.NONE
      }
    val severityMaxExecutors: Severity =
      if (dynamicMaxExecutors.getOrElse(0).intValue > THRESHOLD_MAX_EXECUTORS) {
        Severity.CRITICAL
      } else {
        Severity.NONE
      }


    lazy val jarsSeverity: Severity =
      if (appConfigurationProperties.getOrElse(SPARK_YARN_JARS, "").contains("*")) {
        Severity.CRITICAL
      } else {
        Severity.NONE
      }

    val severity: Severity = Severity.max(severityDriverCores, severityDriverMemory,
      severityExecutorCores, severityExecutorMemory, severityMinExecutors, severityMaxExecutors,
      jarsSeverity, severityExecutorMemoryOverhead, severityDriverMemoryOverhead,
      serializerSeverity, shuffleAndDynamicAllocSeverity)
  }

}