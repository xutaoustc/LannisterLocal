package com.lannister.core.engine.spark.heuristics

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain.{HeuristicResult => HR, HeuristicResultDetail => HD, _}
import com.lannister.core.engine.spark.fetchers.SparkApplicationData
import com.lannister.core.util.MemoryFormatUtils._
import com.lannister.core.util.TimeUtils._


class ExecutorsHeuristic(private val config: HeuristicConfiguration) extends Heuristic{

  import SeverityThresholds._
  val params = config.params
  val maxToMedianRatioSeverityThresholds = parse(params.get("max_to_median_severity_thresholds"))
  val ignoreMaxBytLessThanThreshold = str2Bytes(params.get("ignore_max_bytes_less_than_threshold"))
  val ignoreMaxMillisLessThanThreshold = params.get("ignore_max_millis_less_than_threshold").toLong


  override def apply(data: ApplicationData): HR = {
    import ExecutorsHeuristic.Evaluator
    val evl = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val hds = Seq(
      HD("Total executor storage memory allocated", bytes2Str(evl.totalStorageMemAllocated)),
      HD("Total executor storage memory used", bytes2Str(evl.totalStorageMemUsed)),
      HD("Executor storage memory utilization rate", f"${evl.storageMemoryUtilizationRate}%1.3f"),
      HD("Executor storage memory used distribution", evl.storageMemoryUsedDistr.text(bytes2Str)),
      HD("Executor task time distribution", evl.taskTimeDistr.text(timeFormat)),
      HD("Executor task time sum", (evl.totalTaskTime / SECOND_IN_MS).toString),
      HD("Executor input bytes distribution", evl.inputBytesDistr.text(bytes2Str)),
      HD("Executor shuffle read bytes distribution", evl.shuffleReadBytesDistr.text(bytes2Str)),
      HD("Executor shuffle write bytes distribution", evl.shuffleWriteBytesDistr.text(bytes2Str))
    )

    HR(config.classname, config.name, evl.severity, 0, hds.toList)
  }
}

object ExecutorsHeuristic{
  class Evaluator(heuristic: ExecutorsHeuristic, data: SparkApplicationData) {
    implicit val ignoreMaxBytLessThanThreshold = heuristic.ignoreMaxBytLessThanThreshold
    implicit val maxToMedianRatioSeverityThresholds = heuristic.maxToMedianRatioSeverityThresholds
    lazy val exSummaries = data.store.store.executorList(true)

    lazy val totalStorageMemAllocated = exSummaries.map { _.maxMemory }.sum
    lazy val totalStorageMemUsed = exSummaries.map { _.memoryUsed }.sum
    lazy val storageMemoryUtilizationRate = totalStorageMemUsed.toDouble / totalStorageMemAllocated
    lazy val totalTaskTime = exSummaries.map(_.totalDuration).sum
    lazy val storageMemoryUsedDistr = Distribution(exSummaries.map { _.memoryUsed })
    lazy val taskTimeDistr = Distribution(exSummaries.map { _.totalDuration })
    lazy val inputBytesDistr = Distribution(exSummaries.map { _.totalInputBytes })
    lazy val shuffleReadBytesDistr = Distribution(exSummaries.map { _.totalShuffleRead })
    lazy val shuffleWriteBytesDistr = Distribution(exSummaries.map { _.totalShuffleWrite })

    lazy val storageMemoryUsedSeverity = storageMemoryUsedDistr.severityOfDistribution
    lazy val taskTimeSeverity = taskTimeDistr.severityOfDistribution(
      heuristic.maxToMedianRatioSeverityThresholds, heuristic.ignoreMaxMillisLessThanThreshold)
    lazy val inputBytesSeverity = inputBytesDistr.severityOfDistribution
    lazy val shuffleReadBytesSeverity = shuffleReadBytesDistr.severityOfDistribution
    lazy val shuffleWriteBytesSeverity = shuffleWriteBytesDistr.severityOfDistribution


    lazy val severity = Severity.max(
      storageMemoryUsedSeverity,
      taskTimeSeverity,
      inputBytesSeverity,
      shuffleReadBytesSeverity,
      shuffleWriteBytesSeverity
    )
  }

}
