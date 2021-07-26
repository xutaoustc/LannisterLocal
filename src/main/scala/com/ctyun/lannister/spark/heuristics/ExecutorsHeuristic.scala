package com.ctyun.lannister.spark.heuristics

import com.ctyun.lannister.analysis._
import com.ctyun.lannister.analysis.Severity.Severity
import com.ctyun.lannister.conf.heuristic.HeuristicConfiguration
import com.ctyun.lannister.math.Statistics
import com.ctyun.lannister.spark.data.SparkApplicationData
import com.ctyun.lannister.spark.heuristics.ExecutorsHeuristic.Evaluator
import com.ctyun.lannister.util.MemoryFormatUtils

import org.apache.spark.status.api.v1.ExecutorSummary

class ExecutorsHeuristic(private val heuristicConfig: HeuristicConfiguration) extends Heuristic{

  val maxToMedianRatioSeverityThresholds = SeverityThresholds.parse(
    heuristicConfig.params.get("max_to_median_severity_thresholds"), ascending = true)
  val ignoreMaxBytesLessThanThreshold = MemoryFormatUtils.stringToBytes(
    heuristicConfig.params.get("ignore_max_bytes_less_than_threshold"))
  val ignoreMaxMillisLessThanThreshold =
    heuristicConfig.params.get("ignore_max_millis_less_than_threshold").toLong


  override def apply(data: ApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val resultDetails = Seq(
      new HeuristicResultDetails(
        "Total executor storage memory allocated",
        MemoryFormatUtils.bytesToString(evaluator.totalStorageMemoryAllocated)
      ),
      new HeuristicResultDetails(
        "Total executor storage memory used",
        MemoryFormatUtils.bytesToString(evaluator.totalStorageMemoryUsed)
      ),
      new HeuristicResultDetails(
        "Executor storage memory utilization rate",
        f"${evaluator.storageMemoryUtilizationRate}%1.3f"
      ),
      new HeuristicResultDetails(
        "Executor storage memory used distribution",
        evaluator.storageMemoryUsedDistribution.formatDistribution(MemoryFormatUtils.bytesToString)
      ),
      new HeuristicResultDetails(
        "Executor task time distribution",
        evaluator.taskTimeDistribution.formatDistribution(Statistics.readableTimespan)
      ),
      new HeuristicResultDetails(
        "Executor task time sum",
        (evaluator.totalTaskTime / Statistics.SECOND_IN_MS).toString
      ),
      new HeuristicResultDetails(
        "Executor input bytes distribution",
        evaluator.inputBytesDistribution.formatDistribution(MemoryFormatUtils.bytesToString)
      ),
      new HeuristicResultDetails(
        "Executor shuffle read bytes distribution",
        evaluator.shuffleReadBytesDistribution.formatDistribution(MemoryFormatUtils.bytesToString)
      ),
      new HeuristicResultDetails(
        "Executor shuffle write bytes distribution",
        evaluator.shuffleWriteBytesDistribution.formatDistribution(MemoryFormatUtils.bytesToString)
      )
    )
    new HeuristicResult(
      heuristicConfig.classname,
      heuristicConfig.name,
      evaluator.severity,
      0,
      resultDetails.toList
    )
  }
}

object ExecutorsHeuristic{
  class Evaluator(executorsHeuristic: ExecutorsHeuristic, data: SparkApplicationData) {
    lazy val executorSummaries: Seq[ExecutorSummary] = data.store.store.executorList(true)

    lazy val totalStorageMemoryAllocated: Long = executorSummaries.map { _.maxMemory }.sum
    lazy val totalStorageMemoryUsed: Long = executorSummaries.map { _.memoryUsed }.sum
    lazy val storageMemoryUtilizationRate: Double =
      totalStorageMemoryUsed.toDouble / totalStorageMemoryAllocated.toDouble
    lazy val storageMemoryUsedDistribution: Distribution =
      Distribution(executorSummaries.map { _.memoryUsed })
    lazy val storageMemoryUsedSeverity = severityOfDistribution(storageMemoryUsedDistribution,
        executorsHeuristic.ignoreMaxBytesLessThanThreshold)

    lazy val totalTaskTime : Long = executorSummaries.map(_.totalDuration).sum
    lazy val taskTimeDistribution: Distribution =
      Distribution(executorSummaries.map { _.totalDuration })
    lazy val taskTimeSeverity = severityOfDistribution(taskTimeDistribution,
      executorsHeuristic.ignoreMaxMillisLessThanThreshold)

    lazy val inputBytesDistribution: Distribution =
      Distribution(executorSummaries.map { _.totalInputBytes })
    lazy val inputBytesSeverity: Severity = severityOfDistribution(inputBytesDistribution,
      executorsHeuristic.ignoreMaxBytesLessThanThreshold)

    lazy val shuffleReadBytesDistribution: Distribution =
      Distribution(executorSummaries.map { _.totalShuffleRead })
    lazy val shuffleReadBytesSeverity = severityOfDistribution(shuffleReadBytesDistribution,
      executorsHeuristic.ignoreMaxBytesLessThanThreshold)

    lazy val shuffleWriteBytesDistribution: Distribution =
      Distribution(executorSummaries.map { _.totalShuffleWrite })
    lazy val shuffleWriteBytesSeverity = severityOfDistribution(shuffleWriteBytesDistribution,
      executorsHeuristic.ignoreMaxBytesLessThanThreshold)

    lazy val severity: Severity = Severity.max(
      storageMemoryUsedSeverity,
      taskTimeSeverity,
      inputBytesSeverity,
      shuffleReadBytesSeverity,
      shuffleWriteBytesSeverity
    )


    private[heuristics] def severityOfDistribution(
      distribution: Distribution,
      ignoreMaxLessThanThreshold: Long,
      severityThresholds: SeverityThresholds = executorsHeuristic.maxToMedianRatioSeverityThresholds
    ): Severity = {
      if (distribution.max < ignoreMaxLessThanThreshold) {
        Severity.NONE
      } else if (distribution.median == 0L) {
        severityThresholds.of(Long.MaxValue)
      } else {
        severityThresholds.of(
          BigDecimal(distribution.max) / BigDecimal(distribution.median))
      }
    }
  }


  case class Distribution(min: Long, p25: Long, median: Long, p75: Long, max: Long) {
    def formatDistribution(longFormatter: Long => String, separator: String = ", "): String = {
      val labels = Seq(
        s"min: ${longFormatter(min)}",
        s"p25: ${longFormatter(p25)}",
        s"median: ${longFormatter(median)}",
        s"p75: ${longFormatter(p75)}",
        s"max: ${longFormatter(max)}"
      )
      labels.mkString(separator)
    }
  }

  object Distribution {
    def apply(values: Seq[Long]): Distribution = {
      val sortedValues = values.sorted
      val sortedValuesAsJava = sortedValues.map(Long.box).toList
      Distribution(
        sortedValues.min,
        p25 = Statistics.percentile(sortedValuesAsJava, 25),
        Statistics.median(sortedValuesAsJava),
        p75 = Statistics.percentile(sortedValuesAsJava, 75),
        sortedValues.max
      )
    }
  }
}
