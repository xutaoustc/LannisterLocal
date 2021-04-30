package com.ctyun.lannister.spark.heuristics

import com.ctyun.lannister.analysis.Severity.Severity
import com.ctyun.lannister.analysis.{ApplicationData, Heuristic, HeuristicResult, HeuristicResultDetails, Severity, SeverityThresholds}
import com.ctyun.lannister.conf.heuristic.HeuristicConfigurationData
import com.ctyun.lannister.math.Statistics
import com.ctyun.lannister.spark.data.SparkApplicationData
import org.apache.spark.status.api.v1.{ExecutorSummary, StageData, StageStatus}
import scala.concurrent.duration
import scala.concurrent.duration.Duration


/**
  * @author anlexander
  * @date 2021/4/30
  */

class StagesHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic{

  import StagesHeuristic._

  val stageFailureRateSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.params.get(STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)

  val taskFailureRateSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.params.get(TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)

  val stageRuntimeMillisSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.params.get(STAGE_RUNTIME_MINUTES_SEVERITY_THRESHOLDS_KEY), ascending = true)

  override def apply(data: ApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    def formatStagesWithHighTaskFailureRates(stagesWithHighTaskFailureRates: Seq[(StageData, Double)]): String =
      stagesWithHighTaskFailureRates
        .map { case (stageData, taskFailureRate) => formatStageWithHighTaskFailureRate(stageData, taskFailureRate) }
        .mkString("\n")

    def formatStageWithHighTaskFailureRate(stageData: StageData, taskFailureRate: Double): String =
      f"stage ${stageData.stageId}, attempt ${stageData.attemptId} (task failure rate: ${taskFailureRate}%1.3f)"

    def formatStagesWithLongAverageExecutorRuntimes(stagesWithLongAverageExecutorRuntimes: Seq[(StageData, Long)]): String =
      stagesWithLongAverageExecutorRuntimes
        .map { case (stageData, runtime) => formatStageWithLongRuntime(stageData, runtime) }
        .mkString("\n")

    def formatStageWithLongRuntime(stageData: StageData, runtime: Long): String =
      f"stage ${stageData.stageId}, attempt ${stageData.attemptId} (runtime: ${Statistics.readableTimespan(runtime)})"

    val resultDetails = Seq(
      new HeuristicResultDetails("Spark completed stages count", evaluator.numCompletedStages.toString),
      new HeuristicResultDetails("Spark failed stages count", evaluator.numFailedStages.toString),
      new HeuristicResultDetails("Spark stage failure rate", f"${evaluator.stageFailureRate.getOrElse(0.0D)}%.3f"),
      new HeuristicResultDetails(
        "Spark stages with high task failure rates",
        formatStagesWithHighTaskFailureRates(evaluator.stagesWithHighTaskFailureRates)
      ),
      new HeuristicResultDetails(
        "Spark stages with long average executor runtimes",
        formatStagesWithLongAverageExecutorRuntimes(evaluator.stagesWithLongAverageExecutorRuntimes)
      )
    )

    val result = new HeuristicResult(
      heuristicConfigurationData.classname,
      heuristicConfigurationData.name,
      evaluator.severity,
      0,
      resultDetails.toList
    )
    result

  }

  override def getHeuristicConfData: HeuristicConfigurationData = ???

}

object StagesHeuristic {

  val STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "stage_failure_rate_severity_thresholds"
  val TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "stage_task_failure_rate_severity_thresholds"
  val STAGE_RUNTIME_MINUTES_SEVERITY_THRESHOLDS_KEY = "stage_runtime_minutes_severity_thresholds"
  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"


  def minutesSeverityThresholdsToMillisSeverityThresholds(minutesSeverityThresholds: SeverityThresholds): SeverityThresholds = SeverityThresholds(
    Duration(minutesSeverityThresholds.low.longValue, duration.MINUTES).toMillis,
    Duration(minutesSeverityThresholds.moderate.longValue, duration.MINUTES).toMillis,
    Duration(minutesSeverityThresholds.severe.longValue, duration.MINUTES).toMillis,
    Duration(minutesSeverityThresholds.critical.longValue, duration.MINUTES).toMillis,
    minutesSeverityThresholds.ascending
  )

  class Evaluator(stagesHeuristic: StagesHeuristic, data: SparkApplicationData) {
    lazy val stageDatas: Seq[StageData] = data.store.store.stageList(null)

    lazy val appConfigurationProperties: Map[String, String] = data.store.store.environmentInfo().sparkProperties.toMap

    lazy val executorSummaries: Seq[ExecutorSummary] = data.store.store.executorList(true)

    lazy val numCompletedStages: Int = stageDatas.count { _.status == StageStatus.COMPLETE }

    lazy val numFailedStages: Int = stageDatas.count { _.status == StageStatus.FAILED }

    lazy val stageFailureRate: Option[Double] = {
      val numStages = numCompletedStages + numFailedStages
      if (numStages == 0) None else Some(numFailedStages.toDouble / numStages.toDouble)
    }

    lazy val stagesWithHighTaskFailureRates: Seq[(StageData, Double)] =
      stagesWithHighTaskFailureRateSeverities.map { case (stageData, taskFailureRate, _) => (stageData, taskFailureRate) }

    lazy val severity: Severity = Severity.max((stageFailureRateSeverity +: (taskFailureRateSeverities ++ runtimeSeverities)): _*)

    private lazy val stageFailureRateSeverityThresholds = stagesHeuristic.stageFailureRateSeverityThresholds

    private lazy val taskFailureRateSeverityThresholds = stagesHeuristic.taskFailureRateSeverityThresholds

    private lazy val stageRuntimeMillisSeverityThresholds = stagesHeuristic.stageRuntimeMillisSeverityThresholds

    private lazy val stageFailureRateSeverity: Severity = {
      stageFailureRateSeverityThresholds.severityOf(stageFailureRate.getOrElse[Double](0.0D))
    }

    lazy val stagesWithLongAverageExecutorRuntimes: Seq[(StageData, Long)] =
      stagesAndAverageExecutorRuntimeSeverities
        .collect { case (stageData, runtime, severity) if severity.id > Severity.MODERATE.id => (stageData, runtime) }

    private lazy val stagesWithHighTaskFailureRateSeverities: Seq[(StageData, Double, Severity)] =
      stagesAndTaskFailureRateSeverities.filter { case (_, _, severity) => severity.id > Severity.MODERATE.id }

    private lazy val stagesAndTaskFailureRateSeverities: Seq[(StageData, Double, Severity)] = for {
      stageData <- stageDatas
      (taskFailureRate, severity) = taskFailureRateAndSeverityOf(stageData)
    } yield (stageData, taskFailureRate, severity)

    private lazy val taskFailureRateSeverities: Seq[Severity] =
      stagesAndTaskFailureRateSeverities.map { case (_, _, severity) => severity }

    private lazy val stagesAndAverageExecutorRuntimeSeverities: Seq[(StageData, Long, Severity)] = for {
      stageData <- stageDatas
      (runtime, severity) = averageExecutorRuntimeAndSeverityOf(stageData)
    } yield (stageData, runtime, severity)

    private lazy val runtimeSeverities: Seq[Severity] = stagesAndAverageExecutorRuntimeSeverities.map { case (_, _, severity) => severity }

    private lazy val executorInstances: Int =
      appConfigurationProperties.get(SPARK_EXECUTOR_INSTANCES_KEY).map(_.toInt).getOrElse(executorSummaries.size)

    private def taskFailureRateAndSeverityOf(stageData: StageData): (Double, Severity) = {
      val taskFailureRate = taskFailureRateOf(stageData).getOrElse(0.0D)
      (taskFailureRate, taskFailureRateSeverityThresholds.severityOf(taskFailureRate))
    }

    private def taskFailureRateOf(stageData: StageData): Option[Double] = {
      // Currently, the calculation doesn't include skipped or active tasks.
      val numCompleteTasks = stageData.numCompleteTasks
      val numFailedTasks = stageData.numFailedTasks
      val numTasks = numCompleteTasks + numFailedTasks
      if (numTasks == 0) None else Some(numFailedTasks.toDouble / numTasks.toDouble)
    }

    private def averageExecutorRuntimeAndSeverityOf(stageData: StageData): (Long, Severity) = {
      val averageExecutorRuntime = stageData.executorRunTime / executorInstances
      (averageExecutorRuntime, stageRuntimeMillisSeverityThresholds.severityOf(averageExecutorRuntime))
    }
  }

}
