package com.lannister.core.engine.spark.heuristics

import scala.concurrent.duration.Duration

import com.lannister.analysis.Severity
import com.lannister.analysis.Severity.Severity
import com.lannister.analysis.SeverityThresholds
import com.lannister.core.conf.heuristic.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult, HeuristicResultDetails}
import com.lannister.core.engine.spark.data.SparkApplicationData
import com.lannister.core.math.Statistics._

import org.apache.spark.status.api.v1.{StageData, StageStatus}

class StagesHeuristic(private val heuristicConfig: HeuristicConfiguration)
  extends Heuristic{

  import StagesHeuristic._

  val stageFailureRateSeverityThresholds: SeverityThresholds = SeverityThresholds.parse(
    heuristicConfig.params.get(STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)
  val taskFailureRateSeverityThresholds: SeverityThresholds = SeverityThresholds.parse(
    heuristicConfig.params.get(TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)
  val stageRuntimeMillisSeverityThresholds: SeverityThresholds = SeverityThresholds.parse(
    heuristicConfig.params.get(STAGE_RUNTIME_MINUTES_SEVERITY_THRESHOLDS_KEY), ascending = true,
    x => Duration(x).toMillis)

  override def apply(data: ApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val resultDetails = Seq(
      new HeuristicResultDetails("Spark completed stages count",
        evaluator.numCompletedStages.toString),
      new HeuristicResultDetails("Spark failed stages count",
        evaluator.numFailedStages.toString),
      new HeuristicResultDetails("Spark stage failure rate",
        s"${evaluator.stageFailureRate.getOrElse(0.0D)}"),
      new HeuristicResultDetails("Spark stages with high task failure rates",
        evaluator.stagesWithHighTaskFailureRates.map { case (stageData, taskFailureRate) =>
          s"stage${stageData.stageId}.${stageData.attemptId} (task fail rate: $taskFailureRate)" }
          .mkString("\n")
      ),
      new HeuristicResultDetails("Spark stages with long average executor runtimes",
        evaluator.stagesWithLongAverageExecutorTime.map { case (stg, runtime) =>
          s"stage${stg.stageId}.${stg.attemptId} (runtime: ${readableTimespan(runtime)})" }
          .mkString(", ")
      ),
      new HeuristicResultDetails("Total input bytes", evaluator.inputBytesTotal.toString),
      new HeuristicResultDetails("Total output bytes", evaluator.outputBytesTotal.toString),
      new HeuristicResultDetails("Total shuffle read bytes",
        evaluator.shuffleReadBytesTotal.toString),
      new HeuristicResultDetails("Total shuffle write bytes",
        evaluator.shuffleWriteBytesTotal.toString)
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

object StagesHeuristic {
  val STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "stage_failure_rate_severity_thresholds"
  val TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "stage_task_failure_rate_severity_thresholds"
  val STAGE_RUNTIME_MINUTES_SEVERITY_THRESHOLDS_KEY = "stage_runtime_minutes_severity_thresholds"

  class Evaluator(stagesHeuristic: StagesHeuristic, data: SparkApplicationData) {
    lazy val stageDatas = data.store.store.stageList(null)
    lazy val numCompletedStages = stageDatas.count { _.status == StageStatus.COMPLETE }
    lazy val numFailedStages = stageDatas.count { _.status == StageStatus.FAILED }
    lazy val stageFailureRate: Option[Double] = {
      val numStages = numCompletedStages + numFailedStages
      if (numStages == 0) None else Some(numFailedStages.toDouble / numStages.toDouble)
    }
    private lazy val stageFailureRateSeverity = stagesHeuristic.stageFailureRateSeverityThresholds
      .of(stageFailureRate.getOrElse[Double](0.0D))

    lazy val inputBytesTotal = stageDatas.map(_.inputBytes).sum
    lazy val outputBytesTotal = stageDatas.map(_.outputBytes).sum
    lazy val shuffleReadBytesTotal = stageDatas.map(_.shuffleReadBytes).sum
    lazy val shuffleWriteBytesTotal = stageDatas.map(_.shuffleWriteBytes).sum


    private def taskFailureRateOf(stageData: StageData): Option[Double] = {
      // Currently, the calculation doesn't include skipped or active tasks.
      val numCompleteTasks = stageData.numCompleteTasks
      val numFailedTasks = stageData.numFailedTasks
      val numTasks = numCompleteTasks + numFailedTasks
      if (numTasks == 0) None else Some(numFailedTasks.toDouble / numTasks.toDouble)
    }
    private def taskFailureRateAndSeverityOf(stageData: StageData): (Double, Severity) = {
      val taskFailRate = taskFailureRateOf(stageData).getOrElse(0.0D)
      (taskFailRate, stagesHeuristic.taskFailureRateSeverityThresholds.of(taskFailRate))
    }
    private lazy val stagesAndTaskFailureRateSeverities = for {
      stageData <- stageDatas
      (taskFailureRate, severity) = taskFailureRateAndSeverityOf(stageData)
    } yield (stageData, taskFailureRate, severity)
    lazy val stagesWithHighTaskFailureRates = stagesAndTaskFailureRateSeverities
      .filter { case (_, _, severity) => severity.id > Severity.MODERATE.id }
      .map { case (stageData, taskFailureRate, _) => (stageData, taskFailureRate) }
    private lazy val taskFailureRateSeverities = stagesAndTaskFailureRateSeverities
      .map { case (_, _, severity) => severity }


    private val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"
    lazy val appConfigurationProperties = data.store.store.environmentInfo().sparkProperties.toMap
    private lazy val executorInstances = appConfigurationProperties
      .get(SPARK_EXECUTOR_INSTANCES_KEY).map(_.toInt)
      .getOrElse(data.store.store.executorList(true).size)
    private def averageExecutorRuntimeAndSeverityOf(stageData: StageData): (Long, Severity) = {
      val avgExecutorTime = stageData.executorRunTime / executorInstances
      (avgExecutorTime, stagesHeuristic.stageRuntimeMillisSeverityThresholds.of(avgExecutorTime))
    }
    private lazy val stagesAndAvgExecutorTimeSeverities = for {
      stageData <- stageDatas
      (runtime, severity) = averageExecutorRuntimeAndSeverityOf(stageData)
    } yield (stageData, runtime, severity)
    lazy val stagesWithLongAverageExecutorTime = stagesAndAvgExecutorTimeSeverities.collect {
      case (stageData, time, severity) if severity.id > Severity.MODERATE.id => (stageData, time) }
    private lazy val runtimeSeverities: Seq[Severity] = stagesAndAvgExecutorTimeSeverities
      .map { case (_, _, severity) => severity }

    lazy val severity: Severity = Severity.max(
      stageFailureRateSeverity +: (taskFailureRateSeverities ++ runtimeSeverities): _*)
  }

}
