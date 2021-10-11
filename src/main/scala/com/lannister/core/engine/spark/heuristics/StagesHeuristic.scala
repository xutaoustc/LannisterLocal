package com.lannister.core.engine.spark.heuristics

import scala.concurrent.duration.Duration

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult => HR, HeuristicResultDetail => HD, Severity, SeverityThresholds}
import com.lannister.core.domain.Severity.{bigger, Severity}
import com.lannister.core.engine.spark.fetchers.SparkApplicationData
import com.lannister.core.util.TimeUtils._
import com.lannister.core.util.Utils.failureRate

import org.apache.spark.status.api.v1.{StageData, StageStatus}

class StagesHeuristic(private val config: HeuristicConfiguration)
  extends Heuristic{

  import StagesHeuristic._
  import SeverityThresholds._
  val params = config.params
  val STAGE_FAILURE_RATE_SEVERITY_THRES = "stage_failure_rate_severity_thresholds"
  val TASK_FAILURE_RATE_SEVERITY_THRES = "stage_task_failure_rate_severity_thresholds"
  val STAGE_RUNTIME_MINUTES_SEVERITY_THRES = "stage_runtime_minutes_severity_thresholds"
  val stageFailRateSeverityThres = parse(params.get(STAGE_FAILURE_RATE_SEVERITY_THRES) )
  val taskFailRateSeverityThres = parse(params.get(TASK_FAILURE_RATE_SEVERITY_THRES) )
  val stageRuntimeMillisSeverityThres =
    parse(params.get(STAGE_RUNTIME_MINUTES_SEVERITY_THRES), true, Duration(_).toMillis)

  override def apply(data: ApplicationData): HR = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val hds = Seq(
      HD("Spark completed stages count", evaluator.numCompletedStages.toString),
      HD("Spark failed stages count", evaluator.numFailedStages.toString),
      HD("Spark stage failure rate", s"${evaluator.stageFailRate}"),
      HD("Spark stages with high task failure rates",
        evaluator.stagesWithHighTaskFailRates.map { case (stg, taskFailRate) =>
          s"stage${stg.stageId}.${stg.attemptId} (task fail rate: $taskFailRate)" }.mkString("\n")
      ),
      HD("Spark stages with long average executor runtimes",
        evaluator.stagesWithLongAverageExecutorTime.map { case (stg, time) =>
          s"stage${stg.stageId}.${stg.attemptId} (runtime: ${timeFormat(time)})" }.mkString(", ")
      ),
      HD("Total input bytes", evaluator.inputBytesTotal.toString),
      HD("Total output bytes", evaluator.outputBytesTotal.toString),
      HD("Total shuffle read bytes", evaluator.shuffleReadBytesTotal.toString),
      HD("Total shuffle write bytes", evaluator.shuffleWriteBytesTotal.toString)
    )

    HR(config.classname, config.name, evaluator.severity, 0, hds.toList)
  }

}

object StagesHeuristic {

  class Evaluator(heuristic: StagesHeuristic, data: SparkApplicationData) {
    lazy val stageData = data.store.store.stageList(null)
    lazy val appConfig = data.store.store.environmentInfo().sparkProperties.toMap
    lazy val exSummaries = data.store.store.executorList(true)

    lazy val numCompletedStages = stageData.count { _.status == StageStatus.COMPLETE }
    lazy val numFailedStages = stageData.count { _.status == StageStatus.FAILED }
    lazy val stageFailRate = failureRate(numFailedStages, numCompletedStages).getOrElse(0.0D)
    lazy val inputBytesTotal = stageData.map(_.inputBytes).sum
    lazy val outputBytesTotal = stageData.map(_.outputBytes).sum
    lazy val shuffleReadBytesTotal = stageData.map(_.shuffleReadBytes).sum
    lazy val shuffleWriteBytesTotal = stageData.map(_.shuffleWriteBytes).sum
    private lazy val stageTaskFailRateSeverity = for {
      stage <- stageData
      (taskFailureRate, severity) = taskFailureRateAndSeverity(stage)
    } yield (stage, taskFailureRate, severity)
    private lazy val stageAvgExecutorTimeSeverity = for {
      stage <- stageData
      (avgRuntime, severity) = averageExecutorRuntimeAndSeverityOf(stage)
    } yield (stage, avgRuntime, severity)

    lazy val stagesWithHighTaskFailRates = stageTaskFailRateSeverity
      .filter { case (_, _, severity) => bigger(severity, Severity.MODERATE) }
      .map { case (stageData, taskFailureRate, _) => (stageData, taskFailureRate) }
    lazy val stagesWithLongAverageExecutorTime = stageAvgExecutorTimeSeverity.collect {
      case (stage, time, severity) if bigger(severity, Severity.MODERATE) => (stage, time) }


    private lazy val stageFailRateSeverity = heuristic.stageFailRateSeverityThres.of(stageFailRate)
    private lazy val taskFailRateSeverities = stageTaskFailRateSeverity.map { case (_, _, s) => s }
    private lazy val runtimeSeverities = stageAvgExecutorTimeSeverity.map { case (_, _, s) => s }
    lazy val severity: Severity = Severity.max(
      stageFailRateSeverity +: (taskFailRateSeverities ++ runtimeSeverities): _*)


    private def taskFailureRateAndSeverity(stage: StageData): (Double, Severity) = {
      val taskFailRate = failureRate(stage.numFailedTasks, stage.numCompleteTasks).getOrElse(0.0D)
      (taskFailRate, heuristic.taskFailRateSeverityThres.of(taskFailRate))
    }

    private def averageExecutorRuntimeAndSeverityOf(stage: StageData): (Long, Severity) = {
      val EXECUTOR_INSTANCES = "spark.executor.instances"
      val executorInstances = appConfig.get(EXECUTOR_INSTANCES).map(_.toInt)
        .getOrElse(exSummaries.size)

      val avgExecutorTime = stage.executorRunTime / executorInstances
      (avgExecutorTime, heuristic.stageRuntimeMillisSeverityThres.of(avgExecutorTime))
    }
  }

}
