package com.lannister.core.engine.spark.heuristics

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult => HR, HeuristicResultDetail => HD, Severity, SeverityThresholds}
import com.lannister.core.domain.Severity.{bigger, Severity}
import com.lannister.core.engine.spark.fetchers.SparkApplicationData
import com.lannister.core.util.Utils.failureRate

import org.apache.spark.status.api.v1.{StageData, StageStatus}

class StagesHeuristic(private val config: HeuristicConfiguration) extends Heuristic{

  import SeverityThresholds._
  import StagesHeuristic._
  val params = config.params
  val STAGE_FAILURE_RATE_SEVERITY_THRES = "stage_failure_rate_severity_thresholds"
  val TASK_FAILURE_RATE_SEVERITY_THRES = "stage_task_failure_rate_severity_thresholds"
  val stageFailRateSeverityThres = parse(params.get(STAGE_FAILURE_RATE_SEVERITY_THRES) )
  val taskFailRateSeverityThres = parse(params.get(TASK_FAILURE_RATE_SEVERITY_THRES) )


  override def apply(data: ApplicationData): HR = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val hds = Seq(
      HD("Spark completed stages count", evaluator.numCompletedStages.toString),
      HD("Spark failed stages count", evaluator.numFailedStages.toString),
      HD("Spark stage failure rate", s"${evaluator.stageFailRate}"),
      HD("Total input bytes", evaluator.inputBytesTotal.toString),
      HD("Total output bytes", evaluator.outputBytesTotal.toString),
      HD("Total shuffle read bytes", evaluator.shuffleReadBytesTotal.toString),
      HD("Total shuffle write bytes", evaluator.shuffleWriteBytesTotal.toString),
      HD("Spark stages with high task failure rates", evaluator.stagesWithHighTaskFailRates)
    )

    HR(config.classname, config.name, evaluator.severity, 0, hds.toList)
  }

}

object StagesHeuristic {

  class Evaluator(heuristic: StagesHeuristic, data: SparkApplicationData) {
    lazy val stageData = data.store.store.stageList(null)

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
    lazy val stagesWithHighTaskFailRates = stageTaskFailRateSeverity
      .filter { case (_, _, severity) => bigger(severity, Severity.MODERATE) }
      .map { case (stage, taskFailRate, _) =>
        s"stage${stage.stageId}.${stage.attemptId} (task fail rate: $taskFailRate)"}.mkString("\n")

    private lazy val stageFailRateSeverity = heuristic.stageFailRateSeverityThres.of(stageFailRate)
    private lazy val taskFailRateSeverities = stageTaskFailRateSeverity.map { case (_, _, s) => s }
    lazy val severity: Severity = Severity.max(stageFailRateSeverity +: taskFailRateSeverities: _*)


    private def taskFailureRateAndSeverity(stage: StageData): (Double, Severity) = {
      val taskFailRate = failureRate(stage.numFailedTasks, stage.numCompleteTasks).getOrElse(0.0D)
      (taskFailRate, heuristic.taskFailRateSeverityThres.of(taskFailRate))
    }
  }

}
