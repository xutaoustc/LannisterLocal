package com.ctyun.lannister.spark.heuristics

import com.ctyun.lannister.analysis.Severity.Severity
import com.ctyun.lannister.analysis._
import com.ctyun.lannister.conf.heuristic.HeuristicConfigurationData
import com.ctyun.lannister.math.Statistics
import com.ctyun.lannister.spark.data.SparkApplicationData
import org.apache.spark.status.api.v1.{StageData, StageStatus}

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
    SeverityThresholds.parse(heuristicConfigurationData.params.get(STAGE_RUNTIME_MINUTES_SEVERITY_THRESHOLDS_KEY), ascending = true, x=>Duration(x).toMillis)

  override def apply(data: ApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val resultDetails = Seq(
      new HeuristicResultDetails("Spark completed stages count", evaluator.numCompletedStages.toString),
      new HeuristicResultDetails("Spark failed stages count", evaluator.numFailedStages.toString),
      new HeuristicResultDetails("Spark stage failure rate", s"${evaluator.stageFailureRate.getOrElse(0.0D)}"),
      new HeuristicResultDetails("Spark stages with high task failure rates", evaluator.stagesWithHighTaskFailureRates
        .map { case (stageData, taskFailureRate) => s"stage ${stageData.stageId}, attempt ${stageData.attemptId} (task failure rate: $taskFailureRate)" }.mkString("\n")
      ),
      new HeuristicResultDetails(
        "Spark stages with long average executor runtimes", evaluator.stagesWithLongAverageExecutorRuntimes
          .map { case (stageData, runtime) => s"stage ${stageData.stageId}.${stageData.attemptId} (runtime: ${Statistics.readableTimespan(runtime)})" }.mkString(", ")
      ),
      new HeuristicResultDetails("Total input bytes", evaluator.inputBytesTotal.toString),
      new HeuristicResultDetails("Total output bytes", evaluator.outputBytesTotal.toString),
      new HeuristicResultDetails("Total shuffle read bytes", evaluator.shuffleReadBytesTotal.toString),
      new HeuristicResultDetails("Total shuffle write bytes", evaluator.shuffleWriteBytesTotal.toString)
    )

    new HeuristicResult(
      heuristicConfigurationData.classname,
      heuristicConfigurationData.name,
      evaluator.severity,
      0,
      resultDetails.toList
    )
  }

  override def getHeuristicConfData: HeuristicConfigurationData = ???

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
    private lazy val stageFailureRateSeverity = stagesHeuristic.stageFailureRateSeverityThresholds.severityOf(stageFailureRate.getOrElse[Double](0.0D))

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
      val taskFailureRate = taskFailureRateOf(stageData).getOrElse(0.0D)
      (taskFailureRate, stagesHeuristic.taskFailureRateSeverityThresholds.severityOf(taskFailureRate))
    }
    private lazy val stagesAndTaskFailureRateSeverities = for {
      stageData <- stageDatas
      (taskFailureRate, severity) = taskFailureRateAndSeverityOf(stageData)
    } yield (stageData, taskFailureRate, severity)
    lazy val stagesWithHighTaskFailureRates = stagesAndTaskFailureRateSeverities.filter { case (_, _, severity) => severity.id > Severity.MODERATE.id }
                                                                                .map { case (stageData, taskFailureRate, _) => (stageData, taskFailureRate) }
    private lazy val taskFailureRateSeverities = stagesAndTaskFailureRateSeverities.map { case (_, _, severity) => severity }


    private val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"
    lazy val appConfigurationProperties = data.store.store.environmentInfo().sparkProperties.toMap
    private lazy val executorInstances = appConfigurationProperties.get(SPARK_EXECUTOR_INSTANCES_KEY).map(_.toInt)
                                                                   .getOrElse(data.store.store.executorList(true).size)
    private def averageExecutorRuntimeAndSeverityOf(stageData: StageData): (Long, Severity) = {
      val averageExecutorRuntime = stageData.executorRunTime / executorInstances
      (averageExecutorRuntime, stagesHeuristic.stageRuntimeMillisSeverityThresholds.severityOf(averageExecutorRuntime))
    }
    private lazy val stagesAndAverageExecutorRuntimeSeverities = for {
      stageData <- stageDatas
      (runtime, severity) = averageExecutorRuntimeAndSeverityOf(stageData)
    } yield (stageData, runtime, severity)
    lazy val stagesWithLongAverageExecutorRuntimes = stagesAndAverageExecutorRuntimeSeverities.collect { case (stageData, runtime, severity) if severity.id > Severity.MODERATE.id => (stageData, runtime) }
    private lazy val runtimeSeverities: Seq[Severity] = stagesAndAverageExecutorRuntimeSeverities.map { case (_, _, severity) => severity }


    lazy val severity: Severity = Severity.max((stageFailureRateSeverity +: (taskFailureRateSeverities ++ runtimeSeverities)): _*)
  }

}
