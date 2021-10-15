package com.lannister.core.engine.spark.heuristics

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult => HR, HeuristicResultDetail => HD, Severity}
import com.lannister.core.domain.Severity.Severity
import com.lannister.core.domain.SeverityThresholds.parse
import com.lannister.core.engine.spark.fetchers.SparkApplicationData
import com.lannister.core.engine.spark.heuristics.AppHeuristic.Evaluator

class AppHeuristic (private val config: HeuristicConfiguration) extends Heuristic{
  val TASKS_COUNT_SEVERITY_THRES = "tasks_count_severity_thresholds"
  val tasksCountSeverityThres = parse(config.params.get(TASKS_COUNT_SEVERITY_THRES) )

  override def apply(data: ApplicationData): HR = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val hds = Seq(
      HD("Spark completed tasks count", evaluator.totalTasksCount.toString),
      HD("Spark result tasks count", evaluator.totalResultTasksCount.toString),
      HD("Total input bytes", evaluator.inputBytesTotal.toString),
      HD("Total output bytes", evaluator.outputBytesTotal.toString),
      HD("Total shuffle read bytes", evaluator.shuffleReadBytesTotal.toString),
      HD("Total shuffle write bytes", evaluator.shuffleWriteBytesTotal.toString)
    )

    HR(config.classname, config.name, evaluator.severity, 0, hds.toList)
  }
}

object AppHeuristic {

  class Evaluator(heuristic: AppHeuristic, data: SparkApplicationData) {
    private lazy val allJobs = data.store.store.jobsList(null)
    private lazy val allStages = data.store.store.stageList(null)

    // Not all jobs have stages
    private lazy val resultStageIDs = allJobs.flatMap {
      job => if (job.stageIds.isEmpty) None else Option(job.stageIds.max) }.toSet
    private lazy val resultStages = allStages.filter { stg => resultStageIDs.contains(stg.stageId) }

    lazy val totalResultTasksCount = resultStages.map { _.numCompleteTasks }.sum
    lazy val totalTasksCount = allStages.map { _.numCompleteTasks }.sum
    lazy val inputBytesTotal = allStages.map(_.inputBytes).sum
    // tasks field in StageData is None, we can not use it to compute sum value
    lazy val outputBytesTotal = allStages.map(_.outputBytes).sum
    lazy val shuffleReadBytesTotal = allStages.map(_.shuffleReadBytes).sum
    lazy val shuffleWriteBytesTotal = allStages.map(_.shuffleWriteBytes).sum

    private lazy val tasksCountSeverity = heuristic.tasksCountSeverityThres.of(totalTasksCount)
    lazy val severity: Severity = Severity.max(tasksCountSeverity)
  }
}
