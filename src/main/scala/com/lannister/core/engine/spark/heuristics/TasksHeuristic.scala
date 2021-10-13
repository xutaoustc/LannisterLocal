package com.lannister.core.engine.spark.heuristics

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult => HR, HeuristicResultDetail => HD, Severity}
import com.lannister.core.domain.Severity.Severity
import com.lannister.core.domain.SeverityThresholds.parse
import com.lannister.core.engine.spark.fetchers.SparkApplicationData
import com.lannister.core.engine.spark.heuristics.TasksHeuristic.Evaluator

class TasksHeuristic (private val config: HeuristicConfiguration) extends Heuristic {
  val TASKS_COUNT_SEVERITY_THRES = "tasks_count_severity_thresholds"
  val tasksCountSeverityThres = parse(config.params.get(TASKS_COUNT_SEVERITY_THRES) )

  override def apply(data: ApplicationData): HR = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val hds = Seq(
      HD("Spark completed tasks count", evaluator.totalTasksCount.toString),
      HD("Spark result tasks count", evaluator.totalResultTasksCount.toString)
    )

    HR(config.classname, config.name, evaluator.severity, 0, hds.toList)
  }
}

object TasksHeuristic {

  class Evaluator(heuristic: TasksHeuristic, data: SparkApplicationData) {
    private lazy val resultStageIDs = data.store.store.jobsList(null)
      .map(job => job.stageIds.max).toSet
    private lazy val resultStages = data.store.store.stageList(null)
      .filter(stage => resultStageIDs.contains(stage.stageId))
    lazy val totalResultTasksCount = resultStages.map(stage => stage.numCompleteTasks).sum

    private lazy val tasksData = data.store.store.stageList(null)
      .map(stage => stage.tasks)
      .filter(!_.isEmpty).map(_.get)
    lazy val totalTasksCount = tasksData.map(x => x.size).sum


    private lazy val tasksCountSeverity = heuristic.tasksCountSeverityThres.of(totalTasksCount)

    lazy val severity: Severity = Severity.max(tasksCountSeverity)
  }
}
