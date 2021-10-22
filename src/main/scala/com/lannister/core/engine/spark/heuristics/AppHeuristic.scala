package com.lannister.core.engine.spark.heuristics

import scala.collection.mutable.ListBuffer

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult => HR, HeuristicResultDetail => HD, Severity}
import com.lannister.core.domain.Severity.Severity
import com.lannister.core.domain.SeverityThresholds.parse
import com.lannister.core.engine.spark.fetchers.SparkApplicationData
import com.lannister.core.engine.spark.heuristics.AppHeuristic.Evaluator
import org.apache.commons.lang3.StringUtils

import org.apache.spark.status.api.v1.TaskSorting._
import org.apache.spark.status.api.v1.TaskStatus._


class AppHeuristic (private val config: HeuristicConfiguration) extends Heuristic{
  val TASKS_COUNT_SEVERITY_THRES = "tasks_count_severity_thresholds"
  val TASKS_OUTPUT_SMALL_FILE_THRES = "tasks_output_small_file_thresholds"
  val tasksCountSeverityThres = parse(config.params.get(TASKS_COUNT_SEVERITY_THRES) )
  val tasksOutputSmallFileThres = config.params.get(TASKS_OUTPUT_SMALL_FILE_THRES).toLong

  override def apply(data: ApplicationData): HR = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    var hds = ListBuffer(
      HD("Spark app Duration", evaluator.duration.toString),
      HD("Spark result tasks count", evaluator.resultTasksCount.toString),
      HD("Spark small file result tasks count", evaluator.smallOutputResultTasksCount.toString),
      HD("Spark completed tasks count", evaluator.completeTasksCount.toString),
      HD("Spark failed tasks count", evaluator.failedTasksCount.toString),
      HD("Total input bytes", evaluator.inputBytesTotal.toString),
      HD("Total output bytes", evaluator.outputBytesTotal.toString),
      HD("Total shuffle read bytes", evaluator.shuffleReadBytesTotal.toString),
      HD("Total shuffle write bytes", evaluator.shuffleWriteBytesTotal.toString)
    )

    if(StringUtils.isNotEmpty(evaluator.hostsCount)) {
      hds += HD("Spark failed hosts count", evaluator.hostsCount)
    }
    if(StringUtils.isNotEmpty(evaluator.errorMessagesCount)) {
      hds += HD("Spark failed error message count", evaluator.errorMessagesCount)
    }

    HR(config.classname, config.name, evaluator.severity, 0, hds.toList)
  }
}

object AppHeuristic {

  class Evaluator(heuristic: AppHeuristic, data: SparkApplicationData) {
    private lazy val app = data.store.store.applicationInfo()
    private lazy val allJobs = data.store.store.jobsList(null)
    private lazy val allStages = data.store.store.stageList(null)

    lazy val duration = app.attempts.map { attempt => attempt.duration }.max

    // Not all jobs have stages
    private lazy val resultStageIDs = allJobs.flatMap {
      job => if (job.stageIds.isEmpty) None else Option(job.stageIds.max) }.toSet
    private lazy val resultStages = allStages.filter { stg => resultStageIDs.contains(stg.stageId) }
    lazy val resultTasksCount = resultStages.map { _.numCompleteTasks }.sum
    lazy val smallOutputResultTasksCount = resultStages.map { stg =>
      data.store.store.taskList(stg.stageId, stg.attemptId, Int.MaxValue)
        .flatMap { task => task.taskMetrics }.map { _.outputMetrics.bytesWritten }
        .filter { _ < heuristic.tasksOutputSmallFileThres }.size
    }.sum


    lazy val completeTasksCount = allStages.map { _.numCompleteTasks }.sum
    lazy val failedTasksCount = allStages.map{ _.numFailedTasks }.sum
    lazy val inputBytesTotal = allStages.map(_.inputBytes).sum
    // tasks field in StageData is None, we can not use it to compute sum value
    lazy val outputBytesTotal = allStages.map(_.outputBytes).sum
    lazy val shuffleReadBytesTotal = allStages.map(_.shuffleReadBytes).sum
    lazy val shuffleWriteBytesTotal = allStages.map(_.shuffleWriteBytes).sum

    import scala.collection.JavaConverters._
    private lazy val failedTasksHostAndMessage = allStages.filter { _.numFailedTasks != 0 }
      .flatMap {stg =>
        data.store.store.taskList(
          stg.stageId, stg.attemptId, 0, Int.MaxValue, ID, (FAILED:: Nil).asJava
        ).map { task => (task.host, task.errorMessage.getOrElse("None")) }
      }
    lazy val hostsCount = failedTasksHostAndMessage.map { case (h, _) => (h, 1) }
      .groupBy(_._1).map(t => (t._1, t._2.size)).map(_.productIterator.mkString(":")).mkString(",")
    lazy val errorMessagesCount = failedTasksHostAndMessage.map { case (_, m) => (m, 1) }
      .groupBy(_._1).map(t => (t._1, t._2.size)).map(_.productIterator.mkString(":")).mkString(",")



    private lazy val completeTasksCountSeverity =
      heuristic.tasksCountSeverityThres.of(completeTasksCount)
    lazy val severity: Severity = Severity.max(completeTasksCountSeverity)
  }
}
