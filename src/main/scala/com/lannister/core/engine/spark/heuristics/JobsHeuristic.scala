package com.lannister.core.engine.spark.heuristics

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult => HR, HeuristicResultDetail => HD, Severity, SeverityThresholds}
import com.lannister.core.engine.spark.fetchers.SparkApplicationData
import com.lannister.core.util.Utils._

import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.v1.JobData

class JobsHeuristic(private val config: HeuristicConfiguration) extends Heuristic {

  import JobsHeuristic._
  import SeverityThresholds._
  val params = config.params
  val JOB_FAILURE_RATE_SEVERITY_THRESHOLDS = "job_failure_rate_severity_thresholds"
  val TASK_FAILURE_RATE_SEVERITY_THRESHOLDS = "job_task_failure_rate_severity_thresholds"
  val jobFailRateSeverityThres = parse(params.get(JOB_FAILURE_RATE_SEVERITY_THRESHOLDS))
  val taskFailRateSeverityThres = parse(params.get(TASK_FAILURE_RATE_SEVERITY_THRESHOLDS))


  override def apply(data: ApplicationData): HR = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val hds = Seq(
      HD("Spark completed jobs count", evaluator.numCompletedJobs.toString),
      HD("Spark failed jobs count", evaluator.numFailedJobs.toString),
      HD("Spark job failure rate", evaluator.jobFailRate.toString),
      HD("Spark failed jobs list", evaluator.failedJobsList),
      HD("Spark completed tasks count", evaluator.numCompletedTasks.toString),
      HD("Spark jobs with high task failure rates", evaluator.jobWithHighTaskFailureRates)
    )

    HR(config.getClassname, config.getName, evaluator.severity, 0, hds.toList)
  }
}

object JobsHeuristic {

  class Evaluator(heuristic: JobsHeuristic, data: SparkApplicationData) {
    lazy val jobData = data.store.store.jobsList(null)

    lazy val numCompletedJobs = jobData.count { _.status == JobExecutionStatus.SUCCEEDED }
    lazy val numFailedJobs = jobData.count { _.status == JobExecutionStatus.FAILED }
    lazy val jobFailRate = failureRate(numFailedJobs, numCompletedJobs).getOrElse(0.0D)
    lazy val failedJobs = jobData.filter { _.status == JobExecutionStatus.FAILED }
    lazy val failedJobsList = failedJobs.map(job => s"job ${job.jobId}, ${job.name}").mkString("\n")
    lazy val numCompletedTasks = jobData.map(_.numCompletedTasks).sum

    private lazy val jobTaskFailRateSeverity =
      for {
        job <- jobData
        (taskFailureRate, severity) = taskFailureRateAndSeverity(job)
      } yield (job, taskFailureRate, severity)

    lazy val jobWithHighTaskFailureRates = jobTaskFailRateSeverity
      .filter { case (_, _, severity) => Severity.bigger(severity, Severity.MODERATE) }
      .map { case (job, taskFailureRate, _) =>
        s"job ${job.jobId}, ${job.name} (task fail rate: $taskFailureRate)" }
      .mkString("\n")

    private lazy val wholeJobFailRateSeverity = heuristic.jobFailRateSeverityThres.of(jobFailRate)
    private lazy val jobSeverity = jobTaskFailRateSeverity.map { case (_, _, severity) => severity }
    lazy val severity = Severity.max(wholeJobFailRateSeverity +: jobSeverity: _*)


    private def taskFailureRateAndSeverity(job: JobData) = {
      val taskFailureRate = failureRate(job.numFailedTasks, job.numCompletedTasks).getOrElse(0.0D)
      (taskFailureRate, heuristic.taskFailRateSeverityThres.of(taskFailureRate))
    }
  }
}
