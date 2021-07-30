package com.lannister.core.engine.spark.heuristics

import com.lannister.analysis.SeverityThresholds
import com.lannister.core.conf.heuristic.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult, HeuristicResultDetails, Severity}
import com.lannister.core.domain.Severity.Severity
import com.lannister.core.engine.spark.data.SparkApplicationData

import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.v1.JobData

class JobsHeuristic(private val heuristicConfig: HeuristicConfiguration) extends Heuristic {

  import JobsHeuristic._

  val jobFailureRateSeverityThresholds: SeverityThresholds = SeverityThresholds.parse(
    heuristicConfig.getParams.get(JOB_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)
  val taskFailureRateSeverityThresholds: SeverityThresholds = SeverityThresholds.parse(
    heuristicConfig.getParams.get(TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)


  override def apply(data: ApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val resultDetails = Seq(
      HeuristicResultDetails("Spark completed jobs count", evaluator.numCompletedJobs.toString),
      HeuristicResultDetails("Spark failed jobs count", evaluator.numFailedJobs.toString),
      HeuristicResultDetails("Spark job failure rate",
        evaluator.jobFailureRate.getOrElse(0.0D).toString),
      HeuristicResultDetails("Spark failed jobs list",
        evaluator.failedJobs.map(job => s"job ${job.jobId}, ${job.name}").mkString("\n")),
      HeuristicResultDetails("Spark jobs with high task failure rates",
        evaluator.jobsWithHighTaskFailureRates.map { case (jobData, taskFailureRate) =>
          s"job ${jobData.jobId}, ${jobData.name} (task fail rate: ${taskFailureRate})" }
          .mkString("\n")
      ),
      HeuristicResultDetails("Spark completed tasks count", evaluator.numCompletedTasks.toString)
    )

    new HeuristicResult(
      heuristicConfig.getClassname,
      heuristicConfig.getName,
      evaluator.severity,
      0,
      resultDetails.toList
    )
  }
}

object JobsHeuristic {
  val JOB_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "job_failure_rate_severity_thresholds"
  val TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "job_task_failure_rate_severity_thresholds"

  class Evaluator(jobsHeuristic: JobsHeuristic, data: SparkApplicationData) {
    lazy val jobDatas = data.store.store.jobsList(null)
    lazy val failedJobs = jobDatas.filter { _.status == JobExecutionStatus.FAILED }
    lazy val numCompletedJobs = jobDatas.count { _.status == JobExecutionStatus.SUCCEEDED }
    lazy val numFailedJobs = jobDatas.count { _.status == JobExecutionStatus.FAILED }
    lazy val jobFailureRate = { val numJobs = numCompletedJobs + numFailedJobs
      if (numJobs == 0) None else Some(numFailedJobs.toDouble / numJobs.toDouble) }
    private lazy val jobFailureRateSeverity = jobsHeuristic.jobFailureRateSeverityThresholds
      .of(jobFailureRate.getOrElse[Double](0.0D))

    lazy val numCompletedTasks = jobDatas.map(_.numCompletedTasks).sum

    private lazy val jobsAndTaskFailureRateSeverities =
      for {jobData <- jobDatas
          (taskFailureRate, severity) = taskFailureRateAndSeverityOf(jobData)
      } yield (jobData, taskFailureRate, severity)
    lazy val jobsWithHighTaskFailureRates = jobsAndTaskFailureRateSeverities
      .filter { case (_, _, severity) => severity.id > Severity.MODERATE.id }
      .map { case (jobData, taskFailureRate, _) => (jobData, taskFailureRate) }

    private lazy val taskFailureRateSeverities: Seq[Severity] = jobsAndTaskFailureRateSeverities
      .map { case (_, _, severity) => severity }

    lazy val severity = Severity.max((jobFailureRateSeverity +: taskFailureRateSeverities): _*)



    private def taskFailureRateAndSeverityOf(jobData: JobData): (Double, Severity) = {
      val taskFailureRate = taskFailureRateOf(jobData).getOrElse(0.0D)
      (taskFailureRate, jobsHeuristic.taskFailureRateSeverityThresholds.of(taskFailureRate))
    }

    private def taskFailureRateOf(jobData: JobData): Option[Double] = {
      // Currently, the calculation doesn't include skipped or active tasks.
      val numCompletedTasks = jobData.numCompletedTasks
      val numFailedTasks = jobData.numFailedTasks
      val numTasks = numCompletedTasks + numFailedTasks
      if (numTasks == 0) None else Some(numFailedTasks.toDouble / numTasks.toDouble)
    }
  }
}
