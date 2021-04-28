package com.ctyun.lannister.spark.heuristics

import com.ctyun.lannister.analysis.Severity.Severity
import com.ctyun.lannister.analysis.{ApplicationData, Heuristic, HeuristicResult, HeuristicResultDetails, Severity, SeverityThresholds}
import com.ctyun.lannister.conf.heuristic.HeuristicConfigurationData
import com.ctyun.lannister.spark.data.SparkApplicationData
import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.v1.JobData

class JobsHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData) extends Heuristic {

  import JobsHeuristic._

  val jobFailureRateSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParams.get(JOB_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)
  val taskFailureRateSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParams.get(TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)

  override def getHeuristicConfData = ???

  override def apply(data: ApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    val resultDetails = Seq(
      new HeuristicResultDetails("Spark completed jobs count", evaluator.numCompletedJobs.toString),
      new HeuristicResultDetails("Spark failed jobs count", evaluator.numFailedJobs.toString),
      new HeuristicResultDetails("Spark job failure rate", evaluator.jobFailureRate.getOrElse(0.0D).toString),
      new HeuristicResultDetails("Spark failed jobs list", evaluator.failedJobs.map(job=>s"job ${job.jobId}, ${job.name}").mkString("\n")),
      new HeuristicResultDetails("Spark jobs with high task failure rates",evaluator.jobsWithHighTaskFailureRates
        .map { case (jobData, taskFailureRate) => s"job ${jobData.jobId}, ${jobData.name} (task failure rate: ${taskFailureRate})" }.mkString("\n")
      )
    )

    new HeuristicResult(
      heuristicConfigurationData.getClassname,
      heuristicConfigurationData.getName,
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
    private lazy val jobFailureRateSeverity = jobsHeuristic.jobFailureRateSeverityThresholds.severityOf(jobFailureRate.getOrElse[Double](0.0D))

    private lazy val jobsAndTaskFailureRateSeverities = for {jobData <- jobDatas
                                                          (taskFailureRate, severity) = taskFailureRateAndSeverityOf(jobData)
                                                        } yield (jobData, taskFailureRate, severity)
    lazy val jobsWithHighTaskFailureRates = jobsAndTaskFailureRateSeverities.filter { case (_, _, severity) => severity.id > Severity.MODERATE.id }
                                                                            .map { case (jobData, taskFailureRate, _) => (jobData, taskFailureRate) }

    private lazy val taskFailureRateSeverities: Seq[Severity] = jobsAndTaskFailureRateSeverities.map { case (_, _, severity) => severity }
    lazy val severity: Severity = Severity.max((jobFailureRateSeverity +: taskFailureRateSeverities): _*)



    private def taskFailureRateAndSeverityOf(jobData: JobData): (Double, Severity) = {
      val taskFailureRate = taskFailureRateOf(jobData).getOrElse(0.0D)
      (taskFailureRate, jobsHeuristic.taskFailureRateSeverityThresholds.severityOf(taskFailureRate))
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
