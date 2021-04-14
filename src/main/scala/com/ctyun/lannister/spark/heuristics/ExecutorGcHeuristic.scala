package com.ctyun.lannister.spark.heuristics

import com.ctyun.lannister.analysis.Severity.Severity
import com.ctyun.lannister.analysis.{ApplicationData, Heuristic, HeuristicResult, HeuristicResultDetails, Severity, SeverityThresholds}
import com.ctyun.lannister.conf.heuristic.HeuristicConfigurationData
import com.ctyun.lannister.spark.data.SparkApplicationData
import com.ctyun.lannister.spark.heuristics.ExecutorGcHeuristic.{Evaluator, GC_SEVERITY_A_THRESHOLDS_KEY, GC_SEVERITY_D_THRESHOLDS_KEY}
import org.apache.spark.status.api.v1.ExecutorSummary
import scala.collection.JavaConverters

class ExecutorGcHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData) extends Heuristic{


  import ExecutorGcHeuristic._
  import JavaConverters._

  val gcSeverityAThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.params.get(GC_SEVERITY_A_THRESHOLDS_KEY), ascending = true)
  val gcSeverityDThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.params.get(GC_SEVERITY_D_THRESHOLDS_KEY), ascending = false)

  override def getHeuristicConfData: HeuristicConfigurationData = ???

  override def apply(data: ApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])
    var resultDetails = Seq(
      new HeuristicResultDetails("GC time to Executor Run time ratio", evaluator.ratio.toString),
      new HeuristicResultDetails("Total GC time", evaluator.gcTime.toString),
      new HeuristicResultDetails("Total Executor Runtime", evaluator.executorRunTimeTotal.toString)
    )

    if (evaluator.severityTimeA.id > Severity.LOW.id) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Gc ratio high", "The job is spending too much time on GC. We recommend increasing the executor memory.")
    }
    if (evaluator.severityTimeD.id > Severity.LOW.id) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Gc ratio low", "The job is spending too less time in GC. Please check if you have asked for more executor memory than required.")
    }

    new HeuristicResult(
      heuristicConfigurationData.classname,
      heuristicConfigurationData.name,
      evaluator.severityTimeA,
      0,
      resultDetails.asJava
    )
  }
}

object ExecutorGcHeuristic {
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"

  val GC_SEVERITY_A_THRESHOLDS_KEY: String = "gc_severity_A_threshold"
  val GC_SEVERITY_D_THRESHOLDS_KEY: String = "gc_severity_D_threshold"


  class Evaluator(executorGcHeuristic: ExecutorGcHeuristic, data: SparkApplicationData) {
    lazy val executorAndDriverSummaries: Seq[ExecutorSummary] = data.store.store.executorList(false)
    lazy val executorSummaries: Seq[ExecutorSummary] = executorAndDriverSummaries.filterNot(_.id.equals("driver"))
    var (gcTime, executorRunTimeTotal) = getTimeValues(executorSummaries)
    var ratio: Double = gcTime.toDouble / executorRunTimeTotal.toDouble
    lazy val severityTimeA: Severity = executorGcHeuristic.gcSeverityAThresholds.severityOf(ratio)
    lazy val severityTimeD: Severity = executorGcHeuristic.gcSeverityDThresholds.severityOf(ratio)

    private def getTimeValues(executorSummaries: Seq[ExecutorSummary]): (Long, Long) = {
      var jvmGcTimeTotal: Long = 0
      var executorRunTimeTotal: Long = 0
      executorSummaries.foreach(executorSummary => {
        jvmGcTimeTotal+=executorSummary.totalGCTime
        executorRunTimeTotal+=executorSummary.totalDuration
      })
      (jvmGcTimeTotal, executorRunTimeTotal)
    }
  }
}