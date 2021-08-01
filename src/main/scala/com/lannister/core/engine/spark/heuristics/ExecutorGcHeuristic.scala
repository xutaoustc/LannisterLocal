package com.lannister.core.engine.spark.heuristics

import com.lannister.core.conf.heuristic.HeuristicConfiguration
import com.lannister.core.domain.{ApplicationData, Heuristic, HeuristicResult, HeuristicResultDetail, Severity, SeverityThresholds}
import com.lannister.core.domain.Severity.Severity
import com.lannister.core.engine.spark.fetchers.SparkApplicationData

class ExecutorGcHeuristic(private val heuristicConfig: HeuristicConfiguration) extends Heuristic{

  import ExecutorGcHeuristic._

  val gcSeverityAThresholds: SeverityThresholds = SeverityThresholds.parse(true,
    heuristicConfig.params.get(GC_SEVERITY_A_THRESHOLDS_KEY) )
  val gcSeverityDThresholds: SeverityThresholds = SeverityThresholds.parse(false,
    heuristicConfig.params.get(GC_SEVERITY_D_THRESHOLDS_KEY) )


  override def apply(data: ApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])
    var resultDetails = Seq(
      new HeuristicResultDetail("GC time to Executor Run time ratio", evaluator.ratio.toString),
      new HeuristicResultDetail("Total GC time", evaluator.gcTime.toString),
      new HeuristicResultDetail("Total Executor Runtime", evaluator.executorRunTimeTotal.toString)
    )

    if (evaluator.severityTimeA.id > Severity.LOW.id) {
      resultDetails = resultDetails :+
        new HeuristicResultDetail("Gc ratio high",
          "The job is spending too much time on GC. We recommend increasing the executor memory.")
    }
    if (evaluator.severityTimeD.id > Severity.LOW.id) {
      resultDetails = resultDetails :+
        new HeuristicResultDetail("Gc ratio low", "The job is spending too less time in GC.")
    }

    new HeuristicResult(
      heuristicConfig.classname,
      heuristicConfig.name,
      evaluator.severityTimeA,
      0,
      resultDetails.toList
    )
  }
}

object ExecutorGcHeuristic {
  val GC_SEVERITY_A_THRESHOLDS_KEY: String = "gc_severity_A_threshold"
  val GC_SEVERITY_D_THRESHOLDS_KEY: String = "gc_severity_D_threshold"

  class Evaluator(executorGcHeuristic: ExecutorGcHeuristic, data: SparkApplicationData) {
    var (gcTime, executorRunTimeTotal) = data.store.store
           .executorList(false).filterNot(_.id.equals("driver"))
           .foldLeft((0L, 0L))( (v, n) => (v._1 + n.totalGCTime, v._2 + n.totalDuration) )
    var ratio: Double = gcTime.toDouble / executorRunTimeTotal.toDouble
    lazy val severityTimeA: Severity = executorGcHeuristic.gcSeverityAThresholds.of(ratio)
    lazy val severityTimeD: Severity = executorGcHeuristic.gcSeverityDThresholds.of(ratio)
  }
}
