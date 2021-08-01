package com.lannister.core.engine.spark.heuristics

import scala.collection.mutable.ListBuffer

import com.lannister.core.conf.heuristic.HeuristicConfiguration
import com.lannister.core.domain._
import com.lannister.core.engine.spark.fetchers.SparkApplicationData

class ExecutorGcHeuristic(private val config: HeuristicConfiguration) extends Heuristic{

  import SeverityThresholds._
  val GC_SEVERITY_A_THRESHOLDS_KEY = "gc_severity_A_threshold"
  val GC_SEVERITY_D_THRESHOLDS_KEY = "gc_severity_D_threshold"
  val gcSeverityAThresholds = parse(true, config.params.get(GC_SEVERITY_A_THRESHOLDS_KEY) )
  val gcSeverityDThresholds = parse(false, config.params.get(GC_SEVERITY_D_THRESHOLDS_KEY) )


  override def apply(data: ApplicationData): HeuristicResult = {
    import ExecutorGcHeuristic._
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    var buf = ListBuffer(
      HeuristicResultDetail("GC time to Executor Run time ratio", evaluator.ratio.toString),
      HeuristicResultDetail("Total GC time", evaluator.gcTime.toString),
      HeuristicResultDetail("Total Executor Runtime", evaluator.executorRunTimeTotal.toString)
    )
    if ( Severity.bigger(evaluator.severityTimeA, Severity.LOW) ) {
      buf += HeuristicResultDetail("Gc ratio high", "The job is spending too much time on GC.")
    }
    if ( Severity.bigger(evaluator.severityTimeD, Severity.LOW) ) {
      buf += HeuristicResultDetail("Gc ratio low", "The job is spending too less time in GC.")
    }

    HeuristicResult(config.classname, config.name, evaluator.severityTimeA, 0, buf.toList)
  }
}

object ExecutorGcHeuristic {

  class Evaluator(executorGcHeuristic: ExecutorGcHeuristic, data: SparkApplicationData) {
    var (gcTime, executorRunTimeTotal) =
      data.store.store.executorList(false)
        .filterNot(_.id.equals("driver"))
        .foldLeft((0L, 0L))( (v, n) => (v._1 + n.totalGCTime, v._2 + n.totalDuration) )
    var ratio: Double = gcTime.toDouble / executorRunTimeTotal.toDouble
    lazy val severityTimeA = executorGcHeuristic.gcSeverityAThresholds.of(ratio)
    lazy val severityTimeD = executorGcHeuristic.gcSeverityDThresholds.of(ratio)
  }
}
