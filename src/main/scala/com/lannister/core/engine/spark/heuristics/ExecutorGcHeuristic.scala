package com.lannister.core.engine.spark.heuristics

import scala.collection.mutable.ListBuffer

import com.lannister.core.conf.HeuristicConfiguration
import com.lannister.core.domain._
import com.lannister.core.domain.{HeuristicResult => HR}
import com.lannister.core.domain.{HeuristicResultDetail => HD}
import com.lannister.core.engine.spark.fetchers.SparkApplicationData

/*
* Pass class and data to Evaluator(because the class instance has config info),
* and Evaluator provide the following output:
*   1. Provide info for HD
*   2. Provide total severity level for HR
* */
class ExecutorGcHeuristic(private val config: HeuristicConfiguration) extends Heuristic{

  import SeverityThresholds._
  import ExecutorGcHeuristic._
  val GC_SEVERITY_A_THRESHOLDS_KEY = "gc_severity_A_threshold"
  val GC_SEVERITY_D_THRESHOLDS_KEY = "gc_severity_D_threshold"
  val thresholdsGcSeverityA = parse(config.params.get(GC_SEVERITY_A_THRESHOLDS_KEY))
  val thresholdsGcSeverityD = parse(config.params.get(GC_SEVERITY_D_THRESHOLDS_KEY), false)


  override def apply(data: ApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data.asInstanceOf[SparkApplicationData])

    var hds = ListBuffer(
      HD("Total GC time", evaluator.gcTime.toString),
      HD("Total Executor Runtime", evaluator.executorRunTimeTotal.toString),
      HD("GC time to Executor Run time ratio", evaluator.ratio.toString)
    )
    Option( Severity.bigger(evaluator.severityTimeA, Severity.LOW) )
      .filter { _ == true }
      .foreach { _ => hds += HD("Gc ratio high", "The job is spending too much time on GC.") }
    Option( Severity.bigger(evaluator.severityTimeD, Severity.LOW) )
      .filter { _ == true }
      .foreach { _ => hds += HD("Gc ratio low", "The job is spending too less time in GC.") }

    HR(config.classname, config.name, evaluator.severityTimeA, 0, hds.toList)
  }
}

object ExecutorGcHeuristic {

  class Evaluator(heuristic: ExecutorGcHeuristic, data: SparkApplicationData) {
    lazy val exSummaries = data.store.store.executorList(false)

    var (gcTime, executorRunTimeTotal) = exSummaries.filterNot(_.id.equals("driver"))
        .foldLeft((0L, 0L))( (v, n) => (v._1 + n.totalGCTime, v._2 + n.totalDuration) )
    var ratio: Double = gcTime.toDouble / executorRunTimeTotal.toDouble
    lazy val severityTimeA = heuristic.thresholdsGcSeverityA.of(ratio)
    lazy val severityTimeD = heuristic.thresholdsGcSeverityD.of(ratio)
  }
}
