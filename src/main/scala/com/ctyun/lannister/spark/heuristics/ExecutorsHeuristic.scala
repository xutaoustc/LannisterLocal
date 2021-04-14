package com.ctyun.lannister.spark.heuristics

import com.ctyun.lannister.analysis.{ApplicationData, Heuristic, HeuristicResult}
import com.ctyun.lannister.conf.heuristic.HeuristicConfigurationData

class ExecutorsHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData) extends Heuristic{

  override def getHeuristicConfData: HeuristicConfigurationData = ???

  override def apply(data: ApplicationData): HeuristicResult = { null }
}
