package com.ctyun.lannister.spark.heuristics

import com.ctyun.lannister.analysis.{Heuristic, HeuristicResult}
import com.ctyun.lannister.conf.heuristic.HeuristicConfigurationData
import com.ctyun.lannister.spark.data.SparkApplicationData

class ExecutorsHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData) extends Heuristic[SparkApplicationData]{
  override def apply(data: SparkApplicationData): HeuristicResult = ???

  override def getHeuristicConfdata: HeuristicConfigurationData = ???
}
