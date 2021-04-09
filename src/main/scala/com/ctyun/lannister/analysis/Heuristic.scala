package com.ctyun.lannister.analysis

import com.ctyun.lannister.conf.heuristic.HeuristicConfigurationData

trait Heuristic[T<:ApplicationData] {
  def apply[T](data:T): HeuristicResult

  def getHeuristicConfData: HeuristicConfigurationData
}