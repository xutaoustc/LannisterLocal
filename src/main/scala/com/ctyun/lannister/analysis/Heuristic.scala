package com.ctyun.lannister.analysis

import com.ctyun.lannister.conf.heuristic.HeuristicConfigurationData

trait Heuristic[T <: ApplicationData ] {
  def apply(data:T):HeuristicResult

  def getHeuristicConfdata: HeuristicConfigurationData
}
