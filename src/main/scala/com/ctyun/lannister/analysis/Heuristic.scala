package com.ctyun.lannister.analysis

import com.ctyun.lannister.conf.heuristic.HeuristicConfigurationData

trait Heuristic {
  def apply(data:ApplicationData): HeuristicResult

  def getHeuristicConfData: HeuristicConfigurationData
}