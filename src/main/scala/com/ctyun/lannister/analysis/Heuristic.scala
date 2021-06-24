package com.ctyun.lannister.analysis

trait Heuristic {
  def apply(data: ApplicationData): HeuristicResult
}
