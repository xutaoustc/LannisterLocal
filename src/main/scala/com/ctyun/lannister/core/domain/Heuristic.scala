package com.ctyun.lannister.core.domain

trait Heuristic {
  def apply(data: ApplicationData): HeuristicResult
}
