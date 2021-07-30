package com.lannister.core.domain

trait Heuristic {
  def apply(data: ApplicationData): HeuristicResult
}
