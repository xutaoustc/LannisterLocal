package com.lannister.core.domain

import Severity.Severity

case class HeuristicResult(
  heuristicClass: String,
  heuristicName: String,
  severity: Severity,
  score: Int,
  heuristicResultDetails: List[HeuristicResultDetails]
)

case class HeuristicResultDetails(name: String, value: String)

object HeuristicResult{
  val NO_DATA = HeuristicResult(
    "NoDataReceived",
    "No Data Received",
    Severity.LOW,
    0,
    HeuristicResultDetails("No Data Received", "") :: Nil)
}
