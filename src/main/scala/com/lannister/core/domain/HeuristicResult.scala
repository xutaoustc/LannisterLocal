package com.lannister.core.domain

import Severity.Severity

case class HeuristicResult(
    heuristicClass: String,
    heuristicName: String,
    severity: Severity,
    score: Int,
    heuristicResultDetails: List[HeuristicResultDetail]
)

case class HeuristicResultDetail(name: String, value: String)

object HeuristicResult{
  val NO_DATA = HeuristicResult(
    "NoDataReceived",
    "No Data Received",
    Severity.LOW,
    0,
    HeuristicResultDetail("No Data Received", "") :: Nil)
}
