package com.ctyun.lannister.analysis

import com.ctyun.lannister.analysis.Severity.Severity

case class HeuristicResult(heuristicClass:String,
                           heuristicName:String,
                           severity: Severity,
                           score:Int,
                           heuristicResultDetails:List[HeuristicResultDetails]) {
}

object HeuristicResult{
  val NO_DATA = new HeuristicResult(
    "NoDataReceived",
    "No Data Received",
    Severity.LOW,
    0,
    List( HeuristicResultDetails("No Data Received", "", "")))
}