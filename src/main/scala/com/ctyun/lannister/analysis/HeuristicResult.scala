package com.ctyun.lannister.analysis
import java.util
import java.util.ArrayList

import com.ctyun.lannister.analysis.Severity.Severity

case class HeuristicResult(heuristicClass:String,
                           heuristicName:String,
                           severity: Severity,
                           score:Int,
                           heuristicResultDetails:util.List[HeuristicResultDetails]){
  /**
   * Add the App Heuristic Result Detail without details
   */
  def addResultDetail(name: String, value: String): Unit = {
    heuristicResultDetails.add(new HeuristicResultDetails(name, value, null))
  }

  def addResultDetail(name: String, value: String, details: String): Unit = {
    heuristicResultDetails.add(new HeuristicResultDetails(name, value, details))
  }
}

object HeuristicResult{
  val NO_DATA = new HeuristicResult(
    "NoDataReceived",
    "No Data Received",
    Severity.LOW,
    0,
    new ArrayList( util.Arrays.asList(HeuristicResultDetails("No Data Received", "", ""))));
}