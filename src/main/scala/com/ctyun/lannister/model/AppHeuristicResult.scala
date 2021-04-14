package com.ctyun.lannister.model

import com.ctyun.lannister.analysis.Severity.Severity

class AppHeuristicResult {
  var id:Long = _
  var heuristicClass:String = _
  var heuristicName:String = _
  var severity:Severity = _
  var score:Int = _
  var appId:Long = _
}
