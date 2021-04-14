package com.ctyun.lannister.model

import com.ctyun.lannister.analysis.Severity.Severity

class AppResult {
  var id:Long = _
  var appId:String = _
  var trackingUrl:String = _
  var queueName:String = _
  var username:String = _
  var startTime:Long = _
  var finishTime:Long = _
  var name:String = _
  var jobType:String = _
  var resourceUsed:Long = _
  var totalDelay:Long = _
  var resourceWasted:Long = _
  var severity:Severity = _
  var score:Int = _
}
