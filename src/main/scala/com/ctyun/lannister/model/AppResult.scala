package com.ctyun.lannister.model

import com.baomidou.mybatisplus.annotation.{IdType, TableField, TableId, TableName}
import com.ctyun.lannister.analysis.Severity.Severity

import scala.collection.mutable

@TableName("app_result")
class AppResult {
  @TableId(`type` = IdType.AUTO)
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
  var severityId:Int = _
  var score:Int = _
  @TableField(`exist` = false)
  var severity:Severity = _
  @TableField(`exist` = false)
  var heuristicResults = mutable.ListBuffer[AppHeuristicResult]()
}
