package com.ctyun.lannister.model

import com.baomidou.mybatisplus.annotation.{IdType, TableField, TableId, TableName}
import com.ctyun.lannister.analysis.Severity.Severity

@TableName("app_heuristic_result")
class AppHeuristicResult {
  @TableId(`type` = IdType.AUTO)
  var id:Long = _
  var heuristicClass:String = _
  var heuristicName:String = _
  @TableField(`exist` = false)
  var severity:Severity = _
  var severityId:Int = _
  var score:Int = _
  var appId:Long = _
}
