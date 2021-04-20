package com.ctyun.lannister.model

import com.baomidou.mybatisplus.annotation.{IdType, TableField, TableId, TableName}
import com.ctyun.lannister.analysis.Severity.Severity

import scala.collection.mutable

@TableName("app_heuristic_result")
class AppHeuristicResult extends AppBase{
  var heuristicClass:String = _
  var heuristicName:String = _
  var severityId:Int = _
  var score:Int = _
  var resultId:Long = _

  @TableField(`exist` = false)
  var severity:Severity = _
  @TableField(`exist` = false)
  var heuristicResultDetails = mutable.ListBuffer[AppHeuristicResultDetails]()
}
