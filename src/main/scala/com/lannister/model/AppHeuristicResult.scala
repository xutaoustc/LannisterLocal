package com.lannister.model

import scala.collection.mutable

import com.baomidou.mybatisplus.annotation.{TableField, TableName}
import com.lannister.core.domain.Severity.Severity

@TableName("app_heuristic_result")
class AppHeuristicResult extends AppBase{
  var heuristicClass: String = _
  var heuristicName: String = _
  var severityId: Int = _
  var score: Int = _
  var resultId: Long = _

  @TableField(`exist` = false)
  var severity: Severity = _
  @TableField(`exist` = false)
  var heuristicResultDetails = mutable.ListBuffer[AppHeuristicResultDetails]()
}
