package com.lannister.model

import com.baomidou.mybatisplus.annotation.{TableField, TableName}
import com.lannister.core.domain.HeuristicResult
import com.lannister.core.domain.Severity.Severity

@TableName("app_heuristic_result")
class AppHeuristicResult extends AppBase {
  var heuristicClass: String = _
  var heuristicName: String = _
  var severityId: Int = _
  var score: Int = _
  var resultId: Long = _

  @TableField(`exist` = false)
  var severity: Severity = _
  @TableField(`exist` = false)
  var appHDs : List[AppHeuristicResultDetail] = _
}

object AppHeuristicResult{
  implicit def hr2AppHr(hr : HeuristicResult) : AppHeuristicResult = {
    val appHR = new AppHeuristicResult
    appHR.heuristicClass = hr.heuristicClass
    appHR.heuristicName = hr.heuristicName
    appHR.severity = hr.severity
    appHR.severityId = hr.severity.id
    appHR.score = hr.score
    appHR
  }
}
