package com.lannister.model

import scala.collection.mutable

import com.baomidou.mybatisplus.annotation.{TableField, TableName}
import com.lannister.core.domain.Severity
import com.lannister.core.domain.Severity.Severity

@TableName("app_result")
class AppResult extends AppBase {
  var appId: String = _
  var trackingUrl: String = _
  var queueName: String = _
  var username: String = _
  var startTime: Long = _
  var finishTime: Long = _
  var name: String = _
  var jobType: String = _
  var successfulJob: Boolean = _
  var severityId: Int = _
  var score: Int = _
  @TableField(`exist` = false)
  var severity: Severity = Severity.NONE
  @TableField(`exist` = false)
  var appHRs = mutable.ListBuffer[AppHeuristicResult]()
}
